"""OIDC authorization-code/PKCE sessions and physically isolated workspaces."""
import hashlib
import json
import os
import re
from pathlib import Path
import secrets
import threading
import time
import uuid
from contextlib import asynccontextmanager
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx
import jwt
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, RedirectResponse
from starlette.routing import Route
from app.catalog import connection
from app.locking import DataLock
from app.limits import RequestLimit


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class Gateway:
    def __init__(self, factory, base):
        self.factory, self.base = factory, Path(base).resolve()
        self.base.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.base.chmod(0o700)
        self.metadata = self.base / 'security.sqlite3'
        self.issuer = os.environ['DATA_PLATFORM_OIDC_ISSUER'].rstrip('/')
        self.client = os.environ['DATA_PLATFORM_OIDC_CLIENT_ID']
        self.redirect = os.environ['DATA_PLATFORM_OIDC_REDIRECT_URI']
        redirect = urlsplit(self.redirect)
        if (not redirect.hostname or redirect.username is not None or redirect.password is not None
                or redirect.fragment or redirect.query or redirect.path != '/auth/callback'):
            raise ValueError('OIDC redirect must be an absolute callback URL without credentials, query or fragment.')
        self.secure = redirect.scheme == 'https'
        self.allowed = {urlsplit(self.issuer).hostname}
        self.allowed.update(filter(None, os.environ.get('DATA_PLATFORM_OIDC_ALLOWED_HOSTS', '').split(',')))
        self.http_dev = os.environ.get('DATA_PLATFORM_ALLOW_HTTP_OIDC') == '1'
        self.check_url(self.issuer)
        if not self.secure and not (redirect.scheme == 'http' and self.http_dev and redirect.hostname in ('localhost', '127.0.0.1')):
            raise ValueError('OIDC redirect must use HTTPS, except explicitly enabled loopback development.')
        self.apps, self.guard = {}, threading.RLock()
        with connection(self.metadata) as con:
            con.executescript('''
            CREATE TABLE IF NOT EXISTS users(sub TEXT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS workspaces(id TEXT PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS memberships(workspace_id TEXT NOT NULL, sub TEXT NOT NULL, role TEXT NOT NULL, PRIMARY KEY(workspace_id,sub));
            CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, sub TEXT NOT NULL, workspace_id TEXT NOT NULL, expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS api_tokens(id TEXT PRIMARY KEY,sub TEXT NOT NULL,workspace_id TEXT NOT NULL,name TEXT NOT NULL,created REAL NOT NULL,expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS states(id TEXT PRIMARY KEY, verifier TEXT NOT NULL, nonce TEXT NOT NULL, expires REAL NOT NULL);
            ''')
        data_lock=DataLock(self.base)
        @asynccontextmanager
        async def lifespan(_):
            data_lock.acquire()
            try:
                yield
            finally:
                if data_lock.depth==1:
                    from starlette.concurrency import run_in_threadpool
                    for app in self.apps.values():
                        await run_in_threadpool(app.state.flows.close)
                        await run_in_threadpool(app.state.pipelines.close)
                        app.state.data_lock.close()
                data_lock.close()
        self.app = Starlette(routes=[Route('/{path:path}', self.route,
            methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'])], lifespan=lifespan)
        self.app.add_middleware(RequestLimit,limit=21*1024*1024)
        async def bad_json(request, exc):
            return JSONResponse({'detail':'Use valid JSON.'},400)
        self.app.add_exception_handler(json.JSONDecodeError,bad_json)

    async def __call__(self, scope, receive, send):
        async def secured_send(message):
            if message['type'] == 'http.response.start':
                headers = list(message.get('headers', []))
                headers.append((b'cache-control', b'no-store'))
                headers.append((b'x-content-type-options', b'nosniff'))
                if not any(key.lower() == b'content-security-policy' for key, _ in headers):
                    headers.append((b'content-security-policy', b"default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'"))
                message = {**message, 'headers': headers}
            await send(message)
        await self.app(scope, receive, secured_send)

    def check_url(self, value):
        parsed = urlsplit(value)
        local = parsed.hostname in ('localhost', '127.0.0.1')
        if parsed.hostname not in self.allowed or parsed.username is not None or parsed.password is not None or parsed.fragment or not (
                parsed.scheme == 'https' or (self.http_dev and local and parsed.scheme == 'http')):
            raise ValueError('OIDC endpoint is outside configured HTTPS hosts.')

    def discovery(self):
        with httpx.Client(timeout=10) as client:
            response = client.get(self.issuer + '/.well-known/openid-configuration')
            response.raise_for_status()
            document = response.json()
        if document.get('issuer') != self.issuer:
            raise ValueError('OIDC discovery issuer mismatch.')
        for key in ('authorization_endpoint', 'token_endpoint', 'jwks_uri'):
            self.check_url(document[key])
        return document

    def login(self):
        config = self.discovery()
        endpoint = config['authorization_endpoint']
        reserved = {'client_id', 'redirect_uri', 'response_type', 'scope', 'state', 'nonce',
                    'code_challenge', 'code_challenge_method'}
        if any(name in reserved for name, _ in parse_qsl(urlsplit(endpoint).query, keep_blank_values=True)):
            raise ValueError('OIDC authorization endpoint contains conflicting parameters.')
        state, verifier, nonce = secrets.token_urlsafe(32), secrets.token_urlsafe(64), secrets.token_urlsafe(32)
        import base64
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
        with connection(self.metadata) as con:
            con.execute('DELETE FROM states WHERE expires<?', (time.time(),))
            con.execute('INSERT INTO states VALUES(?,?,?,?)', (digest(state), verifier, nonce, time.time() + 600))
        response = RedirectResponse(endpoint + ('&' if urlsplit(endpoint).query else '?') + urlencode({
            'client_id': self.client, 'redirect_uri': self.redirect, 'response_type': 'code',
            'scope': 'openid profile email', 'state': state, 'nonce': nonce,
            'code_challenge': challenge, 'code_challenge_method': 'S256'}))
        response.set_cookie('dp_state', state, max_age=600, httponly=True, secure=self.secure, samesite='lax', path='/auth')
        return response

    def callback(self, request):
        state = request.query_params.get('state', '')
        cookie = request.cookies.get('dp_state', '')
        code = request.query_params.get('code', '')
        if not state or len(state) > 256 or len(code) > 4096 or not secrets.compare_digest(state, cookie):
            raise ValueError('Invalid OIDC state.')
        with connection(self.metadata) as con:
            saved = con.execute('DELETE FROM states WHERE id=? AND expires>? RETURNING *', (digest(state), time.time())).fetchone()
        if not saved or not code:
            raise ValueError('OIDC login expired or was already used.')
        config = self.discovery()
        data = {'grant_type': 'authorization_code', 'code': code, 'redirect_uri': self.redirect,
                'client_id': self.client, 'code_verifier': saved['verifier']}
        if os.environ.get('DATA_PLATFORM_OIDC_CLIENT_SECRET'):
            data['client_secret'] = os.environ['DATA_PLATFORM_OIDC_CLIENT_SECRET']
        with httpx.Client(timeout=10) as client:
            response = client.post(config['token_endpoint'], data=data)
            response.raise_for_status()
            token = response.json()['id_token']
            keys = client.get(config['jwks_uri'])
            keys.raise_for_status()
            jwks = keys.json()['keys']
        header = jwt.get_unverified_header(token)
        if header.get('alg') != 'RS256':
            raise ValueError('Unsupported identity signing algorithm.')
        matches = [key for key in jwks[:64] if key.get('kid') == header.get('kid')]
        if len(matches) != 1:
            raise ValueError('Identity signing key not found.')
        claims = jwt.decode(token, jwt.PyJWK.from_dict(matches[0]).key, algorithms=['RS256'],
            audience=self.client, issuer=self.issuer,
            options={'require': ['exp', 'iat', 'sub', 'aud', 'iss', 'nonce']})
        if not secrets.compare_digest(str(claims['nonce']), saved['nonce']):
            raise ValueError('Identity nonce mismatch.')
        if claims.get('azp', self.client) != self.client or (isinstance(claims['aud'], list) and len(claims['aud']) > 1 and 'azp' not in claims):
            raise ValueError('Identity authorized-party mismatch.')
        subject = claims['sub']
        if not isinstance(subject, str) or not 1 <= len(subject) <= 255:
            raise ValueError('Invalid identity subject.')
        name = str(claims.get('preferred_username', subject))[:200]
        raw = secrets.token_urlsafe(48)
        with connection(self.metadata) as con:
            con.execute('INSERT INTO users VALUES(?,?) ON CONFLICT(sub) DO UPDATE SET name=excluded.name', (subject, name))
            member = con.execute('SELECT workspace_id FROM memberships WHERE sub=? ORDER BY workspace_id LIMIT 1', (subject,)).fetchone()
            if not member:
                workspace = uuid.uuid4().hex
                con.execute('INSERT INTO workspaces VALUES(?,?)', (workspace, name + "'s workspace"))
                con.execute('INSERT INTO memberships VALUES(?,?,?)', (workspace, subject, 'owner'))
            else:
                workspace = member['workspace_id']
            con.execute('DELETE FROM sessions WHERE expires<?', (time.time(),))
            con.execute('INSERT INTO sessions VALUES(?,?,?,?)', (digest(raw), subject, workspace, min(time.time() + 28800, claims['exp'])))
        response = RedirectResponse('/', status_code=303)
        response.delete_cookie('dp_state', path='/auth')
        response.set_cookie('dp_session', raw, max_age=28800, httponly=True, secure=self.secure, samesite='lax')
        return response

    def session(self, request):
        authorization = request.headers.get('authorization', '')
        if authorization.startswith('Bearer '):
            value = authorization[7:]
            if not value or len(value) > 256:
                return None
            with connection(self.metadata) as con:
                return con.execute("""SELECT api_tokens.id,api_tokens.sub,api_tokens.workspace_id,api_tokens.expires,
                    users.name,CASE WHEN memberships.role='viewer' THEN 'viewer' ELSE 'editor' END AS role,1 AS service
                    FROM api_tokens JOIN users ON users.sub=api_tokens.sub JOIN memberships
                    ON memberships.sub=api_tokens.sub AND memberships.workspace_id=api_tokens.workspace_id
                    WHERE api_tokens.id=? AND api_tokens.expires>?""", (digest(value), time.time())).fetchone()
        value = request.cookies.get('dp_session', '')
        if not value or len(value) > 256:
            return None
        with connection(self.metadata) as con:
            return con.execute('''SELECT sessions.*,users.name,memberships.role,0 AS service FROM sessions JOIN users ON users.sub=sessions.sub
                JOIN memberships ON memberships.sub=sessions.sub AND memberships.workspace_id=sessions.workspace_id
                WHERE sessions.id=? AND sessions.expires>?''', (digest(value), time.time())).fetchone()

    async def route(self, request: Request):
        from starlette.concurrency import run_in_threadpool
        path = request.url.path
        host = request.headers.get('host', '').split(':')[0]
        allowed_hosts = set(os.environ.get('DATA_PLATFORM_ALLOWED_HOSTS', 'localhost,127.0.0.1,testserver').split(','))
        if host not in allowed_hosts:
            return JSONResponse({'detail': 'Host not allowed'}, 403)
        if path == '/auth/login' and request.method == 'GET':
            try:
                return await run_in_threadpool(self.login)
            except Exception:
                return JSONResponse({'detail': 'Identity provider is unavailable or misconfigured.'}, 503)
        if path == '/auth/callback' and request.method == 'GET':
            try:
                return await run_in_threadpool(self.callback, request)
            except Exception:
                return JSONResponse({'detail': 'Identity verification failed. Start a new login.'}, 400)
        session = await run_in_threadpool(self.session, request)
        if not session:
            if path == '/':
                return FileResponse(Path(__file__).parent / 'static' / 'login.html')
            if path.startswith('/static/'):
                candidate = (Path(__file__).parent / path.lstrip('/')).resolve()
                root = (Path(__file__).parent / 'static').resolve()
                if candidate.is_relative_to(root) and candidate.is_file():
                    return FileResponse(candidate)
            return JSONResponse({'detail': 'Sign in to access this workspace.'}, 401)
        if request.method not in ('GET', 'HEAD', 'OPTIONS') and not session['service']:
            expected = f'{request.url.scheme}://{request.headers.get("host")}'
            if request.headers.get('origin') != expected:
                return JSONResponse({'detail': 'Same-origin request required.'}, 403)
        if session['service'] and (path.startswith('/auth/') or path.startswith('/api/workspace')):
            return JSONResponse({'detail': 'Service tokens cannot manage identity or workspace membership.'}, 403)
        if path == '/api/workspace/tokens':
            if session['role'] != 'owner':
                return JSONResponse({'detail': 'Workspace owner role required.'}, 403)
            if request.method == 'GET':
                with connection(self.metadata) as con:
                    return JSONResponse([dict(row) for row in con.execute('SELECT id,name,created,expires FROM api_tokens WHERE workspace_id=?', (session['workspace_id'],))])
            body = await request.json()
            if not isinstance(body,dict):
                return JSONResponse({'detail':'Use a JSON object.'},400)
            if request.method == 'DELETE':
                with connection(self.metadata) as con:
                    con.execute('DELETE FROM api_tokens WHERE workspace_id=? AND id=?', (session['workspace_id'], body.get('id')))
                return JSONResponse({'revoked': True})
            if request.method == 'POST':
                name = str(body.get('name', '')).strip()
                lifetime = body.get('lifetime_seconds', 86400)
                if not 1 <= len(name) <= 100 or not isinstance(lifetime, int) or not 60 <= lifetime <= 2592000:
                    return JSONResponse({'detail': 'Use a name and token lifetime between 60 seconds and 30 days.'}, 400)
                token = 'dp_' + secrets.token_urlsafe(48)
                with connection(self.metadata) as con:
                    con.execute('INSERT INTO api_tokens VALUES(?,?,?,?,?,?)',
                        (digest(token), session['sub'], session['workspace_id'], name, time.time(), time.time() + lifetime))
                return JSONResponse({'id': digest(token), 'token': token, 'note': 'Store securely; this token is shown only once.'}, 201)
        if path == '/auth/logout' and request.method == 'POST':
            with connection(self.metadata) as con:
                con.execute('DELETE FROM sessions WHERE id=?', (session['id'],))
            response = JSONResponse({'signed_out': True})
            response.delete_cookie('dp_session')
            return response
        if path == '/api/session' and request.method == 'GET':
            with connection(self.metadata) as con:
                workspaces = [dict(row) for row in con.execute('''SELECT workspaces.*,memberships.role FROM workspaces
                    JOIN memberships ON memberships.workspace_id=workspaces.id WHERE memberships.sub=? AND (?=0 OR workspaces.id=?)''', (session['sub'], session['service'], session['workspace_id']))]
            return JSONResponse({'mode': 'oidc', 'user': {'id': session['sub'], 'name': session['name']},
                                 'workspace_id': session['workspace_id'], 'role': session['role'], 'workspaces': workspaces})
        if path == '/api/workspaces' and request.method == 'POST':
            body = await request.json()
            if not isinstance(body,dict):
                return JSONResponse({'detail':'Use a JSON object.'},400)
            name = str(body.get('name', '')).strip()
            if not 1 <= len(name) <= 200:
                return JSONResponse({'detail': 'Workspace name requires 1–200 characters.'}, 400)
            ident = uuid.uuid4().hex
            with connection(self.metadata) as con:
                con.execute('INSERT INTO workspaces VALUES(?,?)', (ident, name))
                con.execute('INSERT INTO memberships VALUES(?,?,?)', (ident, session['sub'], 'owner'))
            return JSONResponse({'id': ident}, 201)
        if path.startswith('/api/workspaces/'):
            parts = path.strip('/').split('/')
            if len(parts) == 4 and parts[-1] == 'select' and request.method == 'POST':
                with connection(self.metadata) as con:
                    if not con.execute('SELECT 1 FROM memberships WHERE workspace_id=? AND sub=?', (parts[2], session['sub'])).fetchone():
                        return JSONResponse({'detail': 'Workspace access denied.'}, 403)
                    con.execute('UPDATE sessions SET workspace_id=? WHERE id=?', (parts[2], session['id']))
                return JSONResponse({'workspace_id': parts[2]})
        if path == '/api/workspace/members':
            if session['role'] != 'owner':
                return JSONResponse({'detail': 'Workspace owner role required.'}, 403)
            if request.method == 'GET':
                with connection(self.metadata) as con:
                    return JSONResponse([dict(row) for row in con.execute('''SELECT memberships.sub,memberships.role,users.name
                        FROM memberships JOIN users ON users.sub=memberships.sub WHERE workspace_id=?''', (session['workspace_id'],))])
            if request.method in ('PUT', 'DELETE'):
                body = await request.json()
                if not isinstance(body,dict):
                    return JSONResponse({'detail':'Use a JSON object.'},400)
                subject, role = body.get('user_id'), body.get('role')
                with connection(self.metadata) as con:
                    if not con.execute('SELECT 1 FROM users WHERE sub=?', (subject,)).fetchone():
                        return JSONResponse({'detail': 'This user must sign in before receiving membership.'}, 404)
                    if subject == session['sub']:
                        return JSONResponse({'detail': 'Owners cannot change their own membership here.'}, 400)
                    if request.method == 'DELETE':
                        con.execute('DELETE FROM memberships WHERE workspace_id=? AND sub=?', (session['workspace_id'], subject))
                    else:
                        if role not in ('owner', 'editor', 'viewer'):
                            return JSONResponse({'detail': 'Role must be owner, editor, or viewer.'}, 400)
                        con.execute('INSERT INTO memberships VALUES(?,?,?) ON CONFLICT(workspace_id,sub) DO UPDATE SET role=excluded.role',
                                    (session['workspace_id'], subject, role))
                return JSONResponse({'updated': True})
        if session['role'] == 'viewer' and request.method not in ('GET', 'HEAD', 'OPTIONS') and path != '/api/query' and not re.fullmatch(r'/api/documents/[a-f0-9]{32}/(?:items/\d+|python)/run',path) and not re.fullmatch(r'/api/semantic-models/[a-f0-9]{32}/query',path):
            return JSONResponse({'detail': 'This action requires editor or owner role.'}, 403)
        def workspace_app():
            with self.guard:
                ident = session['workspace_id']
                if ident not in self.apps:
                    self.apps[ident] = self.factory(self.base / 'workspaces' / ident)
                    self.apps[ident].state.data_lock.acquire()
                    self.apps[ident].state.flows.start()
                    if os.environ.get('DATA_PLATFORM_ORCHESTRATOR') != 'dagster':
                        self.apps[ident].state.pipelines.start()
                return self.apps[ident]
        app = await run_in_threadpool(workspace_app)
        request.scope.setdefault('state', {}).update({'workspace_id': session['workspace_id'], 'user_id': session['sub'], 'role': session['role']})
        # Reuse the already-parsed request stream for the underlying workspace ASGI app.
        async def response(scope, receive, send):
            await app(scope, receive, send)
        return response
