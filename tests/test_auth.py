import hashlib
import json
import secrets
import time
from urllib.parse import parse_qs, urlsplit

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from app.catalog import connection
from app.main import create_app


@pytest.fixture
def gateway(tmp_path, monkeypatch):
    monkeypatch.setenv('DATA_PLATFORM_AUTH_MODE', 'oidc')
    monkeypatch.setenv('DATA_PLATFORM_OIDC_ISSUER', 'http://127.0.0.1:8081/realms/data-platform')
    monkeypatch.setenv('DATA_PLATFORM_OIDC_CLIENT_ID', 'data-platform-web')
    monkeypatch.setenv('DATA_PLATFORM_OIDC_REDIRECT_URI', 'http://127.0.0.1:8000/auth/callback')
    monkeypatch.setenv('DATA_PLATFORM_ALLOW_HTTP_OIDC', '1')
    return create_app(tmp_path)


def session(gateway, client, subject, workspace, role='owner'):
    token = secrets.token_urlsafe(32)
    with connection(gateway.metadata) as con:
        con.execute('INSERT OR IGNORE INTO users VALUES(?,?)', (subject, subject))
        con.execute('INSERT OR IGNORE INTO workspaces VALUES(?,?)', (workspace, workspace))
        con.execute('INSERT OR REPLACE INTO memberships VALUES(?,?,?)', (workspace, subject, role))
        con.execute('INSERT INTO sessions VALUES(?,?,?,?)', (hashlib.sha256(token.encode()).hexdigest(), subject, workspace, time.time() + 3600))
    client.cookies.set('dp_session', token)
    client.headers['Origin'] = 'http://testserver'


def test_workspace_isolation_and_viewer_permissions(gateway):
    with TestClient(gateway) as owner, TestClient(gateway) as other:
        assert owner.get('/api/datasets').status_code == 401
        session(gateway, owner, 'alice', 'a' * 32)
        session(gateway, other, 'bob', 'b' * 32)
        dataset = owner.post('/api/datasets', files={'file': ('x.csv', b'x\n1\n')})
        assert dataset.status_code == 201, dataset.text
        ident = dataset.json()['id']
        assert other.get('/api/datasets').json() == []
        assert other.get('/api/datasets/' + ident).status_code == 404
        assert other.post('/api/workspaces/' + 'a' * 32 + '/select').status_code == 403
        assert owner.put('/api/workspace/members', json={'user_id': 'bob', 'role': 'viewer'}).status_code == 200
        assert other.post('/api/workspaces/' + 'a' * 32 + '/select').status_code == 200
        assert other.get('/api/datasets').json()[0]['id'] == ident
        assert other.post('/api/query', json={'sql': 'SELECT * FROM ' + dataset.json()['table_name']}).status_code == 200
        assert other.post('/api/datasets', files={'file': ('bad.csv', b'x\n2\n')}).status_code == 403
        assert other.patch('/api/datasets/' + ident, json={'name': 'Stolen'}).status_code == 403
        assert other.post('/api/transformations', json={'name': 'Unauthorized', 'sql': 'SELECT 1'}).status_code == 403
        notebook=owner.post('/api/documents',json={'kind':'notebook','name':'Read only notebook','cells':[{'kind':'sql','text':'SELECT 42 AS answer'}]}).json()
        assert other.post('/api/documents/'+notebook['id']+'/items/0/run',json={'revision':1}).json()['rows']==[[42]]
        assert other.put('/api/documents/'+notebook['id'],json=notebook).status_code==403
        assert other.get('/api/workspace/members').status_code == 403
        assert owner.request('DELETE', '/api/workspace/members', json={'user_id': 'bob'}).status_code == 200
        assert other.get('/api/datasets').status_code == 401


def test_workspace_sessions_csrf_and_logout(gateway):
    with TestClient(gateway) as client:
        session(gateway, client, 'alice', 'c' * 32)
        assert client.post('/api/workspaces', json={'name': 'More data'}, headers={'origin': 'https://evil.example'}).status_code == 403
        created = client.post('/api/workspaces', json={'name': 'More data'})
        assert created.status_code == 201
        assert client.post('/api/workspaces/' + created.json()['id'] + '/select').status_code == 200
        assert client.get('/api/session').json()['workspace_id'] == created.json()['id']
        assert client.post('/auth/logout').status_code == 200
        assert client.get('/api/session').status_code == 401


@pytest.mark.parametrize('bad', [None, 'nonce', 'audience', 'signature', 'expired'])
def test_oidc_code_flow_verifies_identity(gateway, monkeypatch, bad):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    wrong = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk['kid'] = 'test-key'
    config = {'issuer': gateway.issuer, 'authorization_endpoint': gateway.issuer + '/auth',
              'token_endpoint': gateway.issuer + '/token', 'jwks_uri': gateway.issuer + '/certs'}
    state = {}
    class Response:
        def __init__(self, value): self.value = value
        def raise_for_status(self): pass
        def json(self): return self.value
    class IssuerClient:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get(self, url): return Response({'keys': [jwk]} if url.endswith('/certs') else config)
        def post(self, url, data):
            assert data['code_verifier']
            claims = {'iss': gateway.issuer, 'sub': 'oidc-user', 'aud': gateway.client,
                      'iat': int(time.time()) - 60, 'exp': int(time.time()) + 600,
                      'nonce': state['nonce'], 'preferred_username': 'User'}
            if bad == 'nonce': claims['nonce'] = 'wrong'
            if bad == 'audience': claims['aud'] = 'another-client'
            if bad == 'expired': claims['exp'] = int(time.time()) - 10
            return Response({'id_token': jwt.encode(claims, wrong if bad == 'signature' else key, algorithm='RS256', headers={'kid': 'test-key'})})
    with TestClient(gateway, follow_redirects=False) as client:
        monkeypatch.setattr('app.auth.httpx.Client', IssuerClient)
        login = client.get('/auth/login')
        assert login.status_code == 307
        parameters = parse_qs(urlsplit(login.headers['location']).query)
        state['nonce'] = parameters['nonce'][0]
        assert parameters['code_challenge_method'] == ['S256']
        callback = client.get('/auth/callback', params={'state': parameters['state'][0], 'code': 'one-time-code'})
        if bad:
            assert callback.status_code == 400
            assert client.get('/api/session').status_code == 401
        else:
            assert callback.status_code == 303
            assert 'HttpOnly' in callback.headers['set-cookie']
            assert client.get('/api/session').json()['user']['id'] == 'oidc-user'
        assert client.get('/auth/callback', params={'state': parameters['state'][0], 'code': 'one-time-code'}).status_code == 400


def test_oidc_rejects_unconfigured_insecure_provider(tmp_path, monkeypatch):
    monkeypatch.setenv('DATA_PLATFORM_AUTH_MODE', 'oidc')
    monkeypatch.setenv('DATA_PLATFORM_OIDC_ISSUER', 'http://public.example/realm')
    monkeypatch.setenv('DATA_PLATFORM_OIDC_CLIENT_ID', 'test')
    monkeypatch.setenv('DATA_PLATFORM_OIDC_REDIRECT_URI', 'https://platform.example/auth/callback')
    monkeypatch.delenv('DATA_PLATFORM_ALLOW_HTTP_OIDC', raising=False)
    with pytest.raises(ValueError):
        create_app(tmp_path)



def test_scoped_automation_tokens_are_revocable_and_not_owner_credentials(gateway):
    with TestClient(gateway) as owner, TestClient(gateway) as machine:
        session(gateway, owner, 'automation-owner', 'd' * 32)
        created=owner.post('/api/workspace/tokens',json={'name':'Orchestrator','lifetime_seconds':3600})
        assert created.status_code == 201
        token=created.json()
        machine.headers['Authorization']='Bearer '+token['token']
        assert machine.get('/api/session').json()['role']=='editor'
        assert machine.post('/api/pipelines',json={'name':'Job','sql':'SELECT 1'}).status_code==201
        assert machine.get('/api/workspace/members').status_code==403
        assert machine.post('/api/workspaces',json={'name':'No escalation'}).status_code==403
        listing=owner.get('/api/workspace/tokens').json()
        assert 'token' not in listing[0]
        assert owner.request('DELETE','/api/workspace/tokens',json={'id':token['id']}).status_code==200
        assert machine.get('/api/datasets').status_code==401
