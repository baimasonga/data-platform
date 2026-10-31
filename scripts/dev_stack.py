"""Generate local development secrets and operate the pinned, loopback-only stack."""
import argparse
import json
import os
import sys
from pathlib import Path
import secrets
import subprocess

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / 'deploy'
RUNTIME = DEPLOY / 'runtime'
ENV = RUNTIME / 'stack.env'


def init():
    RUNTIME.mkdir(mode=0o700, exist_ok=True)
    RUNTIME.chmod(0o700)
    if not ENV.exists():
        values = {name: secrets.token_hex(24) for name in
                  ('POSTGRES_PASSWORD', 'KEYCLOAK_ADMIN_PASSWORD', 'S3_ACCESS_KEY', 'S3_SECRET_KEY')}
        ENV.write_text(''.join(f'{name}={value}\n' for name, value in values.items()))
        ENV.chmod(0o600)
    else:
        values = dict(line.split('=', 1) for line in ENV.read_text().splitlines() if line)
    # This directory is ignored by Git; credentials are never printed.
    s3 = {'identities': [{'name': 'platform', 'credentials': [{'accessKey': values['S3_ACCESS_KEY'],
          'secretKey': values['S3_SECRET_KEY']}], 'actions': ['Admin', 'Read', 'Write', 'List', 'Tagging']}]}
    (RUNTIME / 's3.json').write_text(json.dumps(s3))
    (RUNTIME / 's3.json').chmod(0o600)
    properties = {
        'connector.name': 'iceberg', 'iceberg.catalog.type': 'rest',
        'iceberg.rest-catalog.uri': 'http://iceberg:8181',
        'iceberg.rest-catalog.warehouse': 's3://warehouse/',
        'fs.native-s3.enabled': 'true', 's3.endpoint': 'http://objectstore:8333',
        's3.region': 'us-east-1', 's3.path-style-access': 'true',
        's3.aws-access-key': values['S3_ACCESS_KEY'], 's3.aws-secret-key': values['S3_SECRET_KEY']}
    (RUNTIME / 'trino-iceberg.properties').write_text(''.join(f'{key}={value}\n' for key, value in properties.items()))
    (RUNTIME / 'trino-iceberg.properties').chmod(0o600)
    print('Development configuration prepared. Secret files remain in deploy/runtime/.')


def compose(*arguments):
    subprocess.run(['docker', 'compose', '--env-file', str(ENV), '-f', str(DEPLOY / 'compose.yml'), *arguments], check=True)


def notebook_image():
    command=['docker','--host=unix:///var/run/docker.sock']
    image=subprocess.run(command+['image','inspect','data-platform-python:dev','--format','{{.Id}}'],
                         capture_output=True,text=True)
    if image.returncode:
        return None
    return image.stdout.strip()


def wait_readiness():
    import time
    import httpx
    pending={'oidc','catalog','trino','objectstore'}
    deadline=time.monotonic()+180
    with httpx.Client(timeout=2) as client:
        while pending and time.monotonic()<deadline:
            for service in tuple(pending):
                try:
                    if service=='oidc':
                        response=client.get('http://127.0.0.1:8081/realms/data-platform/.well-known/openid-configuration')
                        ready=response.status_code==200 and response.json().get('issuer')=='http://127.0.0.1:8081/realms/data-platform'
                    elif service=='catalog':
                        response=client.get('http://127.0.0.1:8181/v1/config')
                        ready=response.status_code==200 and 'defaults' in response.json()
                    elif service=='trino':
                        response=client.get('http://127.0.0.1:8082/v1/info')
                        ready=response.status_code==200 and not response.json().get('starting',True)
                    else:
                        response=client.get('http://127.0.0.1:8333/')
                        ready=response.status_code==403 and 'AccessDenied' in response.text
                    if ready:
                        pending.remove(service)
                        print(service+' protocol readiness passed.')
                except (httpx.HTTPError,ValueError):
                    pass
            if pending:
                time.sleep(1)
    if pending:
        raise SystemExit('Services did not become ready: '+', '.join(sorted(pending)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'up', 'down', 'status', 'platform', 'dagster', 'developer', 'notebooks'])
    args = parser.parse_args()
    if args.action in ('init', 'up'):
        init()
    if args.action == 'up':
        compose('up', '-d')
        wait_readiness()
    elif args.action == 'down':
        compose('down')  # Named volumes deliberately retained.
    elif args.action == 'status':
        compose('ps')

    elif args.action == 'notebooks':
        command=['docker','--host=unix:///var/run/docker.sock','build','-t','data-platform-python:dev']
        if os.environ.get('CODEX_PROXY_CERT'):
            command+=['--secret','id=proxy_ca,src='+os.environ['CODEX_PROXY_CERT']]
        subprocess.run(command+[str(DEPLOY/'python')],check=True)
        print('Python notebook runner built. Restart the platform to enable it.')

    elif args.action == 'platform':
        if not os.environ.get('DATA_PLATFORM_PYTHON_IMAGE'):
            image=notebook_image()
            if image:
                os.environ['DATA_PLATFORM_PYTHON_IMAGE']=image
            else:
                print('Python execution is disabled until scripts/dev_stack.py notebooks builds the runner.',flush=True)
        os.environ.update({'DATA_PLATFORM_AUTH_MODE':'oidc',
            'DATA_PLATFORM_OIDC_ISSUER':'http://127.0.0.1:8081/realms/data-platform',
            'DATA_PLATFORM_OIDC_CLIENT_ID':'data-platform-web',
            'DATA_PLATFORM_OIDC_REDIRECT_URI':'http://127.0.0.1:8000/auth/callback',
            'DATA_PLATFORM_ALLOW_HTTP_OIDC':'1',
            'DATA_PLATFORM_TRINO_URL':'http://127.0.0.1:8082'})
        if (RUNTIME / 'dagster.env').exists():
            os.environ['DATA_PLATFORM_ORCHESTRATOR']='dagster'
        os.chdir(ROOT)
        os.execv(str(ROOT / '.venv/bin/python'), [str(ROOT / '.venv/bin/python'), '-m', 'uvicorn', 'app.main:create_app', '--factory', '--host', '127.0.0.1', '--port', '8000', '--no-access-log'])
    elif args.action == 'dagster':
        secret = RUNTIME / 'dagster.env'
        if not secret.exists():
            raise SystemExit('Create a workspace automation token and save DATA_PLATFORM_SERVICE_TOKEN in deploy/runtime/dagster.env first.')
        os.environ.update(dict(line.split('=',1) for line in secret.read_text().splitlines() if line))
        os.environ['DATA_PLATFORM_URL']='http://127.0.0.1:8000'
        home=RUNTIME / 'dagster-home'
        home.mkdir(exist_ok=True)
        os.environ['DAGSTER_HOME']=str(home)
        (home / 'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
        os.chdir(ROOT)
        binary=ROOT / '.dagster-venv/bin/dagster'
        os.execv(str(binary), [str(binary),'dev','-h','127.0.0.1','-p','3000','-f',str(DEPLOY / 'dagster_definitions.py')])

    elif args.action == 'developer':
        import httpx
        values=dict(line.split('=',1) for line in ENV.read_text().splitlines() if line)
        credentials=RUNTIME / 'developer.env'
        if credentials.exists():
            print('Development account already prepared; existing credentials preserved.')
            raise SystemExit(0)
        with httpx.Client(timeout=10) as client:
            token=client.post('http://127.0.0.1:8081/realms/master/protocol/openid-connect/token',data={'grant_type':'password','client_id':'admin-cli','username':'admin','password':values['KEYCLOAK_ADMIN_PASSWORD']})
            token.raise_for_status()
            headers={'Authorization':'Bearer '+token.json()['access_token']}
            base='http://127.0.0.1:8081/admin/realms/data-platform/users'
            existing=client.get(base,params={'username':'developer','exact':'true'},headers=headers)
            existing.raise_for_status()
            if existing.json():
                raise SystemExit('Developer account exists without the local credential file; refusing to overwrite it.')
            password=secrets.token_urlsafe(32)
            response=client.post(base,headers=headers,json={'username':'developer','enabled':True,'email':'developer@example.invalid','emailVerified':True,'firstName':'Development','lastName':'User','requiredActions':[], 'credentials':[{'type':'password','value':password,'temporary':False}]})
            response.raise_for_status()
        credentials.write_text('username=developer\npassword='+password+'\n')
        credentials.chmod(0o600)
        print('Development account prepared; credentials are stored only in deploy/runtime/developer.env.')
