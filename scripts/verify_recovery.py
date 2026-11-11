"""Cold, encrypted recovery drill using disposable stacks, never the development volumes.

Run with .venv/bin/python scripts/verify_recovery.py --report /tmp/recovery.json.
Requires the pinned Compose images locally or registry access. Only volumes created
by this invocation are removed. No credentials or expanded Compose config are logged.
"""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import backup, dev_stack


def run(arguments, helper_diagnostics=False):
    # Captured service errors can include credentials. Report the operation only.
    result = subprocess.run(arguments, capture_output=True, text=True)
    if result.returncode:
        operation = ' '.join(arguments[:4])
        if 'compose' in arguments:
            operation = 'docker compose ' + arguments[arguments.index('-f') + 2]
        # Only the offline tar/list helper has credential-free stderr. Never
        # expose expanded Compose errors or service configuration.
        detail = result.stderr[-1000:].strip() if helper_diagnostics else ''
        raise RuntimeError('Recovery operation failed: ' + operation + (': ' + detail if detail else ''))
    return result.stdout


def private_json(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as target:
        json.dump(value, target, indent=2)


def ports():
    # Hold all sockets together so each selected port is distinct.
    from contextlib import ExitStack
    with ExitStack() as stack:
        selected = {}
        for service in ('postgres', 'keycloak', 'objectstore', 'iceberg', 'trino'):
            listener = stack.enter_context(socket.socket())
            listener.bind(('127.0.0.1', 0))
            selected[service] = listener.getsockname()[1]
        return selected


def configure(template, project, selected, destination):
    """Generate a private config with fresh volumes/network and loopback-only ports."""
    config = json.loads(json.dumps(template))
    config['name'] = project
    for name, volume in config['volumes'].items():
        if volume.get('external'):
            raise ValueError('Recovery drill cannot use external volumes.')
        volume['name'] = project + '_' + name
    for name, network in config.get('networks', {}).items():
        if network.get('external'):
            raise ValueError('Recovery drill cannot use external networks.')
        network['name'] = project + '_' + name
    for name, service in config['services'].items():
        if 'container_name' in service:
            raise ValueError('Recovery drill cannot use fixed container names.')
        for port in service.get('ports', []):
            port.update(host_ip='127.0.0.1', published=str(selected[name]))
        for mount in service.get('volumes', []):
            if mount['type'] == 'volume':
                # Images can ship files in /data (SeaweedFS includes filerldb2).
                # Restore must start from an empty volume, not an image copy.
                mount.setdefault('volume', {})['nocopy'] = True
    config['services']['keycloak']['command'] = [
        'start-dev', '--import-realm', '--http-port=8080',
        '--hostname=http://127.0.0.1:' + str(selected['keycloak'])]
    private_json(destination, config)
    return config


class Stack:
    def __init__(self, project, config, selected):
        self.project, self.path, self.ports = project, config, selected

    def compose(self, *arguments):
        return run(['docker', '--host=unix:///var/run/docker.sock', 'compose',
                    '-p', self.project, '-f', str(self.path), *arguments])

    def assert_stopped(self):
        if self.compose('ps', '--status', 'running', '-q').strip():
            raise RuntimeError('Stop all drill services before capturing volumes.')

    def volume(self, name):
        volume = self.project + '_' + name
        metadata = json.loads(run(['docker', '--host=unix:///var/run/docker.sock',
                                   'volume', 'inspect', volume]))[0]
        if metadata.get('Labels', {}).get('com.docker.compose.project') != self.project:
            raise RuntimeError('Refusing a volume outside the disposable drill project.')
        return volume

    def ready(self):
        import httpx
        deadline = time.monotonic() + 240
        pending = {'postgres', 'keycloak', 'objectstore', 'iceberg', 'trino'}
        with httpx.Client(timeout=3, trust_env=False) as client:
            while pending and time.monotonic() < deadline:
                for service in tuple(pending):
                    try:
                        endpoint = 'http://127.0.0.1:' + str(self.ports[service])
                        if service == 'postgres':
                            self.compose('exec', '-T', 'postgres', 'pg_isready', '-U', 'platform', '-d', 'platform')
                            passed = True
                        elif service == 'keycloak':
                            result = client.get(endpoint + '/realms/data-platform/.well-known/openid-configuration')
                            passed = result.status_code == 200 and result.json().get('issuer') == endpoint + '/realms/data-platform'
                        elif service == 'objectstore':
                            result = client.get(endpoint + '/')
                            passed = result.status_code == 403 and 'AccessDenied' in result.text
                        elif service == 'iceberg':
                            result = client.get(endpoint + '/v1/config')
                            passed = result.status_code == 200 and 'defaults' in result.json()
                        else:
                            result = client.get(endpoint + '/v1/info')
                            passed = result.status_code == 200 and not result.json().get('starting', True)
                        if passed:
                            pending.remove(service)
                    except (httpx.HTTPError, ValueError, RuntimeError):
                        pass
                if pending:
                    time.sleep(1)
        if pending:
            raise RuntimeError('Recovery services not ready: ' + ', '.join(sorted(pending)))

    def clean(self):
        # Projects use unguessable names and are created only by this invocation.
        self.compose('down', '--volumes', '--remove-orphans')


def transfer_volume(stack, name, directory, image, restore=False):
    stack.assert_stopped()
    volume = stack.volume(name)
    arguments = ['docker', '--host=unix:///var/run/docker.sock', 'run', '--rm',
                 '--network', 'none', '--read-only', '--user', '0',
                 '--mount', 'type=volume,src=' + volume + ',dst=/data' + ('' if restore else ',readonly'),
                 '--mount', 'type=bind,src=' + str(directory) + ',dst=/backup' + (',readonly' if restore else ''),
                 '--entrypoint', 'tar', image]
    if restore:
        # Only newly created, empty drill volumes may receive archive contents.
        empty = run(['docker', '--host=unix:///var/run/docker.sock', 'run', '--rm',
             '--network', 'none', '--read-only', '--user', '0',
             '--mount', 'type=volume,src=' + volume + ',dst=/data,readonly',
             '--entrypoint', 'sh', image, '-c', 'ls -A /data'], helper_diagnostics=True)
        if empty.strip():
            raise RuntimeError('Restore destination volume is not empty: ' + name)
        arguments += ['--numeric-owner', '-xpf', '/backup/' + name + '.tar', '-C', '/data']
    else:
        arguments += ['--numeric-owner', '-cpf', '/backup/' + name + '.tar', '-C', '/data', '.']
    run(arguments, helper_diagnostics=True)


@contextmanager
def application_environment(selected):
    values = {'DATA_PLATFORM_TRINO_URL': 'http://127.0.0.1:' + str(selected['trino']),
              'DATA_PLATFORM_CONNECTOR_HOSTS': '127.0.0.1',
              'DATA_PLATFORM_CONNECTOR_PORTS': str(selected['postgres']),
              'DATA_PLATFORM_ORCHESTRATOR': 'dagster'}
    previous = {name: os.environ.get(name) for name in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def identity(selected, values, create=False):
    import httpx
    endpoint = 'http://127.0.0.1:' + str(selected['keycloak'])
    with httpx.Client(timeout=20, trust_env=False) as client:
        token = client.post(endpoint + '/realms/master/protocol/openid-connect/token', data={
            'grant_type': 'password', 'client_id': 'admin-cli', 'username': 'admin',
            'password': values['KEYCLOAK_ADMIN_PASSWORD']})
        token.raise_for_status()
        headers = {'Authorization': 'Bearer ' + token.json()['access_token']}
        url = endpoint + '/admin/realms/data-platform/users'
        if create:
            response = client.post(url, headers=headers, json={
                'username': 'recovery-probe', 'enabled': True, 'email': 'recovery@example.invalid'})
            response.raise_for_status()
        response = client.get(url, headers=headers, params={'username': 'recovery-probe', 'exact': 'true'})
        response.raise_for_status()
        users = response.json()
        if len(users) != 1:
            raise AssertionError('Recovery identity is missing or ambiguous.')
        return users[0]['id']


def s3_bucket(selected, values):
    import boto3
    import botocore.session
    from botocore.configprovider import ConstantProvider
    session = botocore.session.Session()
    session.get_component('config_store').set_config_provider('profile', ConstantProvider(None))
    client = boto3.Session(botocore_session=session).client(
        's3', endpoint_url='http://127.0.0.1:' + str(selected['objectstore']), region_name='us-east-1',
        aws_access_key_id=values['S3_ACCESS_KEY'], aws_secret_access_key=values['S3_SECRET_KEY'])
    client.create_bucket(Bucket='warehouse')


def request(client, method, path, **kwargs):
    response = client.request(method, path, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError('Recovery API check failed: ' + method + ' ' + path + ' (' + str(response.status_code) + ')')
    return response.json()


def seed(base, selected, values):
    import psycopg
    from fastapi.testclient import TestClient
    from app.main import create_local_app
    from app.lakehouse import Lakehouse
    with psycopg.connect(host='127.0.0.1', port=selected['postgres'], dbname='platform',
                         user='platform', password=values['POSTGRES_PASSWORD']) as connection:
        connection.execute('CREATE TABLE public.recovery_probe(id BIGINT PRIMARY KEY, amount BIGINT)')
        connection.execute('INSERT INTO public.recovery_probe VALUES(1,7),(2,11)')
    s3_bucket(selected, values)
    expected = {'identity_id': identity(selected, values, create=True)}
    with application_environment(selected), TestClient(create_local_app(base)) as client:
        connector = request(client, 'POST', '/api/connectors', json={
            'name': 'Recovery source', 'host': '127.0.0.1', 'port': selected['postgres'],
            'database': 'platform', 'username': 'platform', 'password': values['POSTGRES_PASSWORD'],
            'schema_name': 'public', 'table': 'recovery_probe', 'mode': 'snapshot'})
        request(client, 'POST', '/api/connectors/' + connector['id'] + '/import')
        expected['connector_id'] = connector['id']
        dataset = request(client, 'POST', '/api/datasets', files={'file': ('recovery.csv', b'amount\n2\n3\n')})
        expected.update(dataset_id=dataset['id'], table=dataset['table_name'], versions=[])
        for number, content, total in ((1, b'amount\n2\n3\n', 5), (2, b'amount\n20\n30\n', 50)):
            if number == 2:
                request(client, 'POST', '/api/datasets/' + dataset['id'] + '/versions',
                        files={'file': ('recovery.csv', content)})
            version = request(client, 'GET', '/api/datasets/' + dataset['id'] + '/versions')[0]
            lake = Lakehouse(client.app.state.flows.workflows)
            publication = lake.publish(dataset['id'], version_id=version['id'])
            snapshots = request(client, 'GET', '/api/datasets/' + dataset['id'] + '/lakehouse/' + version['id'] + '/snapshots')
            expected['versions'].append({'id': version['id'], 'total': total, 'table': publication['table'],
                                         'source': content.decode(), 'snapshot_id': snapshots['rows'][0][0]})
    return expected


def check_restored(base, selected, values, expected):
    import duckdb
    from fastapi.testclient import TestClient
    from app.main import create_local_app
    if identity(selected, values) != expected['identity_id']:
        raise AssertionError('Restored identity ID changed.')
    with application_environment(selected), TestClient(create_local_app(base)) as client:
        native = request(client, 'POST', '/api/query', json={'sql': 'SELECT sum(amount) FROM ' + expected['table']})
        if native['rows'] != [[50]]:
            raise AssertionError('Restored native dataset changed.')
        connection = request(client, 'POST', '/api/connectors/' + expected['connector_id'] + '/test')
        if not connection.get('connected'):
            raise AssertionError('Restored connector could not authenticate.')
        imported = request(client, 'POST', '/api/connectors/' + expected['connector_id'] + '/import')
        rows = request(client, 'POST', '/api/query', json={'sql': 'SELECT sum(amount) FROM ' + imported['table_name']})
        if rows['rows'] != [[18]]:
            raise AssertionError('Restored PostgreSQL source or connector key changed.')
        versions = request(client, 'GET', '/api/datasets/' + expected['dataset_id'] + '/versions')
        if {item['id'] for item in versions} != {item['id'] for item in expected['versions']}:
            raise AssertionError('Restored immutable version IDs changed.')
        for version in expected['versions']:
            from app.workflows import quote
            native_version = next(item for item in versions if item['id'] == version['id'])
            with duckdb.connect(str(base / 'datasets.duckdb')) as database:
                total = database.execute('SELECT sum(amount) FROM ' + quote(native_version['table_name'])).fetchone()[0]
            if total != version['total']:
                raise AssertionError('Restored native immutable table changed.')
            path = '/api/datasets/' + expected['dataset_id'] + '/versions/' + version['id'] + '/source'
            response = client.get(path)
            if response.status_code != 200 or response.content != version['source'].encode():
                raise AssertionError('Restored original version source changed.')
            remote = request(client, 'POST', '/api/query', json={
                'engine': 'trino', 'sql': 'SELECT sum(amount) FROM ' + version['table']})
            if remote['rows'] != [[version['total']]]:
                raise AssertionError('Restored published version changed.')
            snapshots = request(client, 'GET', '/api/datasets/' + expected['dataset_id'] + '/lakehouse/' + version['id'] + '/snapshots')
            if snapshots['rows'][0][0] != version['snapshot_id']:
                raise AssertionError('Restored Iceberg snapshot ID changed.')


def drill():
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='platform-recovery-') as temporary:
        directory = Path(temporary)
        deploy = directory / 'deploy'
        deploy.mkdir(mode=0o700)
        for name in ('compose.yml', 'keycloak-realm.json', 'trino-jvm.config'):
            shutil.copyfile(ROOT / 'deploy' / name, deploy / name)
        # Reuse the supported generator, directed only at the private drill directory.
        original = dev_stack.RUNTIME, dev_stack.ENV
        try:
            dev_stack.RUNTIME, dev_stack.ENV = deploy / 'runtime', deploy / 'runtime/stack.env'
            dev_stack.init()
        finally:
            dev_stack.RUNTIME, dev_stack.ENV = original
        values = dict(line.split('=', 1) for line in (deploy / 'runtime/stack.env').read_text().splitlines())
        template = json.loads(run(['docker', '--host=unix:///var/run/docker.sock', 'compose',
            '--env-file', str(deploy / 'runtime/stack.env'), '-f', str(deploy / 'compose.yml'), 'config', '--format', 'json']))
        selected = ports()
        token = uuid.uuid4().hex[:16]
        stacks = []
        cleanup_errors = []
        try:
            for phase in ('source', 'restored'):
                project = 'recovery-' + token + '-' + phase
                path = directory / (phase + '.json')
                config = configure(template, project, selected, path)
                stacks.append(Stack(project, path, selected))
            source, restored = stacks
            print('Starting disposable source stack.', flush=True)
            source.compose('up', '-d')
            source.ready()
            print('Source protocols ready; seeding identity, source rows and two published versions.', flush=True)
            expected = seed(directory / 'application', selected, values)
            source.compose('stop', '--timeout', '60')
            source.assert_stopped()
            print('Application closed and source services stopped; capturing cold volumes.', flush=True)
            bundle = directory / 'bundle'
            bundle.mkdir(mode=0o700)
            keyfile = directory / 'backup.key'
            backup.key(keyfile, create=True)
            backup.backup(directory / 'application', bundle / 'application.dpb', keyfile)
            shutil.copytree(deploy, bundle / 'deploy')
            private_json(bundle / 'compose-template.json', template)
            image = config['services']['postgres']['image']
            for name in ('postgres', 'catalog', 'objects'):
                transfer_volume(source, name, bundle, image)
            private_json(bundle / 'manifest.json', {'format': 1, 'method': 'cold-volume-copy',
                'images': {name: service['image'] for name, service in config['services'].items()},
                'volumes': ['postgres', 'catalog', 'objects']})
            archive = directory / 'coordinated.dpb'
            backup.backup(bundle, archive, keyfile)
            recovered = directory / 'recovered'
            backup.restore(archive, recovered, keyfile)
            print('Encrypted coordinated archive authenticated and restored.', flush=True)
            # VFS Docker copies image layers per container. Release stopped source
            # containers before creating restored ones; retain the captured volumes.
            source.compose('down')
            # Runtime credentials must also come from the recovered backup.
            shutil.rmtree(deploy)
            shutil.copytree(recovered / 'deploy', deploy)
            restored.path.unlink()
            configure(json.loads((recovered / 'compose-template.json').read_text()),
                      restored.project, selected, restored.path)
            recovered_values = dict(line.split('=', 1) for line in (deploy / 'runtime/stack.env').read_text().splitlines())
            restored.compose('create')
            for name in ('postgres', 'catalog', 'objects'):
                transfer_volume(restored, name, recovered, image, restore=True)
            backup.restore(recovered / 'application.dpb', directory / 'restored-application', keyfile)
            restored.compose('up', '-d')
            restored.ready()
            print('Restored protocols ready; checking retained data, credentials and snapshots.', flush=True)
            check_restored(directory / 'restored-application', selected, recovered_values, expected)
            return {'status': 'passed', 'method': 'cold-volume-copy', 'published_versions': 2,
                    'checks': ['native data', 'immutable version IDs', 'original sources',
                               'PostgreSQL rows', 'connector credential decryption', 'Keycloak identity ID',
                               'Trino published aggregates', 'Iceberg snapshot IDs'],
                    'elapsed_seconds': round(time.monotonic() - started, 2)}
        finally:
            for stack in reversed(stacks):
                try:
                    stack.clean()
                except Exception:
                    cleanup_errors.append(stack.project)
            if cleanup_errors:
                raise RuntimeError('Disposable recovery projects need cleanup: ' + ', '.join(cleanup_errors))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True, help='New JSON report path; never overwritten.')
    arguments = parser.parse_args()
    if arguments.report.exists():
        parser.error('Report already exists; choose a new path.')
    try:
        result = drill()
    except KeyboardInterrupt:
        print('Recovery drill interrupted; disposable-project cleanup attempted.', file=sys.stderr)
        sys.exit(130)
    except Exception as error:
        # Tracebacks/HTTP errors may contain generated credentials; keep logs bounded.
        print('Recovery drill failed (' + type(error).__name__ + '). ' +
              (str(error) if isinstance(error, (RuntimeError, AssertionError)) else 'Inspect the failing operation locally.'), file=sys.stderr)
        sys.exit(1)
    private_json(arguments.report, result)
    print('Coordinated recovery drill passed; private JSON report saved.')
