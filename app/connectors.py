"""Allowlisted, read-only PostgreSQL snapshots and integer-watermark ingestion."""
import json
import os
import re
from pathlib import Path
import tempfile
import uuid

from cryptography.fernet import Fernet
import psycopg

from app.workers import prepare
from app.workflows import now


class Connectors:
    def __init__(self, workflows):
        self.workflows = workflows
        path = workflows.base / 'connector.key'
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(descriptor, 'wb') as output:
                output.write(Fernet.generate_key())
        self.cipher = Fernet(path.read_bytes())

    def validate_host(self, host, port):
        hosts = set(os.environ.get('DATA_PLATFORM_CONNECTOR_HOSTS', '127.0.0.1,localhost').split(','))
        ports = {int(value) for value in os.environ.get('DATA_PLATFORM_CONNECTOR_PORTS', '5433').split(',')}
        if host not in hosts or port not in ports:
            raise ValueError('Source host or port is not in the server connector allowlist.')

    def list(self):
        with self.workflows.catalog() as con:
            return [dict(row) for row in con.execute('''SELECT id,name,host,port,database_name,username,
                source_schema,source_table,mode,watermark_column,watermark,target_id,created_at FROM connectors ORDER BY created_at DESC''')]

    def create(self, values):
        self.validate_host(values['host'], values['port'])
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',values['database']) or not re.fullmatch(r'[A-Za-z0-9_.@-]{1,100}',values['username']):
            raise ValueError('Use simple database and username identifiers; connection strings are not accepted in these fields.')
        if values['mode'] not in ('snapshot', 'incremental'):
            raise ValueError('Choose snapshot or incremental mode.')
        if values['mode'] == 'incremental' and not values.get('watermark_column'):
            raise ValueError('Incremental mode requires an integer watermark column.')
        secret = self.cipher.encrypt(values['password'].encode()).decode()
        ident = uuid.uuid4().hex
        with self.workflows.catalog() as con:
            con.execute('INSERT INTO connectors VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (ident, values['name'], values['host'], values['port'], values['database'], values['username'], secret,
                 values['schema'], values['table'], values['mode'], values.get('watermark_column'), None, None, None, now()))
        return {'id': ident}

    def config(self, ident):
        with self.workflows.catalog() as con:
            row = con.execute('SELECT * FROM connectors WHERE id=?', (ident,)).fetchone()
        if not row:
            raise LookupError('Connector not found')
        self.validate_host(row['host'], row['port'])
        config = {'host': row['host'], 'port': row['port'], 'dbname': row['database_name'], 'user': row['username'],
            'password': self.cipher.decrypt(row['credential'].encode()).decode(),
            'sslmode': 'prefer' if row['host'] in ('127.0.0.1', 'localhost') else 'verify-full',
            'schema': row['source_schema'], 'table': row['source_table'], 'mode': row['mode'],
            'watermark_column': row['watermark_column'], 'checkpoint': json.loads(row['watermark']) if row['watermark'] else None,
            'expected_schema': json.loads(row['schema_json']) if row['schema_json'] and row['mode'] == 'incremental' else None}
        if row['target_id']:
            config['target_table'] = self.workflows.dataset(row['target_id'])['table_name']
        return dict(row), config

    def test(self, ident):
        _, config = self.config(ident)
        try:
            with psycopg.connect(**{key: config[key] for key in ('host','port','dbname','user','password','sslmode')}, connect_timeout=5,
                                 options='-c statement_timeout=5000') as pg:
                pg.execute('SET TRANSACTION READ ONLY')
                tables = pg.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1,2 LIMIT 200").fetchall()
            return {'connected': True, 'tables': [{'schema': row[0], 'table': row[1]} for row in tables]}
        except Exception:
            raise ValueError('Cannot connect to the source or read its catalogue. Verify credentials, TLS, allowlist and source permissions.') from None

    def run(self, ident, pipeline_run=None):
        with self.workflows.lock:
            self.workflows.recover()
            row, config = self.config(ident)
            with tempfile.TemporaryDirectory(prefix='data-platform-connector-') as temporary:
                config_file = Path(temporary) / 'connection.json'
                config_file.write_text(json.dumps(config))
                config_file.chmod(0o600)
                parquet = prepare('postgres', config_file, '', self.workflows.database, None, temporary)
                result = json.loads((Path(temporary) / 'worker.json').read_text())
                if result['new_rows'] == 0 and row['target_id']:
                    return {'id': row['target_id'], 'new_rows': 0, 'unchanged': True}
                inputs = [{'name': f'PostgreSQL {row["source_schema"]}.{row["source_table"]}', 'dataset_id': None,
                           'version_id': None, 'connector_id': ident, 'watermark_before': config['checkpoint'], 'watermark_after': result['watermark']}]
                published = self.workflows.publish(parquet, parquet, row['name'] + '.parquet', target=row['target_id'], kind='postgres', inputs=inputs, pipeline_run=pipeline_run,
                    connector_checkpoint={'id': ident, 'schema': result['schema'], 'watermark': result['watermark']})
                return {**published, 'new_rows': result['new_rows']}
