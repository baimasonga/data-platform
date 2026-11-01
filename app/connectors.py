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
        with workflows.catalog() as con:
            con.execute("UPDATE connector_sync_runs SET status='interrupted',finished_at=?,error='Server stopped during synchronization. Retry explicitly or through its schedule.' WHERE status='running'",(now(),))

    def validate_host(self, host, port):
        hosts = set(os.environ.get('DATA_PLATFORM_CONNECTOR_HOSTS', '127.0.0.1,localhost').split(','))
        ports = {int(value) for value in os.environ.get('DATA_PLATFORM_CONNECTOR_PORTS', '5433').split(',')}
        if host not in hosts or port not in ports:
            raise ValueError('Source host or port is not in the server connector allowlist.')

    def list(self):
        with self.workflows.catalog() as con:
            return [dict(row) for row in con.execute('''SELECT c.id,c.name,c.host,c.port,c.database_name,c.username,
                c.source_schema,c.source_table,c.mode,c.watermark_column,c.watermark,c.target_id,c.created_at,
                s.phase,s.last_attempt,s.last_success,s.last_error FROM connectors c
                LEFT JOIN connector_sync_state s ON s.connector_id=c.id ORDER BY c.created_at DESC''')]

    def create(self, values):
        self.validate_host(values['host'], values['port'])
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',values['database']) or not re.fullmatch(r'[A-Za-z0-9_.@-]{1,100}',values['username']):
            raise ValueError('Use simple database and username identifiers; connection strings are not accepted in these fields.')
        if values['mode'] not in ('snapshot', 'incremental','cdc'):
            raise ValueError('Choose snapshot, incremental or live CDC mode.')
        if values['mode'] == 'incremental' and not values.get('watermark_column'):
            raise ValueError('Incremental mode requires an integer watermark column.')
        secret = self.cipher.encrypt(values['password'].encode()).decode()
        ident = uuid.uuid4().hex
        with self.workflows.catalog() as con:
            con.execute('INSERT INTO connectors VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (ident, values['name'], values['host'], values['port'], values['database'], values['username'], secret,
                 values['schema'], values['table'], values['mode'], values.get('watermark_column'), None, None, None, now()))
            con.execute('INSERT INTO connector_sync_state(connector_id) VALUES(?)',(ident,))
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
        config['connector_id']=ident
        with self.workflows.catalog() as con:
            state=con.execute('SELECT * FROM connector_sync_state WHERE connector_id=?',(ident,)).fetchone()
        state=dict(state) if state else {}
        config['cdc_state']={**state,'columns':json.loads(state['schema_json']) if state.get('schema_json') else None,
                             'primary_key':json.loads(state['primary_key']) if state.get('primary_key') else None}
        return dict(row), config

    def test(self, ident):
        _, config = self.config(ident)
        try:
            with psycopg.connect(**{key: config[key] for key in ('host','port','dbname','user','password','sslmode')}, connect_timeout=5,
                                 options='-c statement_timeout=5000') as pg:
                pg.execute('SET TRANSACTION READ ONLY')
                tables = pg.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1,2 LIMIT 200").fetchall()
                if config['mode']=='cdc':
                    from app.cdc import describe
                    if int(pg.execute('SHOW server_version_num').fetchone()[0])<150000 or pg.execute('SHOW wal_level').fetchone()[0]!='logical':
                        raise ValueError('CDC requires PostgreSQL 15+ with wal_level=logical.')
                    replication=pg.execute('SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname=current_user').fetchone()[0]
                    metadata=describe(pg,config)
                    owner=pg.execute('SELECT pg_has_role(relowner,\'USAGE\') FROM pg_class WHERE oid=%s',(metadata['relation_oid'],)).fetchone()[0]
                    if not replication or not owner or not pg.execute("SELECT has_database_privilege(current_database(),'CREATE')").fetchone()[0]:
                        raise ValueError('CDC needs replication permission, database CREATE permission and source table ownership.')
            return {'connected': True, 'tables': [{'schema': row[0], 'table': row[1]} for row in tables]}
        except ValueError:
            raise
        except Exception:
            raise ValueError('Cannot connect to the source or read its catalogue. Verify credentials, TLS, allowlist and source permissions.') from None

    def run(self, ident, pipeline_run=None):
        run=uuid.uuid4().hex
        with self.workflows.lock:
            self.config(ident)
            with self.workflows.catalog() as con:
                con.execute('INSERT OR IGNORE INTO connector_sync_state(connector_id) VALUES(?)',(ident,))
                con.execute('UPDATE connector_sync_state SET last_attempt=?,last_error=NULL WHERE connector_id=?',(now(),ident))
                con.execute("INSERT INTO connector_sync_runs(id,connector_id,started_at,status) VALUES(?,?,?,'running')",(run,ident,now()))
            try:
                result=self._run(ident,pipeline_run)
                with self.workflows.catalog() as con:
                    con.execute("UPDATE connector_sync_state SET phase=CASE WHEN phase='new' THEN 'active' ELSE phase END,last_success=?,last_stats=? WHERE connector_id=?",
                                (now(),json.dumps(result.get('stats',{'new_rows':result.get('new_rows',0)})),ident))
                    con.execute("UPDATE connector_sync_runs SET status='succeeded',finished_at=?,stats=? WHERE id=?",
                                (now(),json.dumps({key:value for key,value in result.items() if key!='sha256'}),run))
                return result
            except Exception as exc:
                _,config=self.config(ident)
                error=str(exc).replace(config['password'],'[redacted]')[:1000]
                with self.workflows.catalog() as con:
                    con.execute('UPDATE connector_sync_state SET last_error=? WHERE connector_id=?',(error,ident))
                    con.execute("UPDATE connector_sync_runs SET status='failed',finished_at=?,error=? WHERE id=?",(now(),error,run))
                raise ValueError(error) from None

    def _run(self, ident, pipeline_run=None):
        with self.workflows.lock:
            self.workflows.recover()
            row, config = self.config(ident)
            if row['mode']=='cdc' and config['cdc_state'].get('phase') in ('stopped','stopping'):
                raise ValueError('Live sync is stopped. Reinitialize it explicitly to resume.')
            with tempfile.TemporaryDirectory(prefix='data-platform-connector-') as temporary:
                config_file = Path(temporary) / 'connection.json'
                config_file.write_text(json.dumps(config))
                config_file.chmod(0o600)
                parquet = prepare('postgres-cdc' if row['mode']=='cdc' else 'postgres', config_file, '', self.workflows.database, None, temporary)
                result = json.loads((Path(temporary) / 'worker.json').read_text())
                unchanged=not result.get('changed',True) if row['mode']=='cdc' else result['new_rows']==0
                if unchanged and row['target_id']:
                    if row['mode']=='cdc':
                        with self.workflows.catalog() as con:
                            con.execute('UPDATE connectors SET watermark=? WHERE id=?',(json.dumps(result['watermark']),ident))
                        self._ack(ident,result)
                    return {'id': row['target_id'], 'new_rows': 0, 'unchanged': True,**({'stats':result['stats']} if 'stats' in result else {})}
                inputs = [{'name': f'PostgreSQL {row["source_schema"]}.{row["source_table"]}', 'dataset_id': None,
                           'version_id': None, 'connector_id': ident, 'watermark_before': config['checkpoint'], 'watermark_after': result['watermark']}]
                published = self.workflows.publish(parquet, parquet, row['name'] + '.parquet', target=row['target_id'], kind='postgres-cdc' if row['mode']=='cdc' else 'postgres', inputs=inputs, pipeline_run=pipeline_run,
                    connector_checkpoint={'id': ident, 'schema': result['schema'], 'watermark': result['watermark'],'cdc':result.get('cdc')})
                if row['mode']=='cdc':self._ack(ident,result)
                return {**published, 'new_rows': result['new_rows'],**({'stats':result['stats']} if 'stats' in result else {})}

    def _ack(self,ident,result):
        from app.cdc import control
        _,config=self.config(ident)
        try:
            control(config,config['cdc_state'],'ack')
            phase,error='active',None
        except Exception:
            phase,error='ack_pending','Published locally; source acknowledgement is pending. The next sync will retry it.'
        with self.workflows.catalog() as con:
            con.execute('UPDATE connector_sync_state SET phase=?,last_error=? WHERE connector_id=?',(phase,error,ident))

    def health(self,ident):
        row,config=self.config(ident)
        state=config['cdc_state']
        result={key:state.get(key) for key in ('phase','last_attempt','last_success','last_error','source_commit_at')}
        result['stats']=json.loads(state['last_stats']) if state.get('last_stats') else None
        result['orchestrator']=os.environ.get('DATA_PLATFORM_ORCHESTRATOR','embedded')
        with self.workflows.catalog() as con:
            result['schedules']=[dict(item) for item in con.execute('SELECT id,enabled,interval_seconds,next_run FROM pipelines WHERE connector_id=?',(ident,))]
        if state.get('last_error') and state.get('phase')!='ack_pending':result['phase']='attention'
        if row['mode']=='cdc' and state.get('last_attempt') and state.get('phase') not in ('stopped','stopping'):
            from app.cdc import control
            try:result.update(control(config,state,'health'))
            except Exception as exc:
                result.update(phase='attention',last_error=str(exc).replace(config['password'],'[redacted]')[:1000])
        return result

    def history(self,ident):
        with self.workflows.catalog() as con:
            if not con.execute('SELECT 1 FROM connectors WHERE id=?',(ident,)).fetchone():
                raise LookupError('Connector not found')
            return [{**dict(row),'stats':json.loads(row['stats']) if row['stats'] else None}
                    for row in con.execute('SELECT * FROM connector_sync_runs WHERE connector_id=? ORDER BY started_at DESC LIMIT 100',(ident,))]

    def stop_cdc(self,ident,confirmation):
        with self.workflows.lock:
            with self.workflows.catalog() as con:
                row=con.execute('SELECT name,mode FROM connectors WHERE id=?',(ident,)).fetchone()
                state=con.execute('SELECT phase,last_attempt FROM connector_sync_state WHERE connector_id=?',(ident,)).fetchone()
            if not row:raise LookupError('Connector not found')
            if row['name']!=confirmation:raise ValueError('Connector name does not match.')
            if row['mode']!='cdc':raise ValueError('This connection is not a live CDC source.')
            if not state or not state['last_attempt'] or state['phase']=='stopped':
                with self.workflows.catalog() as con:
                    con.execute("UPDATE connector_sync_state SET phase='stopped' WHERE connector_id=?",(ident,))
                    con.execute('UPDATE pipelines SET enabled=0,next_run=NULL WHERE connector_id=?',(ident,))
                return {'stopped':True}
            row,config=self.config(ident)
            with self.workflows.catalog() as con:
                con.execute("UPDATE connector_sync_state SET phase='stopping' WHERE connector_id=?",(ident,))
                con.execute('UPDATE pipelines SET enabled=0,next_run=NULL WHERE connector_id=?',(ident,))
            from app.cdc import control
            try:control(config,config['cdc_state'],'cleanup')
            except Exception as exc:
                detail='Source cleanup failed. Verify connectivity and replication-object ownership before retrying.'
                with self.workflows.catalog() as con:
                    con.execute('UPDATE connector_sync_state SET last_error=? WHERE connector_id=?',(detail,ident))
                raise ValueError(detail) from exc
            with self.workflows.catalog() as con:
                con.execute("UPDATE connector_sync_state SET phase='stopped' WHERE connector_id=?",(ident,))
            return {'stopped':True}

    def update_password(self,ident,password):
        with self.workflows.lock:
            with self.workflows.catalog() as con:
                if not con.execute('SELECT 1 FROM connectors WHERE id=?',(ident,)).fetchone():
                    raise LookupError('Connector not found')
                con.execute('UPDATE connectors SET credential=? WHERE id=?',(self.cipher.encrypt(password.encode()).decode(),ident))
        return {'updated':True}

    def reset_cdc(self,ident,confirmation):
        with self.workflows.lock:
            self.stop_cdc(ident,confirmation)
            with self.workflows.catalog() as con:
                con.execute('UPDATE connectors SET watermark=NULL,schema_json=NULL WHERE id=?',(ident,))
                con.execute("UPDATE connector_sync_state SET phase='new',last_error=NULL WHERE connector_id=?",(ident,))
            return self.run(ident)
