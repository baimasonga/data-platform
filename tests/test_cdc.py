"""Protocol regression tests and opt-in real PostgreSQL logical-replication checks."""
import json
import os
from pathlib import Path
import struct
import uuid
import os
import stat

import pytest
import psycopg
from psycopg import sql
from fastapi.testclient import TestClient

from app.cdc import decode, lsn, lsn_text
from app.main import create_local_app


@pytest.mark.parametrize('mode', ['snapshot', 'cdc'])
def test_connector_worker_credentials_are_private_at_creation(tmp_path, monkeypatch, mode):
    import app.connectors as connectors
    actual_open = os.open
    created = []
    def checked_open(path, flags, mode_bits=0o777, *args, **kwargs):
        descriptor = actual_open(path, flags, mode_bits, *args, **kwargs)
        if str(path).endswith('/connection.json'):
            created.append((flags, mode_bits, stat.S_IMODE(os.fstat(descriptor).st_mode)))
        return descriptor
    def inspect_worker(kind, config_file, *args):
        assert kind == ('postgres-cdc' if mode == 'cdc' else 'postgres')
        assert stat.S_IMODE(config_file.stat().st_mode) == 0o600
        assert json.loads(config_file.read_text())['password'] == 'test-secret'
        raise ValueError('synthetic worker failure')
    monkeypatch.setattr(connectors.os, 'open', checked_open)
    monkeypatch.setattr(connectors, 'prepare', inspect_worker)
    with TestClient(create_local_app(tmp_path)) as client:
        source = client.post('/api/connectors', json={'name': 'private', 'host': '127.0.0.1', 'port': 5433,
            'database': 'platform', 'username': 'platform', 'password': 'test-secret',
            'schema_name': 'public', 'table': 'records', 'mode': mode}).json()
        response = client.post('/api/connectors/' + source['id'] + '/import')
        assert response.status_code == 400
    assert len(created) == 1
    flags, requested, actual = created[0]
    assert flags & os.O_EXCL and requested == actual == 0o600


def message(tag,*parts):return tag.encode()+b''.join(parts)
def uint(value,size=4):return value.to_bytes(size,'big')
def string(value):return value.encode()+b'\0'
def tuple_data(values):
    return uint(len(values),2)+b''.join(b'n' if value is None else b't'+uint(len(str(value).encode()))+str(value).encode() for value in values)


def protocol_config():
    return {'checkpoint':'0/10','relation_oid':42,'schema':'public','table':'rows',
            'columns':[{'name':'id','oid':20,'typmod':-1,'key':True},{'name':'value','oid':25,'typmod':-1,'key':False}]}


def protocol_rows():
    return [message('B',uint(32,8),uint(0,8),uint(7)),
        message('R',uint(42),string('public'),string('rows'),b'd',uint(2,2),
                b'\1',string('id'),uint(20),struct.pack('!i',-1),b'\0',string('value'),uint(25),struct.pack('!i',-1)),
        message('I',uint(42),b'N',tuple_data(['1','hello'])),
        message('C',b'\0',uint(32,8),uint(40,8),uint(0,8))]


def test_pgoutput_commit_boundaries_replay_and_schema_guards():
    rows=protocol_rows()
    events,checkpoint,stamp=decode(rows,protocol_config())
    assert events==[('I',None,['1','hello'])] and checkpoint=='0/28'
    assert stamp=='2000-01-01T00:00:00+00:00'
    assert decode(rows,{**protocol_config(),'checkpoint':'0/28'})[0]==[]
    with pytest.raises(ValueError,match='inside a transaction'):decode(rows[:-1],protocol_config())
    with pytest.raises(ValueError,match='schema changed'):decode(rows,{**protocol_config(),'relation_oid':99})
    with pytest.raises(ValueError,match='Truncated'):decode([b'B\0'],protocol_config())
    assert lsn_text(lsn('A/FFFFFFFF'))=='A/FFFFFFFF'


def test_unstarted_cdc_connection_can_be_deleted_without_contacting_source(tmp_path,monkeypatch):
    with TestClient(create_local_app(tmp_path)) as client:
        connector=client.post('/api/connectors',json={'name':'Not yet started','host':'127.0.0.1','port':5433,'database':'platform',
            'username':'example','password':'unavailable','schema_name':'public','table':'records','mode':'cdc'}).json()
        monkeypatch.setenv('DATA_PLATFORM_CONNECTOR_HOSTS','127.0.0.2')
        assert client.request('DELETE','/api/connectors/'+connector['id'],json={'confirmation_name':'Not yet started'}).status_code==200


@pytest.fixture
def live(tmp_path):
    if os.environ.get('DATA_PLATFORM_TEST_CDC')!='1':
        pytest.skip('Set DATA_PLATFORM_TEST_CDC=1 with the self-hosted PostgreSQL service for live CDC tests.')
    values=dict(line.split('=',1) for line in (Path(__file__).resolve().parents[1]/'deploy/runtime/stack.env').read_text().splitlines() if line)
    config=dict(host='127.0.0.1',port=5433,dbname='platform',user='platform',password=values['POSTGRES_PASSWORD'])
    schema='cdc_test_'+uuid.uuid4().hex[:12]
    with psycopg.connect(**config,autocommit=True) as pg:
        pg.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        pg.execute(sql.SQL('CREATE TABLE {}(id BIGINT,label TEXT,amount NUMERIC(12,2),PRIMARY KEY(id) INCLUDE(label))').format(sql.Identifier(schema,'records')))
        pg.execute(sql.SQL("INSERT INTO {} VALUES(1,'one',10.25),(2,'two',20.50)").format(sql.Identifier(schema,'records')))
        app=create_local_app(tmp_path)
        with TestClient(app) as client:
            connector=client.post('/api/connectors',json={'name':'Live records','host':'127.0.0.1','port':5433,'database':'platform',
                'username':'platform','password':values['POSTGRES_PASSWORD'],'schema_name':schema,'table':'records','mode':'cdc'})
            assert connector.status_code==201,connector.text
            ident=connector.json()['id']
            try:yield client,pg,schema,ident,tmp_path,config
            finally:
                # Clean only this fixture's exact objects, including after a failed bootstrap.
                from app.cdc import names
                name,pub=names(ident)
                if pg.execute('SELECT 1 FROM pg_replication_slots WHERE slot_name=%s',(name,)).fetchone():
                    pg.execute('SELECT pg_drop_replication_slot(%s)',(name,))
                pg.execute(sql.SQL('DROP PUBLICATION IF EXISTS {}').format(sql.Identifier(pub)))
                pg.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))


def sync(client,ident):
    result=client.post('/api/connectors/'+ident+'/import')
    assert result.status_code==200,result.text
    return result.json()


def query(client,table):
    result=client.post('/api/query',json={'sql':'SELECT * FROM "'+table+'" ORDER BY id'})
    assert result.status_code==200,result.text
    return result.json()['rows']


def test_live_cdc_snapshot_updates_deletes_keys_truncate_and_noop(live):
    client,pg,schema,ident,_,_=live
    assert client.post('/api/connectors/'+ident+'/test').status_code==200
    first=sync(client,ident)
    assert first['row_count']==2 and first['stats']['bootstrap']
    table=first['table_name']
    pg.execute(sql.SQL("UPDATE {} SET label='changed',amount=12.75 WHERE id=1").format(sql.Identifier(schema,'records')))
    pg.execute(sql.SQL('DELETE FROM {} WHERE id=2').format(sql.Identifier(schema,'records')))
    pg.execute(sql.SQL("INSERT INTO {} VALUES(3,'three',30.25)").format(sql.Identifier(schema,'records')))
    changed=sync(client,ident)
    assert changed['stats']=={'inserted':1,'updated':1,'deleted':1,'truncated':0,'bootstrap':False}
    assert query(client,table)==[[1,'changed','12.75'],[3,'three','30.25']]
    assert sync(client,ident)['unchanged']
    assert len(client.get('/api/datasets/'+first['id']+'/versions').json())==2
    pg.execute(sql.SQL('UPDATE {} SET id=4 WHERE id=3').format(sql.Identifier(schema,'records')))
    sync(client,ident)
    assert query(client,table)[1][0]==4
    pg.execute(sql.SQL('TRUNCATE {}').format(sql.Identifier(schema,'records')))
    emptied=sync(client,ident)
    assert emptied['row_count']==0 and query(client,table)==[]
    assert client.post('/api/datasets/'+first['id']+'/quality/run').status_code==200
    pg.execute(sql.SQL("INSERT INTO {} VALUES(5,'after truncate',5)").format(sql.Identifier(schema,'records')))
    sync(client,ident)
    assert query(client,table)==[[5,'after truncate','5.00']]
    health=client.get('/api/connectors/'+ident+'/health').json()
    assert health['phase']=='active' and health['last_success'] and health['slot_status']!='lost'
    assert all(run['status']=='succeeded' for run in client.get('/api/connectors/'+ident+'/runs').json())


def test_live_quality_gate_keeps_local_and_source_checkpoints_and_retries(live):
    client,pg,schema,ident,_,_=live
    first=sync(client,ident)
    before=client.get('/api/connectors').json()[0]['watermark']
    assert client.put('/api/datasets/'+first['id']+'/quality',json={'rules':[{'column_name':'amount','rule':'max','threshold':40}]}).status_code==200
    pg.execute(sql.SQL('UPDATE {} SET amount=90 WHERE id=1').format(sql.Identifier(schema,'records')))
    failed=client.post('/api/connectors/'+ident+'/import')
    assert failed.status_code==400 and 'Quality contract failed' in failed.text
    assert client.get('/api/connectors').json()[0]['watermark']==before
    from app.cdc import names
    remote=pg.execute('SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s',(names(ident)[0],)).fetchone()[0]
    assert remote==json.loads(before)
    assert query(client,first['table_name'])[0][2]=='10.25'
    assert client.get('/api/connectors/'+ident+'/runs').json()[0]['status']=='failed'
    assert client.put('/api/datasets/'+first['id']+'/quality',json={'rules':[]}).status_code==200
    sync(client,ident)
    assert query(client,first['table_name'])[0][2]=='90.00'
    assert len(client.get('/api/datasets/'+first['id']+'/versions').json())==2


def test_live_ack_failure_recovers_after_restart_without_duplicate_version(live,monkeypatch):
    client,pg,schema,ident,directory,_=live
    first=sync(client,ident)
    pg.execute(sql.SQL("UPDATE {} SET label='after restart' WHERE id=1").format(sql.Identifier(schema,'records')))
    import app.cdc as cdc
    real_control=cdc.control
    def unavailable(config,state,action):
        if action=='ack':raise OSError('Simulated unavailable source acknowledgement')
        return real_control(config,state,action)
    monkeypatch.setattr(cdc,'control',unavailable)
    assert sync(client,ident)['version']==2
    assert client.get('/api/connectors').json()[0]['phase']=='ack_pending'
    monkeypatch.setattr(cdc,'control',real_control)
    client.__exit__(None,None,None)
    with TestClient(create_local_app(directory)) as restarted:
        assert sync(restarted,ident)['unchanged']
        assert restarted.get('/api/connectors').json()[0]['phase']=='active'
        assert len(restarted.get('/api/datasets/'+first['id']+'/versions').json())==2
        assert query(restarted,first['table_name'])[0][1]=='after restart'


def test_live_transactions_crossing_idle_checkpoints_and_toast_values(live):
    client,pg,schema,ident,_,config=live
    long_text='large payload '+('abc123'*3000)
    pg.execute(sql.SQL('ALTER TABLE {} ADD COLUMN detail TEXT').format(sql.Identifier(schema,'records')))
    pg.execute(sql.SQL('ALTER TABLE {} ALTER COLUMN detail SET STORAGE EXTERNAL').format(sql.Identifier(schema,'records')))
    pg.execute(sql.SQL('UPDATE {} SET detail=%s').format(sql.Identifier(schema,'records')),(long_text,))
    first=sync(client,ident)
    with psycopg.connect(**config) as pending:
        pending.execute(sql.SQL("UPDATE {} SET label='transaction held' WHERE id=1").format(sql.Identifier(schema,'records')))
        assert sync(client,ident)['unchanged']
        pending.commit()
    sync(client,ident)
    rows=query(client,first['table_name'])
    assert rows[0][1]=='transaction held' and rows[0][3]==long_text


def test_live_schema_loss_explicit_reset_and_resource_cleanup(live):
    client,pg,schema,ident,_,_=live
    first=sync(client,ident)
    pg.execute(sql.SQL('ALTER TABLE {} ADD COLUMN extra TEXT').format(sql.Identifier(schema,'records')))
    failed=client.post('/api/connectors/'+ident+'/import')
    assert failed.status_code==400 and 'schema changed' in failed.text
    reset=client.post('/api/connectors/'+ident+'/reinitialize',json={'confirmation_name':'Live records'})
    assert reset.status_code==200,reset.text
    assert reset.json()['version']==2
    from app.cdc import names
    pg.execute('SELECT pg_drop_replication_slot(%s)',(names(ident)[0],))
    assert client.post('/api/connectors/'+ident+'/import').status_code==400
    assert client.get('/api/datasets/'+first['id']).json()['row_count']==2
    reset=client.post('/api/connectors/'+ident+'/reinitialize',json={'confirmation_name':'Live records'})
    assert reset.status_code==200,reset.text
    from app.catalog import connection
    with connection(client.app.state.pipelines.metadata) as con:
        con.execute('UPDATE connector_sync_state SET timeline=timeline+1 WHERE connector_id=?',(ident,))
    assert client.get('/api/connectors/'+ident+'/health').json()['phase']=='attention'
    assert client.post('/api/connectors/'+ident+'/import').status_code==400
    assert client.post('/api/connectors/'+ident+'/stop',json={'confirmation_name':'wrong'}).status_code==400
    stopped=client.post('/api/connectors/'+ident+'/stop',json={'confirmation_name':'Live records'})
    assert stopped.status_code==200,stopped.text
    assert client.post('/api/connectors/'+ident+'/import').status_code==400
    assert not pg.execute('SELECT 1 FROM pg_replication_slots WHERE slot_name=%s',(names(ident)[0],)).fetchone()
    assert not pg.execute('SELECT 1 FROM pg_publication WHERE pubname=%s',(names(ident)[1],)).fetchone()
    assert client.request('DELETE','/api/connectors/'+ident,json={'confirmation_name':'Live records'}).status_code==200


def test_live_source_pipeline_idempotency_scheduling_and_dashboard_refresh(live):
    client,pg,schema,ident,_,_=live
    first=sync(client,ident)
    pipeline=client.post('/api/pipelines',json={'name':'Automatic live data','connector_id':ident,'enabled':True,'interval_seconds':60}).json()
    metric=client.post('/api/documents',json={'kind':'metric','name':'Total','sql':'SELECT sum(amount) AS total FROM '+first['table_name'],'value_column':'total'}).json()
    endpoint='/api/documents/'+metric['id']+'/items/0/run'
    assert client.post(endpoint,json={'revision':1}).json()['value']=='30.75'
    pg.execute(sql.SQL('DELETE FROM {} WHERE id=2').format(sql.Identifier(schema,'records')))
    key=uuid.uuid4().hex
    run=client.post('/api/pipelines/'+pipeline['id']+'/run',headers={'Idempotency-Key':key})
    assert run.status_code==200,run.text
    assert client.post('/api/pipelines/'+pipeline['id']+'/run',headers={'Idempotency-Key':key}).json()['already_published']
    assert client.post(endpoint,json={'revision':1}).json()['value']=='10.25'
    pg.execute(sql.SQL("INSERT INTO {} VALUES(9,'scheduled',5)").format(sql.Identifier(schema,'records')))
    from app.catalog import connection
    with connection(client.app.state.pipelines.metadata) as con:
        con.execute("UPDATE pipelines SET next_run='2000-01-01T00:00:00+00:00' WHERE id=?",(pipeline['id'],))
    client.app.state.pipelines.tick()
    assert len(query(client,first['table_name']))==2
    assert client.post('/api/connectors/'+ident+'/stop',json={'confirmation_name':'Live records'}).status_code==200
    assert client.get('/api/pipelines').json()[0]['enabled']==0


def test_live_empty_bootstrap_and_managed_dataset_guard(live):
    client,pg,schema,ident,_,_=live
    pg.execute(sql.SQL('TRUNCATE {}').format(sql.Identifier(schema,'records')))
    first=sync(client,ident)
    assert first['row_count']==0
    replacement=client.post('/api/datasets/'+first['id']+'/versions',files={'file':('replacement.csv',b'id,label,amount\n1,manual,1\n','text/csv')})
    assert replacement.status_code==400 and 'managed by a live CDC' in replacement.text
    assert client.get('/api/datasets/'+first['id']+'/export').status_code==200


def test_live_publication_filters_and_credential_rotation_fail_closed(live):
    client,pg,schema,ident,_,config=live
    first=sync(client,ident)
    from app.cdc import names
    pg.execute(sql.SQL('ALTER PUBLICATION {} SET TABLE {} WHERE (id > 1)').format(sql.Identifier(names(ident)[1]),sql.Identifier(schema,'records')))
    failed=client.post('/api/connectors/'+ident+'/import')
    assert failed.status_code==400 and 'row filter changed' in failed.text
    assert client.post('/api/connectors/'+ident+'/reinitialize',json={'confirmation_name':'Live records'}).status_code==200
    assert client.put('/api/connectors/'+ident+'/credentials',json={'password':'incorrect-test-password'}).status_code==200
    assert client.post('/api/connectors/'+ident+'/import').status_code==400
    assert client.put('/api/connectors/'+ident+'/credentials',json={'password':config['password']}).status_code==200
    assert sync(client,ident)['unchanged']
    assert 'credential' not in client.get('/api/connectors').json()[0]
    assert 'password' not in client.get('/api/connectors').text


def test_live_rejected_schema_and_transaction_rollback_do_not_publish(live):
    client,pg,schema,ident,_,config=live
    first=sync(client,ident)
    with psycopg.connect(**config) as transaction:
        transaction.execute(sql.SQL("UPDATE {} SET label='rolled back' WHERE id=1").format(sql.Identifier(schema,'records')))
        transaction.rollback()
    assert sync(client,ident)['unchanged']
    assert query(client,first['table_name'])[0][1]=='one'
    pg.execute(sql.SQL('ALTER TABLE {} ADD COLUMN generated BIGINT GENERATED ALWAYS AS (id*2) STORED').format(sql.Identifier(schema,'records')))
    failed=client.post('/api/connectors/'+ident+'/import')
    assert failed.status_code==400 and 'generated columns' in failed.text
    assert len(client.get('/api/datasets/'+first['id']+'/versions').json())==1
    pg.execute(sql.SQL('CREATE UNLOGGED TABLE {}(id BIGINT PRIMARY KEY)').format(sql.Identifier(schema,'unlogged_records')))
    rejected=client.post('/api/connectors',json={'name':'Unlogged source','host':'127.0.0.1','port':5433,'database':'platform',
        'username':'platform','password':config['password'],'schema_name':schema,'table':'unlogged_records','mode':'cdc'}).json()
    assert client.post('/api/connectors/'+rejected['id']+'/test').status_code==400
    assert client.post('/api/connectors/'+rejected['id']+'/import').status_code==400


def test_live_snapshot_has_no_gap_while_source_changes(live):
    import threading
    import time
    client,pg,schema,ident,_,_=live
    pg.execute(sql.SQL("INSERT INTO {} SELECT i,'initial',i FROM generate_series(3,500) i").format(sql.Identifier(schema,'records')))
    response=[]
    thread=threading.Thread(target=lambda:response.append(client.post('/api/connectors/'+ident+'/import')))
    thread.start()
    from app.cdc import names
    deadline=time.monotonic()+15
    while time.monotonic()<deadline:
        active=pg.execute('SELECT active FROM pg_replication_slots WHERE slot_name=%s',(names(ident)[0],)).fetchone()
        if active and active[0]:break
        time.sleep(0.005)
    else:
        thread.join()
        pytest.fail('Exported snapshot was not observed while bootstrap was active.')
    with pg.transaction():
        pg.execute(sql.SQL("UPDATE {} SET label='concurrent' WHERE id=1").format(sql.Identifier(schema,'records')))
        pg.execute(sql.SQL('DELETE FROM {} WHERE id=2').format(sql.Identifier(schema,'records')))
        pg.execute(sql.SQL("INSERT INTO {} VALUES(999,'new during bootstrap',9)").format(sql.Identifier(schema,'records')))
    thread.join(40)
    assert not thread.is_alive()
    assert response[0].status_code==200,response[0].text
    first=response[0].json()
    assert query(client,first['table_name'])[0][1]=='one'
    changed=sync(client,ident)
    assert changed['stats']['inserted']==1 and changed['stats']['updated']==1 and changed['stats']['deleted']==1
    rows=query(client,first['table_name'])
    assert len(rows)==500 and rows[0][1]=='concurrent' and rows[1][0]==3 and rows[-1][0]==999


def test_live_small_decoding_batches_do_not_ack_unread_transactions(live,monkeypatch,tmp_path):
    import threading
    from app.connectors import Connectors
    from app.workflows import Workflows
    import app.cdc as cdc
    import duckdb
    client,pg,schema,ident,directory,_=live
    first=sync(client,ident)
    for number in (3,4,5):
        pg.execute(sql.SQL("INSERT INTO {} VALUES(%s,'batch',1)").format(sql.Identifier(schema,'records')),(number,))
    row,config=Connectors(Workflows(directory,threading.RLock())).config(ident)
    monkeypatch.setattr(cdc,'MAX_MESSAGES',3)
    output=tmp_path/'small-batch.parquet'
    captured=cdc.capture(config,str(directory/'datasets.duckdb'),output)
    assert captured['stats']['inserted']==1
    with duckdb.connect() as storage:
        assert storage.execute('SELECT count(*) FROM read_parquet(?)',[str(output)]).fetchone()[0]==3
    assert cdc.lsn(captured['watermark'])<cdc.lsn(pg.execute('SELECT pg_current_wal_flush_lsn()::text').fetchone()[0])
    # A non-consuming peek leaves every transaction available to the normal worker.
    sync(client,ident)
    assert len(query(client,first['table_name']))==5


def test_live_source_workflow_preserves_noop_version_and_transforms_its_output(live):
    import time
    client,pg,schema,ident,_,_=live
    source=client.post('/api/pipelines',json={'name':'Workflow source','connector_id':ident}).json()['id']
    transform=client.post('/api/pipelines',json={'name':'Workflow transform','sql':'SELECT id,label,amount*2 AS amount FROM {{steps.ingest}}'}).json()['id']
    flow=client.post('/api/flows',json={'name':'Source workflow','steps':[
        {'id':'ingest','name':'Ingest','kind':'pipeline','pipeline_id':source},
        {'id':'transform','name':'Transform','kind':'pipeline','pipeline_id':transform,'depends_on':['ingest']},
        {'id':'check','name':'Check','kind':'quality','input_step':'transform','depends_on':['transform'],
         'rules':[{'column_name':'amount','rule':'max','threshold':50}]}]}).json()
    versions=[]
    for _ in range(2):
        response=client.post('/api/flows/'+flow['id']+'/run',json={'revision':1})
        assert response.status_code==202,response.text
        run=response.json()['run_ids'][0];deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            result=client.get('/api/flows/'+flow['id']+'/runs/'+run).json()
            if result['status'] not in ('queued','running'):break
            time.sleep(.05)
        assert result['status']=='succeeded',result
        versions.append(result['steps'][0]['result']['version_id'])
    assert versions[0]==versions[1]
    output=result['steps'][1]['result']
    assert query(client,output['table_name'])==[[1,'one','20.50'],[2,'two','41.00']]
