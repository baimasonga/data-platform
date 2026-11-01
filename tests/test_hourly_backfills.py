import hashlib
import json
import pytest
from fastapi.testclient import TestClient
from app.catalog import connection
from app.main import create_local_app
from app.flows import Backfill,sql_tree
from app.partitions import partition_window,backfill_windows
from test_flows import pipeline,flow,step,finish


@pytest.fixture
def client(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:yield client


@pytest.mark.parametrize('start,end',[
    ('2026-01-01T00:00','2026-01-01T01:00'),
    ('2026-01-01T00:30Z','2026-01-01T01:00Z'),
    ('2026-01-01T00:00:01Z','2026-01-01T01:00Z'),
    ('2026-01-02T00:00Z','2026-01-01T00:00Z'),
    ('2026-01-01T00:00Z','2026-01-03T00:00Z'),
    ('2026-01-01','2026-01-02'),('nonsense timezone','2026-01-01T01:00Z'),
    ('9999-12-31T23:00Z','9999-12-31T23:00Z'),
])
def test_hourly_ranges_require_aligned_aware_bounded_timestamps(start,end):
    with pytest.raises(ValueError):Backfill(revision=1,granularity='hour',start=start,end=end)


def test_utc_windows_handle_offsets_dst_midnight_and_maximum_range():
    body=Backfill(revision=1,granularity='hour',start='2026-11-01T01:00:00-04:00',end='2026-11-01T01:00:00-05:00')
    windows=body.partitions()
    assert len(windows)==2
    assert [w['start'] for w in windows]==['2026-11-01T05:00:00+00:00','2026-11-01T06:00:00+00:00']
    assert windows[0]['end']==windows[1]['start']
    assert len(backfill_windows('2026-01-01T00:00Z','2026-01-02T23:00Z','hour'))==48
    assert partition_window('2026-01-01T23:00Z','hour')['end']=='2026-01-02T00:00:00+00:00'
    assert partition_window('2024-02-29')['end']=='2024-03-01T00:00:00+00:00'
    assert len(Backfill(revision=1,start='2026-01-01',end='2026-01-31').partitions())==31


def test_sql_templates_produce_typed_half_open_utc_windows():
    tree=sql_tree('SELECT {{partition_start}} AS begin_at,{{partition_end}} AS end_at,CAST({{partition_date}} AS DATE) AS day',partition='2026-11-01T01:00-04:00',granularity='hour')
    assert '2026-11-01T05:00:00+00:00' in tree.sql() and '2026-11-01T06:00:00+00:00' in tree.sql()
    assert '2026-11-01' in tree.sql()
    with pytest.raises(ValueError):sql_tree('SELECT {{partition_hour_unknown}}')


def test_hourly_backfills_are_isolated_half_open_and_idempotent(client):
    source=client.post('/api/datasets',files={'file':('events.csv',b'at,amount\n2026-01-01T23:00:00Z,2\n2026-01-01T23:59:59Z,3\n2026-01-02T00:00:00Z,7\n2026-01-02T01:00:00Z,99\n')}).json()
    live=client.post('/api/datasets',files={'file':('live.csv',b'amount\n42\n')}).json()
    saved=pipeline(client,'Hourly',f'SELECT amount,{{{{partition_start}}}} AS window_start FROM "{source["table_name"]}" WHERE CAST("at" AS TIMESTAMPTZ)>={{{{partition_start}}}} AND CAST("at" AS TIMESTAMPTZ)<{{{{partition_end}}}}',target=live['id'])
    next_step=pipeline(client,'Totals','SELECT SUM(amount) AS amount FROM {{steps.hour}}')
    definition=flow(client,[step('hour',saved),step('total',next_step,['hour'])])
    body={'revision':1,'granularity':'hour','start':'2026-01-01T23:00:00Z','end':'2026-01-02T00:00:00Z'}
    response=client.post('/api/flows/'+definition['id']+'/backfill',json=body,headers={'Idempotency-Key':'hours'})
    assert response.status_code==202,response.text
    ids=response.json()['run_ids'];assert len(ids)==2
    equivalent={**body,'start':'2026-01-01T18:00:00-05:00','end':'2026-01-01T19:00:00-05:00'}
    replay=client.post('/api/flows/'+definition['id']+'/backfill',json=equivalent,headers={'Idempotency-Key':'hours'})
    assert replay.status_code==202 and replay.json()['run_ids']==ids
    changed=client.post('/api/flows/'+definition['id']+'/backfill',json={**body,'end':'2026-01-02T01:00:00Z'},headers={'Idempotency-Key':'hours'})
    assert changed.status_code==409
    results=[finish(client,definition,id) for id in ids]
    assert all(r['status']=='succeeded' for r in results),json.dumps(results,indent=2)
    outputs=[]
    for result in results:
        assert result['partition_granularity']=='hour' and result['detached']
        assert result['snapshot']['partition']['start']==result['partition_start']
        assert result['snapshot']['pins'][source['table_name']]['dataset_id']==source['id']
        outputs.append(result['steps'][0]['result']['id'])
    assert len(set(outputs))==2
    totals=[client.post('/api/query',json={'sql':'SELECT amount FROM '+result['steps'][1]['result']['physical_table']}).json()['rows'] for result in results]
    assert totals==[[[5]],[[7]]]
    assert client.get('/api/pipelines').json()[1]['target_id']==live['id']
    assert len(client.get('/api/datasets/'+live['id']+'/versions').json())==1
    assert all(result['partition_key'] in client.get('/api/datasets/'+result['steps'][0]['result']['id']).json()['name'] for result in results)


def test_daily_idempotency_fingerprints_survive_upgrade_and_default_granularity(client):
    first=pipeline(client,'Daily','SELECT CAST({{partition_date}} AS DATE) AS day')
    definition=flow(client,[step('daily',first)])
    legacy={'revision':1,'start':'2026-01-01','end':'2026-01-01'}
    fingerprint=hashlib.sha256(json.dumps({'backfill':True,**legacy},sort_keys=True).encode()).hexdigest()
    with connection(client.app.state.flows.metadata) as con:
        con.execute('INSERT INTO flow_request_keys VALUES(?,?,?,?)',('old-key',definition['id'],fingerprint,json.dumps(['legacy-run'])))
    result=client.post('/api/flows/'+definition['id']+'/backfill',json={**legacy,'granularity':'day'},headers={'Idempotency-Key':'old-key'})
    assert result.status_code==202 and result.json()=={'run_ids':['legacy-run'],'already_queued':True}


def test_hourly_sources_are_rejected_before_any_work_is_queued(client):
    source=client.post('/api/connectors',json={'name':'Source','host':'127.0.0.1','port':5433,'database':'platform','username':'example',
        'password':'synthetic-test-only','schema_name':'public','table':'example','mode':'snapshot'}).json()
    saved=client.post('/api/pipelines',json={'name':'Source pipeline','connector_id':source['id']}).json()['id']
    definition=flow(client,[step('source',saved)])
    response=client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'granularity':'hour','start':'2026-01-01T00:00Z','end':'2026-01-01T01:00Z'})
    assert response.status_code==400 and 'SQL pipelines only' in response.text
    assert client.get('/api/flows/'+definition['id']+'/runs').json()==[]
    assert client.get('/api/connectors/'+source['id']+'/runs').json()==[]


def test_hourly_queue_capacity_rejection_is_atomic(client):
    manager=client.app.state.flows;manager.stop.set();manager.thread.join(3)
    saved=pipeline(client,'Queued','SELECT 1 AS amount')
    first=flow(client,[step('first',saved)])
    second=flow(client,[step('second',saved)])
    initial=client.post('/api/flows/'+first['id']+'/backfill',json={'revision':1,'start':'2026-01-01','end':'2026-01-31'})
    assert initial.status_code==202 and len(initial.json()['run_ids'])==31
    response=client.post('/api/flows/'+second['id']+'/backfill',json={'revision':1,'granularity':'hour','start':'2026-01-01T00:00Z','end':'2026-01-02T23:00Z'},headers={'Idempotency-Key':'queue-full'})
    assert response.status_code==429
    assert client.get('/api/flows/'+second['id']+'/runs').json()==[]
    with connection(manager.metadata) as con:
        assert con.execute('SELECT count(*) FROM flow_runs').fetchone()[0]==31
        assert not con.execute("SELECT 1 FROM flow_request_keys WHERE key='queue-full'").fetchone()


def test_hourly_empty_output_preserves_schema(client):
    saved=pipeline(client,'Empty hour','SELECT CAST(1 AS BIGINT) AS amount WHERE FALSE')
    definition=flow(client,[step('empty',saved)])
    result=client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'granularity':'hour','start':'2026-01-01T00:00Z','end':'2026-01-01T00:00Z'})
    assert result.status_code==202,result.text
    completed=finish(client,definition,result.json()['run_ids'][0])
    assert completed['status']=='succeeded',completed
    output=completed['steps'][0]['result']
    dataset=client.get('/api/datasets/'+output['id']).json()
    assert dataset['row_count']==0 and dataset['profile'][0]['type']=='BIGINT'


def test_hourly_windows_and_frozen_sql_survive_restart_and_retry(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:
        manager=client.app.state.flows;manager.stop.set();manager.thread.join(3)
        saved=pipeline(client,'Frozen hour','SELECT EXTRACT(hour FROM {{partition_start}}) AS hour')
        definition=flow(client,[step('hour',saved)])
        response=client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'granularity':'hour','start':'2026-01-01T23:00Z','end':'2026-01-01T23:00Z'})
        assert response.status_code==202,response.text
        ident=response.json()['run_ids'][0]
        assert client.put('/api/pipelines/'+saved,json={'name':'Changed','sql':'SELECT 99 AS hour'}).status_code==200
    with TestClient(create_local_app(tmp_path)) as client:
        prior=client.get('/api/flows/'+definition['id']+'/runs/'+ident).json()
        assert prior['status']=='interrupted' and prior['partition_start']=='2026-01-01T23:00:00+00:00'
        assert prior['snapshot']['partition']['end']=='2026-01-02T00:00:00+00:00'
        assert client.post('/api/flows/'+definition['id']+'/runs/'+ident+'/retry').status_code==200
        result=finish(client,definition,ident)
        assert result['status']=='succeeded' and result['attempt']==2,result
        physical=result['steps'][0]['result']['physical_table']
        assert client.post('/api/query',json={'sql':'SELECT * FROM '+physical}).json()['rows']==[[23]]


def test_pre_hourly_daily_runs_migrate_and_retry_without_changed_windows(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:
        manager=client.app.state.flows;manager.stop.set();manager.thread.join(3)
        metadata=manager.metadata
        saved=pipeline(client,'Legacy day','SELECT CAST({{partition_date}} AS DATE) AS day')
        definition=flow(client,[step('day',saved)])
        response=client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'start':'2026-01-31','end':'2026-01-31'})
        ident=response.json()['run_ids'][0]
    # Reproduce the version-12 daily schema and snapshot, then open normally.
    with connection(metadata) as con:
        snapshot=json.loads(con.execute('SELECT snapshot FROM flow_runs WHERE id=?',(ident,)).fetchone()[0]);snapshot.pop('partition')
        con.execute('UPDATE flow_runs SET snapshot=? WHERE id=?',(json.dumps(snapshot),ident))
        for column in ('partition_granularity','partition_start','partition_end'):con.execute('ALTER TABLE flow_runs DROP COLUMN '+column)
        con.execute('DELETE FROM schema_migrations WHERE version=13')
    with TestClient(create_local_app(tmp_path)) as client:
        prior=client.get('/api/flows/'+definition['id']+'/runs/'+ident).json()
        assert prior['partition_granularity']=='day' and prior['partition_key']=='2026-01-31'
        assert prior['partition_start']=='2026-01-31T00:00:00+00:00' and prior['partition_end']=='2026-02-01T00:00:00+00:00'
        assert client.post('/api/flows/'+definition['id']+'/runs/'+ident+'/retry').status_code==200
        result=finish(client,definition,ident)
        assert result['status']=='succeeded',result
        physical=result['steps'][0]['result']['physical_table']
        assert client.post('/api/query',json={'sql':'SELECT * FROM '+physical}).json()['rows']==[['2026-01-31']]
