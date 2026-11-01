import time
from contextlib import contextmanager
import pytest
from fastapi.testclient import TestClient
from app.main import create_local_app
from app.catalog import connection


@pytest.fixture
def client(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:yield client


def pipeline(client,name,sql,target=None):
    response=client.post('/api/pipelines',json={'name':name,'sql':sql,'target_id':target})
    assert response.status_code==201,response.text
    return response.json()['id']


def flow(client,steps,**options):
    result=client.post('/api/flows',json={'name':'Workflow','steps':steps,**options})
    assert result.status_code==201,result.text
    return result.json()


def step(id,pipeline,dependencies=None):
    return {'id':id,'name':id,'kind':'pipeline','pipeline_id':pipeline,'depends_on':dependencies or []}


def finish(client,definition,run):
    deadline=time.monotonic()+45
    while time.monotonic()<deadline:
        result=client.get('/api/flows/'+definition['id']+'/runs/'+run)
        assert result.status_code==200,result.text
        data=result.json()
        if data['status'] not in ('queued','running'):return data
        time.sleep(.05)
    pytest.fail('Workflow failed to finish within its test budget')


def run(client,definition,**options):
    result=client.post('/api/flows/'+definition['id']+'/run',json={'revision':definition['revision'],**options})
    assert result.status_code==202,result.text
    return finish(client,definition,result.json()['run_ids'][0])


def test_dependencies_quality_and_pinned_outputs(client):
    first=pipeline(client,'First','SELECT 2 AS amount')
    second=pipeline(client,'Second','SELECT amount*3 AS amount FROM {{steps.first}}')
    definition=flow(client,[step('first',first),step('second',second,['first']),
        {'id':'check','name':'Check','kind':'quality','input_step':'second','depends_on':['second'],
         'rules':[{'column_name':'amount','rule':'min','threshold':5}]}])
    result=run(client,definition)
    assert result['status']=='succeeded',result
    assert all(s['status']=='succeeded' for s in result['steps'])
    output=result['steps'][1]['result']
    rows=client.post('/api/query',json={'sql':'SELECT * FROM "'+output['physical_table']+'"'}).json()['rows']
    assert rows==[[6]]
    assert result['steps'][2]['result']['quality_results'][0]['passed']
    assert client.request('DELETE','/api/pipelines/'+first,json={'confirmation_name':'First'}).status_code==409


def test_failure_blocks_dependents_but_independent_branch_runs(client):
    first=pipeline(client,'First','SELECT 2 AS amount')
    independent=pipeline(client,'Independent','SELECT 42 AS independent')
    dependent=pipeline(client,'Dependent','SELECT amount FROM {{steps.check}}')
    definition=flow(client,[step('first',first),{'id':'check','name':'Check','kind':'quality','input_step':'first','depends_on':['first'],
        'rules':[{'column_name':'amount','rule':'min','threshold':100}]},step('last',dependent,['check']),step('independent',independent)])
    result=run(client,definition)
    assert result['status']=='failed'
    assert {s['step_id']:s['status'] for s in result['steps']}=={'first':'succeeded','check':'failed','last':'blocked','independent':'succeeded'}
    count=len(client.get('/api/datasets').json())
    response=client.post('/api/flows/'+definition['id']+'/runs/'+result['id']+'/retry')
    assert response.status_code==200,response.text
    retried=finish(client,definition,result['id'])
    assert retried['attempt']==2 and retried['status']=='failed'
    assert len(client.get('/api/datasets').json())==count


def test_graph_validation_and_revision_guards(client):
    first=pipeline(client,'First','SELECT 1 AS a')
    for steps in ([step('a',first,['a'])],[step('a',first,['b']),step('b',first,['a'])],[step('a',first,['missing'])]):
        assert client.post('/api/flows',json={'name':'Bad','steps':steps}).status_code==422
    definition=flow(client,[step('a',first)])
    assert client.put('/api/flows/'+definition['id'],json={**definition,'revision':0}).status_code==409
    assert client.post('/api/flows/'+definition['id']+'/run',json={'revision':9}).status_code==409
    assert client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'start':'2026-01-01','end':'2026-03-01'}).status_code==422


def test_backfill_outputs_are_separate_and_do_not_retarget_live_pipeline(client):
    source=client.post('/api/datasets',files={'file':('dates.csv',b'day,amount\n2026-01-01,5\n2026-01-02,8\n')}).json()
    sql='SELECT amount FROM '+source['table_name']+' WHERE day=CAST({{partition_date}} AS DATE)'
    live=client.post('/api/datasets',files={'file':('live.csv',b'amount\n42\n')}).json()
    saved=pipeline(client,'Daily',sql,target=live['id'])
    definition=flow(client,[step('daily',saved)])
    response=client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'start':'2026-01-01','end':'2026-01-02'},headers={'Idempotency-Key':'dated-backfill'})
    assert response.status_code==202,response.text
    ids=response.json()['run_ids']
    assert client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'start':'2026-01-01','end':'2026-01-02'},headers={'Idempotency-Key':'dated-backfill'}).json()['run_ids']==ids
    results=[finish(client,definition,id) for id in ids]
    assert all(r['status']=='succeeded' for r in results),results
    assert len({r['steps'][0]['result']['id'] for r in results})==2
    assert client.get('/api/pipelines').json()[0]['target_id']==live['id']
    assert len(client.get('/api/datasets/'+live['id']+'/versions').json())==1
    assert client.request('DELETE','/api/datasets/'+source['id'],json={'confirmation_name':'dates.csv'}).status_code==409


def test_retry_uses_frozen_pipeline_sql_target_and_skips_completed_steps(client,monkeypatch):
    first=pipeline(client,'First','SELECT 1 AS amount')
    second=pipeline(client,'Second','SELECT amount+1 AS amount FROM {{steps.first}}')
    # Use the real coordinator callback with a transient failure at the second pipeline.
    definition=flow(client,[step('first',first),step('second',second,['first'])])
    original=client.app.state.pipelines.execute;failed=[]
    def flaky(name,sql,target,run,connector):
        if name=='Second' and not failed:failed.append(True);raise ValueError('Temporary engine unavailable')
        return original(name,sql,target,run,connector)
    monkeypatch.setattr(client.app.state.pipelines,'execute',flaky)
    result=run(client,definition)
    assert result['status']=='failed'
    first_output=result['steps'][0]['result']
    assert client.put('/api/pipelines/'+second,json={'name':'Edited','sql':'SELECT 999 AS amount'}).status_code==200
    assert client.post('/api/flows/'+definition['id']+'/runs/'+result['id']+'/retry').status_code==200
    result=finish(client,definition,result['id'])
    assert result['status']=='succeeded',result
    assert result['steps'][0]['result']==first_output
    output=result['steps'][1]['result']
    assert client.post('/api/query',json={'sql':'SELECT * FROM '+output['physical_table']}).json()['rows']==[[2]]


def test_cancelled_queue_and_restart_resume(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:
        client.app.state.flows.stop.set();client.app.state.flows.thread.join(3)
        first=pipeline(client,'First','SELECT 1 AS amount')
        definition=flow(client,[step('first',first)])
        response=client.post('/api/flows/'+definition['id']+'/run',json={'revision':1})
        id=response.json()['run_ids'][0]
        assert client.put('/api/flows/'+definition['id'],json=definition).status_code==409
    with TestClient(create_local_app(tmp_path)) as client:
        assert client.get('/api/flows/'+definition['id']+'/runs/'+id).json()['status']=='interrupted'
        assert client.post('/api/flows/'+definition['id']+'/runs/'+id+'/retry').status_code==200
        assert finish(client,definition,id)['status']=='succeeded'


def test_running_cancellation_is_responsive_and_stops_between_steps(client,monkeypatch):
    import threading
    entered=threading.Event();release=threading.Event()
    first=pipeline(client,'Blocking','SELECT 1 AS a');second=pipeline(client,'Next','SELECT 2 AS b')
    definition=flow(client,[step('first',first),step('second',second,['first'])],enabled=True,interval_seconds=60)
    original=client.app.state.pipelines.execute
    def wait(name,*args):
        entered.set();assert release.wait(10)
        return original(name,*args)
    monkeypatch.setattr(client.app.state.pipelines,'execute',wait)
    response=client.post('/api/flows/'+definition['id']+'/run',json={'revision':1});id=response.json()['run_ids'][0]
    try:
        assert entered.wait(10)
        assert client.get('/api/flows/'+definition['id']+'/runs/'+id).json()['status']=='running'
        paused=client.post('/api/flows/'+definition['id']+'/pause',json={'revision':1})
        assert paused.status_code==200 and not paused.json()['enabled'] and paused.json()['next_run'] is None
        assert client.post('/api/flows/'+definition['id']+'/runs/'+id+'/cancel').status_code==200
    finally:release.set()
    result=finish(client,definition,id)
    assert result['status']=='cancelled'
    assert [s['status'] for s in result['steps']]==['succeeded','cancelled']


def test_start_input_versions_are_pinned_even_if_dataset_changes_while_queued(client):
    client.app.state.flows.stop.set();client.app.state.flows.thread.join(3)
    source=client.post('/api/datasets',files={'file':('input.csv',b'amount\n4\n')}).json()
    saved=pipeline(client,'Pinned','SELECT '+source['table_name']+'.amount FROM '+source['table_name'])
    definition=flow(client,[step('first',saved)])
    id=client.post('/api/flows/'+definition['id']+'/run',json={'revision':1}).json()['run_ids'][0]
    assert client.post('/api/datasets/'+source['id']+'/versions',files={'file':('input.csv',b'amount\n99\n')}).status_code==201
    client.app.state.flows.start()
    result=finish(client,definition,id)
    assert result['status']=='succeeded',result
    output=result['steps'][0]['result']
    assert client.post('/api/query',json={'sql':'SELECT * FROM '+output['physical_table']}).json()['rows']==[[4]]


def test_child_publication_success_is_reused_if_coordinator_loses_result(client,monkeypatch):
    saved=pipeline(client,'Accepted','SELECT 7 AS amount');definition=flow(client,[step('first',saved)])
    original=client.app.state.pipelines.execute;lost=[]
    def after_publication(*args):
        result=original(*args)
        if not lost:lost.append(True);raise OSError('Coordinator lost response after acceptance')
        return result
    monkeypatch.setattr(client.app.state.pipelines,'execute',after_publication)
    result=run(client,definition)
    assert result['status']=='failed' and len(client.get('/api/datasets').json())==1
    assert client.post('/api/flows/'+definition['id']+'/runs/'+result['id']+'/retry').status_code==200
    result=finish(client,definition,result['id'])
    assert result['status']=='succeeded',result
    assert len(client.get('/api/datasets').json())==1
    assert result['steps'][0]['result']['version_id']


def test_publication_uses_the_checked_version_not_a_newer_dataset_version(client,monkeypatch,tmp_path):
    import duckdb
    from app.lakehouse import Lakehouse
    saved=pipeline(client,'First','SELECT 5 AS amount')
    definition=flow(client,[step('first',saved),{'id':'check','name':'Check','kind':'quality','depends_on':['first'],'input_step':'first',
        'rules':[{'column_name':'amount','rule':'min','threshold':1}]},
        {'id':'publish','name':'Publish','kind':'publish','input_step':'check','depends_on':['check']}])
    manager=client.app.state.flows;original=manager.quality;published=[]
    def newer(source,rules):
        result=original(source,rules)
        file=tmp_path/'newer.parquet'
        with duckdb.connect() as storage:storage.execute('COPY (SELECT 99 AS amount) TO ? (FORMAT PARQUET)',[str(file)])
        manager.workflows.publish(file,file,'Newer.parquet',target=source['id'])
        return result
    monkeypatch.setattr(manager,'quality',newer)
    monkeypatch.setattr(Lakehouse,'publish',lambda self,ident,version_id=None:published.append(version_id) or {'version_id':version_id})
    result=run(client,definition)
    assert result['status']=='succeeded',result
    source=result['steps'][0]['result']
    assert published==[source['version_id']]
    assert manager.workflows.versions(source['id'])[0]['id']!=published[0]


def test_sources_cannot_be_backfilled_and_saved_flows_can_be_scheduled(client):
    source=client.post('/api/connectors',json={'name':'Source','host':'127.0.0.1','port':5433,'database':'platform','username':'example',
        'password':'not-a-real-credential','schema_name':'public','table':'example','mode':'snapshot'}).json()
    saved=client.post('/api/pipelines',json={'name':'Source pipeline','connector_id':source['id']}).json()['id']
    definition=flow(client,[step('source',saved)])
    assert client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'start':'2026-01-01','end':'2026-01-01'}).status_code==400
    assert client.get('/api/connectors/'+source['id']+'/runs').json()==[]
    daily=flow(client,[step('daily',pipeline(client,'Scheduled','SELECT 1 AS a'))],enabled=True,interval_seconds=60)
    with connection(client.app.state.flows.metadata) as con:con.execute("UPDATE flows SET next_run='2000-01-01T00:00:00+00:00' WHERE id=?",(daily['id'],))
    client.app.state.flows.tick()
    with connection(client.app.state.flows.metadata) as con:id=con.execute('SELECT id FROM flow_runs WHERE flow_id=?',(daily['id'],)).fetchone()[0]
    assert finish(client,daily,id)['status']=='succeeded'


def test_workflow_does_not_replace_a_target_changed_after_queueing(client):
    client.app.state.flows.stop.set();client.app.state.flows.thread.join(3)
    target=client.post('/api/datasets',files={'file':('target.csv',b'amount\n1\n')}).json()
    saved=pipeline(client,'Output','SELECT 2 AS amount',target['id']);definition=flow(client,[step('output',saved)])
    id=client.post('/api/flows/'+definition['id']+'/run',json={'revision':1}).json()['run_ids'][0]
    assert client.post('/api/datasets/'+target['id']+'/versions',files={'file':('target.csv',b'amount\n99\n')}).status_code==201
    client.app.state.flows.start();result=finish(client,definition,id)
    assert result['status']=='failed' and 'changed since' in result['steps'][0]['error']
    assert client.post('/api/query',json={'sql':'SELECT * FROM '+target['table_name']}).json()['rows']==[[99]]


def test_retry_of_older_failed_workflow_does_not_retarget_newer_live_output(client,monkeypatch):
    saved=pipeline(client,'Output','SELECT 7 AS amount');definition=flow(client,[step('output',saved)])
    original=client.app.state.pipelines.execute;failed=[]
    def once(*args):
        if not failed:failed.append(True);raise ValueError('Temporary failure')
        return original(*args)
    monkeypatch.setattr(client.app.state.pipelines,'execute',once)
    old=run(client,definition)
    assert old['status']=='failed'
    accepted=client.post('/api/pipelines/'+saved+'/run').json()
    assert client.post('/api/flows/'+definition['id']+'/runs/'+old['id']+'/retry').status_code==200
    retried=finish(client,definition,old['id'])
    assert retried['status']=='failed' and 'newer pipeline run' in retried['steps'][0]['error']
    assert client.get('/api/pipelines').json()[0]['target_id']==accepted['id']
    assert len(client.get('/api/datasets').json())==1


def test_empty_historical_partitions_keep_the_sql_schema(client):
    saved=pipeline(client,'Empty date','SELECT CAST(1 AS BIGINT) AS amount,CAST({{partition_date}} AS DATE) AS day WHERE FALSE')
    definition=flow(client,[step('daily',saved),{'id':'check','name':'Check','kind':'quality','input_step':'daily','depends_on':['daily'],
        'rules':[{'column_name':'amount','rule':'not_null'}]}])
    response=client.post('/api/flows/'+definition['id']+'/backfill',json={'revision':1,'start':'2026-01-01','end':'2026-01-01'})
    assert response.status_code==202,response.text
    result=finish(client,definition,response.json()['run_ids'][0])
    assert result['status']=='succeeded',result
    output=result['steps'][0]['result']
    dataset=client.get('/api/datasets/'+output['id']).json()
    assert dataset['row_count']==0
    assert [column['type'] for column in dataset['profile']]==['BIGINT','DATE']
