from datetime import datetime,timezone,timedelta
import json
import pytest
from fastapi.testclient import TestClient
from app.catalog import connection
from app.main import create_local_app
from app.flows import Request
from app.partitions import catchup_windows
from test_flows import pipeline,flow,step,finish


CURRENT=datetime(2026,11,2,12,45,tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def schedule_clock(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return CURRENT.astimezone(tz) if tz else CURRENT.replace(tzinfo=None)
    monkeypatch.setattr('app.flows.datetime',Clock)
    monkeypatch.setattr('app.flows.now',lambda:CURRENT.isoformat())


@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setenv('DATA_PLATFORM_ORCHESTRATOR','dagster')
    with TestClient(create_local_app(tmp_path)) as client:
        manager=client.app.state.flows;manager.stop.set();manager.thread.join(3);manager.stop.clear()
        yield client


def definition(client,**options):
    saved=pipeline(client,'Catch up','SELECT EXTRACT(hour FROM {{partition_start}}) AS hour')
    return flow(client,[step('hour',saved)],enabled=True,interval_seconds=60,catch_up='hour',catch_up_start=(CURRENT-timedelta(hours=2)).replace(minute=0,second=0,microsecond=0).isoformat(),**options)


def due(client,saved):
    with connection(client.app.state.flows.metadata) as con:con.execute("UPDATE flows SET next_run='2000-01-01T00:00:00+00:00' WHERE id=?",(saved['id'],))


def dispatch(client,saved,key='dispatch'):
    result=client.post('/api/flows/'+saved['id']+'/schedule',json={'revision':saved['revision']},headers={'Idempotency-Key':key})
    assert result.status_code==202,result.text
    return result.json()


def test_planner_only_emits_closed_windows_and_bounds_large_backlogs():
    current=datetime(2026,11,2,12,45,tzinfo=timezone.utc)
    windows,count=catchup_windows('2026-11-02T10:00Z','hour',current)
    assert count==2 and len(windows)==2 and windows[-1]['end']=='2026-11-02T12:00:00+00:00'
    windows,count=catchup_windows('2026-11-01','day',current)
    assert count==1 and windows[0]['end']=='2026-11-02T00:00:00+00:00'
    windows,count=catchup_windows('2000-01-01T00:00Z','hour',current)
    assert len(windows)==48 and count>10000
    assert catchup_windows('2026-11-03','day',current)==([],0)


def test_schedule_dispatch_advances_cursor_atomically_and_replays_without_duplicates(client):
    saved=definition(client)
    before=client.get('/api/flows/'+saved['id']+'/schedule').json()
    assert before['due_partitions']==2 and client.get('/api/flows/'+saved['id']+'/runs').json()==[]
    queued=dispatch(client,saved)
    assert len(queued['run_ids'])==2
    assert dispatch(client,saved)['run_ids']==queued['run_ids']
    after=client.get('/api/flows/'+saved['id']+'/schedule').json()
    assert after['due_partitions']==0 and after['next_partition']==before['next_batch'][-1]['end']
    records=[client.get('/api/flows/'+saved['id']+'/runs/'+id).json() for id in queued['run_ids']]
    assert all(r['detached'] and r['trigger']=='catch_up' for r in records)
    for ident in queued['run_ids']:client.app.state.flows.execute(ident)
    assert all(client.get('/api/flows/'+saved['id']+'/runs/'+id).json()['status']=='succeeded' for id in queued['run_ids'])
    due(client,saved)
    assert dispatch(client,saved,'next')['run_ids']==[]


def test_pause_resume_and_ordinary_edits_preserve_cursor(client):
    saved=definition(client);queued=dispatch(client,saved)
    for ident in queued['run_ids']:client.app.state.flows.execute(ident)
    cursor=client.get('/api/flows/'+saved['id']+'/schedule').json()['next_partition']
    paused=client.post('/api/flows/'+saved['id']+'/pause',json={'revision':1}).json()
    assert dispatch(client,paused,'paused')['run_ids']==[]
    changed=client.put('/api/flows/'+saved['id'],json={**paused,'name':'Renamed','enabled':True}).json()
    assert changed['revision']==3
    assert client.get('/api/flows/'+saved['id']+'/schedule').json()['next_partition']==cursor
    reset={**changed,'catch_up_start':(CURRENT-timedelta(hours=1)).replace(minute=0,second=0,microsecond=0).isoformat()}
    response=client.put('/api/flows/'+saved['id'],json=reset);assert response.status_code==200,response.text
    assert client.get('/api/flows/'+saved['id']+'/schedule').json()['next_partition']==reset['catch_up_start']


def test_failed_cancelled_and_interrupted_catchup_runs_require_explicit_retry(client):
    saved=definition(client);queued=dispatch(client,saved)
    cancelled=queued['run_ids'][0]
    assert client.post('/api/flows/'+saved['id']+'/runs/'+cancelled+'/cancel').status_code==200
    client.app.state.flows.execute(queued['run_ids'][1])
    due(client,saved)
    result=dispatch(client,saved,'blocked')
    assert result['run_ids']==[] and 'Retry unsuccessful' in result['reason']
    status=client.get('/api/flows/'+saved['id']+'/schedule').json()
    assert status['blocked_runs'][0]['id']==cancelled
    assert client.post('/api/flows/'+saved['id']+'/runs/'+cancelled+'/retry').status_code==200
    client.app.state.flows.execute(cancelled)
    assert client.get('/api/flows/'+saved['id']+'/schedule').json()['blocked_runs']==[]


def test_queue_capacity_uses_available_slots_and_retains_remaining_cursor(client):
    saved=definition(client)
    with connection(client.app.state.flows.metadata) as con:
        for index in range(63):
            con.execute("INSERT INTO flow_runs(id,flow_id,revision,snapshot,partition_date,detached,status,created_at) VALUES(?, 'other',1,'{}','2026-01-01',0,'queued','2026-01-01')",(str(index),))
    before=client.get('/api/flows/'+saved['id']+'/schedule').json()
    queued=dispatch(client,saved)
    assert len(queued['run_ids'])==1
    after=client.get('/api/flows/'+saved['id']+'/schedule').json()
    assert after['due_partitions']==1 and after['next_partition']==before['next_batch'][0]['end']


def test_source_pipelines_are_rejected_and_later_definition_drift_keeps_cursor(client):
    source=client.post('/api/connectors',json={'name':'Source','host':'127.0.0.1','port':5433,'database':'platform','username':'example','password':'synthetic-only','schema_name':'public','table':'example','mode':'snapshot'}).json()
    saved=client.post('/api/pipelines',json={'name':'Source pipeline','connector_id':source['id']}).json()['id']
    response=client.post('/api/flows',json={'name':'Invalid catch-up','catch_up':'day','catch_up_start':'2026-01-01','steps':[step('source',saved)]})
    assert response.status_code==400 and 'SQL pipelines only' in response.text
    normal=definition(client)
    pipeline_id=normal['steps'][0]['pipeline_id']
    assert client.put('/api/pipelines/'+pipeline_id,json={'name':'Changed to source','connector_id':source['id']}).status_code==200
    before=client.get('/api/flows/'+normal['id']+'/schedule').json()['next_partition']
    response=client.post('/api/flows/'+normal['id']+'/schedule',json={'revision':1})
    assert response.status_code==400,response.text
    status=client.get('/api/flows/'+normal['id']+'/schedule').json()
    assert status['next_partition']==before and 'SQL pipelines only' in status['last_error']
    assert client.get('/api/flows/'+normal['id']+'/runs').json()==[]


def test_restart_does_not_duplicate_accepted_windows_or_automatically_retry(tmp_path,monkeypatch):
    monkeypatch.setenv('DATA_PLATFORM_ORCHESTRATOR','dagster')
    with TestClient(create_local_app(tmp_path)) as client:
        manager=client.app.state.flows;manager.stop.set();manager.thread.join(3)
        saved=definition(client);queued=dispatch(client,saved)
        cursor=client.get('/api/flows/'+saved['id']+'/schedule').json()['next_partition']
    with TestClient(create_local_app(tmp_path)) as client:
        replay=dispatch(client,saved)
        assert replay['run_ids']==queued['run_ids']
        status=client.get('/api/flows/'+saved['id']+'/schedule').json()
        assert status['next_partition']==cursor and len(status['blocked_runs'])==2
        assert len(client.get('/api/flows/'+saved['id']+'/runs').json())==2


def test_embedded_tick_uses_catchup_policy_and_stale_revision_cannot_dispatch(client):
    saved=definition(client)
    client.app.state.flows.tick()
    records=client.get('/api/flows/'+saved['id']+'/runs').json()
    assert len(records)==2 and all(r['trigger']=='catch_up' for r in records)
    client.app.state.flows.tick()
    assert len(client.get('/api/flows/'+saved['id']+'/runs').json())==2
    assert client.post('/api/flows/'+saved['id']+'/schedule',json={'revision':99}).status_code==409


def test_enqueue_and_checkpoint_roll_back_together_after_injected_failure(client,monkeypatch):
    saved=definition(client);manager=client.app.state.flows
    before=client.get('/api/flows/'+saved['id']+'/schedule').json()['next_partition']
    original=manager.insert_runs
    def fail(*args,**kwargs):
        original(*args,**kwargs)
        raise RuntimeError('Injected failure before checkpoint commit')
    monkeypatch.setattr(manager,'insert_runs',fail)
    with pytest.raises(RuntimeError):manager.schedule(saved['id'],Request(revision=1),'atomic')
    assert client.get('/api/flows/'+saved['id']+'/runs').json()==[]
    assert client.get('/api/flows/'+saved['id']+'/schedule').json()['next_partition']==before
    monkeypatch.setattr(manager,'insert_runs',original)
    assert len(manager.schedule(saved['id'],Request(revision=1),'atomic')['run_ids'])==2


def test_concurrent_dispatch_does_not_duplicate_partitions(client):
    from concurrent.futures import ThreadPoolExecutor
    saved=definition(client);manager=client.app.state.flows
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda key:manager.schedule(saved['id'],Request(revision=1),key),['first','second']))
    assert sum(len(result['run_ids']) for result in results)==2
    assert len(client.get('/api/flows/'+saved['id']+'/runs').json())==2


def test_failed_old_policy_does_not_block_explicitly_changed_start(client):
    saved=definition(client);queued=dispatch(client,saved)
    for id in queued['run_ids']:assert client.post('/api/flows/'+saved['id']+'/runs/'+id+'/cancel').status_code==200
    assert client.get('/api/flows/'+saved['id']+'/schedule').json()['blocked_runs']
    changed={**saved,'catch_up_start':(CURRENT-timedelta(hours=1)).replace(minute=0,second=0,microsecond=0).isoformat()}
    response=client.put('/api/flows/'+saved['id'],json=changed)
    assert response.status_code==200,response.text
    assert client.get('/api/flows/'+saved['id']+'/schedule').json()['blocked_runs']==[]


def test_full_queue_does_not_advance_cursor_and_daily_limit_is_bounded(client):
    assert len(catchup_windows('2000-01-01','day',CURRENT)[0])==31
    saved=definition(client)
    before=client.get('/api/flows/'+saved['id']+'/schedule').json()['next_partition']
    with connection(client.app.state.flows.metadata) as con:
        for index in range(64):con.execute("INSERT INTO flow_runs(id,flow_id,revision,snapshot,partition_date,detached,status,created_at) VALUES(?, 'other',1,'{}','2026-01-01',0,'queued','2026-01-01')",('full'+str(index),))
    result=dispatch(client,saved)
    assert result['run_ids']==[] and 'queue is full' in result['reason']
    assert client.get('/api/flows/'+saved['id']+'/schedule').json()['next_partition']==before


def test_schedule_route_rejects_manual_partition_overrides(client):
    saved=definition(client)
    response=client.post('/api/flows/'+saved['id']+'/schedule',json={'revision':1,'partition_date':'2026-01-01'})
    assert response.status_code==422
    assert client.get('/api/flows/'+saved['id']+'/runs').json()==[]


def test_explicit_restart_keeps_history_and_uses_new_generation(client):
    saved=definition(client);queued=dispatch(client,saved)
    before=client.get('/api/flows/'+saved['id']+'/schedule').json()
    assert client.post('/api/flows/'+saved['id']+'/schedule/reset',json={'revision':1,'confirmation_name':saved['name']}).status_code==409
    for id in queued['run_ids']:assert client.post('/api/flows/'+saved['id']+'/runs/'+id+'/cancel').status_code==200
    response=client.post('/api/flows/'+saved['id']+'/schedule/reset',json={'revision':1,'confirmation_name':'wrong'})
    assert response.status_code==409
    reset=client.post('/api/flows/'+saved['id']+'/schedule/reset',json={'revision':1,'confirmation_name':saved['name']})
    assert reset.status_code==200,reset.text
    assert reset.json()['generation']==before['generation']+1 and not reset.json()['blocked_runs']
    newer=dispatch(client,saved,'after-reset')
    assert len(newer['run_ids'])==2 and not set(newer['run_ids'])&set(queued['run_ids'])
    assert len(client.get('/api/flows/'+saved['id']+'/runs').json())==4
