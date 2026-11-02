from datetime import datetime,timezone,timedelta
import pytest
from fastapi.testclient import TestClient
from app.main import create_local_app
from app.partitions import partition_window,backfill_windows,catchup_windows
from app.flows import Backfill
from test_flows import pipeline,flow,step


@pytest.mark.parametrize('start,end',[
    ('2024-02-01','2024-03-01'),('2023-02-01','2023-03-01'),
    ('2026-12-01','2027-01-01'),('2000-02-01','2000-03-01'),('2100-02-01','2100-03-01'),
])
def test_month_windows_follow_real_calendar_boundaries(start,end):
    window=partition_window(start,'month')
    assert window['key']==start and window['end']==end+'T00:00:00+00:00'
    assert window['start']==start+'T00:00:00+00:00'
    expected=(datetime.fromisoformat(end)-datetime.fromisoformat(start)).days
    assert (datetime.fromisoformat(window['end'])-datetime.fromisoformat(window['start'])).days==expected


def test_weeks_start_monday_and_cross_iso_years_without_gaps():
    windows=backfill_windows('2020-12-28','2021-01-04','week')
    assert len(windows)==2 and windows[0]['end']==windows[1]['start']
    assert windows[-1]['end']=='2021-01-11T00:00:00+00:00'
    assert all(datetime.fromisoformat(w['start']).weekday()==0 for w in windows)


@pytest.mark.parametrize('kind,start,end',[
    ('week','2026-01-06','2026-01-12'),('month','2024-02-02','2024-03-01'),
    ('month','2024-02','2024-03'),('month','2026-02-01','2026-01-01'),
    ('month','2024-01-01','2026-01-01'),('week','2026-01-05','2026-07-06'),
    ('month','9999-12-01','9999-12-01'),('week','9999-12-27','9999-12-27'),
])
def test_ranges_require_aligned_supported_bounded_calendar_partitions(kind,start,end):
    with pytest.raises(ValueError):Backfill(revision=1,granularity=kind,start=start,end=end)


def test_maximum_calendar_batches_and_closed_only_catchup():
    assert len(backfill_windows('2024-01-01','2025-12-01','month'))==24
    assert len(backfill_windows('2026-01-05','2026-06-29','week'))==26
    current=datetime(2024,3,1,tzinfo=timezone.utc)
    windows,due=catchup_windows('2024-01-01','month',current)
    assert due==2 and windows[-1]['end']==current.isoformat()
    assert catchup_windows('2024-01-01','month',current-timedelta(microseconds=1))[1]==1
    assert catchup_windows('2024-03-01','month',current)==([],0)
    monday=datetime(2026,11,2,tzinfo=timezone.utc)
    windows,due=catchup_windows('2026-10-26','week',monday)
    assert due==1 and windows[0]['end']==monday.isoformat()
    assert catchup_windows('2026-10-26','week',monday-timedelta(microseconds=1))==([],0)
    assert len(catchup_windows('2000-01-01','month',monday)[0])==24


@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setenv('DATA_PLATFORM_ORCHESTRATOR','dagster')
    with TestClient(create_local_app(tmp_path)) as client:
        manager=client.app.state.flows;manager.stop.set();manager.thread.join(3);manager.stop.clear()
        yield client


@pytest.mark.parametrize('kind,start,end,totals',[
    ('month','2024-02-01','2024-03-01',[5,7]),
    ('week','2024-02-26','2024-03-04',[9,11]),
])
def test_calendar_sql_backfills_are_half_open_pinned_isolated_and_idempotent(client,kind,start,end,totals):
    csv=b'event_at,amount\n2024-02-01T00:00:00Z,2\n2024-02-29T23:59:59Z,3\n2024-03-01T00:00:00Z,4\n2024-03-03T23:59:59Z,2\n2024-03-04T00:00:00Z,11\n2024-03-31T23:59:59Z,-10\n2024-04-01T00:00:00Z,99\n'
    source=client.post('/api/datasets',files={'file':('events.csv',csv)}).json()
    live=client.post('/api/datasets',files={'file':('live.csv',b'amount\n42\n')}).json()
    saved=pipeline(client,'Calendar',f'SELECT SUM(amount) AS amount FROM "{source["table_name"]}" WHERE CAST(event_at AS TIMESTAMPTZ)>={{{{partition_start}}}} AND CAST(event_at AS TIMESTAMPTZ)<{{{{partition_end}}}}',live['id'])
    definition=flow(client,[step('calendar',saved)])
    body={'revision':1,'granularity':kind,'start':start,'end':end}
    response=client.post('/api/flows/'+definition['id']+'/backfill',json=body,headers={'Idempotency-Key':'calendar'})
    assert response.status_code==202,response.text
    ids=response.json()['run_ids'];assert len(ids)==2
    assert client.post('/api/flows/'+definition['id']+'/backfill',json=body,headers={'Idempotency-Key':'calendar'}).json()['run_ids']==ids
    assert client.post('/api/datasets/'+source['id']+'/versions',files={'file':('new.csv',b'event_at,amount\n2024-02-01T00:00:00Z,999\n')}).status_code==201
    observed=[]
    for id in ids:
        client.app.state.flows.execute(id)
        record=client.get('/api/flows/'+definition['id']+'/runs/'+id).json()
        assert record['status']=='succeeded',record
        assert record['partition_granularity']==kind and record['detached']
        assert record['snapshot']['partition']['granularity']==kind
        physical=record['steps'][0]['result']['physical_table']
        observed.append(client.post('/api/query',json={'sql':'SELECT amount FROM '+physical}).json()['rows'][0][0])
    assert observed==totals
    assert client.get('/api/pipelines').json()[0]['target_id']==live['id']
    assert len(client.get('/api/datasets/'+live['id']+'/versions').json())==1


@pytest.mark.parametrize('kind,start,end', [('week','2026-10-19','2026-11-02'),('month','2026-08-01','2026-11-01')])
def test_calendar_catchup_advances_by_calendar_windows_not_fixed_days(client,monkeypatch,kind,start,end):
    import app.flows as module
    current=datetime(2026,11,2,12,tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return current.astimezone(tz) if tz else current.replace(tzinfo=None)
    monkeypatch.setattr(module,'datetime',Clock)
    monkeypatch.setattr(module,'now',lambda:current.isoformat())
    saved=pipeline(client,'Calendar catchup','SELECT {{partition_start}} AS begins,{{partition_end}} AS ends')
    definition=flow(client,[step('calendar',saved)],enabled=True,catch_up=kind,catch_up_start=start)
    response=client.post('/api/flows/'+definition['id']+'/schedule',json={'revision':1},headers={'Idempotency-Key':'calendar-catchup'})
    assert response.status_code==202,response.text
    ids=response.json()['run_ids'];assert len(ids)==(2 if kind=='week' else 3)
    status=client.get('/api/flows/'+definition['id']+'/schedule').json()
    assert status['next_partition']==end and status['due_partitions']==0
    for id in ids:client.app.state.flows.execute(id)
    assert all(r['status']=='succeeded' and r['schedule_generation']==1 for r in client.get('/api/flows/'+definition['id']+'/runs').json())
    assert client.post('/api/flows/'+definition['id']+'/pause',json={'revision':1}).status_code==200
    assert client.get('/api/flows/'+definition['id']+'/schedule').json()['next_partition']==end


@pytest.mark.parametrize('kind,start,end',[('week','2026-10-26','2026-11-02'),('month','2026-10-01','2026-11-01')])
def test_calendar_history_and_checkpoint_survive_restart_and_retry(tmp_path,monkeypatch,kind,start,end):
    from test_flows import finish
    import app.flows as module
    current=datetime(2026,11,2,0,tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return current.astimezone(tz) if tz else current.replace(tzinfo=None)
    monkeypatch.setenv('DATA_PLATFORM_ORCHESTRATOR','dagster')
    monkeypatch.setattr(module,'datetime',Clock);monkeypatch.setattr(module,'now',lambda:current.isoformat())
    with TestClient(create_local_app(tmp_path)) as client:
        manager=client.app.state.flows;manager.stop.set();manager.thread.join(3)
        saved=pipeline(client,'Persisted calendar','SELECT {{partition_end}} AS end_at')
        definition=flow(client,[step('calendar',saved)],enabled=True,catch_up=kind,catch_up_start=start)
        response=client.post('/api/flows/'+definition['id']+'/schedule',json={'revision':1},headers={'Idempotency-Key':'persisted'})
        assert response.status_code==202,response.text
        ids=response.json()['run_ids'];assert len(ids)==1
    with TestClient(create_local_app(tmp_path)) as client:
        status=client.get('/api/flows/'+definition['id']+'/schedule').json()
        assert status['next_partition']==end and len(status['blocked_runs'])==1
        record=client.get('/api/flows/'+definition['id']+'/runs/'+ids[0]).json()
        assert record['partition_start']==start+'T00:00:00+00:00' and record['partition_end']==end+'T00:00:00+00:00'
        assert client.post('/api/flows/'+definition['id']+'/runs/'+ids[0]+'/retry').status_code==200
        assert finish(client,definition,ids[0])['status']=='succeeded'
        assert client.get('/api/flows/'+definition['id']+'/schedule').json()['next_partition']==end
