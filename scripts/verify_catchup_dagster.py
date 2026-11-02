"""Exercise the real Dagster catch-up sensor/job against a disposable local app."""
from datetime import datetime,timedelta,timezone
import importlib.util
import os
from pathlib import Path
import uuid
import httpx
from dagster import Definitions,DagsterInstance,build_sensor_context

ROOT=Path(__file__).resolve().parents[1]
os.environ.setdefault('DATA_PLATFORM_URL','http://127.0.0.1:8012')
os.environ.setdefault('DATA_PLATFORM_SERVICE_TOKEN','local-mode-verification-only')
spec=importlib.util.spec_from_file_location('catchup_definitions',ROOT/'deploy/dagster_definitions.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
Definitions.validate_loadable(module.defs)
with httpx.Client(base_url=os.environ['DATA_PLATFORM_URL'],timeout=30,headers={'Authorization':'Bearer '+os.environ['DATA_PLATFORM_SERVICE_TOKEN']}) as client:
    def request(method,path,**kwargs):
        response=client.request(method,path,**kwargs);response.raise_for_status();return response.json()
    pipeline_name='Dagster catch-up '+uuid.uuid4().hex[:8]
    pipeline=request('POST','/api/pipelines',json={'name':pipeline_name,'sql':'SELECT {{partition_start}} AS begins'})
    kind=os.environ.get('DATA_PLATFORM_VERIFY_PARTITION','hour')
    current=datetime.now(timezone.utc)
    if kind=='hour':start=(current-timedelta(hours=2)).replace(minute=0,second=0,microsecond=0).isoformat()
    elif kind=='week':start=(current-timedelta(days=current.weekday()+14)).date().isoformat()
    elif kind=='month':
        index=current.year*12+current.month-3
        start=f'{index//12:04d}-{index%12+1:02d}-01'
    else:raise ValueError('Verification supports hour, week or month.')
    definition=request('POST','/api/flows',json={'name':'Dagster automatic catch-up','enabled':True,'interval_seconds':60,'catch_up':kind,'catch_up_start':start,'steps':[{'id':'hour','name':'Hour','kind':'pipeline','pipeline_id':pipeline['id']}]})
    try:
        assert request('GET','/api/flows/'+definition['id']+'/runs')==[],'Embedded scheduling must be disabled for this check.'
        with DagsterInstance.ephemeral() as instance:
            evaluation=module.flow_schedules.evaluate_tick(build_sensor_context(instance=instance))
        configs=[run.run_config for run in evaluation.run_requests if run.run_config['ops']['invoke_flow']['config']['flow_id']==definition['id']]
        assert len(configs)==1 and configs[0]['ops']['invoke_flow']['config']['scheduled'] is True
        # If dispatch never reached the app, later polling must be able to emit
        # a new job even though the server's next_run has not advanced.
        original_clock=module.datetime
        class LaterClock(datetime):
            @classmethod
            def now(cls,tz=None):return datetime.now(tz)+timedelta(seconds=120)
        module.datetime=LaterClock
        try:
            with DagsterInstance.ephemeral() as instance:
                later=module.flow_schedules.evaluate_tick(build_sensor_context(instance=instance))
            original_key=next(run.run_key for run in evaluation.run_requests if run.run_config==configs[0])
            later_key=next(run.run_key for run in later.run_requests if run.run_config==configs[0])
            assert later_key!=original_key
        finally:module.datetime=original_clock
        job=module.defs.get_job_def('platform_flow');run_id=uuid.uuid4().hex
        assert job.execute_in_process(run_config=configs[0],run_id=run_id).success
        first=request('GET','/api/flows/'+definition['id']+'/runs')
        assert len(first)==2 and all(run['status']=='succeeded' and run['trigger']=='catch_up' and run['detached'] and run['partition_granularity']==kind for run in first)
        assert job.execute_in_process(run_config=configs[0],run_id=run_id).success
        second=request('GET','/api/flows/'+definition['id']+'/runs')
        assert {run['id'] for run in second}=={run['id'] for run in first}
        # A fresh poll before the next due time may return no runs; the job must succeed.
        assert job.execute_in_process(run_config=configs[0],run_id=uuid.uuid4().hex).success
        status=request('GET','/api/flows/'+definition['id']+'/schedule')
        assert status['due_partitions']==0 and not status['blocked_runs']
    finally:
        request('DELETE','/api/flows/'+definition['id'],json={'revision':1,'confirmation_name':definition['name']})
        request('DELETE','/api/pipelines/'+pipeline['id'],json={'confirmation_name':pipeline_name})
print('Real Dagster '+kind+' catch-up verified: sensor policy/re-dispatch, multiple completed windows, batch completion, idempotent replay and empty-poll success.')
