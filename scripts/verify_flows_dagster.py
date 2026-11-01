"""Verify the actual Dagster workflow job against an isolated local test server."""
import importlib.util
import os
from pathlib import Path
import uuid
import time
import httpx
from dagster import Definitions,DagsterInstance,DagsterRunStatus,RunsFilter

ROOT=Path(__file__).resolve().parents[1]
os.environ.setdefault('DATA_PLATFORM_URL','http://127.0.0.1:8005')
os.environ.setdefault('DATA_PLATFORM_SERVICE_TOKEN','local-mode-verification-only')
spec=importlib.util.spec_from_file_location('workflow_definitions',ROOT/'deploy/dagster_definitions.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
Definitions.validate_loadable(module.defs)
with httpx.Client(base_url=os.environ['DATA_PLATFORM_URL'],timeout=15,
                  headers={'Authorization':'Bearer '+os.environ['DATA_PLATFORM_SERVICE_TOKEN']}) as client:
    def request(method,path,**kwargs):
        response=client.request(method,path,**kwargs);response.raise_for_status();return response.json()
    pipeline_name='Dagster flow '+uuid.uuid4().hex[:8]
    pipeline=request('POST','/api/pipelines',json={'name':pipeline_name,'sql':'SELECT 42 AS answer'})
    sensor=os.environ.get('DATA_PLATFORM_VERIFY_SENSOR')=='1'
    flow=request('POST','/api/flows',json={'name':'Dagster workflow verification','enabled':sensor,'interval_seconds':60,'steps':[
        {'id':'calculate','name':'Calculate','kind':'pipeline','pipeline_id':pipeline['id']}]})
    job=module.defs.get_job_def('platform_flow');run_id=uuid.uuid4().hex
    config={'ops':{'invoke_flow':{'config':{'flow_id':flow['id'],'revision':flow['revision']}}}}
    try:
        assert job.execute_in_process(run_config=config,run_id=run_id).success
        first=request('GET','/api/flows/'+flow['id']+'/runs')
        assert job.execute_in_process(run_config=config,run_id=run_id).success
        second=request('GET','/api/flows/'+flow['id']+'/runs')
        assert len(first)==len(second)==1 and second[0]['status']=='succeeded'
        if sensor:
            deadline=time.monotonic()+180
            with DagsterInstance.get() as instance:
                while time.monotonic()<deadline:
                    records=instance.get_runs(filters=RunsFilter(job_name='platform_flow'),limit=20)
                    scheduled=[run for run in records if run.run_config.get('ops',{}).get('invoke_flow',{}).get('config',{}).get('flow_id')==flow['id']]
                    if any(run.status==DagsterRunStatus.SUCCESS for run in scheduled):break
                    time.sleep(1)
                else:raise AssertionError('A live Dagster sensor did not complete the scheduled workflow.')
    finally:
        request('DELETE','/api/flows/'+flow['id'],json={'revision':1,'confirmation_name':flow['name']})
        request('DELETE','/api/pipelines/'+pipeline['id'],json={'confirmation_name':pipeline_name})
print('Dagster workflow verified: definitions load, real asynchronous job execution, idempotent replay'+(' and live sensor scheduling' if sensor else '')+'.')
