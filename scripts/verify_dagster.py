"""Exercise the actual Dagster job and sensor against the authenticated API."""
import importlib.util
import json
import os
from pathlib import Path
import time
import uuid

import httpx
from dagster import Definitions, build_sensor_context

ROOT=Path(__file__).resolve().parents[1]
os.environ.update(dict(line.split('=',1) for line in (ROOT/'deploy/runtime/dagster.env').read_text().splitlines() if line))
os.environ['DATA_PLATFORM_URL']='http://127.0.0.1:8000'
spec=importlib.util.spec_from_file_location('platform_definitions',ROOT/'deploy/dagster_definitions.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
Definitions.validate_loadable(module.defs)
state=json.loads((ROOT/'deploy/runtime/verification.json').read_text())
with httpx.Client(timeout=180,headers={'Authorization':'Bearer '+os.environ['DATA_PLATFORM_SERVICE_TOKEN']}) as client:
    base=os.environ['DATA_PLATFORM_URL']
    def request(method,path,**kwargs):
        response=client.request(method,base+path,**kwargs)
        response.raise_for_status()
        return response.json()
    job=module.defs.get_job_def('platform_pipeline')
    run=uuid.uuid4().hex
    config={'ops':{'invoke_platform':{'config':{'pipeline_id':state['pipeline_id']}}}}
    result=job.execute_in_process(run_config=config,run_id=run)
    assert result.success
    first=request('GET','/api/pipelines/'+state['pipeline_id']+'/runs')
    assert job.execute_in_process(run_config=config,run_id=run).success
    second=request('GET','/api/pipelines/'+state['pipeline_id']+'/runs')
    assert len(first)==len(second)
    # Verify a live daemon-driven sensor run, then pause it to keep the fixture quiet.
    definition={'name':'Dagster schedule verification '+uuid.uuid4().hex[:8],'sql':'SELECT 99 AS scheduled_value','enabled':True,'interval_seconds':60}
    ident=request('POST','/api/pipelines',json=definition)['id']
    deadline=time.monotonic()+180
    try:
        while time.monotonic()<deadline:
            runs=request('GET','/api/pipelines/'+ident+'/runs')
            if any(item['status']=='succeeded' for item in runs):
                break
            time.sleep(2)
        else:
            raise AssertionError('Dagster sensor did not produce a successful scheduled run within 180 seconds.')
    finally:
        current=next(item for item in request('GET','/api/pipelines') if item['id']==ident)
        request('PUT','/api/pipelines/'+ident,json={**current,'enabled':False})
print('Dagster verified: definitions load, authenticated job execution, idempotent replay, live sensor scheduling and schedule pause.')
