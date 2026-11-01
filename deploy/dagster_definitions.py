"""Dagster adapter: run the platform's workspace-scoped pipeline API idempotently."""
import os
import time
from datetime import datetime, timezone

import httpx
from dagster import Definitions, RunRequest, RetryPolicy, SensorResult, job, op, sensor, DefaultSensorStatus


def request(method, path, **kwargs):
    base = os.environ['DATA_PLATFORM_URL'].rstrip('/')
    with httpx.Client(timeout=180, headers={'Authorization': 'Bearer ' + os.environ['DATA_PLATFORM_SERVICE_TOKEN']}) as client:
        response = client.request(method, base + path, **kwargs)
        response.raise_for_status()
        return response.json()


@op(config_schema={'pipeline_id': str}, retry_policy=RetryPolicy(max_retries=2, delay=5))
def invoke_platform(context):
    pipeline = context.op_config['pipeline_id']
    result = request('POST', '/api/pipelines/' + pipeline + '/run', headers={'Idempotency-Key': context.run_id})
    context.add_output_metadata({'dataset_id': result['id'], 'platform_run_id': result.get('run_id', '')})
    return result['id']


@job
def platform_pipeline():
    invoke_platform()


@sensor(job=platform_pipeline, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def platform_schedules(_):
    pipelines = request('GET', '/api/pipelines')
    current = datetime.now(timezone.utc).isoformat()
    return SensorResult(run_requests=[RunRequest(run_key=item['id'] + ':' + item['next_run'],
        run_config={'ops': {'invoke_platform': {'config': {'pipeline_id': item['id']}}}})
        for item in pipelines if item['enabled'] and item['next_run'] and item['next_run'] <= current])


@op(config_schema={'flow_id':str,'revision':int},retry_policy=RetryPolicy(max_retries=2,delay=5))
def invoke_flow(context):
    ident=context.op_config['flow_id']
    queued=request('POST','/api/flows/'+ident+'/run',json={'revision':context.op_config['revision']},
                   headers={'Idempotency-Key':context.run_id})
    run=queued['run_ids'][0]
    context.add_output_metadata({'workflow_run_id':run})
    deadline=time.monotonic()+1800
    while time.monotonic()<deadline:
        result=request('GET','/api/flows/'+ident+'/runs/'+run)
        if result['status']=='succeeded':return run
        if result['status'] not in ('queued','running'):
            raise RuntimeError('Workflow '+run+' ended '+result['status']+'. Inspect its step history; retry explicitly in the platform.')
        time.sleep(1)
    raise TimeoutError('Workflow is still active after 30 minutes. Inspect or cancel it in the platform.')


@job
def platform_flow():
    invoke_flow()


@sensor(job=platform_flow,minimum_interval_seconds=30,default_status=DefaultSensorStatus.RUNNING)
def flow_schedules(_):
    current=datetime.now(timezone.utc).isoformat()
    return SensorResult(run_requests=[RunRequest(run_key='flow:'+item['id']+':'+item['next_run'],
        run_config={'ops':{'invoke_flow':{'config':{'flow_id':item['id'],'revision':item['revision']}}}})
        for item in request('GET','/api/flows') if item['enabled'] and item['next_run'] and item['next_run']<=current])


defs = Definitions(jobs=[platform_pipeline,platform_flow], sensors=[platform_schedules,flow_schedules])
