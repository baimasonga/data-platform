"""Live test: a workflow publishes its checked immutable version, not newer data."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def verify():
    import boto3
    import botocore.session
    from botocore.configprovider import ConstantProvider
    import duckdb
    from fastapi.testclient import TestClient
    from app.main import create_local_app
    from app.lakehouse import Lakehouse
    from app.workflows import quote,now
    root=Path(__file__).resolve().parents[1]
    values=dict(line.split('=',1) for line in (root/'deploy/runtime/stack.env').read_text().splitlines() if line)
    local=botocore.session.Session();local.get_component('config_store').set_config_provider('profile',ConstantProvider(None))
    s3=boto3.Session(botocore_session=local).client('s3',endpoint_url='http://127.0.0.1:8333',region_name='us-east-1',
        aws_access_key_id=values['S3_ACCESS_KEY'],aws_secret_access_key=values['S3_SECRET_KEY'])
    if 'warehouse' not in [item['Name'] for item in s3.list_buckets()['Buckets']]:s3.create_bucket(Bucket='warehouse')
    os.environ['DATA_PLATFORM_TRINO_URL']='http://127.0.0.1:8082'
    with tempfile.TemporaryDirectory(prefix='flow-lakehouse-') as directory:
        app=create_local_app(directory)
        with TestClient(app) as client:
            pipeline=client.post('/api/pipelines',json={'name':'Checked output','sql':'SELECT 5 AS amount'}).json()
            definition=client.post('/api/flows',json={'name':'Checked publication','steps':[
                {'id':'data','name':'Data','kind':'pipeline','pipeline_id':pipeline['id']},
                {'id':'check','name':'Check','kind':'quality','input_step':'data','depends_on':['data'],
                 'rules':[{'column_name':'amount','rule':'min','threshold':1}]},
                {'id':'publish','name':'Publish','kind':'publish','input_step':'check','depends_on':['check']}]}).json()
            original=app.state.flows.quality
            def newer(source,rules):
                accepted=original(source,rules)
                file=Path(directory)/'newer.parquet'
                with duckdb.connect() as storage:storage.execute('COPY (SELECT 99 AS amount) TO ? (FORMAT PARQUET)',[str(file)])
                app.state.flows.workflows.publish(file,file,'Newer.parquet',target=source['id'])
                return accepted
            app.state.flows.quality=newer
            run=client.post('/api/flows/'+definition['id']+'/run',json={'revision':1}).json()['run_ids'][0]
            deadline=time.monotonic()+180
            while time.monotonic()<deadline:
                result=client.get('/api/flows/'+definition['id']+'/runs/'+run).json()
                if result['status'] not in ('queued','running'):break
                time.sleep(.1)
            assert result['status']=='succeeded','Workflow publication failed; inspect its private run history.'
            source=result['steps'][0]['result'];publication=result['steps'][2]['result']['lakehouse']
            lake=Lakehouse(app.state.flows.workflows)
            newer_copy=None
            try:
                assert lake.query('SELECT sum(amount) FROM '+publication['table'])['rows']==[[5]]
                assert app.state.flows.workflows.versions(source['id'])[0]['id']!=source['version_id']
                assert lake.publish(source['id'],version_id=source['version_id'])['already_published']
                newer=app.state.flows.workflows.versions(source['id'])[0]
                staging='lakehouse.'+quote(lake.schema)+'."stg_interrupted_probe"'
                final='lakehouse.'+quote(lake.schema)+'.'+quote('ds_'+source['id']+'_v'+str(newer['number']))
                lake.query('CREATE TABLE '+staging+'(amount BIGINT) WITH (format=\'PARQUET\')')
                with app.state.flows.workflows.catalog() as con:
                    con.execute('INSERT INTO lakehouse_exports(version_id,dataset_id,table_name,state,created_at,staging_table) VALUES(?,?,?,?,?,?)',
                        (newer['id'],source['id'],final,'writing',now(),staging))
                newer_copy=lake.publish(source['id'],version_id=newer['id'])
                assert lake.query('SHOW TABLES FROM lakehouse.'+quote(lake.schema)+" LIKE 'stg_interrupted_probe'")['rows']==[]
                assert lake.query('SELECT sum(amount) FROM '+newer_copy['table'])['rows']==[[99]]
            finally:
                lake.remove(source['version_id'],publication['table'])
                if newer_copy:lake.remove(newer['id'],newer_copy['table'])
                lake.query('DROP SCHEMA lakehouse."'+lake.schema+'"')
    print('Workflow Iceberg verified: checked immutable output, idempotent replay, interrupted staging cleanup and remote cleanup.')


if __name__=='__main__':verify()
