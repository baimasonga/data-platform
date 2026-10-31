"""Live integration verification. Reports outcomes, never generated credentials."""
import json
import os
from pathlib import Path
import sys
import tempfile
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def verify():
    import boto3
    import httpx
    import psycopg
    from fastapi.testclient import TestClient
    from app.main import create_local_app
    from app.lakehouse import Lakehouse
    from app.workflows import Workflows
    import threading
    root = Path(__file__).resolve().parents[1]
    values = dict(line.split('=', 1) for line in (root / 'deploy/runtime/stack.env').read_text().splitlines() if line)
    import botocore.session
    from botocore.configprovider import ConstantProvider
    local_s3 = botocore.session.Session()
    # This explicit local S3 client uses generated development credentials, not an AWS identity.
    local_s3.get_component('config_store').set_config_provider('profile', ConstantProvider(None))
    s3 = boto3.Session(botocore_session=local_s3).client('s3', endpoint_url='http://127.0.0.1:8333', region_name='us-east-1',
                      aws_access_key_id=values['S3_ACCESS_KEY'], aws_secret_access_key=values['S3_SECRET_KEY'])
    buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
    if 'warehouse' not in buckets:
        s3.create_bucket(Bucket='warehouse')
    discovery = httpx.get('http://127.0.0.1:8081/realms/data-platform/.well-known/openid-configuration').json()
    assert discovery['issuer'] == 'http://127.0.0.1:8081/realms/data-platform'
    with psycopg.connect(host='127.0.0.1', port=5433, dbname='platform', user='platform', password=values['POSTGRES_PASSWORD']) as pg:
        pg.execute('CREATE SCHEMA IF NOT EXISTS platform_demo')
        pg.execute('CREATE TABLE IF NOT EXISTS platform_demo.sales(id BIGINT PRIMARY KEY, region TEXT, revenue BIGINT)')
        pg.execute('TRUNCATE platform_demo.sales')
        pg.execute("INSERT INTO platform_demo.sales VALUES(1,'North',120),(2,'South',80)")
    os.environ['DATA_PLATFORM_TRINO_URL'] = 'http://127.0.0.1:8082'
    with tempfile.TemporaryDirectory(prefix='platform-live-integration-') as temporary:
        app = create_local_app(temporary)
        with TestClient(app) as client:
            definition = {'name':'Source sales','host':'127.0.0.1','port':5433,'database':'platform','username':'platform',
                          'password':values['POSTGRES_PASSWORD'],'schema_name':'platform_demo','table':'sales',
                          'mode':'incremental','watermark_column':'id'}
            connector = client.post('/api/connectors', json=definition)
            assert connector.status_code == 201, connector.text
            ident = connector.json()['id']
            assert client.post('/api/connectors/'+ident+'/test').json()['connected']
            imported = client.post('/api/connectors/'+ident+'/import')
            assert imported.status_code == 200, imported.text
            dataset = imported.json()['id']
            with psycopg.connect(host='127.0.0.1',port=5433,dbname='platform',user='platform',password=values['POSTGRES_PASSWORD']) as pg:
                pg.execute("INSERT INTO platform_demo.sales VALUES(3,'North',100)")
            incremental = client.post('/api/connectors/'+ident+'/import')
            assert incremental.status_code == 200, incremental.text
            assert incremental.json()['new_rows'] == 1
            assert incremental.json()['id'] == dataset
            assert client.post('/api/connectors/'+ident+'/import').json()['unchanged']
            detail = client.get('/api/datasets/'+dataset).json()
            assert detail['row_count'] == 3
            assert client.put('/api/datasets/'+dataset+'/quality',json={'rules':[{'column_name':'revenue','rule':'max','threshold':150}]}).status_code == 200
            with psycopg.connect(host='127.0.0.1',port=5433,dbname='platform',user='platform',password=values['POSTGRES_PASSWORD']) as pg:
                pg.execute("INSERT INTO platform_demo.sales VALUES(4,'South',200)")
            assert client.post('/api/connectors/'+ident+'/import').status_code == 400
            stored = client.get('/api/connectors').json()[0]
            assert json.loads(stored['watermark']) == 3
            assert client.get('/api/datasets/'+dataset).json()['row_count'] == 3
            exported = client.post('/api/datasets/'+dataset+'/lakehouse')
            assert exported.status_code == 200, exported.text
            assert client.post('/api/datasets/'+dataset+'/lakehouse').json()['already_published']
            lakehouse = Lakehouse(Workflows(Path(temporary), threading.RLock()))
            table = exported.json()['table']
            assert lakehouse.query(f'SELECT sum(revenue), count(*) FROM {table}')['rows'] == [[300,3]]
            browser_query=client.post('/api/query',json={'engine':'trino','sql':f'SELECT sum(revenue),count(*) FROM {table}'})
            assert browser_query.status_code==200,browser_query.text
            assert browser_query.json()['rows']==[[300,3]]
            assert client.get('/api/query-history').json()[0]['engine']=='trino'
            assert client.post('/api/query',json={'engine':'trino','sql':'SELECT * FROM system.runtime.queries'}).status_code==400
            # Verify schema evolution and Iceberg snapshot time travel on an independent table.
            schema = 'lakehouse."' + lakehouse.schema + '"'
            smoke = schema + '."compatibility_probe"'
            lakehouse.query(f'CREATE TABLE {smoke}(id BIGINT) WITH (format=\'PARQUET\')')
            lakehouse.query(f'INSERT INTO {smoke} VALUES(1)')
            snapshot_table = schema + '."compatibility_probe$snapshots"'
            snapshot = lakehouse.query(f'SELECT snapshot_id FROM {snapshot_table} ORDER BY committed_at DESC LIMIT 1')['rows'][0][0]
            lakehouse.query(f'ALTER TABLE {smoke} ADD COLUMN label VARCHAR')
            lakehouse.query(f"INSERT INTO {smoke} VALUES(2,'new')")
            assert lakehouse.query(f'SELECT count(*) FROM {smoke} FOR VERSION AS OF {snapshot}')['rows'] == [[1]]
            assert lakehouse.query(f'SELECT count(*) FROM {smoke}')['rows'] == [[2]]
            lakehouse.query(f'DROP TABLE {smoke}')
            assert client.request('DELETE','/api/datasets/'+dataset,json={'confirmation_name':'Source sales.parquet'}).status_code==409
            assert client.request('DELETE','/api/lakehouse/'+exported.json()['version_id'],json={'confirmation_name':table}).status_code==200
            assert client.get('/api/datasets/'+dataset).status_code==200
            source_pipeline=client.post('/api/pipelines',json={'name':'Source refresh','connector_id':ident})
            assert source_pipeline.status_code==201,source_pipeline.text
            pipeline_id=source_pipeline.json()['id']
            assert client.post('/api/pipelines/'+pipeline_id+'/run').status_code==400
            with psycopg.connect(host='127.0.0.1',port=5433,dbname='platform',user='platform',password=values['POSTGRES_PASSWORD']) as pg:
                pg.execute('UPDATE platform_demo.sales SET revenue=140 WHERE id=4')
            run=client.get('/api/pipelines/'+pipeline_id+'/runs').json()[0]
            retried=client.post('/api/pipelines/'+pipeline_id+'/runs/'+run['id']+'/retry')
            assert retried.status_code==200,retried.text
            assert retried.json()['new_rows']==1
            noop=client.post('/api/pipelines/'+pipeline_id+'/run')
            assert noop.status_code==200 and noop.json()['unchanged']
            assert all(run['status']=='succeeded' for run in client.get('/api/pipelines/'+pipeline_id+'/runs').json())
            lakehouse.query(f'DROP SCHEMA {schema}')
    print('Live stack verified: OIDC discovery, S3 bucket operations, PostgreSQL snapshot/incremental/no-op extraction, quality-gated watermark safety, Iceberg publication, Trino aggregates, schema evolution and snapshot time travel.')


if __name__ == '__main__':
    verify()
