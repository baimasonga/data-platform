import io
import json
import sqlite3
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from app.main import create_app


def upload(client, content=b'x\n1\n2\n', target=None):
    path = '/api/datasets' + ('/' + target + '/versions' if target else '')
    return client.post(path, files={'file': ('sample.csv', content)})


def sql(client, statement):
    response = client.post('/api/query', json={'sql': statement})
    assert response.status_code == 200, response.text
    return response.json()['rows']


def test_versions_stable_sql_restore_and_source(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        first = upload(client).json()
        second = upload(client, b'x\n3\n4\n5\n', first['id'])
        assert second.status_code == 201, second.text
        assert second.json()['table_name'] == first['table_name']
        assert second.json()['version'] == 2
        assert sql(client, f'SELECT sum(x) FROM {first["table_name"]}') == [[12]]
        versions = client.get('/api/datasets/' + first['id'] + '/versions').json()
        assert [v['number'] for v in versions] == [2, 1]
        assert sql(client, f'SELECT sum(x) FROM {versions[1]["table_name"]}') == [[3]]
        assert client.get('/api/datasets/' + first['id'] + '/source').content == b'x\n3\n4\n5\n'
        restored = client.post('/api/datasets/' + first['id'] + '/versions/' + first['version_id'] + '/restore')
        assert restored.status_code == 201, restored.text
        assert restored.json()['version'] == 3
        assert sql(client, f'SELECT sum(x) FROM {first["table_name"]}') == [[3]]
        assert client.get('/api/datasets/' + first['id'] + '/source').content == b'x\n1\n2\n'
    with TestClient(create_app(tmp_path)) as client:
        assert len(client.get('/api/datasets/' + first['id'] + '/versions').json()) == 3
        assert sql(client, f'SELECT sum(x) FROM {first["table_name"]}') == [[3]]


def test_quality_gates_preserve_published_version(client):
    dataset = upload(client).json()
    endpoint = '/api/datasets/' + dataset['id']
    rules = [{'column_name': 'x', 'rule': 'not_null'}, {'column_name': 'x', 'rule': 'unique'},
             {'column_name': 'x', 'rule': 'min', 'threshold': 0}, {'column_name': 'x', 'rule': 'max', 'threshold': 10}]
    assert client.put(endpoint + '/quality', json={'rules': rules}).status_code == 200
    assert client.post(endpoint + '/quality/run').json()['passed']
    for content in [b'x\n-1\n2\n', b'x\n1\n1\n', b'x\n30\n', b'y\n1\n']:
        failed = upload(client, content, dataset['id'])
        assert failed.status_code == 400, failed.text
        assert 'Quality contract failed' in failed.text
    assert len(client.get(endpoint + '/versions').json()) == 1
    assert sql(client, f'SELECT sum(x) FROM {dataset["table_name"]}') == [[3]]
    runs = client.get(endpoint + '/quality').json()['runs']
    assert any(not run['passed'] for run in runs)
    assert upload(client, b'x\n4\n5\n', dataset['id']).status_code == 201


def test_transformations_all_rows_and_lineage(client):
    source = upload(client).json()
    statement = f'SELECT x * 10 AS amount FROM {source["table_name"]}'
    response = client.post('/api/transformations', json={'name': 'Amounts', 'sql': statement})
    assert response.status_code == 201, response.text
    output = response.json()
    assert sql(client, f'SELECT sum(amount) FROM {output["table_name"]}') == [[30]]
    lineage = client.get('/api/datasets/' + output['id'] + '/lineage').json()['versions'][0]
    assert lineage['sql'] == statement
    assert lineage['kind'] == 'transformation'
    assert lineage['inputs'][0]['dataset_id'] == source['id']
    assert lineage['inputs'][0]['version_id'] == source['version_id']
    response = client.post('/api/transformations', json={'name': 'Many rows', 'sql': 'SELECT * FROM range(1005)'})
    assert response.status_code == 201, response.text
    assert response.json()['row_count'] == 1005
    for invalid in ['SELECT * FROM range(100001)', 'CREATE TABLE bad(x INT)', "SELECT * FROM read_text('/etc/passwd')", 'SELECT 1; SELECT 2']:
        assert client.post('/api/transformations', json={'name': 'Invalid', 'sql': invalid}).status_code == 400


def test_retry_is_idempotent_and_retains_source(client, monkeypatch):
    import app.main as main
    with monkeypatch.context() as patch:
        patch.setattr(main, 'prepare_dataset', lambda *args: (_ for _ in ()).throw(ValueError('temporary worker failure')))
        assert upload(client).status_code == 400
    attempt = client.get('/api/imports').json()[0]
    assert attempt['retry_available']
    retried = client.post('/api/imports/' + attempt['id'] + '/retry')
    assert retried.status_code == 200, retried.text
    repeated = client.post('/api/imports/' + attempt['id'] + '/retry')
    assert repeated.status_code == 200 and repeated.json()['already_published']
    assert len(client.get('/api/datasets').json()) == 1
    assert len(client.get('/api/datasets/' + retried.json()['id'] + '/versions').json()) == 1


def test_ready_publication_recovers_exactly_once(tmp_path, monkeypatch):
    from app.workflows import Workflows
    with TestClient(create_app(tmp_path)) as client:
        with monkeypatch.context() as patch:
            patch.setattr(Workflows, 'complete', lambda *args: (_ for _ in ()).throw(OSError('injected metadata outage')))
            assert upload(client).status_code == 400
    with TestClient(create_app(tmp_path)) as client:
        datasets = client.get('/api/datasets').json()
        assert len(datasets) == 1
        assert len(client.get('/api/datasets/' + datasets[0]['id'] + '/versions').json()) == 1
        assert client.get('/api/imports').json()[0]['status'] == 'succeeded'
        assert client.get('/api/system/readiness').json()['pending_publications'] == 0
    with TestClient(create_app(tmp_path)) as client:
        assert len(client.get('/api/datasets').json()) == 1


def test_legacy_migration_preserves_identity_and_saved_sql(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        dataset = upload(client).json()
    with sqlite3.connect(tmp_path / 'catalog.sqlite3') as con:
        version = con.execute('SELECT table_name FROM dataset_versions').fetchone()[0]
        con.execute('DELETE FROM dataset_versions')
    with duckdb.connect(str(tmp_path / 'datasets.duckdb')) as storage:
        storage.execute(f'DROP VIEW {dataset["table_name"]}')
        storage.execute(f'ALTER TABLE {version} RENAME TO {dataset["table_name"]}')
    with TestClient(create_app(tmp_path)) as client:
        assert sql(client, f'SELECT sum(x) FROM {dataset["table_name"]}') == [[3]]
        versions = client.get('/api/datasets/' + dataset['id'] + '/versions').json()
        assert len(versions) == 1 and versions[0]['number'] == 1
        assert client.get('/api/system/readiness').json()['metadata_migrations'] == [1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_orphan_reconciliation_reports_without_deleting(tmp_path):
    create_app(tmp_path)
    table = 'ver_' + 'a' * 32
    with duckdb.connect(str(tmp_path / 'datasets.duckdb')) as storage:
        storage.execute(f'CREATE TABLE {table}(x INT)')
    with TestClient(create_app(tmp_path)) as client:
        findings = client.get('/api/system/readiness').json()['recovery_findings']
        assert any(item['resource'] == table and item['kind'] == 'unreferenced_table' for item in findings)
    with duckdb.connect(str(tmp_path / 'datasets.duckdb')) as storage:
        assert storage.execute(f'SELECT count(*) FROM {table}').fetchone() == (0,)


def test_delete_removes_all_versions_and_retained_job_sources(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        dataset = upload(client).json()
        upload(client, b'x\n9\n', dataset['id'])
        versions = client.get('/api/datasets/' + dataset['id'] + '/versions').json()
        attempts = client.get('/api/imports').json()
        assert client.request('DELETE', '/api/datasets/' + dataset['id'], json={'confirmation_name': 'sample.csv'}).status_code == 200
        assert all(not (tmp_path / 'versions' / version['id']).exists() for version in versions)
        assert all(not (tmp_path / 'staged' / attempt['id']).exists() for attempt in attempts)
        with duckdb.connect(str(tmp_path / 'datasets.duckdb')) as storage:
            assert storage.execute('SHOW TABLES').fetchall() == []


def test_multipart_limit_applies_before_upload_handler(tmp_path, monkeypatch):
    import app.main as main
    monkeypatch.setattr(main, 'MAX_UPLOAD', 4)
    with TestClient(create_app(tmp_path)) as client:
        response = client.post('/api/datasets', content=b'x' * (1024 * 1024 + 5), headers={'content-type': 'multipart/form-data; boundary=x'})
        assert response.status_code == 413
        assert client.get('/api/imports').json() == []


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path)) as instance:
        yield instance



def test_pipeline_runs_update_same_dataset_and_history(client):
    definition = {'name': 'Totals', 'sql': 'SELECT 1 AS amount'}
    created = client.post('/api/pipelines', json=definition)
    assert created.status_code == 201, created.text
    ident = created.json()['id']
    first = client.post('/api/pipelines/' + ident + '/run')
    assert first.status_code == 200, first.text
    second = client.post('/api/pipelines/' + ident + '/run')
    assert second.status_code == 200, second.text
    assert first.json()['id'] == second.json()['id']
    assert second.json()['version'] == 2
    runs = client.get('/api/pipelines/' + ident + '/runs').json()
    assert len(runs) == 2 and all(run['status'] == 'succeeded' for run in runs)
    assert client.post('/api/pipelines/' + ident + '/runs/' + runs[0]['id'] + '/retry').json()['already_published']


def test_pipeline_failure_retry_uses_recorded_sql(client, monkeypatch):
    import app.main as main
    ident = client.post('/api/pipelines', json={'name': 'Retryable', 'sql': 'SELECT 2 AS x'}).json()['id']
    with monkeypatch.context() as patch:
        patch.setattr(main, 'prepare_dataset', lambda *args: (_ for _ in ()).throw(ValueError('injected failure')))
        assert client.post('/api/pipelines/' + ident + '/run').status_code == 400
    run = client.get('/api/pipelines/' + ident + '/runs').json()[0]
    assert run['status'] == 'failed'
    retried = client.post('/api/pipelines/' + ident + '/runs/' + run['id'] + '/retry')
    assert retried.status_code == 200, retried.text
    assert client.get('/api/pipelines/' + ident + '/runs').json()[0]['status'] == 'succeeded'


def test_schedule_due_run_and_pause(tmp_path):
    app = create_app(tmp_path)
    # Explicit ticks test the actual scheduler without a racing wall-clock thread.
    client = TestClient(app)
    created = client.post('/api/pipelines', json={'name': 'Scheduled', 'sql': 'SELECT 3 AS x', 'enabled': True, 'interval_seconds': 60})
    assert created.status_code == 201, created.text
    ident = created.json()['id']
    with sqlite3.connect(tmp_path / 'catalog.sqlite3') as con:
        con.execute("UPDATE pipelines SET next_run='2000-01-01T00:00:00+00:00' WHERE id=?", (ident,))
    app.state.pipelines.tick()
    runs = client.get('/api/pipelines/' + ident + '/runs').json()
    assert len(runs) == 1 and runs[0]['status'] == 'succeeded'
    app.state.pipelines.tick()
    assert len(client.get('/api/pipelines/' + ident + '/runs').json()) == 1
    definition = client.get('/api/pipelines').json()[0]
    assert client.put('/api/pipelines/' + ident, json={**definition, 'enabled': False}).status_code == 200
    assert not client.get('/api/pipelines').json()[0]['enabled']
    assert client.post('/api/pipelines', json={'name': 'Too fast', 'sql': 'SELECT 1', 'enabled': True, 'interval_seconds': 1}).status_code == 400



def test_pipeline_idempotency_key_prevents_duplicate_versions(client):
    ident=client.post('/api/pipelines',json={'name':'Idempotent','sql':'SELECT 1 AS x'}).json()['id']
    first=client.post('/api/pipelines/'+ident+'/run',headers={'Idempotency-Key':'external-run-123'})
    assert first.status_code==200,first.text
    repeated=client.post('/api/pipelines/'+ident+'/run',headers={'Idempotency-Key':'external-run-123'})
    assert repeated.status_code==200 and repeated.json()['already_published']
    assert len(client.get('/api/datasets/'+first.json()['id']+'/versions').json())==1


def test_operational_metrics_and_request_ids(client):
    response=client.get('/api/datasets')
    assert len(response.headers['x-request-id'])==32
    metrics=client.get('/api/metrics').json()
    assert metrics['http']['requests']>=1
    assert metrics['catalog']['datasets']==0



def test_lineage_dependencies_block_accidental_source_deletion(client):
    source=upload(client).json()
    derived=client.post('/api/transformations',json={'name':'Dependent','sql':'SELECT * FROM '+source['table_name']})
    assert derived.status_code==201,derived.text
    deleted=client.request('DELETE','/api/datasets/'+source['id'],json={'confirmation_name':'sample.csv'})
    assert deleted.status_code==409 and 'depend' in deleted.text
    assert client.request('DELETE','/api/datasets/'+derived.json()['id'],json={'confirmation_name':'Dependent.parquet'}).status_code==200
    assert client.request('DELETE','/api/datasets/'+source['id'],json={'confirmation_name':'sample.csv'}).status_code==200


def test_pipeline_deletion_keeps_output_dataset(client):
    ident=client.post('/api/pipelines',json={'name':'Disposable pipeline','sql':'SELECT 1 AS x'}).json()['id']
    result=client.post('/api/pipelines/'+ident+'/run').json()
    assert client.request('DELETE','/api/datasets/'+result['id'],json={'confirmation_name':'Disposable pipeline.parquet'}).status_code==409
    assert client.request('DELETE','/api/pipelines/'+ident,json={'confirmation_name':'wrong'}).status_code==409
    assert client.request('DELETE','/api/pipelines/'+ident,json={'confirmation_name':'Disposable pipeline'}).status_code==200
    assert client.get('/api/datasets/'+result['id']).status_code==200



def test_retry_source_cleanup_keeps_version_and_history(client):
    dataset=upload(client).json()
    removed=client.request('DELETE','/api/imports/'+dataset['import_id']+'/retained-source',json={'confirmation_name':'sample.csv'})
    assert removed.status_code==200
    attempt=client.get('/api/imports').json()[0]
    assert not attempt['retry_available'] and attempt['status']=='succeeded'
    assert client.get('/api/datasets/'+dataset['id']+'/source').content==b'x\n1\n2\n'
