import io
import json

import duckdb
import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        yield client


def upload(client, content=b'region,revenue,units\nNorth,120,3\nSouth,80,2\nNorth,100,\n', name='sales.csv'):
    return client.post('/api/datasets', files={'file': (name, content)})


def test_import_profile_query_and_persistence(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        response = upload(client)
        assert response.status_code == 201, response.text
        dataset = response.json()
        detail = client.get('/api/datasets/' + dataset['id']).json()
        assert detail['row_count'] == 3
        assert detail['profile'][2]['nulls'] == 1
        sql = f'SELECT region, sum(revenue) AS total FROM {dataset["table_name"]} GROUP BY region ORDER BY total DESC'
        result = client.post('/api/query', json={'sql': sql})
        assert result.status_code == 200, result.text
        assert result.json()['rows'] == [['North', 220], ['South', 80]]
        assert client.post('/api/saved-queries', json={'name': 'Revenue', 'sql': sql}).status_code == 201
    with TestClient(create_app(tmp_path)) as restarted:
        assert len(restarted.get('/api/datasets').json()) == 1
        assert len(restarted.get('/api/saved-queries').json()) == 1
        assert restarted.post('/api/query', json={'sql': sql}).json()['rows'][0][1] == 220


@pytest.mark.parametrize('sql', [
    'CREATE TABLE bad(x INT)', 'INSTALL httpfs', 'SELECT 1; SELECT 2',
    "SELECT * FROM read_csv('/etc/passwd')", "SELECT * FROM read_text('/etc/hostname')",
    "SELECT * FROM read_parquet('https://example.com/file.parquet')",
    "COPY (SELECT 1) TO '/tmp/data-platform-should-not-write.csv'",
])
def test_query_rejects_mutation_and_external_access(client, sql):
    response = client.post('/api/query', json={'sql': sql})
    assert response.status_code == 400, response.text


@pytest.mark.parametrize('content,name', [
    (b'a,a\n1,2', 'duplicate.csv'), (b'a,b\n', 'empty.csv'),
    (b'not a file', 'bad.exe'), (b'[{"nested":{"x":1}}]', 'nested.json'),
    (b'{"x":1}', 'object.json'), (b'a,\n1,2', 'blank.csv'),
])
def test_bad_imports_do_not_publish(client, content, name):
    assert upload(client, content, name).status_code == 400
    assert client.get('/api/datasets').json() == []


def test_json_parquet_and_excel(client):
    assert upload(client, json.dumps([{'x': 1}, {'x': None}]).encode(), 'sample.json').status_code == 201
    import pandas as pd
    buffer = io.BytesIO()
    pd.DataFrame({'x': [3, 4]}).to_excel(buffer, index=False)
    assert upload(client, buffer.getvalue(), 'sample.xlsx').status_code == 201
    con = duckdb.connect()
    # Construct a Parquet file using the same engine, without optional Arrow.
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / 'x.parquet'
        con.execute('COPY (SELECT 2 AS x) TO ? (FORMAT PARQUET)', [str(path)])
        response = upload(client, path.read_bytes(), 'sample.parquet')
        assert response.status_code == 201, response.text
    con.close()


def test_result_limit_and_missing_dataset(client):
    result = client.post('/api/query', json={'sql': 'SELECT * FROM range(2000)'}).json()
    assert len(result['rows']) == 1000 and result['truncated']
    assert client.get('/api/datasets/missing').status_code == 404


def test_browser_boundary(client):
    assert client.get('/api/datasets', headers={'host': 'evil.example'}).status_code == 403
    assert client.post('/api/query', headers={'origin': 'https://evil.example'}, json={'sql': 'SELECT 1'}).status_code == 403
    assert client.get('/').status_code == 200
    assert "default-src 'self'" in client.get('/').headers['content-security-policy']


def test_query_timeout(client, monkeypatch):
    import app.main as main
    monkeypatch.setattr(main, 'QUERY_TIMEOUT', 0.01)
    result = client.post('/api/query', json={'sql': 'SELECT sum(sin(i)) FROM range(1000000000000) t(i)'})
    assert result.status_code == 408
    run = client.get('/api/query-history').json()[0]
    assert run['status'] == 'timed_out'
    assert 'time limit' in run['error']
    assert run['result_rows'] is None


@pytest.mark.parametrize('content', [
    b'a,b\n1,2,3\n4,5,6\n',
    b'a,b\n1\n',
    b'a,b\n"unfinished,2\n',
])
def test_malformed_csv_is_rejected_without_publication(client, content):
    result = upload(client, content)
    assert result.status_code == 400, result.text
    assert client.get('/api/datasets').json() == []


@pytest.mark.parametrize('name,content', [
    ('ids.csv', b'id,label\n9007199254740993,first\n,second\n'),
    ('ids.json', b'[{"id":9007199254740993,"label":"first"},{"id":null,"label":"second"}]'),
])
def test_nullable_large_integer_is_exact_in_storage_and_browser_response(client, name, content):
    response = upload(client, content, name)
    assert response.status_code == 201, response.text
    table = response.json()['table_name']
    result = client.post('/api/query', json={
        'sql': f'SELECT id, id = 9007199254740993 AS exact FROM {table} WHERE id IS NOT NULL'
    })
    assert result.status_code == 200, result.text
    assert result.json()['rows'] == [['9007199254740993', True]]


def test_quoted_csv_records_are_preserved(client):
    result = upload(client, b'label,value\n"a,b",1\n"two\nlines",2\n\n')
    assert result.status_code == 201, result.text
    detail = client.get('/api/datasets/' + result.json()['id']).json()
    assert detail['preview']['rows'] == [['a,b', 1], ['two\nlines', 2]]


def test_original_source_and_checksum_survive_restart(tmp_path):
    import hashlib
    content = b'region,revenue\nNorth,120\n'
    with TestClient(create_app(tmp_path)) as client:
        result = upload(client, content).json()
        assert result['sha256'] == hashlib.sha256(content).hexdigest()
    with TestClient(create_app(tmp_path)) as client:
        detail = client.get('/api/datasets/' + result['id']).json()
        assert detail['source'] == {
            'sha256': result['sha256'], 'format': 'csv', 'original_name': 'sales.csv'
        }
        response = client.get('/api/datasets/' + result['id'] + '/source')
        assert response.status_code == 200
        assert response.content == content
        assert 'attachment' in response.headers['content-disposition']
        assert client.get('/api/datasets/missing/source').status_code == 404


def test_api_reference_uses_only_local_assets(client):
    response = client.get('/docs')
    assert response.status_code == 200
    assert 'cdn.jsdelivr.net' not in response.text
    assert '<script src="/static/docs.js" defer></script>' in response.text
    assert "script-src 'self'" in response.headers['content-security-policy']
    assert client.get('/static/docs.js').status_code == 200
    spec = client.get('/openapi.json').json()
    assert '/api/datasets/{ident}/source' in spec['paths']


def test_import_history_records_success_and_validation_failure(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        assert client.get('/api/imports').json() == []
        good = upload(client).json()
        failure = upload(client, b'a,b\n1,2,3\n', 'broken.csv')
        assert failure.status_code == 400
        history = client.get('/api/imports').json()
        assert len(history) == 2
        failed, succeeded = history
        assert failed['filename'] == 'broken.csv'
        assert failed['status'] == 'failed'
        assert 'expected 2' in failed['error']
        assert failed['dataset_id'] is None
        assert failed['finished_at'] is not None
        assert succeeded['id'] == good['import_id']
        assert succeeded['status'] == 'succeeded'
        assert succeeded['dataset_id'] == good['id']
        assert succeeded['row_count'] == 3
        assert succeeded['error'] is None
    with TestClient(create_app(tmp_path)) as client:
        assert client.get('/api/imports').json() == history
        assert len(client.get('/api/datasets').json()) == 1


def test_restart_marks_unfinished_import_as_interrupted(tmp_path):
    import sqlite3
    create_app(tmp_path)
    with sqlite3.connect(tmp_path / 'catalog.sqlite3') as con:
        con.execute('INSERT INTO imports(id,filename,status,started_at) VALUES (?,?,?,?)',
                    ('unfinished', 'pending.csv', 'running', '2026-10-31T00:00:00+00:00'))
    with TestClient(create_app(tmp_path)) as client:
        attempt = client.get('/api/imports').json()[0]
        assert attempt['status'] == 'interrupted'
        assert attempt['finished_at'] is not None
        assert 'Upload the file again' in attempt['error']
        assert client.get('/api/datasets').json() == []


def test_oversized_import_is_recorded_as_failure(client, monkeypatch):
    import app.main as main
    monkeypatch.setattr(main, 'MAX_UPLOAD', 4)
    assert upload(client).status_code == 413
    attempt = client.get('/api/imports').json()[0]
    assert attempt['status'] == 'failed'
    assert 'Maximum upload' in attempt['error']


def test_import_is_visible_while_processing(client, monkeypatch):
    import threading
    import app.main as main
    entered, release = threading.Event(), threading.Event()
    original = main.load_frame
    def blocked(*args):
        entered.set()
        assert release.wait(10)
        return original(*args)
    monkeypatch.setattr(main, 'load_frame', blocked)
    result = []
    worker = threading.Thread(target=lambda: result.append(upload(client)))
    worker.start()
    try:
        assert entered.wait(10)
        attempt = client.get('/api/imports').json()[0]
        assert attempt['status'] == 'running'
        assert attempt['dataset_id'] is None
    finally:
        release.set()
        worker.join(10)
    assert not worker.is_alive()
    assert result[0].status_code == 201
    assert client.get('/api/imports').json()[0]['status'] == 'succeeded'


def test_rename_preserves_data_source_and_saved_queries(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        dataset = upload(client).json()
        sql = f'SELECT count(*) FROM {dataset["table_name"]}'
        client.post('/api/saved-queries', json={'name': 'Count', 'sql': sql})
        response = client.patch('/api/datasets/' + dataset['id'], json={'name': '  Quarterly sales  '})
        assert response.status_code == 200
        assert response.json()['name'] == 'Quarterly sales'
        detail = client.get('/api/datasets/' + dataset['id']).json()
        assert detail['name'] == 'Quarterly sales'
        assert detail['table_name'] == dataset['table_name']
        assert detail['source']['original_name'] == 'sales.csv'
        assert client.post('/api/query', json={'sql': sql}).json()['rows'] == [[3]]
    with TestClient(create_app(tmp_path)) as client:
        assert client.get('/api/datasets').json()[0]['name'] == 'Quarterly sales'
        assert client.get('/api/saved-queries').json()[0]['sql'] == sql


@pytest.mark.parametrize('name', ['', '   ', 'line\nbreak', 'x' * 201])
def test_invalid_dataset_name_is_rejected(client, name):
    dataset = upload(client).json()
    response = client.patch('/api/datasets/' + dataset['id'], json={'name': name})
    assert response.status_code == 422
    assert client.get('/api/datasets').json()[0]['name'] == 'sales.csv'


def test_delete_requires_current_name_and_removes_only_selected_dataset(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        first = upload(client).json()
        second = upload(client, b'x\n42\n', 'keep.csv').json()
        endpoint = '/api/datasets/' + first['id']
        assert client.request('DELETE', endpoint, json={'confirmation_name': 'wrong'}).status_code == 409
        assert client.get(endpoint + '/source').status_code == 200
        client.patch(endpoint, json={'name': 'Renamed'})
        assert client.request('DELETE', endpoint, json={'confirmation_name': 'sales.csv'}).status_code == 409
        response = client.request('DELETE', endpoint, json={'confirmation_name': 'Renamed'})
        assert response.status_code == 200
        assert not (tmp_path / 'sources' / first['id']).exists()
        assert (tmp_path / 'sources' / second['id']).exists()
        assert client.get(endpoint).status_code == 404
        assert client.get(endpoint + '/source').status_code == 404
        with duckdb.connect(str(tmp_path / 'datasets.duckdb')) as storage:
            tables = [row[0] for row in storage.execute('SHOW TABLES').fetchall()]
        assert first['table_name'] not in tables
        assert second['table_name'] in tables
        history = client.get('/api/imports').json()
        deleted_import = next(item for item in history if item['id'] == first['import_id'])
        assert deleted_import['status'] == 'succeeded'
        assert not deleted_import['dataset_available']
        assert any(event['action'] == 'dataset.deleted' for event in client.get('/api/activity').json())
    with TestClient(create_app(tmp_path)) as client:
        assert [row['id'] for row in client.get('/api/datasets').json()] == [second['id']]


@pytest.mark.parametrize('recovery', ['restart', 'retry'])
def test_failed_deletion_can_resume_without_orphaning_metadata(tmp_path, monkeypatch, recovery):
    from pathlib import Path
    with TestClient(create_app(tmp_path)) as client:
        dataset = upload(client).json()
        endpoint = '/api/datasets/' + dataset['id']
        original = Path.unlink
        def blocked(path, *args, **kwargs):
            if path == tmp_path / 'sources' / dataset['id']:
                raise PermissionError('injected cleanup failure')
            return original(path, *args, **kwargs)
        with monkeypatch.context() as patch:
            patch.setattr(Path, 'unlink', blocked)
            assert client.request('DELETE', endpoint, json={'confirmation_name': 'sales.csv'}).status_code == 503
            assert client.get('/api/datasets').json()[0]['deletion_pending']
            assert client.get(endpoint).status_code == 409
            assert client.patch(endpoint, json={'name': 'Other'}).status_code == 409
        if recovery == 'retry':
            assert client.request('DELETE', endpoint, json={'confirmation_name': 'sales.csv'}).status_code == 200
    with TestClient(create_app(tmp_path)) as client:
        assert client.get('/api/datasets').json() == []
        assert not (tmp_path / 'sources' / dataset['id']).exists()
        assert len([e for e in client.get('/api/activity').json() if e['action'] == 'dataset.deleted']) == 1


def test_dataset_management_missing_and_browser_origin_checks(client):
    assert client.patch('/api/datasets/missing', json={'name': 'Name'}).status_code == 404
    assert client.request('DELETE', '/api/datasets/missing', json={'confirmation_name': 'Name'}).status_code == 404
    dataset = upload(client).json()
    endpoint = '/api/datasets/' + dataset['id']
    assert client.request('DELETE', endpoint, json={'confirmation_name': 'sales.csv'},
                          headers={'origin': 'https://evil.example'}).status_code == 403
    assert client.patch(endpoint, json={'name': 'Wrong'},
                        headers={'origin': 'https://evil.example'}).status_code == 403


def test_saved_query_edit_and_delete_persist_without_executing_sql(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        dataset = upload(client).json()
        original = {'name': 'First draft', 'sql': 'SELECT 1'}
        ident = client.post('/api/saved-queries', json=original).json()['id']
        other = client.post('/api/saved-queries', json={'name': 'Keep', 'sql': 'SELECT 2'}).json()['id']
        # Saved SQL may be an unfinished draft; saving must never execute it.
        updated = {'name': '  Updated draft  ', 'sql': 'DROP TABLE ' + dataset['table_name']}
        response = client.put('/api/saved-queries/' + ident, json={**updated, 'expected': original})
        assert response.status_code == 200, response.text
        assert response.json()['name'] == 'Updated draft'
        assert client.get('/api/datasets/' + dataset['id']).status_code == 200
    with TestClient(create_app(tmp_path)) as client:
        saved = next(q for q in client.get('/api/saved-queries').json() if q['id'] == ident)
        assert saved['name'] == 'Updated draft'
        assert saved['sql'] == updated['sql']
        snapshot = {'name': saved['name'], 'sql': saved['sql']}
        response = client.request('DELETE', '/api/saved-queries/' + ident, json=snapshot)
        assert response.status_code == 200
        assert client.get('/api/datasets/' + dataset['id']).status_code == 200
        assert [q['id'] for q in client.get('/api/saved-queries').json()] == [other]
        actions = [e['action'] for e in client.get('/api/activity').json()]
        assert 'query.updated' in actions and 'query.deleted' in actions
    with TestClient(create_app(tmp_path)) as client:
        assert [q['id'] for q in client.get('/api/saved-queries').json()] == [other]


def test_stale_saved_query_edit_and_delete_are_rejected(client):
    original = {'name': 'Original', 'sql': 'SELECT 1'}
    ident = client.post('/api/saved-queries', json=original).json()['id']
    endpoint = '/api/saved-queries/' + ident
    updated = {'name': 'Changed elsewhere', 'sql': 'SELECT 2'}
    assert client.put(endpoint, json={**updated, 'expected': original}).status_code == 200
    assert client.put(endpoint, json={**original, 'expected': original}).status_code == 409
    assert client.request('DELETE', endpoint, json=original).status_code == 409
    saved = client.get('/api/saved-queries').json()[0]
    assert saved['name'] == updated['name'] and saved['sql'] == updated['sql']


@pytest.mark.parametrize('changes', [
    {'name': '   '}, {'sql': ' \n '}, {'name': 'bad\nname'},
    {'name': 'x' * 121}, {'sql': 'x' * 20001},
])
def test_invalid_saved_query_updates_do_not_change_saved_text(client, changes):
    original = {'name': 'Valid', 'sql': 'SELECT 1'}
    ident = client.post('/api/saved-queries', json=original).json()['id']
    assert client.post('/api/saved-queries', json={**original, **changes}).status_code == 422
    assert client.put('/api/saved-queries/' + ident,
                      json={**original, **changes, 'expected': original}).status_code == 422
    saved = client.get('/api/saved-queries').json()[0]
    assert saved['name'] == original['name'] and saved['sql'] == original['sql']


def test_saved_query_management_missing_and_origin_checks(client):
    snapshot = {'name': 'Query', 'sql': 'SELECT 1'}
    assert client.put('/api/saved-queries/missing', json={**snapshot, 'expected': snapshot}).status_code == 404
    assert client.request('DELETE', '/api/saved-queries/missing', json=snapshot).status_code == 404
    ident = client.post('/api/saved-queries', json=snapshot).json()['id']
    headers = {'origin': 'https://evil.example'}
    assert client.put('/api/saved-queries/' + ident, json={**snapshot, 'expected': snapshot}, headers=headers).status_code == 403
    assert client.request('DELETE', '/api/saved-queries/' + ident, json=snapshot, headers=headers).status_code == 403



def test_query_history_success_failure_and_restart(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        assert client.get('/api/query-history').json() == []
        dataset = upload(client).json()
        assert client.get('/api/datasets/' + dataset['id']).status_code == 200
        assert client.get('/api/query-history').json() == []  # Internal previews are excluded.
        response = client.post('/api/query', json={'sql': 'SELECT * FROM range(2000)'})
        assert response.status_code == 200
        first = client.get('/api/query-history').json()[0]
        assert first['id'] == response.json()['query_id']
        assert first['status'] == 'succeeded'
        assert first['sql'] == 'SELECT * FROM range(2000)'
        assert first['result_rows'] == 1000 and first['truncated']
        assert first['elapsed_ms'] >= 0 and first['finished_at']
        assert first['error'] is None
        bad_sql = 'SELECT * FROM missing_history_test_table'
        assert client.post('/api/query', json={'sql': bad_sql}).status_code == 400
        history = client.get('/api/query-history').json()
        failed = history[0]
        assert failed['status'] == 'failed'
        assert failed['sql'] == bad_sql
        assert 'missing_history_test_table' in failed['error']
        assert failed['result_rows'] is None and failed['truncated'] is None
        assert len(history) == 2
    with TestClient(create_app(tmp_path)) as client:
        assert client.get('/api/query-history').json() == history


def test_query_history_preserves_empty_results(client):
    response = client.post('/api/query', json={'sql': 'SELECT 1 WHERE false'})
    assert response.status_code == 200
    run = client.get('/api/query-history').json()[0]
    assert run['status'] == 'succeeded'
    assert run['result_rows'] == 0 and not run['truncated']


def test_query_history_interruption_and_latest_limit(tmp_path):
    import sqlite3
    from datetime import datetime, timezone, timedelta
    create_app(tmp_path)
    with sqlite3.connect(tmp_path / 'catalog.sqlite3') as con:
        for i in range(105):
            started = (datetime(2026, 10, 31, tzinfo=timezone.utc) + timedelta(seconds=i)).isoformat()
            con.execute('INSERT INTO query_runs(id,sql,status,started_at) VALUES (?,?,?,?)',
                        (str(i), f'SELECT {i}', 'running' if i == 104 else 'succeeded', started))
    with TestClient(create_app(tmp_path)) as client:
        history = client.get('/api/query-history').json()
        assert len(history) == 100
        assert history[0]['id'] == '104' and history[-1]['id'] == '5'
        assert history[0]['status'] == 'interrupted'
        assert history[0]['finished_at'] and 'server stopped' in history[0]['error']
        assert all(run['status'] == 'succeeded' for run in history[1:])


def test_query_history_tracks_in_progress_request(client, monkeypatch):
    import threading
    import app.main as main
    entered, release = threading.Event(), threading.Event()
    # Hold process creation after the request was recorded, without an expensive query.
    context = main.mp.get_context('spawn')
    process = context.Process
    def delayed_process(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return process(*args, **kwargs)
    monkeypatch.setattr(context, 'Process', delayed_process)
    monkeypatch.setattr(main.mp, 'get_context', lambda _: context)
    responses = []
    worker = threading.Thread(target=lambda: responses.append(client.post('/api/query', json={'sql': 'SELECT 1'})))
    worker.start()
    try:
        assert entered.wait(10)
        run = client.get('/api/query-history').json()[0]
        assert run['status'] == 'running' and run['finished_at'] is None
    finally:
        release.set()
        worker.join(15)
    assert not worker.is_alive()
    assert responses[0].status_code == 200
    assert client.get('/api/query-history').json()[0]['status'] == 'succeeded'



def test_dataset_csv_export_includes_all_rows_and_cleans_tempfiles(client, monkeypatch):
    import csv
    import app.main as main
    from pathlib import Path
    directories = []
    original = main.tempfile.TemporaryDirectory
    def tracked(*args, **kwargs):
        directory = original(*args, **kwargs)
        directories.append(Path(directory.name))
        return directory
    monkeypatch.setattr(main.tempfile, 'TemporaryDirectory', tracked)
    content = ('number,label\n' + ''.join(f'{i},row-{i}\n' for i in range(1005))).encode()
    dataset = upload(client, content, 'all-rows.csv').json()
    response = client.get('/api/datasets/' + dataset['id'] + '/export')
    assert response.status_code == 200, response.text
    rows = list(csv.reader(io.StringIO(response.content.decode('utf-8-sig'))))
    assert rows[0] == ['number', 'label']
    assert len(rows) == 1006
    assert rows[1] == ['0', 'row-0'] and rows[-1] == ['1004', 'row-1004']
    assert response.headers['x-export-row-count'] == '1005'
    assert response.headers['cache-control'] == 'no-store'
    assert 'all-rows.csv' in response.headers['content-disposition']
    assert response.content.startswith(b'\xef\xbb\xbf')
    assert all(not directory.exists() for directory in directories)
    assert client.get('/api/query-history').json() == []
    assert any(e['action'] == 'dataset.exported' for e in client.get('/api/activity').json())


def test_dataset_export_preserves_digits_and_quotes_spreadsheet_text(client):
    import csv
    content = json.dumps([{'=header': '=1+1', 'space': '  =cmd', 'at': '@SUM(A1)',
                          'minus': '-word', 'plus': '+word', 'number': -42,
                          'large': 9007199254740993, 'quoted': 'a,"b"\n漢字', 'missing': None}]).encode()
    dataset = upload(client, content, 'special.json').json()
    response = client.get('/api/datasets/' + dataset['id'] + '/export')
    assert response.status_code == 200, response.text
    rows = list(csv.reader(io.StringIO(response.content.decode('utf-8-sig'))))
    assert rows[0][0] == "'=header"
    assert rows[1] == ["'=1+1", "'  =cmd", "'@SUM(A1)", "'-word", "'+word", '-42',
                       '9007199254740993', 'a,"b"\n漢字', '']


@pytest.mark.parametrize('name', ['Quarterly sales', 'Région 北'])
def test_export_uses_renamed_dataset_filename_and_rejects_deleted_dataset(client, name):
    dataset = upload(client).json()
    endpoint = '/api/datasets/' + dataset['id']
    assert client.patch(endpoint, json={'name': name}).status_code == 200
    response = client.get(endpoint + '/export')
    assert response.status_code == 200
    from email.message import Message
    header = Message()
    header['Content-Disposition'] = response.headers['content-disposition']
    assert header.get_filename() == name + '.csv'
    assert client.request('DELETE', endpoint, json={'confirmation_name': name}).status_code == 200
    assert client.get(endpoint + '/export').status_code == 404
    assert client.get('/api/datasets/missing/export').status_code == 404
    assert client.get(endpoint + '/export', headers={'host': 'evil.example'}).status_code == 403


def test_export_pending_deletion_and_failure_clean_tempfiles(tmp_path, monkeypatch):
    import app.main as main
    import sqlite3
    from pathlib import Path
    directories = []
    original = main.tempfile.TemporaryDirectory
    def tracked(*args, **kwargs):
        directory = original(*args, **kwargs)
        directories.append(Path(directory.name))
        return directory
    with TestClient(create_app(tmp_path)) as client:
        dataset = upload(client).json()
        monkeypatch.setattr(main.tempfile, 'TemporaryDirectory', tracked)
        with sqlite3.connect(tmp_path / 'catalog.sqlite3') as con:
            con.execute('INSERT INTO dataset_deletions VALUES (?,?)', (dataset['id'], '2026-10-31'))
        assert client.get('/api/datasets/' + dataset['id'] + '/export').status_code == 409
        assert client.get('/api/datasets/missing/export').status_code == 404
        assert all(not directory.exists() for directory in directories)
        assert (tmp_path / 'sources' / dataset['id']).is_file()


def test_export_storage_failure_removes_partial_download(tmp_path, monkeypatch):
    import app.main as main
    from pathlib import Path
    directories = []
    original_directory = main.tempfile.TemporaryDirectory
    def tracked(*args, **kwargs):
        directory = original_directory(*args, **kwargs)
        directories.append(Path(directory.name))
        return directory
    app = create_app(tmp_path)
    with TestClient(app, raise_server_exceptions=False) as client:
        dataset = upload(client).json()
        monkeypatch.setattr(main.tempfile, 'TemporaryDirectory', tracked)
        def broken_cell(value):
            raise OSError('injected export failure')
        monkeypatch.setattr(main, 'csv_cell', broken_cell)
        assert client.get('/api/datasets/' + dataset['id'] + '/export').status_code == 500
        assert directories and all(not directory.exists() for directory in directories)
        assert client.get('/api/datasets').json()[0]['id'] == dataset['id']
