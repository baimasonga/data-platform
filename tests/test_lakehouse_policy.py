import threading
import pytest
from sqlglot.errors import ParseError
from app.main import create_app
from app.workflows import Workflows
from app.lakehouse import Lakehouse


@pytest.fixture
def lakehouse(tmp_path):
    create_app(tmp_path)
    workflows=Workflows(tmp_path,threading.RLock())
    lake=Lakehouse(workflows)
    with workflows.catalog() as con:
        con.execute('INSERT INTO lakehouse_exports(version_id,dataset_id,table_name,state,created_at) VALUES(?,?,?,?,?)',
            ('version','dataset',f'lakehouse."{lake.schema}"."ds_demo_v1"','published','2026-10-31'))
    return lake


@pytest.mark.parametrize('sql',[
    'SELECT * FROM ds_demo_v1',
    'WITH summary AS (SELECT count(*) AS n FROM ds_demo_v1) SELECT * FROM summary',
    'WITH a AS (SELECT * FROM ds_demo_v1), b AS (SELECT * FROM a) SELECT * FROM b',
    'SELECT * FROM (WITH t AS (SELECT * FROM ds_demo_v1) SELECT * FROM t) AS nested',
    "SELECT 'system.runtime.queries' AS harmless_text",
    'SELECT 42 AS value',
])
def test_owned_or_tableless_select_is_accepted(lakehouse,sql):
    result=lakehouse.validate_select(sql)
    assert result
    if 'ds_demo_v1' in sql:
        assert lakehouse.schema in result


@pytest.mark.parametrize('sql',[
    'SELECT * FROM system.runtime.queries',
    'SELECT * FROM lakehouse.information_schema.tables',
    'SELECT * FROM lakehouse.ws_other.ds_demo_v1',
    'SELECT * FROM unknown_table',
    'SELECT * FROM ds_demo_v1 UNION ALL SELECT * FROM lakehouse.ws_other.secret',
    "SELECT * FROM TABLE(system.query(query=>'SELECT * FROM secret'))",
    'SELECT 1; DROP TABLE ds_demo_v1',
    'DELETE FROM ds_demo_v1',
    'WITH changed AS (DELETE FROM ds_demo_v1 RETURNING *) SELECT * FROM changed',
    'SELECT * FROM secret WHERE EXISTS (WITH secret AS (SELECT 1) SELECT 1 FROM secret)',
    'WITH a AS (SELECT * FROM secret), secret AS (SELECT 1) SELECT * FROM a',
])
def test_cross_workspace_system_and_mutating_queries_are_rejected(lakehouse,sql):
    with pytest.raises((ValueError,ParseError)):
        lakehouse.validate_select(sql)



def test_policy_caps_rows_without_discarding_order(lakehouse):
    result=lakehouse.validate_select('SELECT * FROM ds_demo_v1 ORDER BY id DESC LIMIT 10000')
    assert 'ORDER BY id DESC' in result and 'LIMIT 1001' in result
    assert 'LIMIT 10' in lakehouse.validate_select('SELECT * FROM ds_demo_v1 LIMIT 10')


def test_distinct_workspaces_reject_each_others_published_tables_before_transport(tmp_path, monkeypatch):
    lakes = []
    for index, name in enumerate(('ds_alice_v1', 'ds_bob_v1')):
        base = tmp_path / str(index)
        create_app(base)
        workflows = Workflows(base, threading.RLock())
        lake = Lakehouse(workflows)
        with workflows.catalog() as con:
            con.execute('INSERT INTO lakehouse_exports(version_id,dataset_id,table_name,state,created_at) VALUES(?,?,?,?,?)',
                        (name, name, f'lakehouse."{lake.schema}"."{name}"', 'published', '2026-11-11'))
        lakes.append(lake)
    alice, bob = lakes
    sent = []
    monkeypatch.setattr(alice, 'query', lambda sql, interactive=False: sent.append(sql) or {'columns': ['value'], 'rows': [[1]]})
    assert alice.select('SELECT * FROM ds_alice_v1')['rows'] == [[1]]
    for sql in ('SELECT * FROM ds_bob_v1', f'SELECT * FROM lakehouse."{bob.schema}"."ds_bob_v1"',
                f'WITH allowed AS (SELECT * FROM ds_alice_v1) SELECT * FROM allowed UNION ALL SELECT * FROM lakehouse."{bob.schema}"."ds_bob_v1"'):
        with pytest.raises(ValueError):
            alice.select(sql)
    assert len(sent) == 1
