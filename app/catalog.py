"""Versioned, additive metadata migrations for existing installations."""
from contextlib import contextmanager
import sqlite3


@contextmanager
def connection(path):
    con = sqlite3.connect(path, timeout=30)
    con.row_factory = sqlite3.Row
    try:
        with con:
            yield con
    finally:
        con.close()


def migrate(path):
    with connection(path) as con:
        con.executescript('''
        CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS import_jobs(
          import_id TEXT PRIMARY KEY, filename TEXT NOT NULL, target_id TEXT,
          retries INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS dataset_versions(
          id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, number INTEGER NOT NULL,
          table_name TEXT UNIQUE NOT NULL, row_count INTEGER NOT NULL, column_count INTEGER NOT NULL,
          profile TEXT NOT NULL, size_bytes INTEGER NOT NULL, sha256 TEXT, format TEXT,
          original_name TEXT, created_at TEXT NOT NULL, kind TEXT NOT NULL, sql TEXT,
          inputs TEXT NOT NULL, UNIQUE(dataset_id,number));
        CREATE TABLE IF NOT EXISTS publications(
          id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, table_name TEXT NOT NULL,
          state TEXT NOT NULL, payload TEXT, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS quality_rules(
          dataset_id TEXT NOT NULL, column_name TEXT NOT NULL, rule TEXT NOT NULL,
          threshold REAL, PRIMARY KEY(dataset_id,column_name,rule));
        CREATE TABLE IF NOT EXISTS quality_runs(
          id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, created_at TEXT NOT NULL,
          passed INTEGER NOT NULL, results TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS recovery_findings(
          resource TEXT PRIMARY KEY, kind TEXT NOT NULL, detected_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS connectors(
          id TEXT PRIMARY KEY, name TEXT NOT NULL, host TEXT NOT NULL, port INTEGER NOT NULL,
          database_name TEXT NOT NULL, username TEXT NOT NULL, credential TEXT NOT NULL,
          source_schema TEXT NOT NULL, source_table TEXT NOT NULL, mode TEXT NOT NULL,
          watermark_column TEXT, watermark TEXT, schema_json TEXT, target_id TEXT, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS lakehouse_exports(
          version_id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, table_name TEXT NOT NULL,
          state TEXT NOT NULL, created_at TEXT NOT NULL, error TEXT);
        CREATE TABLE IF NOT EXISTS pipelines(
          id TEXT PRIMARY KEY, name TEXT NOT NULL, sql TEXT NOT NULL, target_id TEXT,
          interval_seconds INTEGER NOT NULL, enabled INTEGER NOT NULL,
          next_run TEXT, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pipeline_request_keys(key TEXT PRIMARY KEY,pipeline_id TEXT NOT NULL,run_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pipeline_runs(
          id TEXT PRIMARY KEY, pipeline_id TEXT NOT NULL, sql TEXT NOT NULL, status TEXT NOT NULL,
          started_at TEXT NOT NULL, finished_at TEXT, dataset_id TEXT, error TEXT);
        INSERT OR IGNORE INTO schema_migrations VALUES(1,strftime('%Y-%m-%dT%H:%M:%fZ','now'));
        INSERT OR IGNORE INTO schema_migrations VALUES(3,strftime('%Y-%m-%dT%H:%M:%fZ','now'));
        INSERT OR IGNORE INTO schema_migrations VALUES(2,strftime('%Y-%m-%dT%H:%M:%fZ','now'));
        ''')

        # Additive migrations also cover snapshots created by earlier builds.
        for table, column, definition in [
            ('quality_runs', 'version_id', 'TEXT'),
            ('lakehouse_exports', 'staging_table', 'TEXT')]:
            columns = {row[1] for row in con.execute('PRAGMA table_info(' + table + ')')}
            if column not in columns:
                con.execute('ALTER TABLE ' + table + ' ADD COLUMN ' + column + ' ' + definition)
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(4,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")

        con.execute("CREATE TABLE IF NOT EXISTS analysis_documents(id TEXT PRIMARY KEY,kind TEXT NOT NULL,name TEXT NOT NULL,content TEXT NOT NULL,revision INTEGER NOT NULL,updated_at TEXT NOT NULL)")
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(5,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")

        con.execute("CREATE TABLE IF NOT EXISTS workspace_settings(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(6,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")

        for table in ('query_runs','saved_queries'):
            columns={row[1] for row in con.execute('PRAGMA table_info('+table+')')}
            if 'engine' not in columns:
                con.execute("ALTER TABLE "+table+" ADD COLUMN engine TEXT NOT NULL DEFAULT 'duckdb'")
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(7,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")

        for table in ('pipelines','pipeline_runs'):
            columns={row[1] for row in con.execute('PRAGMA table_info('+table+')')}
            if 'connector_id' not in columns:
                con.execute('ALTER TABLE '+table+' ADD COLUMN connector_id TEXT')
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(8,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")

        con.execute('''CREATE TABLE IF NOT EXISTS notebook_runs(
            id TEXT PRIMARY KEY,document_id TEXT NOT NULL,revision INTEGER NOT NULL,indexes TEXT NOT NULL,
            status TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT,elapsed_ms INTEGER,
            inputs TEXT NOT NULL,result TEXT,error TEXT)''')
        con.execute("CREATE INDEX IF NOT EXISTS notebook_runs_document ON notebook_runs(document_id,started_at)")
        if 'code' not in {row[1] for row in con.execute('PRAGMA table_info(notebook_runs)')}:
            con.execute("ALTER TABLE notebook_runs ADD COLUMN code TEXT NOT NULL DEFAULT '[]'")
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(9,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")

        con.executescript('''
        CREATE TABLE IF NOT EXISTS connector_sync_state(
            connector_id TEXT PRIMARY KEY,phase TEXT NOT NULL DEFAULT 'new',system_id TEXT,database_oid INTEGER,
            relation_oid INTEGER,primary_key TEXT,schema_json TEXT,last_attempt TEXT,last_success TEXT,last_error TEXT,
            last_stats TEXT,source_commit_at TEXT);
        CREATE TABLE IF NOT EXISTS connector_sync_runs(
            id TEXT PRIMARY KEY,connector_id TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT,
            status TEXT NOT NULL,stats TEXT,error TEXT);
        CREATE INDEX IF NOT EXISTS connector_sync_runs_source ON connector_sync_runs(connector_id,started_at);
        ''')
        if 'timeline' not in {row[1] for row in con.execute('PRAGMA table_info(connector_sync_state)')}:
            con.execute('ALTER TABLE connector_sync_state ADD COLUMN timeline INTEGER')
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(10,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")
        for column,definition in [('target_id','TEXT'),('run_name','TEXT'),('result','TEXT'),('detached','INTEGER NOT NULL DEFAULT 0'),('expected_version','TEXT')]:
            if column not in {row[1] for row in con.execute('PRAGMA table_info(pipeline_runs)')}:
                con.execute('ALTER TABLE pipeline_runs ADD COLUMN '+column+' '+definition)
        con.executescript('''
        CREATE TABLE IF NOT EXISTS flows(id TEXT PRIMARY KEY,name TEXT NOT NULL,revision INTEGER NOT NULL,
            content TEXT NOT NULL,enabled INTEGER NOT NULL,interval_seconds INTEGER NOT NULL,next_run TEXT,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS flow_runs(id TEXT PRIMARY KEY,flow_id TEXT NOT NULL,revision INTEGER NOT NULL,
            snapshot TEXT NOT NULL,partition_date TEXT,detached INTEGER NOT NULL,status TEXT NOT NULL,
            created_at TEXT NOT NULL,started_at TEXT,finished_at TEXT,error TEXT,cancel_requested INTEGER NOT NULL DEFAULT 0,
            attempt INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS flow_step_runs(run_id TEXT NOT NULL,step_id TEXT NOT NULL,status TEXT NOT NULL,
            started_at TEXT,finished_at TEXT,result TEXT,error TEXT,PRIMARY KEY(run_id,step_id));
        CREATE TABLE IF NOT EXISTS flow_request_keys(key TEXT PRIMARY KEY,flow_id TEXT NOT NULL,fingerprint TEXT NOT NULL,run_ids TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS flow_runs_flow ON flow_runs(flow_id,created_at);
        ''')
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(11,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")
        con.executescript('''
        CREATE TABLE IF NOT EXISTS semantic_models(id TEXT PRIMARY KEY,name TEXT NOT NULL,revision INTEGER NOT NULL,
            content TEXT NOT NULL,contract TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS semantic_model_versions(model_id TEXT NOT NULL,revision INTEGER NOT NULL,
            content TEXT NOT NULL,contract TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(model_id,revision));
        CREATE TABLE IF NOT EXISTS semantic_runs(id TEXT PRIMARY KEY,model_id TEXT NOT NULL,model_revision INTEGER NOT NULL,
            request TEXT NOT NULL,snapshot TEXT NOT NULL,status TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT,
            inputs TEXT NOT NULL,sql TEXT,validation_query_id TEXT,query_id TEXT,result TEXT,error TEXT);
        CREATE INDEX IF NOT EXISTS semantic_runs_model ON semantic_runs(model_id,started_at);
        ''')
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(12,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")
        for column,definition in [('partition_granularity',"TEXT NOT NULL DEFAULT 'day'"),('partition_start','TEXT'),('partition_end','TEXT')]:
            if column not in {row[1] for row in con.execute('PRAGMA table_info(flow_runs)')}:
                con.execute('ALTER TABLE flow_runs ADD COLUMN '+column+' '+definition)
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(13,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")
        con.execute("CREATE TABLE IF NOT EXISTS flow_schedule_state(flow_id TEXT PRIMARY KEY,next_partition TEXT,last_checked TEXT,last_error TEXT,generation INTEGER NOT NULL DEFAULT 1)")
        if 'trigger' not in {row[1] for row in con.execute('PRAGMA table_info(flow_runs)')}:
            con.execute("ALTER TABLE flow_runs ADD COLUMN trigger TEXT NOT NULL DEFAULT 'legacy'")
            con.execute("UPDATE flow_runs SET trigger='backfill' WHERE detached=1")
        if 'generation' not in {row[1] for row in con.execute('PRAGMA table_info(flow_schedule_state)')}:
            con.execute('ALTER TABLE flow_schedule_state ADD COLUMN generation INTEGER NOT NULL DEFAULT 1')
        if 'schedule_generation' not in {row[1] for row in con.execute('PRAGMA table_info(flow_runs)')}:
            con.execute('ALTER TABLE flow_runs ADD COLUMN schedule_generation INTEGER')
        con.execute("INSERT OR IGNORE INTO schema_migrations VALUES(14,strftime('%Y-%m-%dT%H:%M:%fZ','now'))")
