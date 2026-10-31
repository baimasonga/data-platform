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
