"""Local single-user data workbench. Never expose directly to a public network."""
from __future__ import annotations

import json
import csv
import hashlib
import math
import logging
import multiprocessing as mp
import os
import re
import tempfile
import shutil
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

import duckdb
from fastapi import FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from starlette.concurrency import run_in_threadpool
from starlette.background import BackgroundTask

from app.ingestion import MAX_ROWS, load_frame
from app.catalog import connection
from app.workflows import Workflows, quote
from app.limits import RequestLimit
from app.pipelines import Pipelines
from app.connectors import Connectors
from app.lakehouse import Lakehouse
from app.observability import Metrics, RequestMetrics
from app.analysis import register as register_analysis
from app.locking import DataLock
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Literal
from app.workers import prepare as prepare_dataset, bound

ROOT = Path(__file__).parent
MAX_UPLOAD = 20 * 1024 * 1024
QUERY_TIMEOUT = 10
MAX_QUERY_RESULT_BYTES = 8 * 1024 * 1024
MAX_EXPORT_BYTES = 128 * 1024 * 1024


def clean(value):
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    # JSON numbers beyond this range are rounded by browser JavaScript.
    if isinstance(value, int) and abs(value) > 2**53 - 1:
        return str(value)
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    return str(value)


def csv_cell(value):
    """Preserve numeric types while neutralising spreadsheet formulas in text."""
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return ""
    if isinstance(value, str) and (value.startswith(("\t", "\r", "\n")) or
                                  value.lstrip(" \t\r\n").startswith(("=", "+", "-", "@"))):
        return "'" + value
    return value


def execute_query(database, sql, result_path):
    """Separate process provides an enforceable wall-clock deadline."""
    try:
        bound(cpu=10)
        con = duckdb.connect(database, read_only=True, config={
            "enable_external_access": "false", "memory_limit": "256MB", "threads": "2"
        })
        statements = con.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise ValueError("Run one SELECT or WITH query. Data-changing SQL is disabled.")
        started = time.monotonic()
        cursor = con.execute(sql)
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchmany(1001)
        payload = {"columns": columns, "rows": clean(rows[:1000]),
                   "truncated": len(rows) > 1000,
                   "elapsed_ms": round((time.monotonic() - started) * 1000)}
        con.close()
    except Exception as exc:
        payload = {"error": str(exc)[:1500]}
    encoded = json.dumps(payload)
    if len(encoded.encode()) > MAX_QUERY_RESULT_BYTES:
        encoded = json.dumps({'error':'Query result exceeds the 8 MB response limit. Select fewer or smaller columns.'})
    Path(result_path).write_text(encoded)


class Query(BaseModel):
    sql: str = Field(min_length=1, max_length=20_000)
    engine: Literal['duckdb','trino'] = 'duckdb'


class SavedQuery(Query):
    name: str = Field(min_length=1, max_length=120)

    @field_validator("name", "sql")
    @classmethod
    def meaningful_value(cls, value, info):
        if not value.strip():
            raise ValueError("Enter a query name and SQL text.")
        if info.field_name == "name":
            value = value.strip()
            if any(ord(c) < 32 or ord(c) == 127 for c in value):
                raise ValueError("Query names cannot contain control characters.")
        return value


class SavedQuerySnapshot(BaseModel):
    name: str = Field(max_length=120)
    sql: str = Field(max_length=20_000)
    engine: Literal['duckdb','trino'] = 'duckdb'


class SavedQueryUpdate(SavedQuery):
    expected: SavedQuerySnapshot


class DatasetName(BaseModel):
    name: str = Field(min_length=1, max_length=200)

    @field_validator("name")
    @classmethod
    def meaningful_name(cls, value):
        value = value.strip()
        if not value or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError("Use a non-empty name without control characters.")
        return value


class Transformation(Query):
    name: str = Field(min_length=1, max_length=200)
    target_id: str | None = None

    _valid_name = field_validator('name')(DatasetName.meaningful_name.__func__)

    @field_validator('engine')
    @classmethod
    def workspace_engine(cls,value):
        if value!='duckdb':
            raise ValueError('Transformations use workspace dataset SQL; lakehouse workbench queries are read-only.')
        return value


class ConnectorDefinition(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(default=5433, ge=1, le=65535)
    database: str = Field(min_length=1, max_length=100)
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=2000)
    schema_name: str = Field(default='public', max_length=100)
    table: str = Field(min_length=1, max_length=100)
    mode: str = 'snapshot'
    watermark_column: str | None = Field(default=None, max_length=100)


class PipelineDefinition(DatasetName):
    sql: str = Field(default='', max_length=20_000)
    target_id: str | None = None
    connector_id: str | None = None
    interval_seconds: int = Field(default=0, ge=0, le=31_536_000)
    enabled: bool = False


class QualityRule(BaseModel):
    column_name: str = Field(min_length=1, max_length=500)
    rule: str
    threshold: float | None = Field(default=None, allow_inf_nan=False)

    @field_validator('rule')
    @classmethod
    def supported_rule(cls, value):
        if value not in ('not_null', 'unique', 'min', 'max'):
            raise ValueError('Supported rules: not_null, unique, min, max')
        return value


class QualityContract(BaseModel):
    rules: list[QualityRule] = Field(max_length=200)


class DeleteDataset(BaseModel):
    confirmation_name: str = Field(min_length=1, max_length=200)


def create_local_app(data_dir=None):
    base = Path(data_dir or os.environ.get("DATA_PLATFORM_DIR", "./var")).resolve()
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    base.chmod(0o700)
    with DataLock(base):
        pass
    dbpath = str(base / "datasets.duckdb")
    metadata = base / "catalog.sqlite3"
    lock = threading.RLock()
    sources = base / "sources"
    sources.mkdir(mode=0o700, exist_ok=True)
    with duckdb.connect(dbpath):
        pass

    def meta():
        return connection(metadata)

    with meta() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS datasets (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, table_name TEXT UNIQUE NOT NULL,
          row_count INTEGER NOT NULL, column_count INTEGER NOT NULL,
          size_bytes INTEGER NOT NULL, created_at TEXT NOT NULL, profile TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS dataset_sources (
          dataset_id TEXT PRIMARY KEY, sha256 TEXT NOT NULL,
          format TEXT NOT NULL, original_name TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS saved_queries (
          id TEXT PRIMARY KEY, name TEXT NOT NULL, sql TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT, detail TEXT, created_at TEXT);
        CREATE TABLE IF NOT EXISTS imports (
          id TEXT PRIMARY KEY, filename TEXT NOT NULL, status TEXT NOT NULL,
          started_at TEXT NOT NULL, finished_at TEXT, dataset_id TEXT,
          size_bytes INTEGER, error TEXT);
        CREATE TABLE IF NOT EXISTS dataset_deletions (
          dataset_id TEXT PRIMARY KEY, started_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS query_runs (
          id TEXT PRIMARY KEY, sql TEXT NOT NULL, status TEXT NOT NULL,
          started_at TEXT NOT NULL, finished_at TEXT, elapsed_ms INTEGER,
          result_rows INTEGER, truncated INTEGER, error TEXT);
        """)
        # This development server uses one worker. A running record left by an
        # earlier process is an interrupted attempt, never a claimed success.
        con.execute("""UPDATE imports SET status='interrupted', finished_at=?,
                    error='The server stopped before this import completed. Upload the file again.'
                    WHERE status='running'""", (datetime.now(timezone.utc).isoformat(),))
        con.execute("""UPDATE query_runs SET status='interrupted', finished_at=?,
                    error='The server stopped before this query completed.'
                    WHERE status='running'""", (datetime.now(timezone.utc).isoformat(),))

    workflows = Workflows(base, lock)
    connectors = Connectors(workflows)
    lakehouse = Lakehouse(workflows)
    staged = base / 'staged'
    staged.mkdir(mode=0o700, exist_ok=True)
    job_locks = {}

    def finish_deletion(row):
        with meta() as con:
            versions = con.execute('SELECT id,table_name FROM dataset_versions WHERE dataset_id=?', (row['id'],)).fetchall()
            jobs = con.execute("SELECT import_id FROM import_jobs WHERE target_id=? OR import_id IN (SELECT id FROM imports WHERE dataset_id=?)", (row['id'], row['id'])).fetchall()
        with duckdb.connect(dbpath) as storage:
            relation = storage.execute('SELECT table_type FROM information_schema.tables WHERE table_name=?', [row['table_name']]).fetchone()
            if relation:
                storage.execute(f'DROP {"VIEW" if relation[0] == "VIEW" else "TABLE"} {quote(row["table_name"])}')
            for version in versions:
                storage.execute(f'DROP TABLE IF EXISTS {quote(version["table_name"])}')
        (sources / row['id']).unlink(missing_ok=True)
        for job in jobs:
            directory = staged / job['import_id']
            if directory.exists():
                shutil.rmtree(directory)
        for version in versions:
            directory = base / 'versions' / version['id']
            if directory.exists():
                shutil.rmtree(directory)
        with meta() as con:
            for table in ('dataset_sources', 'dataset_versions', 'quality_rules'):
                con.execute(f'DELETE FROM {table} WHERE dataset_id=?', (row['id'],))
            con.execute('DELETE FROM datasets WHERE id=?', (row['id'],))
            con.execute('DELETE FROM dataset_deletions WHERE dataset_id=?', (row['id'],))
            con.execute('INSERT INTO events(action,detail,created_at) VALUES (?,?,?)',
                        ('dataset.deleted', row['name'], datetime.now(timezone.utc).isoformat()))

    with meta() as con:
        pending = con.execute("""SELECT datasets.* FROM datasets JOIN dataset_deletions
                              ON datasets.id=dataset_deletions.dataset_id""").fetchall()
    for row in pending:
        try:
            finish_deletion(row)
        except Exception:
            logging.getLogger(__name__).warning("Dataset deletion needs retry: %s", row["id"])

    def event(action, detail):
        with meta() as con:
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        (action, detail, datetime.now(timezone.utc).isoformat()))

    app = FastAPI(title="Data Platform", version="0.1.0", docs_url=None, redoc_url=None)
    metrics = Metrics()
    app.add_middleware(RequestMetrics, metrics=metrics)
    app.add_middleware(RequestLimit, limit=MAX_UPLOAD + 1024 * 1024)
    app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")

    @app.exception_handler(RequestValidationError)
    async def safe_validation_error(request, exc):
        return JSONResponse({'detail': [{key: error[key] for key in ('loc', 'msg', 'type')} for error in exc.errors()]}, 422)

    @app.middleware("http")
    async def local_only(request: Request, call_next):
        # Guard local service against DNS rebinding and cross-site browser writes.
        host = request.headers.get("host", "").split(":")[0]
        if host not in set(os.environ.get("DATA_PLATFORM_ALLOWED_HOSTS", "localhost,127.0.0.1,testserver").split(",")):
            return JSONResponse({"detail": "Local access only"}, status_code=403)
        origin = request.headers.get("origin")
        if request.method not in {"GET", "HEAD", "OPTIONS"} and origin:
            if origin != f"{request.url.scheme}://{request.headers.get('host')}":
                return JSONResponse({"detail": "Cross-origin request blocked"}, status_code=403)
        telemetry=request.method=='GET' and request.url.path in ('/api/system/readiness','/api/metrics','/api/imports','/api/query-history')
        if request.url.path.startswith('/api/') and not telemetry:
            def settle_publications():
                with meta() as con:
                    pending = con.execute('SELECT 1 FROM publications LIMIT 1').fetchone()
                if pending:
                    with lock:
                        workflows.recover()
            try:
                await run_in_threadpool(settle_publications)
            except Exception:
                return JSONResponse({'detail': 'Publication recovery is pending. Retry after storage is available.'}, status_code=503)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        )
        return response

    @app.get("/")
    def index():
        return FileResponse(ROOT / "static" / "index.html")

    @app.get("/docs", include_in_schema=False)
    def docs():
        return FileResponse(ROOT / "static" / "docs.html")

    @app.get('/api/session')
    def local_session():
        return {'mode': 'local', 'role': 'owner', 'workspaces': []}

    @app.get("/api/health")
    def health():
        return {"status": "ok", "mode": "local-single-user", "version": "0.1.0"}

    @app.get('/api/connectors')
    def list_connectors():
        return connectors.list()

    @app.post('/api/connectors', status_code=201)
    def create_connector(body: ConnectorDefinition):
        try:
            values = body.model_dump()
            values['schema'] = values.pop('schema_name')
            return connectors.create(values)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    def connector_action(ident, action):
        try:
            return connectors.test(ident) if action == 'test' else connectors.run(ident)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(400, str(exc)[:1000]) from exc

    @app.post('/api/connectors/{ident}/test')
    def test_connector(ident: str):
        return connector_action(ident, 'test')

    @app.post('/api/connectors/{ident}/import')
    def import_connector(ident: str):
        return connector_action(ident, 'import')

    @app.delete('/api/connectors/{ident}')
    def delete_connector(ident: str, body: DeleteDataset):
        with lock, meta() as con:
            row = con.execute('SELECT name FROM connectors WHERE id=?', (ident,)).fetchone()
            if not row:
                raise HTTPException(404, 'Connector not found')
            if body.confirmation_name != row['name']:
                raise HTTPException(409, 'Connector name does not match.')
            if con.execute('SELECT 1 FROM pipelines WHERE connector_id=?',(ident,)).fetchone():
                raise HTTPException(409,'Delete or edit pipelines that use this connector first.')
            con.execute('DELETE FROM connectors WHERE id=?', (ident,))
        return {'deleted': True}

    @app.post('/api/datasets/{ident}/lakehouse')
    def publish_lakehouse(ident: str):
        try:
            return lakehouse.publish(ident)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(400, str(exc)[:1000]) from exc

    @app.get('/api/datasets/{ident}/lakehouse')
    def lakehouse_history(ident: str):
        try:
            return lakehouse.history(ident)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get('/api/datasets/{ident}/lakehouse/{version_id}/snapshots')
    def iceberg_snapshots(ident: str, version_id: str):
        try:
            exports = lakehouse.history(ident)
            export = next((item for item in exports if item['version_id'] == version_id and item['state'] == 'published'), None)
            if not export:
                raise HTTPException(404, 'Published lakehouse version not found')
            table = export['table_name'][:-1] + '$snapshots"'
            return lakehouse.query(f'SELECT snapshot_id,parent_id,operation,committed_at FROM {table} ORDER BY committed_at DESC LIMIT 100')
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, str(exc)[:1000]) from exc

    @app.get('/api/metrics')
    def operational_metrics():
        with meta() as con:
            counts = {table: con.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                for table in ('datasets','dataset_versions','imports','query_runs','pipelines','pipeline_runs')}
        return {'http':metrics.snapshot(),'catalog':counts,'worker_deadline_seconds':30,'query_deadline_seconds':QUERY_TIMEOUT}

    @app.get('/api/lakehouse')
    def all_lakehouse_copies():
        with meta() as con:
            return [dict(row) for row in con.execute('SELECT lakehouse_exports.*,COALESCE(datasets.name,lakehouse_exports.dataset_id) AS dataset_name FROM lakehouse_exports LEFT JOIN datasets ON datasets.id=lakehouse_exports.dataset_id ORDER BY lakehouse_exports.created_at DESC')]

    @app.delete('/api/lakehouse/{version_id}')
    def remove_lakehouse_copy(version_id: str, body: DeleteDataset):
        try:
            return lakehouse.remove(version_id, body.confirmation_name)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(503, 'Lakehouse copy removal failed. Retry after the service is available.') from exc

    @app.get('/api/system/readiness')
    def readiness():
        with meta() as con:
            findings = [dict(row) for row in con.execute('SELECT * FROM recovery_findings')]
            migrations = [row[0] for row in con.execute('SELECT version FROM schema_migrations ORDER BY version')]
            pending = con.execute('SELECT count(*) FROM publications').fetchone()[0]
        return {'mode': 'single-user', 'metadata_migrations': migrations, 'pending_publications': pending,
                'recovery_findings': findings,
                'integrations': {'oidc_configured': os.environ.get('DATA_PLATFORM_AUTH_MODE') == 'oidc',
                    'trino_configured': bool(os.environ.get('DATA_PLATFORM_TRINO_URL')), 'postgres_connector': True,
                    'orchestrator': os.environ.get('DATA_PLATFORM_ORCHESTRATOR', 'embedded')}}

    @app.get('/api/datasets/{ident}/versions')
    def versions(ident: str):
        try:
            workflows.dataset(ident)
            return workflows.versions(ident)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get('/api/datasets/{ident}/versions/{version_id}/source')
    def version_source(ident: str, version_id: str):
        try:
            workflows.dataset(ident)
            source, version = workflows.source(ident, version_id)
            return FileResponse(source, filename=Path(version['original_name'] or 'source').name,
                                media_type='application/octet-stream')
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post('/api/datasets/{ident}/versions/{version_id}/restore', status_code=201)
    def restore_version(ident: str, version_id: str):
        with lock:
            try:
                dataset = workflows.dataset(ident)
                version = next((item for item in workflows.versions(ident) if item['id']==version_id),None)
                if not version:
                    raise LookupError('Dataset version not found')
                try:
                    source, _ = workflows.source(ident, version_id)
                except LookupError:
                    source = None
                sql = f'SELECT * FROM {quote(version["table_name"])}'
                with tempfile.TemporaryDirectory(prefix='data-platform-restore-') as temporary:
                    prepared = prepare_dataset('transformation', '', '', dbpath, sql, temporary)
                    return workflows.publish(prepared, source or prepared, version['original_name'] if source else 'restored.parquet', target=ident,
                        sql=sql, kind='restore', inputs=[{'dataset_id': ident, 'version_id': version_id,
                            'name': dataset['name'], 'table_name': version['table_name']}])
            except LookupError as exc:
                raise HTTPException(404, str(exc)) from exc
            except Exception as exc:
                raise HTTPException(400, str(exc)[:500]) from exc

    @app.get('/api/datasets/{ident}/lineage')
    def lineage(ident: str):
        try:
            workflows.dataset(ident)
            history = workflows.versions(ident)
            return {'dataset_id': ident, 'versions': [{key: version[key] for key in
                ('id', 'number', 'kind', 'sql', 'inputs', 'created_at')} for version in history]}
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.post('/api/datasets/{ident}/versions', status_code=201)
    async def upload_version(ident: str, file: UploadFile = File(...)):
        try:
            workflows.dataset(ident)
        except LookupError as exc:
            await file.close()
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            await file.close()
            raise HTTPException(409, str(exc)) from exc
        import_id = uuid.uuid4().hex
        filename = file.filename or 'dataset.csv'
        try:
            with meta() as con:
                con.execute('INSERT INTO imports(id,filename,status,started_at) VALUES(?,?,?,?)',
                    (import_id, filename[:200], 'running', datetime.now(timezone.utc).isoformat()))
            content = await file.read(MAX_UPLOAD + 1)
            if len(content) > MAX_UPLOAD:
                raise HTTPException(413, 'Maximum upload size is 20 MB.')
            return await run_in_threadpool(ingest, content, filename, import_id, ident)
        except HTTPException as exc:
            fail_import(import_id, exc.detail)
            raise
        except Exception as exc:
            fail_import(import_id, exc)
            raise HTTPException(400, str(exc)[:500]) from exc
        finally:
            await file.close()

    @app.get('/api/datasets/{ident}/quality')
    def get_quality(ident: str):
        try:
            workflows.dataset(ident)
            with meta() as con:
                runs = [dict(row) for row in con.execute('SELECT * FROM quality_runs WHERE dataset_id=? ORDER BY created_at DESC LIMIT 20', (ident,))]
            return {'rules': workflows.rules(ident), 'runs': [{**row, 'results': json.loads(row['results'])} for row in runs]}
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.put('/api/datasets/{ident}/quality')
    def set_quality(ident: str, body: QualityContract):
        with lock:
            try:
                dataset = workflows.dataset(ident)
            except LookupError as exc:
                raise HTTPException(404, str(exc)) from exc
            columns = {column['name'] for column in json.loads(dataset['profile'])}
            seen = set()
            for rule in body.rules:
                if rule.column_name not in columns:
                    raise HTTPException(400, 'Quality column does not exist.')
                key = (rule.column_name, rule.rule)
                if key in seen:
                    raise HTTPException(400, 'Duplicate quality rule.')
                seen.add(key)
                if rule.rule in ('min', 'max') and rule.threshold is None:
                    raise HTTPException(400, 'Numeric bounds require a threshold.')
            with meta() as con:
                con.execute('DELETE FROM quality_rules WHERE dataset_id=?', (ident,))
                con.executemany('INSERT INTO quality_rules VALUES(?,?,?,?)',
                    [(ident, rule.column_name, rule.rule, rule.threshold) for rule in body.rules])
        return {'rules': workflows.rules(ident)}

    @app.post('/api/datasets/{ident}/quality/run')
    def run_quality(ident: str):
        try:
            return clean(workflows.quality(ident))
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    def materialize(body: Transformation, pipeline_run=None):
        with lock:
            try:
                if body.target_id:
                    workflows.dataset(body.target_id)
                with tempfile.TemporaryDirectory(prefix='data-platform-transform-') as temporary:
                    prepared = prepare_dataset('transformation', '', '', dbpath, body.sql, temporary)
                    references = json.loads((Path(temporary) / 'worker.json').read_text())['references']
                    with meta() as con:
                        current = {row['table_name']: dict(row) for row in con.execute('SELECT * FROM datasets')}
                        historical = {row['table_name']: dict(row) for row in con.execute('SELECT * FROM dataset_versions')}
                    inputs = []
                    for reference in references:
                        if reference in current:
                            dataset = current[reference]
                            version = workflows.versions(dataset['id'])[0]
                            inputs.append({'dataset_id': dataset['id'], 'version_id': version['id'], 'name': dataset['name'], 'table_name': reference})
                        elif reference in historical:
                            version = historical[reference]
                            inputs.append({'dataset_id': version['dataset_id'], 'version_id': version['id'], 'name': reference, 'table_name': reference})
                        else:
                            raise ValueError('Transformations can reference only registered dataset tables and SQL functions.')
                    return workflows.publish(prepared, prepared, body.name + '.parquet', target=body.target_id, sql=body.sql, inputs=inputs, pipeline_run=pipeline_run)
            except LookupError as exc:
                raise HTTPException(404, str(exc)) from exc
            except TimeoutError as exc:
                raise HTTPException(408, str(exc)) from exc
            except Exception as exc:
                raise HTTPException(400, str(exc)[:1000]) from exc

    @app.post('/api/transformations', status_code=201)
    def transformation_endpoint(body: Transformation):
        return materialize(body)

    def execute_pipeline(name, sql, target, run, connector_id=None):
        with lock:
            workflows.recover()
            with meta() as con:
                previous = con.execute('SELECT * FROM pipeline_runs WHERE id=?', (run,)).fetchone()
            if previous and previous['status'] == 'succeeded':
                return {'id': previous['dataset_id'], 'already_published': True}
            if connector_id:
                return connectors.run(connector_id,pipeline_run=run)
            return materialize(Transformation(name=name, sql=sql, target_id=target), pipeline_run=run)

    pipelines = Pipelines(metadata, execute_pipeline)
    app.state.pipelines = pipelines
    app.state.data_lock = DataLock(base)

    @asynccontextmanager
    async def lifespan(_):
        app.state.data_lock.acquire()
        if os.environ.get('DATA_PLATFORM_ORCHESTRATOR') != 'dagster':
            pipelines.start()
        try:
            yield
        finally:
            await run_in_threadpool(pipelines.close)
            app.state.data_lock.close()
    app.router.lifespan_context = lifespan

    @app.get('/api/pipelines')
    def list_pipelines():
        with meta() as con:
            return [dict(row) for row in con.execute('SELECT * FROM pipelines ORDER BY created_at DESC')]

    def validate_pipeline(body):
        if body.enabled and body.interval_seconds < 60:
            raise HTTPException(400, 'Enabled schedules require an interval of at least 60 seconds.')
        if body.connector_id:
            try:
                connectors.config(body.connector_id)
            except LookupError as exc:
                raise HTTPException(404,str(exc)) from exc
        elif not body.sql.strip():
            raise HTTPException(400, 'Enter SQL for this pipeline or select a source connector.')
        if body.target_id:
            try:
                workflows.dataset(body.target_id)
            except LookupError as exc:
                raise HTTPException(404, str(exc)) from exc

    @app.post('/api/pipelines', status_code=201)
    def create_pipeline(body: PipelineDefinition):
        validate_pipeline(body)
        ident = uuid.uuid4().hex
        next_run = (datetime.now(timezone.utc) + timedelta(seconds=body.interval_seconds)).isoformat() if body.enabled else None
        with meta() as con:
            con.execute('INSERT INTO pipelines(id,name,sql,target_id,interval_seconds,enabled,next_run,created_at,connector_id) VALUES(?,?,?,?,?,?,?,?,?)',
                (ident, body.name, body.sql, body.target_id, body.interval_seconds, int(body.enabled), next_run, datetime.now(timezone.utc).isoformat(), body.connector_id))
        return {'id': ident}

    @app.put('/api/pipelines/{ident}')
    def edit_pipeline(ident: str, body: PipelineDefinition):
        validate_pipeline(body)
        with pipelines.guard:
            if ident in pipelines.active:
                raise HTTPException(409, 'Wait for the active run to finish before editing this pipeline.')
            with meta() as con:
                if not con.execute('SELECT 1 FROM pipelines WHERE id=?', (ident,)).fetchone():
                    raise HTTPException(404, 'Pipeline not found')
                next_run = (datetime.now(timezone.utc) + timedelta(seconds=body.interval_seconds)).isoformat() if body.enabled else None
                con.execute('UPDATE pipelines SET name=?,sql=?,target_id=?,interval_seconds=?,enabled=?,next_run=?,connector_id=? WHERE id=?',
                    (body.name, body.sql, body.target_id, body.interval_seconds, int(body.enabled), next_run, body.connector_id, ident))
        return {'id': ident}

    @app.delete('/api/pipelines/{ident}')
    def delete_pipeline(ident: str,body: DeleteDataset):
        with pipelines.guard:
            if ident in pipelines.active:
                raise HTTPException(409,'Wait for the active pipeline run to finish.')
            with meta() as con:
                pipeline=con.execute('SELECT name FROM pipelines WHERE id=?',(ident,)).fetchone()
                if not pipeline:
                    raise HTTPException(404,'Pipeline not found')
                if pipeline['name']!=body.confirmation_name:
                    raise HTTPException(409,'Pipeline name does not match.')
                con.execute('DELETE FROM pipeline_runs WHERE pipeline_id=?',(ident,))
                con.execute('DELETE FROM pipeline_request_keys WHERE pipeline_id=?',(ident,))
                con.execute('DELETE FROM pipelines WHERE id=?',(ident,))
                con.execute('INSERT INTO events(action,detail,created_at) VALUES(?,?,?)',('pipeline.deleted',pipeline['name'],datetime.now(timezone.utc).isoformat()))
        return {'deleted':True}

    @app.get('/api/pipelines/{ident}/runs')
    def pipeline_runs(ident: str):
        with meta() as con:
            if not con.execute('SELECT 1 FROM pipelines WHERE id=?', (ident,)).fetchone():
                raise HTTPException(404, 'Pipeline not found')
            return [dict(row) for row in con.execute('SELECT * FROM pipeline_runs WHERE pipeline_id=? ORDER BY started_at DESC LIMIT 100', (ident,))]

    def execute_run(ident, retry=None, key=None):
        try:
            return pipelines.run(ident, retry=retry, key=key)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(409 if 'already running' in str(exc) else 400, str(exc)) from exc

    @app.post('/api/pipelines/{ident}/run')
    def run_pipeline(ident: str, idempotency_key: str | None = Header(default=None, max_length=128)):
        return execute_run(ident, key=idempotency_key)

    @app.post('/api/pipelines/{ident}/runs/{run_id}/retry')
    def retry_pipeline(ident: str, run_id: str):
        return execute_run(ident, retry=run_id)

    @app.get("/api/datasets")
    def datasets():
        with meta() as con:
            rows = con.execute("""SELECT datasets.*, EXISTS(SELECT 1 FROM dataset_deletions
                WHERE dataset_id=datasets.id) AS deletion_pending
                FROM datasets ORDER BY created_at DESC""").fetchall()
        return [{**dict(row), "profile": json.loads(row["profile"])} for row in rows]

    @app.patch("/api/datasets/{ident}")
    def rename_dataset(ident: str, body: DatasetName):
        with lock, meta() as con:
            row = con.execute("SELECT * FROM datasets WHERE id=?", (ident,)).fetchone()
            if not row:
                raise HTTPException(404, "Dataset not found")
            if con.execute("SELECT 1 FROM dataset_deletions WHERE dataset_id=?", (ident,)).fetchone():
                raise HTTPException(409, "Deletion is pending. Retry deleting this dataset.")
            con.execute("UPDATE datasets SET name=? WHERE id=?", (body.name, ident))
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        ("dataset.renamed", f'{row["name"]} → {body.name}', datetime.now(timezone.utc).isoformat()))
        return {"id": ident, "name": body.name}

    @app.delete("/api/datasets/{ident}")
    def delete_dataset(ident: str, body: DeleteDataset):
        with lock:
            workflows.recover()
            with meta() as con:
                row = con.execute("SELECT * FROM datasets WHERE id=?", (ident,)).fetchone()
                if not row:
                    raise HTTPException(404, "Dataset not found")
                if body.confirmation_name != row["name"]:
                    raise HTTPException(409, "Dataset name does not match. Refresh and confirm its current name.")
                if con.execute("SELECT 1 FROM lakehouse_exports WHERE dataset_id=? AND state!='deleted'", (ident,)).fetchone():
                    raise HTTPException(409, 'Remove published or failed lakehouse copies from Connections before deleting this dataset.')
                if con.execute('SELECT 1 FROM connectors WHERE target_id=?',(ident,)).fetchone():
                    raise HTTPException(409,'Delete the source connector before deleting its published dataset.')
                if con.execute('SELECT 1 FROM pipelines WHERE target_id=?',(ident,)).fetchone():
                    raise HTTPException(409,'Delete or retarget pipelines before deleting their output dataset.')
                for version in con.execute('SELECT inputs FROM dataset_versions WHERE dataset_id!=?',(ident,)):
                    if any(item.get('dataset_id')==ident for item in json.loads(version['inputs'])):
                        raise HTTPException(409,'Other dataset versions depend on this dataset. Remove the dependent datasets first.')
                con.execute("INSERT OR IGNORE INTO dataset_deletions VALUES (?,?)",
                            (ident, datetime.now(timezone.utc).isoformat()))
            try:
                finish_deletion(row)
            except Exception as exc:
                raise HTTPException(503, "Deletion is pending. Refresh the catalogue and retry deletion.") from exc
        return {"id": ident, "deleted": True}

    def ingest(content, filename, import_id, target=None):
        directory = staged / import_id
        directory.mkdir(mode=0o700, exist_ok=True)
        source_path = directory / 'source'
        with meta() as con:
            con.execute('INSERT OR IGNORE INTO import_jobs(import_id,filename,target_id,created_at) VALUES(?,?,?,?)',
                        (import_id, filename, target, datetime.now(timezone.utc).isoformat()))
        source_path.write_bytes(content)
        with tempfile.TemporaryDirectory(prefix='data-platform-parse-') as temporary:
            prepared = prepare_dataset('import', source_path, Path(filename).suffix.lower(), dbpath, None, temporary)
            return workflows.publish(prepared, source_path, filename, import_id=import_id, target=target)

    @app.post('/api/imports/{ident}/retry')
    def retry_import(ident: str):
        with lock:
            guard = job_locks.setdefault(ident, threading.Lock())
        if not guard.acquire(blocking=False):
            raise HTTPException(409, 'This import is already processing.')
        try:
            with lock:
                workflows.recover()
            with meta() as con:
                job = con.execute('SELECT * FROM import_jobs WHERE import_id=?', (ident,)).fetchone()
                attempt = con.execute('SELECT * FROM imports WHERE id=?', (ident,)).fetchone()
                if not job or not attempt:
                    raise HTTPException(404, 'Retained import not found')
                if attempt['status'] == 'succeeded':
                    return {'id': attempt['dataset_id'], 'import_id': ident, 'already_published': True}
                if attempt['status'] == 'running':
                    raise HTTPException(409, 'This import is already processing.')
                if not (staged / ident / 'source').is_file():
                    raise HTTPException(404, 'Source no longer available. Upload the file again.')
                con.execute("UPDATE imports SET status='running',finished_at=NULL,error=NULL WHERE id=?", (ident,))
                con.execute('UPDATE import_jobs SET retries=retries+1 WHERE import_id=?', (ident,))
            try:
                return ingest((staged / ident / 'source').read_bytes(), job['filename'], ident, job['target_id'])
            except Exception as exc:
                fail_import(ident, exc)
                raise HTTPException(400, str(exc)[:500]) from exc
        finally:
            guard.release()

    @app.delete('/api/imports/{ident}/retained-source')
    def remove_retry_source(ident: str,body: DeleteDataset):
        with lock:
            guard=job_locks.setdefault(ident,threading.Lock())
        if not guard.acquire(blocking=False):
            raise HTTPException(409,'This import is processing. Wait before removing its retry file.')
        try:
            with lock,meta() as con:
                row=con.execute('SELECT * FROM imports WHERE id=?',(ident,)).fetchone()
                if not row:
                    raise HTTPException(404,'Import not found')
                if row['status']=='running':
                    raise HTTPException(409,'This import is processing.')
                if body.confirmation_name!=row['filename']:
                    raise HTTPException(409,'Import filename does not match.')
                directory=staged/ident
                if directory.exists():
                    shutil.rmtree(directory)
            return {'removed':True}
        finally:
            guard.release()

    @app.get("/api/imports")
    def imports():
        with meta() as con:
            return [{**dict(row), 'retry_available': bool((staged / row['id'] / 'source').is_file())}
                    for row in con.execute("""
                SELECT imports.*, datasets.row_count, datasets.id IS NOT NULL AS dataset_available FROM imports
                LEFT JOIN datasets ON datasets.id=imports.dataset_id AND NOT EXISTS
                  (SELECT 1 FROM dataset_deletions WHERE dataset_id=datasets.id)
                ORDER BY imports.started_at DESC, imports.id DESC LIMIT 100
            """)]

    def fail_import(import_id, error):
        with meta() as con:
            con.execute("""UPDATE imports SET status='failed', finished_at=?, error=?
                        WHERE id=? AND status='running'""",
                        (datetime.now(timezone.utc).isoformat(), str(error)[:500], import_id))

    @app.post("/api/datasets", status_code=201)
    async def upload(file: UploadFile = File(...)):
        import_id = uuid.uuid4().hex
        filename = file.filename or "dataset.csv"
        try:
            with meta() as con:
                con.execute("INSERT INTO imports(id,filename,status,started_at) VALUES (?,?,?,?)",
                            (import_id, filename[:200], "running", datetime.now(timezone.utc).isoformat()))
            content = await file.read(MAX_UPLOAD + 1)
            if len(content) > MAX_UPLOAD:
                raise HTTPException(413, "Maximum upload size is 20 MB.")
            with meta() as con:
                con.execute("UPDATE imports SET size_bytes=? WHERE id=?", (len(content), import_id))
            return await run_in_threadpool(ingest, content, filename, import_id)
        except HTTPException as exc:
            fail_import(import_id, exc.detail)
            raise
        except Exception as exc:
            fail_import(import_id, exc)
            raise HTTPException(400, str(exc)[:500]) from exc
        finally:
            await file.close()

    def run_query(sql):
        result = base / f"result_{uuid.uuid4().hex}.json"
        with lock:
            workflows.recover()
            ctx = mp.get_context("spawn")
            proc = ctx.Process(target=execute_query, args=(dbpath, sql, str(result)))
            try:
                proc.start()
                proc.join(QUERY_TIMEOUT)
                if proc.is_alive():
                    proc.terminate()
                    proc.join(2)
                    if proc.is_alive():
                        proc.kill()
                        proc.join()
                    raise HTTPException(408, "Query exceeded the 10-second time limit.")
                if not result.exists():
                    raise HTTPException(400, "Query worker stopped before completing.")
                payload = json.loads(result.read_text())
                if "error" in payload:
                    raise HTTPException(400, payload["error"])
                return payload
            finally:
                result.unlink(missing_ok=True)

    @app.post("/api/query")
    def query(body: Query):
        ident = uuid.uuid4().hex
        started = time.monotonic()
        with meta() as con:
            con.execute("INSERT INTO query_runs(id,sql,status,started_at,engine) VALUES (?,?,?,?,?)",
                        (ident, body.sql, "running", datetime.now(timezone.utc).isoformat(), body.engine))
        try:
            if body.engine=='trino':
                try:
                    payload=clean(lakehouse.select(body.sql))
                except TimeoutError as exc:
                    raise HTTPException(408,str(exc)) from exc
                except Exception as exc:
                    raise HTTPException(400,str(exc)[:1500]) from exc
            else:
                payload = run_query(body.sql)
            if len(json.dumps(payload).encode()) > MAX_QUERY_RESULT_BYTES:
                raise HTTPException(400,'Query result exceeds the 8 MB response limit. Select fewer or smaller columns.')
        except Exception as exc:
            error = exc if isinstance(exc, HTTPException) else HTTPException(500, "Query could not complete. Try again.")
            status = "timed_out" if error.status_code == 408 else "failed"
            with meta() as con:
                con.execute("""UPDATE query_runs SET status=?, finished_at=?, elapsed_ms=?, error=?
                            WHERE id=?""", (status, datetime.now(timezone.utc).isoformat(),
                            round((time.monotonic() - started) * 1000), str(error.detail)[:1500], ident))
            if error is exc:
                raise
            raise error from exc
        with meta() as con:
            con.execute("""UPDATE query_runs SET status='succeeded', finished_at=?, elapsed_ms=?,
                        result_rows=?, truncated=? WHERE id=?""",
                        (datetime.now(timezone.utc).isoformat(), round((time.monotonic() - started) * 1000),
                         len(payload['rows']), int(payload['truncated']), ident))
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        ("query.executed", f"{len(payload['rows'])} rows returned", datetime.now(timezone.utc).isoformat()))
        return {**payload, "query_id": ident, "engine":body.engine}

    @app.get("/api/query-history")
    def query_history():
        with meta() as con:
            return [dict(row) for row in con.execute("""SELECT * FROM query_runs
                ORDER BY started_at DESC, id DESC LIMIT 100""")]

    @app.get("/api/datasets/{ident}")
    def detail(ident: str):
        with lock:
            with meta() as con:
                row = con.execute("SELECT * FROM datasets WHERE id=?", (ident,)).fetchone()
                source = con.execute("SELECT sha256, format, original_name FROM dataset_sources WHERE dataset_id=?", (ident,)).fetchone()
                deleting = con.execute("SELECT 1 FROM dataset_deletions WHERE dataset_id=?", (ident,)).fetchone()
            if not row:
                raise HTTPException(404, "Dataset not found")
            if deleting:
                raise HTTPException(409, "Deletion is pending. Retry deleting this dataset from the catalogue.")
            return {**dict(row), "profile": json.loads(row["profile"]),
                    "source": dict(source) if source else None,
                    "preview": run_query(f'SELECT * FROM "{row["table_name"]}" LIMIT 50')}


    @app.get("/api/datasets/{ident}/export")
    def export_dataset(ident: str):
        temporary = tempfile.TemporaryDirectory(prefix="data-platform-export-")
        path = Path(temporary.name) / "dataset.csv"
        try:
            # Build the complete download before responding. The same operation
            # lock prevents deletion/import writes during this consistent read.
            with lock:
                with meta() as con:
                    row = con.execute("SELECT * FROM datasets WHERE id=?", (ident,)).fetchone()
                    deleting = con.execute("SELECT 1 FROM dataset_deletions WHERE dataset_id=?", (ident,)).fetchone()
                if not row:
                    raise HTTPException(404, "Dataset not found")
                if deleting:
                    raise HTTPException(409, "Deletion is pending. This dataset cannot be exported.")
                table = '"' + row["table_name"].replace('"', '""') + '"'
                with duckdb.connect(dbpath, read_only=True, config={
                    "enable_external_access": "false", "memory_limit": "256MB", "threads": "2"
                }) as storage, path.open("w", encoding="utf-8-sig", newline="") as output:
                    cursor = storage.execute(f"SELECT * FROM {table}")
                    writer = csv.writer(output, quoting=csv.QUOTE_ALL)
                    writer.writerow([csv_cell(column[0]) for column in cursor.description])
                    count = 0
                    while batch := cursor.fetchmany(100):
                        writer.writerows([[csv_cell(value) for value in values] for values in batch])
                        count += len(batch)
                        if output.tell() > MAX_EXPORT_BYTES:
                            raise HTTPException(413,'CSV export exceeds the 128 MB download limit. Use a narrower query or the retained source file.')
                event("dataset.exported", f'{row["name"]}: {count} rows prepared')
            filename = re.sub(r'[\x00-\x1f\x7f/\\]', "_", row["name"]).strip(" .") or "dataset"
            if not filename.lower().endswith(".csv"):
                filename += ".csv"
            return FileResponse(path, filename=filename, media_type="text/csv; charset=utf-8",
                                headers={"Cache-Control": "no-store", "X-Export-Row-Count": str(count)},
                                background=BackgroundTask(temporary.cleanup))
        except HTTPException:
            temporary.cleanup()
            raise
        except Exception as exc:
            temporary.cleanup()
            raise HTTPException(500, "Unable to prepare CSV export. Try again.") from exc

    @app.get("/api/datasets/{ident}/source")
    def download_source(ident: str):
        try:
            workflows.dataset(ident)
            path, version = workflows.source(ident)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        return FileResponse(path, filename=Path(version['original_name'] or 'source').name,
                            media_type='application/octet-stream')

    @app.get("/api/saved-queries")
    def saved_queries():
        with meta() as con:
            return [dict(r) for r in con.execute("SELECT * FROM saved_queries ORDER BY created_at DESC")]

    @app.post("/api/saved-queries", status_code=201)
    def save_query(body: SavedQuery):
        ident = uuid.uuid4().hex
        with meta() as con:
            con.execute("INSERT INTO saved_queries(id,name,sql,created_at,engine) VALUES (?,?,?,?,?)", (
                ident, body.name, body.sql, datetime.now(timezone.utc).isoformat(), body.engine))
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        ("query.saved", body.name, datetime.now(timezone.utc).isoformat()))
        return {"id": ident}

    @app.put("/api/saved-queries/{ident}")
    def update_saved_query(ident: str, body: SavedQueryUpdate):
        with meta() as con:
            changed = con.execute("""UPDATE saved_queries SET name=?, sql=?, engine=?
                WHERE id=? AND name=? AND sql=? AND engine=?""",
                (body.name, body.sql, body.engine, ident, body.expected.name, body.expected.sql, body.expected.engine)).rowcount
            if not changed:
                if not con.execute("SELECT 1 FROM saved_queries WHERE id=?", (ident,)).fetchone():
                    raise HTTPException(404, "Saved query not found")
                raise HTTPException(409, "This saved query changed. Close this dialog and refresh saved queries before trying again.")
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        ("query.updated", body.name, datetime.now(timezone.utc).isoformat()))
        return {"id": ident, "name": body.name, "sql": body.sql, "engine":body.engine}

    @app.delete("/api/saved-queries/{ident}")
    def delete_saved_query(ident: str, body: SavedQuerySnapshot):
        with meta() as con:
            changed = con.execute("DELETE FROM saved_queries WHERE id=? AND name=? AND sql=? AND engine=?",
                                  (ident, body.name, body.sql, body.engine)).rowcount
            if not changed:
                if not con.execute("SELECT 1 FROM saved_queries WHERE id=?", (ident,)).fetchone():
                    raise HTTPException(404, "Saved query not found")
                raise HTTPException(409, "This saved query changed. Close this dialog and refresh saved queries before trying again.")
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        ("query.deleted", body.name, datetime.now(timezone.utc).isoformat()))
        return {"id": ident, "deleted": True}

    @app.get("/api/activity")
    def activity():
        with meta() as con:
            return [dict(r) for r in con.execute("SELECT * FROM events ORDER BY id DESC LIMIT 50")]

    register_analysis(app, meta, lambda statement: query(Query(sql=statement)))
    return app


def create_app(data_dir=None):
    mode = os.environ.get('DATA_PLATFORM_AUTH_MODE', 'local')
    if mode == 'local':
        return create_local_app(data_dir)
    if mode != 'oidc':
        raise ValueError('DATA_PLATFORM_AUTH_MODE must be local or oidc.')
    from app.auth import Gateway
    return Gateway(create_local_app, data_dir or os.environ.get('DATA_PLATFORM_DIR', './var'))
