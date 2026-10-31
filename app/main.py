"""Local single-user data workbench. Never expose directly to a public network."""
from __future__ import annotations

import json
import hashlib
import math
import logging
import multiprocessing as mp
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

import duckdb
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from starlette.concurrency import run_in_threadpool

from app.ingestion import MAX_ROWS, load_frame

ROOT = Path(__file__).parent
MAX_UPLOAD = 20 * 1024 * 1024
QUERY_TIMEOUT = 10


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


def execute_query(database, sql, result_path):
    """Separate process provides an enforceable wall-clock deadline."""
    try:
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
    Path(result_path).write_text(json.dumps(payload))


class Query(BaseModel):
    sql: str = Field(min_length=1, max_length=20_000)


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


class DeleteDataset(BaseModel):
    confirmation_name: str = Field(min_length=1, max_length=200)


def create_app(data_dir=None):
    base = Path(data_dir or os.environ.get("DATA_PLATFORM_DIR", "./var")).resolve()
    base.mkdir(parents=True, exist_ok=True)
    dbpath = str(base / "datasets.duckdb")
    metadata = base / "catalog.sqlite3"
    lock = threading.RLock()
    sources = base / "sources"
    sources.mkdir(exist_ok=True)
    with duckdb.connect(dbpath):
        pass

    def meta():
        con = sqlite3.connect(metadata)
        con.row_factory = sqlite3.Row
        return con

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

    def finish_deletion(row):
        # The durable marker precedes all destructive work. Each cleanup step is
        # idempotent so a restart or explicit retry can complete interrupted work.
        table = '"' + row["table_name"].replace('"', '""') + '"'
        with duckdb.connect(dbpath) as storage:
            storage.execute(f"DROP TABLE IF EXISTS {table}")
        (sources / row["id"]).unlink(missing_ok=True)
        with meta() as con:
            con.execute("DELETE FROM dataset_sources WHERE dataset_id=?", (row["id"],))
            con.execute("DELETE FROM datasets WHERE id=?", (row["id"],))
            con.execute("DELETE FROM dataset_deletions WHERE dataset_id=?", (row["id"],))
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        ("dataset.deleted", row["name"], datetime.now(timezone.utc).isoformat()))

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
    app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")

    @app.middleware("http")
    async def local_only(request: Request, call_next):
        # Guard local service against DNS rebinding and cross-site browser writes.
        host = request.headers.get("host", "").split(":")[0]
        if host not in {"localhost", "127.0.0.1", "testserver"}:
            return JSONResponse({"detail": "Local access only"}, status_code=403)
        origin = request.headers.get("origin")
        if request.method not in {"GET", "HEAD", "OPTIONS"} and origin:
            if origin != f"{request.url.scheme}://{request.headers.get('host')}":
                return JSONResponse({"detail": "Cross-origin request blocked"}, status_code=403)
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

    @app.get("/api/health")
    def health():
        return {"status": "ok", "mode": "local-single-user", "version": "0.1.0"}

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
            with meta() as con:
                row = con.execute("SELECT * FROM datasets WHERE id=?", (ident,)).fetchone()
                if not row:
                    raise HTTPException(404, "Dataset not found")
                if body.confirmation_name != row["name"]:
                    raise HTTPException(409, "Dataset name does not match. Refresh and confirm its current name.")
                con.execute("INSERT OR IGNORE INTO dataset_deletions VALUES (?,?)",
                            (ident, datetime.now(timezone.utc).isoformat()))
            try:
                finish_deletion(row)
            except Exception as exc:
                raise HTTPException(503, "Deletion is pending. Refresh the catalogue and retry deletion.") from exc
        return {"id": ident, "deleted": True}

    def ingest(content, filename, import_id):
        suffix = Path(filename).suffix.lower()
        frame = load_frame(content, suffix, base)
        if frame.empty or len(frame) > MAX_ROWS or len(frame.columns) > 200:
            raise ValueError("Import requires 1–100,000 rows and 1–200 columns.")
        frame.columns = [str(c) for c in frame.columns]
        if len(set(frame.columns)) != len(frame.columns) or any(not c.strip() for c in frame.columns):
            raise ValueError("Column names must be unique and non-empty.")
        for column in frame.columns:
            if frame[column].map(lambda v: isinstance(v, (dict, list))).any():
                raise ValueError("Nested JSON is not supported yet. Flatten row values first.")
        ident = uuid.uuid4().hex
        table = f"ds_{ident}"
        source_path = sources / ident
        digest = hashlib.sha256(content).hexdigest()
        # Persist original bytes before publishing; compensate on normal failures.
        # Abrupt process death still requires cross-store reconciliation.
        with lock, duckdb.connect(dbpath) as con:
            con.register("incoming", frame)
            con.execute(f'CREATE TABLE "{table}" AS SELECT * FROM incoming')
            try:
                schema = con.execute(f'DESCRIBE "{table}"').fetchall()
                profile = []
                for name, dtype, *_ in schema:
                    quoted = '"' + name.replace('"', '""') + '"'
                    nulls, unique = con.execute(
                        f'SELECT count(*) - count({quoted}), count(DISTINCT {quoted}) FROM "{table}"'
                    ).fetchone()
                    profile.append({"name": name, "type": dtype, "nulls": nulls,
                                    "distinct": unique, "completeness": round(100 * (1 - nulls / len(frame)), 1)})
                source_path.write_bytes(content)
                with meta() as catalog:
                    catalog.execute("INSERT INTO dataset_sources VALUES (?,?,?,?)", (
                        ident, digest, suffix.lstrip("."), filename[:200]))
                    catalog.execute("INSERT INTO datasets VALUES (?,?,?,?,?,?,?,?)", (
                        ident, filename[:200], table, len(frame), len(frame.columns), len(content),
                        datetime.now(timezone.utc).isoformat(), json.dumps(profile)))
                    catalog.execute("""UPDATE imports SET status='succeeded',
                                    finished_at=?, dataset_id=?, size_bytes=? WHERE id=?""",
                                    (datetime.now(timezone.utc).isoformat(), ident, len(content), import_id))
                    catalog.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                                    ("dataset.imported", filename[:200], datetime.now(timezone.utc).isoformat()))
            except Exception:
                source_path.unlink(missing_ok=True)
                con.execute(f'DROP TABLE "{table}"')
                raise
        return {"id": ident, "table_name": table, "row_count": len(frame), "sha256": digest,
                "import_id": import_id}

    @app.get("/api/imports")
    def imports():
        with meta() as con:
            return [dict(row) for row in con.execute("""
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
            con.execute("INSERT INTO query_runs(id,sql,status,started_at) VALUES (?,?,?,?)",
                        (ident, body.sql, "running", datetime.now(timezone.utc).isoformat()))
        try:
            payload = run_query(body.sql)
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
        return {**payload, "query_id": ident}

    @app.get("/api/query-history")
    def query_history():
        with meta() as con:
            return [dict(row) for row in con.execute("""SELECT * FROM query_runs
                ORDER BY started_at DESC, id DESC LIMIT 100""")]

    @app.get("/api/datasets/{ident}")
    def detail(ident: str):
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

    @app.get("/api/datasets/{ident}/source")
    def download_source(ident: str):
        with meta() as con:
            source = con.execute("SELECT * FROM dataset_sources WHERE dataset_id=?", (ident,)).fetchone()
        # Use the stored UUID rather than a request-provided filesystem path.
        if not source or not (sources / source["dataset_id"]).is_file():
            raise HTTPException(404, "Original source file not available")
        return FileResponse(sources / source["dataset_id"],
                            filename=Path(source["original_name"]).name,
                            media_type="application/octet-stream")

    @app.get("/api/saved-queries")
    def saved_queries():
        with meta() as con:
            return [dict(r) for r in con.execute("SELECT * FROM saved_queries ORDER BY created_at DESC")]

    @app.post("/api/saved-queries", status_code=201)
    def save_query(body: SavedQuery):
        ident = uuid.uuid4().hex
        with meta() as con:
            con.execute("INSERT INTO saved_queries VALUES (?,?,?,?)", (
                ident, body.name, body.sql, datetime.now(timezone.utc).isoformat()))
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        ("query.saved", body.name, datetime.now(timezone.utc).isoformat()))
        return {"id": ident}

    @app.put("/api/saved-queries/{ident}")
    def update_saved_query(ident: str, body: SavedQueryUpdate):
        with meta() as con:
            changed = con.execute("""UPDATE saved_queries SET name=?, sql=?
                WHERE id=? AND name=? AND sql=?""",
                (body.name, body.sql, ident, body.expected.name, body.expected.sql)).rowcount
            if not changed:
                if not con.execute("SELECT 1 FROM saved_queries WHERE id=?", (ident,)).fetchone():
                    raise HTTPException(404, "Saved query not found")
                raise HTTPException(409, "This saved query changed. Close this dialog and refresh saved queries before trying again.")
            con.execute("INSERT INTO events(action,detail,created_at) VALUES (?,?,?)",
                        ("query.updated", body.name, datetime.now(timezone.utc).isoformat()))
        return {"id": ident, "name": body.name, "sql": body.sql}

    @app.delete("/api/saved-queries/{ident}")
    def delete_saved_query(ident: str, body: SavedQuerySnapshot):
        with meta() as con:
            changed = con.execute("DELETE FROM saved_queries WHERE id=? AND name=? AND sql=?",
                                  (ident, body.name, body.sql)).rowcount
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

    return app
