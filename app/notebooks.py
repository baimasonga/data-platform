"""Bounded data exchange with ephemeral Docker Python runners; no host-code fallback."""
import json
import os
import re
import subprocess
import threading
import time
import uuid
from typing import Literal

import duckdb
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from app.workflows import now, quote

SLOTS = threading.BoundedSemaphore(1)
DOCKER = ['docker', '--host=unix:///var/run/docker.sock']
MAX_INPUT = 16000000
MAX_OUTPUT = 2000000
RUN_SECONDS = 30


class Display(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    kind: Literal['text', 'table']
    text: str = Field(default='', max_length=20000)
    columns: list[str] = Field(default_factory=list, max_length=50)
    rows: list[list[str | int | float | bool | None]] = Field(default_factory=list, max_length=1000)
    truncated: bool = False


class CellOutput(BaseModel):
    index: int = Field(ge=0, le=19)
    stdout: str = Field(max_length=20000)
    outputs: list[Display] = Field(max_length=8)
    error: str | None = Field(default=None, max_length=2000)


class Result(BaseModel):
    cells: list[CellOutput] = Field(min_length=1, max_length=20)


class PythonRuntime:
    def __init__(self, workflows, clean):
        self.workflows, self.clean = workflows, clean
        self.image = os.environ.get('DATA_PLATFORM_PYTHON_IMAGE', '')
        if self.image and not re.fullmatch(r'sha256:[a-f0-9]{64}', self.image):
            raise ValueError('DATA_PLATFORM_PYTHON_IMAGE must be a locally built immutable image ID.')

    def status(self):
        if not self.image:
            return {'available': False, 'detail': 'Python runtime is not enabled. Run scripts/dev_stack.py notebooks, then restart the platform.'}
        try:
            result = subprocess.run(DOCKER + ['image', 'inspect', self.image, '--format', '{{.Id}}'],
                                    capture_output=True, timeout=5, check=True)
            if result.stdout.decode().strip() != self.image:
                raise ValueError('Image mismatch')
        except (OSError, subprocess.SubprocessError, ValueError):
            return {'available': False, 'detail': 'Python runner image or local Docker daemon is unavailable.'}
        return {'available': True, 'detail': 'Python 3.12 · pandas / NumPy · offline disposable container',
                'timeout_seconds': RUN_SECONDS, 'memory_mb': 512}

    def remove_container(self, name):
        try:
            result = subprocess.run(DOCKER + ['rm', '--force', name], capture_output=True, timeout=10)
        except (OSError, subprocess.SubprocessError):
            return False
        return result.returncode == 0 or b'No such container' in result.stderr

    def recover(self):
        with self.workflows.catalog() as con:
            runs = con.execute("SELECT id FROM notebook_runs WHERE status='running'").fetchall()
        for row in runs:
            # Exact names come only from our run journal, never a broad container sweep.
            if not self.remove_container('platform-python-' + row['id']):
                continue
            with self.workflows.catalog() as con:
                con.execute("UPDATE notebook_runs SET status='interrupted',finished_at=?,error=? WHERE id=?",
                            (now(), 'The platform stopped during execution. Run again explicitly.', row['id']))

    def snapshots(self, ids):
        snapshots, versions, used, count = [], [], 0, 0
        with self.workflows.lock:
            self.workflows.recover()
            with duckdb.connect(self.workflows.database, read_only=True,
                                config={'enable_external_access': 'false', 'memory_limit': '256MB', 'threads': '2'}) as storage:
                for ident in ids:
                    try:
                        dataset = self.workflows.dataset(ident)
                    except (LookupError, ValueError) as exc:
                        raise HTTPException(400, 'A selected input dataset is missing or pending deletion.') from exc
                    version = self.workflows.versions(ident)[0]
                    count += version['row_count']
                    if count > 100000:
                        raise HTTPException(400, 'Python inputs exceed 100,000 rows in total. Select fewer datasets.')
                    cursor = storage.execute('SELECT * FROM ' + quote(version['table_name']))
                    item = {'id': ident, 'name': dataset['name'], 'columns': [c[0] for c in cursor.description], 'rows': []}
                    while batch := cursor.fetchmany(100):
                        rows = self.clean(batch)
                        used += len(json.dumps(rows).encode())
                        if used > MAX_INPUT - 500000:
                            raise HTTPException(400, 'Python input data exceeds 15.5 MB. Select smaller datasets.')
                        item['rows'].extend(rows)
                    snapshots.append(item)
                    versions.append({'dataset_id': ident, 'version_id': version['id'], 'name': dataset['name'],
                                     'number': version['number'], 'rows': version['row_count']})
        return snapshots, versions

    def command(self, name):
        # No volumes, host sockets, platform environment or service credentials enter the runner.
        command = DOCKER + ['create', '--name', name, '--label', 'data-platform.python=1', '--interactive',
            '--network', 'none', '--read-only', '--user', '65534:65534', '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges', '--pids-limit', '64', '--memory', '512m', '--memory-swap', '512m',
            '--cpus', '1', '--ulimit', 'cpu=20:20', '--ulimit', 'nofile=64:64', '--ulimit', 'core=0:0',
            '--tmpfs', '/tmp:rw,noexec,nosuid,size=64m,mode=1777', '--log-driver', 'none']
        for variable in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
                         'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy'):
            command += ['--env', variable + '=']
        return command + [self.image]

    def container(self, name, payload):
        encoded = json.dumps(payload).encode()
        if len(encoded) > MAX_INPUT:
            raise HTTPException(400, 'Python run input exceeds 16 MB.')
        process = None
        provisioning_uncertain = False
        try:
            try:
                subprocess.run(self.command(name), check=True, capture_output=True, timeout=30)
            except subprocess.TimeoutExpired:
                # Killing the CLI cannot cancel a daemon-side create already in progress.
                provisioning_uncertain = True
                raise
            process = subprocess.Popen(DOCKER + ['start', '--attach', '--interactive', name],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            chunks, exceeded = [], threading.Event()

            def read():
                total = 0
                while chunk := process.stdout.read(8192):
                    total += len(chunk)
                    if total > MAX_OUTPUT:
                        exceeded.set()
                        process.kill()
                        break
                    chunks.append(chunk)

            def write():
                try:
                    process.stdin.write(encoded)
                    process.stdin.close()
                except (OSError, ValueError):
                    pass

            reader = threading.Thread(target=read, daemon=True)
            writer = threading.Thread(target=write, daemon=True)
            reader.start()
            writer.start()
            try:
                process.wait(timeout=RUN_SECONDS)
            except subprocess.TimeoutExpired as exc:
                raise HTTPException(408, 'Python exceeded the 30-second execution limit.') from exc
            reader.join(timeout=2)
            if exceeded.is_set():
                raise HTTPException(400, 'Python output exceeds the 2 MB transport limit.')
            if process.returncode or reader.is_alive():
                raise HTTPException(400, 'Python exited without a result. Check memory, CPU and process limits.')
            try:
                result = Result.model_validate_json(b''.join(chunks)).model_dump()
                expected = [cell['index'] for cell in payload['cells']]
                actual = [cell['index'] for cell in result['cells']]
                if actual != expected[:len(actual)] or (len(actual) < len(expected) and not result['cells'][-1]['error']):
                    raise ValueError('Unexpected result cells')
                for cell in result['cells']:
                    for output in cell['outputs']:
                        if any(len(row) != len(output['columns']) for row in output['rows']):
                            raise ValueError('Invalid table shape')
                return result
            except (ValidationError, ValueError) as exc:
                raise HTTPException(400, 'Python returned an invalid or oversized result.') from exc
        except (OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(503, 'The local Python container runtime is unavailable.') from exc
        finally:
            if process:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
            removed = self.remove_container(name)
            if process:
                for pipe in (process.stdin, process.stdout):
                    try:
                        pipe.close()
                    except OSError:
                        pass
            if not removed or provisioning_uncertain:
                # The journal stays running so startup recovery will retry exact-container cleanup.
                raise HTTPException(503, 'Python container cleanup could not be confirmed. Check Docker before running again.')

    def run(self, document, indexes):
        status = self.status()
        if not status['available']:
            raise HTTPException(503, status['detail'])
        if not SLOTS.acquire(blocking=False):
            raise HTTPException(429, 'Another Python run is active. Try again after it finishes.')
        ident, started, result = uuid.uuid4().hex, time.monotonic(), None
        inputs = []
        try:
            cells = [dict(index=index, text=document['cells'][index]['text']) for index in indexes]
            with self.workflows.catalog() as con:
                con.execute('BEGIN IMMEDIATE')
                current=con.execute('SELECT revision FROM analysis_documents WHERE id=?',(document['id'],)).fetchone()
                if not current:
                    raise HTTPException(404,'Notebook was deleted before execution could start.')
                if current['revision']!=document['revision']:
                    raise HTTPException(409,'Notebook changed before execution could start. Reload before running.')
                if con.execute("SELECT 1 FROM notebook_runs WHERE status='running'").fetchone():
                    raise HTTPException(409, 'A prior Python run needs cleanup. Restart the platform after checking Docker.')
                con.execute('INSERT INTO notebook_runs(id,document_id,revision,indexes,status,started_at,inputs,code) VALUES(?,?,?,?,?,?,?,?)',
                            (ident, document['id'], document['revision'], json.dumps(indexes), 'running', now(), '[]',json.dumps(cells)))
            ids = sorted({dataset for index in indexes for dataset in document['cells'][index].get('dataset_ids', [])})
            if len(ids) > 5:
                raise HTTPException(400, 'Select at most five different datasets per Python run.')
            snapshots, inputs = self.snapshots(ids)
            with self.workflows.catalog() as con:
                con.execute('UPDATE notebook_runs SET inputs=? WHERE id=?', (json.dumps(inputs), ident))
            result = self.container('platform-python-' + ident, {'cells': cells, 'datasets': snapshots})
            error = next((cell['error'] for cell in result['cells'] if cell['error']), None)
            result.update(kind='python', run_id=ident, revision=document['revision'], inputs=inputs,source_cells=cells,
                          refreshed_at=now(), elapsed_ms=round((time.monotonic() - started) * 1000),
                          status='failed' if error else 'succeeded')
            with self.workflows.catalog() as con:
                con.execute('UPDATE notebook_runs SET status=?,finished_at=?,elapsed_ms=?,result=?,error=? WHERE id=?',
                            (result['status'], now(), result['elapsed_ms'], json.dumps(result), error, ident))
            return result
        except HTTPException as exc:
            # Keep cleanup failures eligible for recovery; other failures are durable history too.
            state = 'running' if 'cleanup could not be confirmed' in str(exc.detail) else 'failed'
            with self.workflows.catalog() as con:
                con.execute('UPDATE notebook_runs SET status=?,finished_at=?,elapsed_ms=?,error=? WHERE id=?',
                            (state, now(), round((time.monotonic() - started) * 1000), str(exc.detail), ident))
            raise
        except Exception as exc:
            with self.workflows.catalog() as con:
                con.execute("UPDATE notebook_runs SET status='failed',finished_at=?,error=? WHERE id=?",
                            (now(), 'Python run could not prepare its inputs.', ident))
            raise HTTPException(500, 'Python run could not prepare its inputs.') from exc
        finally:
            SLOTS.release()
