"""Durable single-worker SQL pipelines. External orchestrators can replace this adapter."""
from datetime import datetime, timezone, timedelta
import threading
import uuid

from app.catalog import connection
from app.workflows import now


class Pipelines:
    def __init__(self, metadata, execute):
        self.metadata, self.execute = metadata, execute
        self.guard = threading.Lock()
        self.active = set()
        self.stop = threading.Event()
        self.thread = None
        with connection(metadata) as con:
            con.execute("UPDATE pipeline_runs SET status='interrupted',finished_at=?,error='Server stopped before completion.' WHERE status='running'", (now(),))

    def start(self):
        self.stop.clear()
        self.thread = threading.Thread(target=self.loop, daemon=True, name='platform-scheduler')
        self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(40)
            if self.thread.is_alive():
                raise RuntimeError('Pipeline processing did not drain; keep the data-directory lease until process exit.')

    def loop(self):
        while not self.stop.wait(1):
            try:
                self.tick()
            except Exception:
                import logging
                logging.getLogger(__name__).exception('Pipeline scheduler tick failed')

    def tick(self):
        with connection(self.metadata) as con:
            due = con.execute('SELECT id FROM pipelines WHERE enabled=1 AND next_run<=? ORDER BY next_run', (now(),)).fetchall()
        for pipeline in due:
            if self.stop.is_set():
                return
            try:
                self.run(pipeline['id'], scheduled=True)
            except Exception:
                pass  # Each attempted run records the error; keep other schedules alive.

    def run(self, ident, retry=None, scheduled=False, key=None):
        with self.guard:
            if ident in self.active:
                raise ValueError('This pipeline is already running.')
            self.active.add(ident)
        try:
            with connection(self.metadata) as con:
                pipeline = con.execute('SELECT * FROM pipelines WHERE id=?', (ident,)).fetchone()
                if not pipeline:
                    raise LookupError('Pipeline not found')
                if scheduled and (not pipeline['enabled'] or not pipeline['next_run'] or pipeline['next_run'] > now()):
                    return {'skipped': True}
                if key:
                    previous_key = con.execute('SELECT * FROM pipeline_request_keys WHERE key=?', (key,)).fetchone()
                    if previous_key:
                        if previous_key['pipeline_id'] != ident:
                            raise ValueError('Idempotency key belongs to another pipeline.')
                        retry = previous_key['run_id']
                if retry:
                    previous = con.execute('SELECT * FROM pipeline_runs WHERE id=? AND pipeline_id=?', (retry, ident)).fetchone()
                    if not previous:
                        raise LookupError('Pipeline run not found')
                    if previous['status'] == 'succeeded':
                        return {'run_id': retry, 'id': previous['dataset_id'], 'already_published': True}
                    statement = previous['sql']
                    connector_id=previous['connector_id']
                    run = retry
                    con.execute("UPDATE pipeline_runs SET status='running',started_at=?,finished_at=NULL,error=NULL WHERE id=?", (now(), run))
                else:
                    run, statement = uuid.uuid4().hex, pipeline['sql']
                    connector_id=pipeline['connector_id']
                    con.execute('INSERT INTO pipeline_runs(id,pipeline_id,sql,status,started_at,connector_id) VALUES(?,?,?,?,?,?)',
                                (run, ident, statement, 'running', now(), connector_id))
                if key:
                    con.execute('INSERT OR IGNORE INTO pipeline_request_keys VALUES(?,?,?)', (key, ident, run))
                if pipeline['enabled']:
                    next_run = (datetime.now(timezone.utc) + timedelta(seconds=pipeline['interval_seconds'])).isoformat()
                    con.execute('UPDATE pipelines SET next_run=? WHERE id=?', (next_run, ident))
            try:
                result = self.execute(pipeline['name'], statement, pipeline['target_id'], run, connector_id)
                with connection(self.metadata) as con:
                    con.execute("UPDATE pipeline_runs SET status='succeeded',finished_at=?,dataset_id=?,error=NULL WHERE id=? AND status='running'",(now(),result['id'],run))
                    con.execute('UPDATE pipelines SET target_id=? WHERE id=?',(result['id'],ident))
                return {**result, 'run_id': run}
            except Exception as exc:
                detail = getattr(exc, 'detail', str(exc))
                with connection(self.metadata) as con:
                    con.execute("UPDATE pipeline_runs SET status='failed',finished_at=?,error=? WHERE id=? AND status='running'",
                                (now(), str(detail)[:1500], run))
                raise ValueError(str(detail)) from exc
        finally:
            with self.guard:
                self.active.discard(ident)
