"""Dataset versions, publication recovery, contracts and lineage."""
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import uuid
from datetime import datetime, timezone

import duckdb
from app.catalog import connection, migrate


def now():
    return datetime.now(timezone.utc).isoformat()


def quote(name):
    return '"' + name.replace('"', '""') + '"'


class Workflows:
    def __init__(self, base, lock):
        self.base, self.lock = Path(base), lock
        self.metadata = self.base / 'catalog.sqlite3'
        self.database = str(self.base / 'datasets.duckdb')
        self.files = self.base / 'versions'
        self.files.mkdir(mode=0o700, exist_ok=True)
        migrate(self.metadata)
        with lock:
            self.recover()
            self.adopt_legacy()
            self.reconcile()

    def catalog(self):
        return connection(self.metadata)

    def dataset(self, ident):
        with self.catalog() as con:
            row = con.execute('SELECT * FROM datasets WHERE id=?', (ident,)).fetchone()
            deleting = con.execute('SELECT 1 FROM dataset_deletions WHERE dataset_id=?', (ident,)).fetchone()
        if not row:
            raise LookupError('Dataset not found')
        if deleting:
            raise ValueError('Deletion is pending.')
        return dict(row)

    def versions(self, ident):
        with self.catalog() as con:
            rows = con.execute('SELECT * FROM dataset_versions WHERE dataset_id=? ORDER BY number DESC', (ident,)).fetchall()
        return [{**dict(row), 'profile': json.loads(row['profile']), 'inputs': json.loads(row['inputs'])} for row in rows]

    def adopt_legacy(self):
        with self.catalog() as con:
            rows = con.execute('''SELECT * FROM datasets WHERE id NOT IN
                (SELECT dataset_id FROM dataset_versions) AND id NOT IN
                (SELECT dataset_id FROM dataset_deletions)''').fetchall()
        for row in rows:
            ident = row['id']
            physical = 'ver_' + ident
            with duckdb.connect(self.database) as storage:
                exists = storage.execute('SELECT count(*) FROM information_schema.tables WHERE table_name=?', [physical]).fetchone()[0]
                stable = storage.execute('SELECT count(*) FROM information_schema.tables WHERE table_name=?', [row['table_name']]).fetchone()[0]
                if not exists and not stable:
                    continue  # Surface the missing table in reconciliation.
                storage.execute('BEGIN')
                if not exists:
                    storage.execute(f'ALTER TABLE {quote(row["table_name"])} RENAME TO {quote(physical)}')
                storage.execute(f'CREATE OR REPLACE VIEW {quote(row["table_name"])} AS SELECT * FROM {quote(physical)}')
                storage.execute('COMMIT')
            version = ident  # Deterministic adoption makes crash retry idempotent.
            with self.catalog() as con:
                source = con.execute('SELECT * FROM dataset_sources WHERE dataset_id=?', (ident,)).fetchone()
                con.execute('''INSERT OR IGNORE INTO dataset_versions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (version, ident, 1, physical, row['row_count'], row['column_count'], row['profile'],
                     row['size_bytes'], source['sha256'] if source else None, source['format'] if source else None,
                     source['original_name'] if source else None, row['created_at'], 'import', None, '[]'))

    def profile(self, storage, table):
        count = storage.execute(f'SELECT count(*) FROM {quote(table)}').fetchone()[0]
        if not 1 <= count <= 100000:
            raise ValueError('Dataset requires 1–100,000 rows.')
        schema = storage.execute(f'DESCRIBE {quote(table)}').fetchall()
        if not 1 <= len(schema) <= 200:
            raise ValueError('Dataset requires 1–200 columns.')
        result = []
        for name, dtype, *_ in schema:
            nulls, distinct = storage.execute(f'''SELECT count(*)-count({quote(name)}),
                count(DISTINCT {quote(name)}) FROM {quote(table)}''').fetchone()
            result.append({'name': name, 'type': dtype, 'nulls': nulls, 'distinct': distinct,
                            'completeness': round(100 * (1 - nulls / count), 1)})
        return count, result

    def evaluate(self, storage, table, rules):
        count, profile = self.profile(storage, table)
        by_name = {item['name']: item for item in profile}
        results = []
        for rule in rules:
            name, kind = rule['column_name'], rule['rule']
            column = by_name.get(name)
            passed, observed = False, None
            if column:
                if kind == 'not_null':
                    observed = column['nulls']
                    passed = observed == 0
                elif kind == 'unique':
                    observed = column['distinct']
                    passed = observed == count and column['nulls'] == 0
                elif kind in ('min', 'max'):
                    try:
                        observed = storage.execute(f'SELECT {kind}(TRY_CAST({quote(name)} AS DOUBLE)) FROM {quote(table)}').fetchone()[0]
                        invalid = storage.execute(f'''SELECT count(*) FROM {quote(table)} WHERE {quote(name)} IS NOT NULL
                            AND TRY_CAST({quote(name)} AS DOUBLE) IS NULL''').fetchone()[0]
                        passed = invalid == 0 and observed is not None and math.isfinite(observed) and (observed >= rule['threshold'] if kind == 'min' else observed <= rule['threshold'])
                    except duckdb.Error:
                        passed = False
            if isinstance(observed,float) and not math.isfinite(observed):
                observed=None
            results.append({**rule, 'passed': bool(passed), 'observed': observed})
        return count, profile, results

    def rules(self, ident):
        with self.catalog() as con:
            return [dict(row) for row in con.execute('SELECT column_name,rule,threshold FROM quality_rules WHERE dataset_id=?', (ident,))]

    def quality(self, ident):
        with self.lock:
            row = self.dataset(ident)
            with duckdb.connect(self.database, read_only=True) as storage:
                _, _, results = self.evaluate(storage, row['table_name'], self.rules(ident))
            self.record_quality(ident, results, self.versions(ident)[0]['id'])
            return {'passed': all(item['passed'] for item in results), 'results': results}

    def record_quality(self, ident, results, version_id=None):
        with self.catalog() as con:
            con.execute('INSERT INTO quality_runs(id,dataset_id,created_at,passed,results,version_id) VALUES(?,?,?,?,?,?)',
                (uuid.uuid4().hex, ident, now(), int(all(r['passed'] for r in results)), json.dumps(results, default=str), version_id))

    def publish(self, parquet, source, filename, import_id=None, target=None, sql=None, inputs=None, kind=None, pipeline_run=None, connector_checkpoint=None):
        with self.lock:
            self.recover()
            existing = self.dataset(target) if target else None
            ident = target or uuid.uuid4().hex
            version = uuid.uuid4().hex
            physical = 'ver_' + version
            stable = existing['table_name'] if existing else 'ds_' + ident
            number = self.versions(ident)[0]['number'] + 1 if existing else 1
            root = self.files / version
            root.mkdir(mode=0o700)
            timestamp = now()
            with self.catalog() as con:
                con.execute('INSERT INTO publications VALUES(?,?,?,?,?,?)', (version, ident, physical, 'writing', None, timestamp))
            try:
                shutil.copyfile(source, root / 'source')
                digest = hashlib.sha256((root / 'source').read_bytes()).hexdigest()
                with duckdb.connect(self.database, config={'memory_limit': '256MB', 'threads': '2'}) as storage:
                    storage.execute(f'CREATE TABLE {quote(physical)} AS SELECT * FROM read_parquet(?)', [str(parquet)])
                    count, profile, results = self.evaluate(storage, physical, self.rules(ident))
                if results:
                    self.record_quality(ident, results, version)
                if not all(r['passed'] for r in results):
                    raise ValueError('Quality contract failed. The published dataset has not changed.')
                payload = {'id': ident, 'name': existing['name'] if existing else filename[:200], 'stable': stable,
                    'version': version, 'number': number, 'physical': physical, 'count': count, 'profile': profile,
                    'size': (root / 'source').stat().st_size, 'sha256': digest, 'filename': filename[:200],
                    'format': Path(filename).suffix.lstrip('.').lower(), 'created_at': timestamp,
                    'pipeline_run': pipeline_run, 'connector_checkpoint': connector_checkpoint, 'kind': kind or ('transformation' if sql else 'import'), 'sql': sql, 'inputs': inputs or [], 'import_id': import_id}
                with self.catalog() as con:
                    con.execute("UPDATE publications SET state='ready',payload=? WHERE id=?", (json.dumps(payload), version))
                self.complete(payload)
                return {'id': ident, 'table_name': stable, 'row_count': count, 'sha256': digest,
                        'import_id': import_id, 'version_id': version, 'version': number}
            except Exception:
                # A ready journal may already have changed the DuckDB view. Keep
                # its files and marker so startup can complete the same publication.
                with self.catalog() as con:
                    journal = con.execute('SELECT state FROM publications WHERE id=?', (version,)).fetchone()
                if journal and journal['state'] == 'writing':
                    self.discard(version, physical)
                raise

    def complete(self, data):
        with duckdb.connect(self.database) as storage:
            storage.execute(f'CREATE OR REPLACE VIEW {quote(data["stable"])} AS SELECT * FROM {quote(data["physical"])}')
        with self.catalog() as con:
            con.execute('''INSERT INTO datasets VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
                row_count=excluded.row_count,column_count=excluded.column_count,size_bytes=excluded.size_bytes,profile=excluded.profile''',
                (data['id'], data['name'], data['stable'], data['count'], len(data['profile']), data['size'], data['created_at'], json.dumps(data['profile'])))
            con.execute('INSERT OR REPLACE INTO dataset_sources VALUES(?,?,?,?)',
                (data['id'], data['sha256'], data['format'], data['filename']))
            con.execute('INSERT OR IGNORE INTO dataset_versions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (data['version'], data['id'], data['number'], data['physical'], data['count'], len(data['profile']), json.dumps(data['profile']),
                 data['size'], data['sha256'], data['format'], data['filename'], data['created_at'], data['kind'], data['sql'], json.dumps(data['inputs'])))
            if data.get('connector_checkpoint'):
                checkpoint = data['connector_checkpoint']
                con.execute('UPDATE connectors SET watermark=?,schema_json=?,target_id=? WHERE id=?',
                    (json.dumps(checkpoint['watermark']), json.dumps(checkpoint['schema']), data['id'], checkpoint['id']))
            if data.get('pipeline_run'):
                con.execute("UPDATE pipeline_runs SET status='succeeded',finished_at=?,dataset_id=?,error=NULL WHERE id=?",
                            (now(), data['id'], data['pipeline_run']))
                con.execute('UPDATE pipelines SET target_id=? WHERE id=(SELECT pipeline_id FROM pipeline_runs WHERE id=?)',
                            (data['id'], data['pipeline_run']))
            if data['import_id']:
                con.execute("UPDATE imports SET status='succeeded', finished_at=?, dataset_id=?, size_bytes=?,error=NULL WHERE id=?",
                    (now(), data['id'], data['size'], data['import_id']))
            con.execute('INSERT INTO events(action,detail,created_at) VALUES(?,?,?)',
                        ('dataset.published', f'{data["name"]}: version {data["number"]}', now()))
            con.execute('DELETE FROM publications WHERE id=?', (data['version'],))

    def discard(self, version, physical):
        with duckdb.connect(self.database) as storage:
            storage.execute(f'DROP TABLE IF EXISTS {quote(physical)}')
        shutil.rmtree(self.files / version, ignore_errors=False) if (self.files / version).exists() else None
        with self.catalog() as con:
            con.execute('DELETE FROM publications WHERE id=?', (version,))

    def recover(self):
        with self.catalog() as con:
            journals = con.execute('SELECT * FROM publications ORDER BY created_at').fetchall()
        for journal in journals:
            if journal['state'] == 'ready':
                self.complete(json.loads(journal['payload']))
            else:
                self.discard(journal['id'], journal['table_name'])

    def reconcile(self):
        with self.catalog() as con, duckdb.connect(self.database) as storage:
            known = {r[0] for r in con.execute('SELECT table_name FROM dataset_versions')}
            known.update(r[0] for r in con.execute('SELECT table_name FROM datasets'))
            actual = {r[0] for r in storage.execute('SHOW TABLES').fetchall()}
            con.execute('DELETE FROM recovery_findings')
            for table in actual - known:
                if re.fullmatch(r'(ds|ver)_[0-9a-f]{32}', table):
                    con.execute('INSERT INTO recovery_findings VALUES(?,?,?)', (table, 'unreferenced_table', now()))
            for table in known - actual:
                con.execute('INSERT INTO recovery_findings VALUES(?,?,?)', (table, 'missing_table', now()))

    def source(self, ident, version=None):
        versions = self.versions(ident)
        row = next((v for v in versions if v['id'] == version), None) if version else (versions[0] if versions else None)
        if not row:
            raise LookupError('Dataset version not found')
        path = self.files / row['id'] / 'source'
        if not path.exists() and row['id'] == ident:  # Legacy retained source.
            path = self.base / 'sources' / ident
        if not path.is_file():
            raise LookupError('Original source file not available')
        return path, row
