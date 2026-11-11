"""Workspace-scoped Iceberg publication through a configured Trino REST endpoint."""
from datetime import date, datetime, time as clock, timezone
from decimal import Decimal
import hashlib
import math
import os
import re
import time
from urllib.parse import urlsplit
import uuid

import duckdb
import httpx
from app.workflows import quote, now


def literal(value, dtype):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, bytes):
        return "from_hex('" + value.hex() + "')"
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError('Non-finite numeric values are not supported by the lakehouse publication adapter.')
    if isinstance(value, int) and abs(value) >= 10**38:
        raise ValueError('Integer value exceeds Iceberg decimal precision.')
    if isinstance(value, datetime):
        if value.tzinfo:
            text = value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f UTC')
        else:
            text = value.isoformat(sep=' ')
    elif isinstance(value, Decimal):
        text = format(value, 'f')
    else:
        text = str(value)
    text = text.replace("'", "''")
    if len(text.encode()) > 400000:
        raise ValueError('A cell exceeds the lakehouse SQL adapter size limit.')
    return f"CAST('{text}' AS {dtype})"


def type_mapping(dtype):
    types = {'BOOLEAN':'BOOLEAN','TINYINT':'TINYINT','SMALLINT':'SMALLINT','INTEGER':'INTEGER','BIGINT':'BIGINT',
        'UTINYINT':'SMALLINT','USMALLINT':'INTEGER','UINTEGER':'BIGINT','UBIGINT':'DECIMAL(20,0)',
        'HUGEINT':'DECIMAL(38,0)','UHUGEINT':'DECIMAL(38,0)','FLOAT':'REAL','DOUBLE':'DOUBLE',
        'VARCHAR':'VARCHAR','BLOB':'VARBINARY','DATE':'DATE','TIME':'TIME(6)',
        'TIMESTAMP':'TIMESTAMP(6)','TIMESTAMP WITH TIME ZONE':'TIMESTAMP(6) WITH TIME ZONE','UUID':'VARCHAR'}
    if dtype in types:
        return types[dtype]
    if re.fullmatch(r'DECIMAL\(\d+,\d+\)', dtype):
        return dtype
    raise ValueError('Unsupported lakehouse column type: ' + dtype)


class Lakehouse:
    def __init__(self, workflows):
        self.workflows = workflows
        with workflows.catalog() as con:
            row=con.execute("SELECT value FROM workspace_settings WHERE key='lakehouse_schema'").fetchone()
            if not row:
                prior=con.execute('SELECT table_name FROM lakehouse_exports LIMIT 1').fetchone()
                match=re.match(r'lakehouse\."(ws_[a-z0-9]+)"\.',prior['table_name']) if prior else None
                namespace=match[1] if match else 'ws_'+uuid.uuid4().hex
                con.execute("INSERT OR IGNORE INTO workspace_settings VALUES('lakehouse_schema',?)",(namespace,))
                row=con.execute("SELECT value FROM workspace_settings WHERE key='lakehouse_schema'").fetchone()
            self.schema=row['value']
        if not re.fullmatch(r'ws_[a-f0-9]{24,32}',self.schema):
            raise ValueError('Invalid retained lakehouse namespace.')

    def query(self, statement, interactive=False):
        endpoint = os.environ.get('DATA_PLATFORM_TRINO_URL', '').rstrip('/')
        if not endpoint:
            raise ValueError('Configure DATA_PLATFORM_TRINO_URL before using the lakehouse.')
        parsed = urlsplit(endpoint)
        if parsed.scheme != 'https' and not (parsed.scheme == 'http' and parsed.hostname in ('localhost','127.0.0.1')):
            raise ValueError('Trino requires HTTPS except for loopback development.')
        deadline, rows, columns, next_uri = time.monotonic() + 120, [], [], None
        headers={'X-Trino-User':'platform_'+self.schema,'Content-Type':'text/plain; charset=utf-8'}
        if interactive:
            headers['X-Trino-Session']='query_max_run_time=10s,query_max_memory=256MB'
        with httpx.Client(timeout=30, headers=headers) as client:
            response = client.post(endpoint + '/v1/statement', content=statement.encode())
            while True:
                response.raise_for_status()
                page = response.json()
                if 'error' in page:
                    if 'TIME_LIMIT' in page['error'].get('errorName',''):
                        raise TimeoutError(page['error'].get('message','Query deadline exceeded.'))
                    raise ValueError(page['error'].get('message', 'Trino query failed'))
                columns = [column['name'] for column in page.get('columns', [])] or columns
                rows.extend(page.get('data', []))
                next_uri = page.get('nextUri')
                if not next_uri:
                    return {'columns': columns, 'rows': rows}
                if urlsplit(next_uri)[:2] != parsed[:2]:
                    raise ValueError('Trino returned a query URL outside its configured origin.')
                if time.monotonic() > deadline:
                    client.delete(next_uri)
                    raise TimeoutError('Trino exceeded the 120-second deadline.')
                time.sleep(0.05)
                response = client.get(next_uri)

    def publish(self, ident, version_id=None):
        with self.workflows.lock:
            self.workflows.recover()
            dataset = self.workflows.dataset(ident)
            versions=self.workflows.versions(ident)
            version=next((item for item in versions if item['id']==version_id),None) if version_id else versions[0]
            if not version:raise ValueError('Dataset version not found for lakehouse publication.')
            final = 'ds_' + ident + '_v' + str(version['number'])
            full = 'lakehouse.' + quote(self.schema) + '.' + quote(final)
            with self.workflows.catalog() as con:
                previous = con.execute('SELECT * FROM lakehouse_exports WHERE version_id=?', (version['id'],)).fetchone()
            if previous and previous['state'] == 'published':
                return {'table': previous['table_name'], 'version_id': version['id'], 'already_published': True}
            if previous and previous['staging_table']:
                # Preserve recovery ownership: remove the old attempt before replacing its journal marker.
                self.query('DROP TABLE IF EXISTS ' + previous['staging_table'])
            staging = 'stg_' + uuid.uuid4().hex
            stage = 'lakehouse.' + quote(self.schema) + '.' + quote(staging)
            with self.workflows.catalog() as con:
                con.execute('INSERT OR REPLACE INTO lakehouse_exports(version_id,dataset_id,table_name,state,created_at,error,staging_table) VALUES(?,?,?,?,?,?,?)',
                    (version['id'], ident, full, 'writing', now(), None, stage))
            try:
                self.query(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{quote(self.schema)} WITH (location='s3://warehouse/{self.schema}/')")
                exists = self.query(f"SHOW TABLES FROM lakehouse.{quote(self.schema)} LIKE '{final}'")['rows']
                if not exists:
                    with duckdb.connect(self.workflows.database, read_only=True, config={'enable_external_access':'false','memory_limit':'256MB','threads':'2'}) as storage:
                        cursor = storage.execute('SELECT * FROM ' + quote(version['table_name']))
                        mapping = [type_mapping(str(item[1])) for item in cursor.description]
                        definition = ','.join(quote(item[0]) + ' ' + dtype for item,dtype in zip(cursor.description, mapping))
                        self.query(f"CREATE TABLE {stage} ({definition}) WITH (format='PARQUET')")
                        while batch := cursor.fetchmany(100):
                            values, size = [], 0
                            for row in batch:
                                value = '(' + ','.join(literal(cell,dtype) for cell,dtype in zip(row,mapping)) + ')'
                                if values and size + len(value.encode()) > 500000:
                                    self.query(f'INSERT INTO {stage} VALUES ' + ','.join(values))
                                    values, size = [], 0
                                values.append(value); size += len(value.encode())
                            if values:
                                self.query(f'INSERT INTO {stage} VALUES ' + ','.join(values))
                    if self.query(f'SELECT count(*) FROM {stage}')['rows'][0][0] != version['row_count']:
                        raise ValueError('Lakehouse row-count validation failed.')
                    self.query(f'ALTER TABLE {stage} RENAME TO {quote(final)}')
                if self.query(f'SELECT count(*) FROM {full}')['rows'][0][0] != version['row_count']:
                    raise ValueError('Published table does not match the source version.')
                with self.workflows.catalog() as con:
                    con.execute("UPDATE lakehouse_exports SET state='published',error=NULL WHERE version_id=?", (version['id'],))
                return {'table': full, 'version_id': version['id'], 'rows': version['row_count']}
            except Exception as exc:
                try:
                    self.query(f'DROP TABLE IF EXISTS {stage}')
                except Exception:
                    pass
                with self.workflows.catalog() as con:
                    con.execute("UPDATE lakehouse_exports SET state='failed',error=? WHERE version_id=?", (str(exc)[:1000], version['id']))
                raise

    def history(self, ident):
        self.workflows.dataset(ident)
        with self.workflows.catalog() as con:
            exports = [dict(row) for row in con.execute('SELECT * FROM lakehouse_exports WHERE dataset_id=? ORDER BY created_at DESC', (ident,))]
        return exports

    def remove(self, version_id, confirmation):
        with self.workflows.lock:
            with self.workflows.catalog() as con:
                row = con.execute('SELECT * FROM lakehouse_exports WHERE version_id=?', (version_id,)).fetchone()
            if not row:
                raise LookupError('Lakehouse copy not found')
            if confirmation != row['table_name']:
                raise ValueError('Lakehouse table name does not match.')
            if row['state'] != 'deleted':
                self.query('DROP TABLE IF EXISTS ' + row['table_name'])
                if row['staging_table']:
                    self.query('DROP TABLE IF EXISTS ' + row['staging_table'])
                with self.workflows.catalog() as con:
                    con.execute("UPDATE lakehouse_exports SET state='deleted',error=NULL WHERE version_id=?", (version_id,))
            return {'deleted': True}

    def validate_select(self, statement):
        import sqlglot
        from sqlglot import exp
        from sqlglot.optimizer.scope import Scope, traverse_scope
        statements=sqlglot.parse(statement,read='trino')
        if len(statements)!=1 or not isinstance(statements[0],(exp.Select,exp.Union,exp.Intersect,exp.Except)):
            raise ValueError('Run one read-only SELECT or WITH query.')
        tree=statements[0]
        prohibited={'insert','delete','update','create','drop','alter','merge','command','copy','set','transaction','commit','rollback','into'}
        if any(node.key in prohibited for node in tree.walk()):
            raise ValueError('Data-changing SQL is disabled.')
        with self.workflows.catalog() as con:
            names={row['table_name'].split('.')[-1].strip('"').lower() for row in
                   con.execute("SELECT table_name FROM lakehouse_exports WHERE state='published'")}
        cte_tables = {id(table) for scope in traverse_scope(tree) for table in scope.tables
                      if not table.db and not table.catalog
                      and isinstance(scope.sources.get(table.alias_or_name), Scope)}
        for table in tree.find_all(exp.Table):
            if not isinstance(table.this,exp.Identifier):
                raise ValueError('External or arbitrary table functions are disabled.')
            if id(table) in cte_tables:
                continue
            if table.catalog.lower() not in ('','lakehouse') or table.db.lower() not in ('',self.schema):
                raise ValueError('Queries may reference only this workspace lakehouse schema.')
            if table.name.lower() not in names:
                raise ValueError('Table is not a published lakehouse copy in this workspace.')
            table.set('catalog',exp.to_identifier('lakehouse'))
            table.set('db',exp.to_identifier(self.schema,quoted=True))
        limit=tree.args.get('limit')
        if limit is not None:
            expression=limit.expression
            if not isinstance(expression,exp.Literal) or not expression.is_int:
                raise ValueError('Use a non-negative integer LIMIT for workbench queries.')
            if int(expression.this)<0:
                raise ValueError('LIMIT must not be negative.')
            if int(expression.this)>1001:
                tree=tree.limit(1001)
        else:
            tree=tree.limit(1001)
        return tree.sql(dialect='trino')

    def select(self, statement):
        started=time.monotonic()
        validated=self.validate_select(statement)
        result=self.query(validated,interactive=True)
        return {'columns':result['columns'],'rows':result['rows'][:1000],
                'truncated':len(result['rows'])>1000,'elapsed_ms':round((time.monotonic()-started)*1000)}
