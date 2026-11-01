"""Bounded child processes for file parsing and SELECT materialisation."""
import json
import multiprocessing as mp
import re
from pathlib import Path


def bound(cpu=20):
    try:
        import resource
    except ImportError:
        return  # Windows retains the process deadline and engine limits.
    resource.setrlimit(resource.RLIMIT_CPU,(cpu,cpu))
    resource.setrlimit(resource.RLIMIT_FSIZE,(128*1024**2,128*1024**2))
    try:
        virtual=int(Path('/proc/self/statm').read_text().split()[0])*resource.getpagesize()
    except OSError:
        virtual=0
    memory=max(3*1024**3,virtual+1024**3)
    resource.setrlimit(resource.RLIMIT_AS,(memory,memory))


def _work(kind, source, suffix, database, sql, output, result):
    config = {}
    try:
        bound()
        import duckdb
        import pandas as pd
        from app.ingestion import load_frame, MAX_ROWS
        references = []
        extra = {}
        source_types = None
        empty_sql_types = None
        if kind == 'postgres-cdc':
            from app.cdc import capture
            config=json.loads(Path(source).read_text())
            payload=capture(config,database,output)
            Path(result).write_text(json.dumps(payload))
            return
        if kind == 'postgres':
            import psycopg
            from psycopg import sql as pgsql
            config = json.loads(Path(source).read_text())
            connection_keys = ('host', 'port', 'dbname', 'user', 'password', 'sslmode')
            with psycopg.connect(**{key: config[key] for key in connection_keys}, connect_timeout=5,
                                 options='-c statement_timeout=20000') as pg:
                pg.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                schema = pg.execute("SELECT column_name,udt_name,numeric_precision,numeric_scale FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                                    (config['schema'], config['table'])).fetchall()
                if not schema:
                    raise ValueError('Source table not found or not accessible.')
                schema = [list(row) for row in schema]
                if config.get('expected_schema') and config['expected_schema'] != (schema if len(config['expected_schema'][0]) == 4 else [item[:2] for item in schema]):
                    raise ValueError('Source schema changed. Create a new snapshot connector or review the schema before advancing the watermark.')
                table = pgsql.Identifier(config['schema'], config['table'])
                checkpoint = config.get('checkpoint')
                conditions, parameters = pgsql.SQL(''), []
                if config['mode'] == 'incremental':
                    column = config['watermark_column']
                    if {item[0]:item[1] for item in schema}.get(column) not in ('int2', 'int4', 'int8'):
                        raise ValueError('Incremental watermarks require a unique, non-null integer source column.')
                    identifier = pgsql.Identifier(column)
                    if pg.execute(pgsql.SQL('SELECT 1 FROM {} WHERE {} IS NULL LIMIT 1').format(table, identifier)).fetchone():
                        raise ValueError('Watermark column contains nulls.')
                    if pg.execute(pgsql.SQL('SELECT 1 FROM {} GROUP BY {} HAVING count(*)>1 LIMIT 1').format(table, identifier)).fetchone():
                        raise ValueError('Watermark column must be unique.')
                    if checkpoint is not None:
                        conditions = pgsql.SQL(' WHERE {} > %s').format(identifier)
                        parameters.append(checkpoint)
                    order = pgsql.SQL(' ORDER BY {}').format(identifier)
                else:
                    order = pgsql.SQL('')
                with pg.cursor(name='platform_extract') as cursor:
                    cursor.execute(pgsql.SQL('SELECT * FROM {}{}{} LIMIT 100001').format(table, conditions, order), parameters)
                    rows = cursor.fetchmany(MAX_ROWS + 1)
                    columns = [item.name for item in cursor.description]
                source_types = schema
                new_rows = len(rows)
                frame = pd.DataFrame(rows, columns=columns, dtype=object)
                if config['mode'] == 'incremental' and rows:
                    checkpoint = max(row[columns.index(config['watermark_column'])] for row in rows)
                if config['mode'] == 'incremental' and config.get('target_table'):
                    with duckdb.connect(database, read_only=True, config={'enable_external_access':'false','memory_limit':'256MB','threads':'2'}) as current:
                        quoted = '"' + config['target_table'].replace('"', '""') + '"'
                        previous = current.execute('SELECT * FROM ' + quoted)
                        old_columns = [item[0] for item in previous.description]
                        if old_columns != columns:
                            raise ValueError('Published schema does not match the source.')
                        old = pd.DataFrame(previous.fetchmany(MAX_ROWS + 1), columns=columns, dtype=object)
                    frame = pd.concat([old, frame], ignore_index=True)
                extra = {'watermark': checkpoint, 'schema': schema, 'new_rows': new_rows}
        elif kind == 'import':
            frame = load_frame(Path(source).read_bytes(), suffix, Path(output).parent)
        else:
            with duckdb.connect(database, read_only=True, config={
                'enable_external_access': 'false', 'memory_limit': '256MB', 'threads': '2'
            }) as con:
                statements = con.extract_statements(sql)
                if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
                    raise ValueError('Run one SELECT or WITH transformation. Data-changing SQL is disabled.')
                references = sorted(con.get_table_names(sql))
                cursor = con.execute(sql)
                columns = [item[0] for item in cursor.description]
                frame = pd.DataFrame(cursor.fetchmany(MAX_ROWS + 1), columns=columns, dtype=object)
                if kind=='backfill' and frame.empty:empty_sql_types=[str(item[1]) for item in cursor.description]
        if (frame.empty and kind!='backfill') or len(frame) > MAX_ROWS or len(frame.columns) > 200:
            raise ValueError('Import requires 1–100,000 rows and 1–200 columns.')
        names = [str(c) for c in frame.columns]
        if len(set(n.lower() for n in names)) != len(names) or any(not n.strip() or len(n)>500 or any(ord(c)<32 for c in n) for n in names):
            raise ValueError('Column names must be unique (case-insensitive), non-empty, at most 500 characters, and contain no control characters.')
        frame.columns = names
        for name in names:
            if frame[name].map(lambda value: isinstance(value, (dict, list))).any():
                raise ValueError('Nested JSON is not supported. Flatten row values first.')
        with duckdb.connect(config={'memory_limit': '256MB', 'threads': '2'}) as con:
            con.register('incoming', frame)
            # Only this server-generated statement can write the trusted output.
            if empty_sql_types:
                scalar=r'(?:BOOLEAN|TINYINT|SMALLINT|INTEGER|BIGINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT|HUGEINT|UHUGEINT|FLOAT|DOUBLE|VARCHAR|BLOB|DATE|TIME|TIME WITH TIME ZONE|TIMESTAMP(?:_S|_MS|_NS)?|TIMESTAMP WITH TIME ZONE|UUID|INTERVAL|DECIMAL\(\d+,\d+\))'
                if any(not re.fullmatch(scalar,dtype) for dtype in empty_sql_types):raise ValueError('Empty backfill outputs require supported flat scalar SQL types.')
                definition=','.join('"'+name.replace('"','""')+'" '+dtype for name,dtype in zip(names,empty_sql_types))
                con.execute('CREATE TABLE empty_output('+definition+')')
                con.execute('COPY empty_output TO ? (FORMAT PARQUET)',[output])
            elif source_types:
                types={'int2':'SMALLINT','int4':'INTEGER','int8':'BIGINT','float4':'FLOAT','float8':'DOUBLE',
                    'bool':'BOOLEAN','date':'DATE','timestamp':'TIMESTAMP','timestamptz':'TIMESTAMP WITH TIME ZONE',
                    'time':'TIME','bytea':'BLOB','uuid':'UUID'}
                columns=[]
                for name,udt,precision,scale in source_types:
                    dtype=types.get(udt,'VARCHAR')
                    if udt=='numeric' and precision and precision<=38 and scale is not None and 0<=scale<=precision:
                        dtype=f'DECIMAL({precision},{scale})'
                    quoted='"'+name.replace('"','""')+'"'
                    columns.append(f'CAST({quoted} AS {dtype}) AS {quoted}')
                con.execute('COPY (SELECT '+','.join(columns)+' FROM incoming) TO ? (FORMAT PARQUET)',[output])
            else:
                con.execute('COPY incoming TO ? (FORMAT PARQUET)', [output])
        Path(result).write_text(json.dumps({'rows': len(frame), 'references': references, **extra}))
    except Exception as exc:
        error = str(exc)
        if config.get('password'): error = error.replace(config['password'], '[redacted]')
        Path(result).write_text(json.dumps({'error': error[:1500]}))


def prepare(kind, source, suffix, database, sql, directory, timeout=30):
    output = Path(directory) / 'prepared.parquet'
    result = Path(directory) / 'worker.json'
    process = mp.get_context('spawn').Process(target=_work,
        args=(kind, str(source), suffix, database, sql, str(output), str(result)))
    try:
        process.start()
        process.join(timeout)
        if process.is_alive():
            raise TimeoutError('Processing exceeded the 30-second time limit.')
        if not result.exists():
            raise ValueError('Processing worker stopped before completing (resource limit or parser failure).')
        payload = json.loads(result.read_text())
        if 'error' in payload:
            raise ValueError(payload['error'])
        if not output.is_file():
            raise ValueError('Processing did not produce a dataset.')
        return output
    finally:
        if process.is_alive():
            process.terminate()
            process.join(2)
            if process.is_alive():
                process.kill()
                process.join()
        if process.pid is not None:
            process.close()
