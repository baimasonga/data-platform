"""PostgreSQL pgoutput v1 decoding and bounded, checkpointed table replication."""
from datetime import datetime, timedelta, timezone
import json
import struct

import duckdb
import psycopg
from psycopg import sql

from app.workflows import quote

MAX_MESSAGES = 5000
MAX_BYTES = 16_000_000
TYPES = {'int2':'SMALLINT','int4':'INTEGER','int8':'BIGINT','float4':'FLOAT','float8':'DOUBLE',
    'bool':'BOOLEAN','text':'VARCHAR','varchar':'VARCHAR','bpchar':'VARCHAR','date':'DATE',
    'timestamp':'TIMESTAMP','timestamptz':'TIMESTAMP WITH TIME ZONE','time':'TIME','bytea':'BLOB','uuid':'UUID'}
UNCHANGED = object()


def names(ident):
    return 'dp_slot_' + ident, 'dp_pub_' + ident


def lsn(value):
    if isinstance(value,int):
        return value
    high,low=value.split('/')
    return (int(high,16)<<32)+int(low,16)


def lsn_text(value):
    return f'{value>>32:X}/{value & 0xffffffff:X}'


def connect(config, replication=False,timeout=20000):
    options={key:config[key] for key in ('host','port','dbname','user','password','sslmode')}
    options.update(connect_timeout=5,autocommit=True,client_encoding='UTF8',options=f'-c statement_timeout={timeout} -c timezone=UTC -c bytea_output=hex')
    if replication:
        options.update(replication='database',cursor_factory=psycopg.ClientCursor)
    return psycopg.connect(**options)


def dtype(column):
    if column['udt']=='numeric':
        precision,scale=column['precision'],column['scale']
        if precision and precision<=38 and scale is not None and 0<=scale<=precision:
            return f'DECIMAL({precision},{scale})'
        raise ValueError('CDC numeric columns require explicit precision up to 38 and a non-negative scale.')
    if column['udt'] not in TYPES:
        raise ValueError('CDC supports flat scalar PostgreSQL columns only; unsupported type: '+column['udt'])
    return TYPES[column['udt']]


def describe(pg,config):
    relation=pg.execute('''SELECT c.oid,c.relkind,c.relispartition,c.relrowsecurity,c.relreplident,c.relpersistence
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s''',
        (config['schema'],config['table'])).fetchone()
    if not relation or relation[1]!='r' or relation[2] or relation[3] or relation[4] not in ('d','f') or relation[5]!='p':
        raise ValueError('CDC requires a regular, non-partitioned table without row-level security and with default/full replica identity.')
    oid=relation[0]
    if pg.execute('SELECT 1 FROM pg_inherits WHERE inhrelid=%s OR inhparent=%s',(oid,oid)).fetchone():
        raise ValueError('CDC table inheritance and partitioning are not supported.')
    rows=pg.execute('''SELECT a.attname,a.atttypid,a.atttypmod,t.typname,
        i.numeric_precision,i.numeric_scale,
        EXISTS(SELECT 1 FROM pg_index p WHERE p.indrelid=a.attrelid AND p.indisprimary
               AND a.attnum=ANY((p.indkey::smallint[])[0:p.indnkeyatts-1])),a.attgenerated
        FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
        JOIN information_schema.columns i ON i.table_schema=%s AND i.table_name=%s AND i.column_name=a.attname
        WHERE a.attrelid=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum''',
        (config['schema'],config['table'],oid)).fetchall()
    if any(row[-1] for row in rows):
        raise ValueError('CDC generated columns are not supported. Replicate a table with ordinary stored scalar columns.')
    columns=[dict(zip(('name','oid','typmod','udt','precision','scale','key'),row[:-1])) for row in rows]
    if not 1<=len(columns)<=200:
        raise ValueError('CDC requires 1–200 accessible columns.')
    if len({c['name'].lower() for c in columns})!=len(columns) or any(not c['name'].strip() or len(c['name'])>500 or any(ord(ch)<32 for ch in c['name']) for c in columns):
        raise ValueError('CDC column names must be non-empty, case-insensitively unique and without control characters.')
    keys=[c['name'] for c in columns if c['key']]
    if not keys or any(c['udt'] not in ('int2','int4','int8','uuid') for c in columns if c['key']):
        raise ValueError('CDC requires a primary key consisting of integer or UUID columns.')
    for column in columns:
        dtype(column)
    return {'relation_oid':oid,'columns':columns,'primary_key':keys,
            'database_oid':pg.execute('SELECT oid FROM pg_database WHERE datname=current_database()').fetchone()[0]}


def publication(pg,config,create=False):
    _,name=names(config['connector_id'])
    row=pg.execute('''SELECT p.oid,p.puballtables,p.pubinsert,p.pubupdate,p.pubdelete,p.pubtruncate,p.pubviaroot,
        obj_description(p.oid,'pg_publication') FROM pg_publication p WHERE pubname=%s''',(name,)).fetchone()
    marker='data-platform:'+config['connector_id']
    if not row and create:
        with pg.transaction():
            pg.execute(sql.SQL('CREATE PUBLICATION {} FOR TABLE {} WITH (publish=\'insert, update, delete, truncate\')').format(
                sql.Identifier(name),sql.Identifier(config['schema'],config['table'])))
            pg.execute(sql.SQL('COMMENT ON PUBLICATION {} IS {}').format(sql.Identifier(name),sql.Literal(marker)))
        return publication(pg,config)
    if not row or row[7]!=marker or row[1] or not all(row[2:6]) or row[6]:
        raise ValueError('CDC publication is missing or changed. Reinitialize explicitly after reviewing the source.')
    tables=pg.execute('SELECT prrelid,prattrs IS NULL,prqual IS NULL FROM pg_publication_rel WHERE prpubid=%s',(row[0],)).fetchall()
    schemas=pg.execute('SELECT 1 FROM pg_publication_namespace WHERE pnpubid=%s',(row[0],)).fetchone()
    if tables!=[(config['relation_oid'],True,True)] or schemas:
        raise ValueError('CDC publication membership, column list or row filter changed.')


def slot(pg,config,allow_missing=False):
    name,_=names(config['connector_id'])
    row=pg.execute('''SELECT plugin,slot_type,database,active,confirmed_flush_lsn::text,wal_status
        FROM pg_replication_slots WHERE slot_name=%s''',(name,)).fetchone()
    if not row:
        if allow_missing:
            return None
        raise ValueError('CDC slot is missing. Reinitialize live sync explicitly; the previous checkpoint cannot be resumed.')
    if row[0]!='pgoutput' or row[1]!='logical' or row[2]!=config['dbname'] or row[3]:
        raise ValueError('CDC slot identity changed or another consumer is using it.')
    if row[5]=='lost' and not allow_missing:
        raise ValueError('CDC retained WAL was lost. Reinitialize live sync explicitly.')
    return row


def acknowledge(pg,config,checkpoint):
    state=slot(pg,config)
    if lsn(state[4])>lsn(checkpoint):
        raise ValueError('Source checkpoint is ahead of the published dataset. Reinitialize live sync explicitly.')
    if lsn(state[4])<lsn(checkpoint):
        position=pg.execute('SELECT end_lsn::text FROM pg_replication_slot_advance(%s,%s::pg_lsn)',
                            (names(config['connector_id'])[0],checkpoint)).fetchone()[0]
        if lsn(position)>lsn(checkpoint):
            raise ValueError('Source advanced beyond the published checkpoint; stop and reinitialize.')


def control(config,state,action):
    """Trusted operator control path: validate identity before ack, inspection or cleanup."""
    with connect(config,timeout=5000) as pg,connect(config,replication=True,timeout=5000) as replication:
        identity=replication.execute('IDENTIFY_SYSTEM').fetchone()
        system,timeline=identity[0],int(identity[1])
        if int(pg.execute('SHOW server_version_num').fetchone()[0])<150000:raise ValueError('CDC requires PostgreSQL 15 or newer.')
        database=pg.execute('SELECT oid FROM pg_database WHERE datname=current_database()').fetchone()[0]
        if state.get('system_id') and (str(system)!=str(state['system_id']) or database!=state['database_oid']):
            raise ValueError('CDC source identity changed; automatic source acknowledgement and cleanup are blocked.')
        if action!='cleanup' and state.get('system_id') and timeline!=state.get('timeline'):
            raise ValueError('CDC source timeline changed. Reinitialize live sync explicitly after reviewing recovery/failover.')
        name,pub=names(config['connector_id'])
        if action=='cleanup':
            marker=pg.execute("SELECT obj_description(oid,'pg_publication') FROM pg_publication WHERE pubname=%s",(pub,)).fetchone()
            if marker and marker[0]!='data-platform:'+config['connector_id']:
                raise ValueError('CDC publication ownership changed; cleanup is blocked.')
            current=slot(pg,config,allow_missing=True)
            if current:
                if not marker:raise ValueError('Cannot prove ownership of this CDC slot without its publication.')
                pg.execute('SELECT pg_drop_replication_slot(%s)',(name,))
            if marker:pg.execute(sql.SQL('DROP PUBLICATION {}').format(sql.Identifier(pub)))
            return {'stopped':True}
        described=describe(pg,config)
        if state.get('relation_oid'):
            if described['relation_oid']!=state['relation_oid'] or described['columns']!=state.get('columns'):
                raise ValueError('CDC source schema changed. Review and reinitialize live sync.')
        publication(pg,{**config,**described})
        if action=='ack':
            acknowledge(pg,config,config['checkpoint'])
            return {'acknowledged':True}
        current=slot(pg,config)
        if config.get('checkpoint') and lsn(current[4])>lsn(config['checkpoint']):
            raise ValueError('Source checkpoint is ahead of the published dataset. Reinitialize live sync explicitly.')
        lag=pg.execute('SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),%s::pg_lsn)::bigint',(config.get('checkpoint') or current[4],)).fetchone()[0]
        retained=pg.execute('SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name=%s',(name,)).fetchone()[0]
        return {'slot_status':current[5],'confirmed_lsn':current[4],'lag_bytes':max(0,lag),'retained_wal_bytes':retained,
                'ack_pending':bool(config.get('checkpoint') and lsn(current[4])<lsn(config['checkpoint']))}


class Reader:
    def __init__(self,data):
        self.data,self.pos=bytes(data),0

    def take(self,n):
        if n<0 or self.pos+n>len(self.data):
            raise ValueError('Truncated pgoutput message.')
        result=self.data[self.pos:self.pos+n];self.pos+=n
        return result

    def number(self,fmt):
        return struct.unpack('!'+fmt,self.take(struct.calcsize(fmt)))[0]

    def string(self):
        end=self.data.find(b'\0',self.pos)
        if end<0:
            raise ValueError('Unterminated pgoutput identifier.')
        return self.take(end-self.pos+1)[:-1].decode('utf8')

    def tuple(self):
        values=[]
        for _ in range(self.number('H')):
            tag=self.take(1)
            if tag==b'n':values.append(None)
            elif tag==b'u':values.append(UNCHANGED)
            elif tag==b't':values.append(self.take(self.number('I')).decode('utf8'))
            else:raise ValueError('Unsupported binary pgoutput tuple value.')
        return values


def decode(messages,config):
    """Only complete transactions are returned; unexpected relations/schema fail closed."""
    events,pending,inside,relations=[],[],False,set()
    checkpoint=None;committed_at=None
    for raw in messages:
        r=Reader(raw);tag=r.take(1)
        if tag==b'B':
            if inside:raise ValueError('Nested CDC transaction.')
            r.take(20);inside=True;pending=[]
        elif tag==b'C':
            if not inside:raise ValueError('CDC commit without begin.')
            r.take(1);r.take(8);end=r.number('Q');stamp=r.number('Q')
            if end>lsn(config['checkpoint']):
                events.extend(pending);checkpoint=lsn_text(end)
                committed_at=(datetime(2000,1,1,tzinfo=timezone.utc)+timedelta(microseconds=stamp)).isoformat()
            pending=[];inside=False
        elif tag==b'R':
            oid=r.number('I');namespace=r.string();table=r.string();identity=r.take(1)
            columns=[]
            for _ in range(r.number('H')):
                flag=r.take(1)[0];name=r.string();type_oid=r.number('I');typmod=r.number('i')
                columns.append((name,type_oid,typmod,bool(flag&1)))
            expected=[(c['name'],c['oid'],c['typmod'],c['key'] if identity==b'd' else True) for c in config['columns']]
            if oid!=config['relation_oid'] or namespace!=config['schema'] or table!=config['table'] or columns!=expected:
                raise ValueError('CDC relation or schema changed. Review and reinitialize live sync.')
            relations.add(oid)
        elif tag in (b'I',b'U',b'D'):
            oid=r.number('I')
            if not inside or oid not in relations:raise ValueError('CDC row has no validated transaction/relation.')
            old=None;marker=r.take(1)
            if marker in (b'K',b'O'):
                old=r.tuple()
                if tag!=b'D':marker=r.take(1)
            new=None
            if tag!=b'D':
                if marker!=b'N':raise ValueError('CDC row lacks a new tuple.')
                new=r.tuple()
            if any(values is not None and len(values)!=len(config['columns']) for values in (old,new)):
                raise ValueError('CDC tuple column count changed.')
            pending.append((tag.decode(),old,new))
        elif tag==b'T':
            count=r.number('I');r.take(1)
            if not inside:raise ValueError('CDC truncate outside a transaction.')
            for _ in range(count):
                if r.number('I')!=config['relation_oid']:raise ValueError('Unexpected truncated table.')
            pending.append(('T',None,None))
        elif tag==b'Y':
            r.take(4);r.string();r.string()
        elif tag==b'O':
            r.take(8);r.string()
        else:
            raise ValueError('Unsupported pgoutput protocol message.')
        if r.pos!=len(r.data):raise ValueError('Unexpected bytes in pgoutput message.')
    if inside:raise ValueError('CDC batch ended inside a transaction; checkpoint was not advanced.')
    return events,checkpoint,committed_at


def write_snapshot(pg,config,storage):
    total=0
    with pg.cursor(name='platform_cdc_snapshot') as cursor:
        cursor.execute(sql.SQL('SELECT * FROM {} LIMIT 100001').format(sql.Identifier(config['schema'],config['table'])))
        while rows:=cursor.fetchmany(100):
            total+=len(rows)
            if total>100000:raise ValueError('CDC dataset exceeds 100,000 rows.')
            insert_rows(storage,config,rows)
    return total


def insert_rows(storage,config,rows):
    casts=','.join('CAST(? AS '+dtype(c)+')' for c in config['columns'])
    # bytea text uses PostgreSQL hex syntax; native snapshot bytes pass directly.
    normalized=[]
    for row in rows:
        normalized.append([bytes.fromhex(value[2:]) if c['udt']=='bytea' and isinstance(value,str) and value.startswith('\\x') else value
                           for c,value in zip(config['columns'],row)])
    storage.executemany('INSERT INTO current_data VALUES('+casts+')',normalized)


def apply(storage,config,events):
    keys=[index for index,c in enumerate(config['columns']) if c['name'] in config['primary_key']]
    where=' AND '.join(quote(config['columns'][i]['name'])+'=CAST(? AS '+dtype(config['columns'][i])+')' for i in keys)
    counts={'inserted':0,'updated':0,'deleted':0,'truncated':0}
    for kind,old,new in events:
        if kind=='T':storage.execute('DELETE FROM current_data');counts['truncated']+=1;continue
        identity=old if old is not None else new
        values=[identity[index] for index in keys]
        if any(value is None or value is UNCHANGED for value in values):raise ValueError('CDC primary key is missing.')
        previous=storage.execute('SELECT * FROM current_data WHERE '+where,values).fetchone()
        if kind=='U' and previous is None:raise ValueError('CDC update target is missing; checkpoint consistency failed.')
        storage.execute('DELETE FROM current_data WHERE '+where,values)
        if kind!='D':
            row=[previous[i] if value is UNCHANGED and previous is not None else value for i,value in enumerate(new)]
            if any(value is UNCHANGED for value in row):raise ValueError('CDC TOAST value has no previous row.')
            insert_rows(storage,config,[row])
        counts[{'I':'inserted','U':'updated','D':'deleted'}[kind]]+=1
    return counts


def capture(config,database,output):
    """Called in the existing resource-bounded extraction child process."""
    with connect(config) as pg, connect(config,replication=True) as replication:
        identity=replication.execute('IDENTIFY_SYSTEM').fetchone()
        system,timeline=identity[0],int(identity[1])
        if int(pg.execute('SHOW server_version_num').fetchone()[0])<150000:raise ValueError('CDC requires PostgreSQL 15 or newer.')
        if pg.execute('SHOW wal_level').fetchone()[0]!='logical':raise ValueError('CDC requires PostgreSQL wal_level=logical.')
        metadata=describe(pg,config)
        config={**config,**metadata,'system_id':system,'timeline':timeline}
        previous=config.get('cdc_state') or {}
        if config.get('checkpoint') and any(str(previous.get(k))!=str(config[k]) for k in ('system_id','database_oid','relation_oid','timeline')):
            raise ValueError('CDC source identity changed. Reinitialize live sync explicitly.')
        if config.get('checkpoint') and previous.get('columns')!=config['columns']:
            raise ValueError('CDC source schema changed. Review and reinitialize live sync.')
        publication(pg,config,create=not config.get('checkpoint'))
        with duckdb.connect(config={'memory_limit':'256MB','threads':'2'}) as storage:
            definition=','.join(quote(c['name'])+' '+dtype(c) for c in config['columns'])
            storage.execute('CREATE TABLE current_data('+definition+')')
            if not config.get('checkpoint'):
                existing=slot(pg,config,allow_missing=True)
                if existing:pg.execute('SELECT pg_drop_replication_slot(%s)',(names(config['connector_id'])[0],))
                result=replication.execute(sql.SQL('CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT').format(
                    sql.Identifier(names(config['connector_id'])[0]))).fetchone()
                checkpoint,snapshot=str(result[1]),result[2]
                with connect(config) as reader:
                    with reader.transaction():
                        reader.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                        reader.execute(sql.SQL('SET TRANSACTION SNAPSHOT {}').format(sql.Literal(snapshot)))
                        if describe(reader,config)!=metadata:raise ValueError('CDC schema changed during initialization.')
                        count=write_snapshot(reader,config,storage)
                stats={'inserted':count,'updated':0,'deleted':0,'truncated':0,'bootstrap':True}
                changed=True;source_commit_at=None
            else:
                acknowledge(pg,config,config['checkpoint'])
                if not config.get('target_table'):raise ValueError('CDC published dataset is missing.')
                with duckdb.connect(database,read_only=True,config={'enable_external_access':'false','memory_limit':'256MB','threads':'2'}) as current:
                    cursor=current.execute('SELECT * FROM '+quote(config['target_table']))
                    while rows:=cursor.fetchmany(100):insert_rows(storage,config,rows)
                fence=pg.execute('SELECT pg_current_wal_flush_lsn()::text').fetchone()[0]
                messages=[];size=0
                # Cursor batches bound client memory even when PostgreSQL emits a large complete transaction.
                with pg.transaction():
                    with pg.cursor(name='platform_cdc_changes') as cursor:
                        cursor.execute('''SELECT data FROM pg_logical_slot_peek_binary_changes(%s,%s::pg_lsn,%s,
                            'proto_version','1','publication_names',%s)''',(names(config['connector_id'])[0],fence,MAX_MESSAGES,names(config['connector_id'])[1]))
                        while rows:=cursor.fetchmany(100):
                            for row in rows:
                                size+=len(row[0]);messages.append(bytes(row[0]))
                                if size>MAX_BYTES or len(messages)>100000:raise ValueError('CDC batch exceeds safe limits. Reduce transaction size or reinitialize from a reviewed snapshot.')
                events,checkpoint,source_commit_at=decode(messages,config)
                checkpoint=checkpoint or config['checkpoint']
                if len(messages)<MAX_MESSAGES:checkpoint=lsn_text(max(lsn(checkpoint),lsn(fence)))
                stats=apply(storage,config,events);stats['bootstrap']=False;changed=bool(events)
            count=storage.execute('SELECT count(*) FROM current_data').fetchone()[0]
            if count>100000:raise ValueError('CDC dataset exceeds 100,000 rows.')
            storage.execute('COPY current_data TO ? (FORMAT PARQUET)',[str(output)])
        cdc={key:config[key] for key in ('system_id','database_oid','relation_oid','columns','primary_key','timeline')}
        cdc['source_commit_at']=source_commit_at or previous.get('source_commit_at')
        return {'rows':count,'new_rows':stats['inserted'],'watermark':checkpoint,
                'schema':[[c['name'],c['udt'],c['precision'],c['scale']] for c in config['columns']],
                'cdc':cdc,'stats':stats,'changed':changed,'references':[]}
