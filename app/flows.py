"""Durable bounded dependency workflows, immutable inputs and dated backfills."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re
import threading
import uuid

from fastapi import Header, HTTPException
from pydantic import BaseModel, Field, model_validator
from typing import Literal
import sqlglot
from sqlglot import exp

from app.catalog import connection
from app.workflows import now
from app.partitions import partition_window,backfill_windows,catchup_windows


class Rule(BaseModel):
    column_name: str = Field(min_length=1,max_length=500)
    rule: Literal['not_null','unique','min','max']
    threshold: float | None = Field(default=None,allow_inf_nan=False)

    @model_validator(mode='after')
    def bound(self):
        if self.rule in ('min','max') and self.threshold is None:
            raise ValueError('Numeric quality rules require a bound.')
        return self


class Step(BaseModel):
    id: str = Field(pattern=r'^[a-z][a-z0-9_]{0,39}$')
    name: str = Field(min_length=1,max_length=100)
    kind: Literal['pipeline','quality','publish']
    depends_on: list[str] = Field(default_factory=list,max_length=20)
    pipeline_id: str | None = None
    input_step: str | None = None
    rules: list[Rule] = Field(default_factory=list,max_length=30)


class Flow(BaseModel):
    name: str = Field(min_length=1,max_length=200)
    revision: int = Field(default=0,ge=0)
    enabled: bool = False
    interval_seconds: int = Field(default=3600,ge=60,le=31536000)
    steps: list[Step] = Field(min_length=1,max_length=20)
    catch_up: Literal['none','day','hour'] = 'none'
    catch_up_start: str | None = Field(default=None,max_length=40)

    @model_validator(mode='after')
    def graph(self):
        self.name=self.name.strip()
        if not self.name or any(ord(c)<32 for c in self.name):raise ValueError('Use a meaningful workflow name.')
        ordered(self.steps)
        if self.catch_up!='none':
            if not self.catch_up_start:raise ValueError('Choose the first partition for automatic catch-up.')
            self.catch_up_start=partition_window(self.catch_up_start,self.catch_up)['key']
        else:self.catch_up_start=None
        return self


class Request(BaseModel):
    revision: int = Field(ge=1)
    partition_date: date | None = None

    @model_validator(mode='after')
    def window(self):
        if self.partition_date:partition_window(self.partition_date.isoformat())
        return self


class ScheduleRequest(BaseModel):
    model_config={'extra':'forbid'}
    revision: int = Field(ge=1)


class Backfill(BaseModel):
    revision: int = Field(ge=1)
    start: str = Field(min_length=10,max_length=40)
    end: str = Field(min_length=10,max_length=40)
    granularity: Literal['day','hour'] = 'day'

    @model_validator(mode='after')
    def dates(self):
        windows=self.partitions()
        self.start,self.end=windows[0]['key'],windows[-1]['key']
        return self

    def partitions(self):return backfill_windows(self.start,self.end,self.granularity)


class Delete(BaseModel):
    revision: int = Field(ge=1)
    confirmation_name: str


def ordered(steps):
    items={step.id:step for step in steps}
    if len(items)!=len(steps):raise ValueError('Step IDs must be unique.')
    for step in steps:
        if len(set(step.depends_on))!=len(step.depends_on) or any(dep not in items or dep==step.id for dep in step.depends_on):
            raise ValueError('Dependencies must name distinct other steps in this workflow.')
        if step.kind=='pipeline' and not step.pipeline_id:raise ValueError('Select a saved pipeline for every pipeline step.')
        if step.kind!='pipeline' and (step.input_step not in step.depends_on or items[step.input_step].kind=='publish'):
            raise ValueError('Quality/publication steps need a dataset-producing input included in their dependencies.')
        if step.kind=='quality' and not step.rules:raise ValueError('Quality steps require at least one explicit rule.')
    result=[]
    while len(result)<len(items):
        ready=[step.id for step in steps if step.id not in result and all(dep in result for dep in step.depends_on)]
        if not ready:raise ValueError('Workflow dependencies contain a cycle.')
        result.extend(ready)
    return result


def sql_tree(statement,outputs=None,partition=None,granularity='day'):
    window=partition_window(partition or '2000-01-01',granularity)
    def replace(match):
        key=match.group(1)
        if key=='partition_date':
            return "'"+window['date']+"'"
        if key in ('partition_start','partition_end'):
            return "CAST('"+window['start' if key=='partition_start' else 'end']+"' AS TIMESTAMPTZ)"
        if key.startswith('steps.') and re.fullmatch(r'[a-z][a-z0-9_]{0,39}',key[6:]):
            return '"'+((outputs or {}).get(key[6:]) or ('flow_ref_'+key[6:]))+'"'
        raise ValueError('Unknown SQL template. Use {{partition_date}}, {{partition_start}}, {{partition_end}} or {{steps.step_id}}.')
    rendered=re.sub(r'\{\{\s*([^{}]+?)\s*\}\}',replace,statement)
    if '{{' in rendered or '}}' in rendered:raise ValueError('Malformed workflow SQL placeholder.')
    trees=sqlglot.parse(rendered,read='duckdb')
    if len(trees)!=1 or not isinstance(trees[0],(exp.Select,exp.Union,exp.Except,exp.Intersect)):
        raise ValueError('Workflow SQL must contain one SELECT or WITH query.')
    return trees[0]


class Flows:
    def __init__(self,metadata,workflows,pipelines,quality,publish):
        self.metadata,self.workflows,self.pipelines=metadata,workflows,pipelines
        self.quality,self.publish=quality,publish
        self.stop=threading.Event();self.thread=None
        with connection(metadata) as con:
            con.execute("UPDATE flow_runs SET status='interrupted',finished_at=?,error='Server stopped. Retry explicitly to resume saved steps.' WHERE status IN ('queued','running')",(now(),))
            con.execute("UPDATE flow_step_runs SET status='interrupted',finished_at=?,error='Server stopped during this step.' WHERE status='running'",(now(),))

    def start(self):
        if self.thread and self.thread.is_alive():return
        self.stop.clear();self.thread=threading.Thread(target=self.loop,daemon=True,name='workflow-worker');self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(60)
            if self.thread.is_alive():raise RuntimeError('Workflow step did not drain. Keep its data-directory lease until process exit.')
        with connection(self.metadata) as con:
            con.execute("UPDATE flow_runs SET status='interrupted',finished_at=?,error='Server stopped before execution. Retry explicitly.' WHERE status='queued'",(now(),))

    def loop(self):
        import os
        while not self.stop.wait(.25):
            try:
                if os.environ.get('DATA_PLATFORM_ORCHESTRATOR')!='dagster':self.tick()
                with connection(self.metadata) as con:
                    row=con.execute("SELECT id FROM flow_runs WHERE status='queued' ORDER BY created_at,id LIMIT 1").fetchone()
                if row:self.execute(row['id'])
            except Exception:
                import logging
                logging.getLogger(__name__).error('Workflow worker could not complete a queued operation; inspect durable run history.')

    def definition(self,con,ident):
        row=con.execute('SELECT * FROM flows WHERE id=?',(ident,)).fetchone()
        if not row:raise HTTPException(404,'Workflow not found')
        active=bool(con.execute("SELECT 1 FROM flow_runs WHERE flow_id=? AND status IN ('queued','running') LIMIT 1",(ident,)).fetchone())
        return {'catch_up':'none','catch_up_start':None,**json.loads(row['content']),'has_active_runs':active,'id':ident,'revision':row['revision'],'next_run':row['next_run'],'updated_at':row['updated_at']}

    def freeze(self,con,flow,detached):
        frozen={};pins={};writers=set()
        for step in flow['steps']:
            if step['kind']!='pipeline':continue
            row=con.execute('SELECT * FROM pipelines WHERE id=?',(step['pipeline_id'],)).fetchone()
            if not row:raise HTTPException(400,'A referenced pipeline was deleted.')
            definition=dict(row)
            if definition['id'] in frozen:raise HTTPException(400,'A saved pipeline may appear only once per workflow.')
            if definition['connector_id']:
                if detached:raise HTTPException(400,'Historical backfills support SQL pipelines only. Sources cannot reconstruct historical changes.')
                source=con.execute('SELECT target_id FROM connectors WHERE id=?',(definition['connector_id'],)).fetchone()
                if not source:raise HTTPException(400,'Source connection not found')
                owner='source:'+definition['connector_id'];target=source['target_id']
            else:
                try:tree=sql_tree(definition['sql'])
                except (ValueError,sqlglot.errors.ParseError) as exc:raise HTTPException(400,str(exc)[:1000]) from exc
                refs=re.findall(r'\{\{\s*steps\.([a-z][a-z0-9_]*)\s*\}\}',definition['sql'])
                if any(ref not in step['depends_on'] for ref in refs):raise HTTPException(400,'SQL output references must also be direct dependencies.')
                aliases={cte.alias_or_name for cte in tree.find_all(exp.CTE)}
                for table in tree.find_all(exp.Table):
                    if not isinstance(table.this,exp.Identifier) or table.name.startswith('flow_ref_') or table.name in aliases:continue
                    version=con.execute('''SELECT v.id,v.dataset_id,v.table_name FROM dataset_versions v JOIN datasets d ON d.id=v.dataset_id
                        WHERE d.table_name=? OR v.table_name=? ORDER BY v.number DESC LIMIT 1''',(table.name,table.name)).fetchone()
                    if not version:raise HTTPException(400,'SQL references an unavailable table. Use an upstream step reference for new outputs.')
                    pins[table.name]=dict(version)
                owner='sql:'+definition['id'];target=definition['target_id']
            for resource in (owner,'target:'+target if target else None):
                if resource and resource in writers:raise HTTPException(400,'Two steps cannot own the same source or output dataset.')
                if resource:writers.add(resource)
            frozen[definition['id']]=definition
            if not definition['connector_id'] and definition['target_id']:
                version=con.execute('SELECT id FROM dataset_versions WHERE dataset_id=? ORDER BY number DESC LIMIT 1',(definition['target_id'],)).fetchone()
                if not version:raise HTTPException(400,'Pipeline output dataset is missing.')
                definition['expected_version']=version['id']
        return {'flow':flow,'pipelines':frozen,'pins':pins,'order':ordered([Step(**step) for step in flow['steps']])}

    def enqueue(self,ident,body,key=None,backfill=False,scheduled=False):
        payload=body.model_dump(mode='json')
        # Preserve pre-hourly daily idempotency keys across upgrades. Hourly
        # endpoints are canonical UTC strings before fingerprinting.
        if backfill and body.granularity=='day':payload.pop('granularity')
        if scheduled:payload['scheduled']=True
        fingerprint=hashlib.sha256(json.dumps({'backfill':backfill,**payload},sort_keys=True).encode()).hexdigest()
        with connection(self.metadata) as con:
            con.execute('BEGIN IMMEDIATE')
            if key:
                cached=con.execute('SELECT * FROM flow_request_keys WHERE key=?',(key,)).fetchone()
                if cached:
                    if cached['flow_id']!=ident or cached['fingerprint']!=fingerprint:raise HTTPException(409,'Idempotency key belongs to a different request.')
                    return {'run_ids':json.loads(cached['run_ids']),'already_queued':True}
            flow=self.definition(con,ident)
            if flow['revision']!=body.revision:raise HTTPException(409,'Workflow changed. Reload before running.')
            if scheduled and (not flow['enabled'] or not flow['next_run'] or flow['next_run']>now()):return {'run_ids':[],'skipped':True}
            if con.execute("SELECT 1 FROM flow_runs WHERE flow_id=? AND status IN ('queued','running')",(ident,)).fetchone():
                raise HTTPException(409,'This workflow already has queued or running work.')
            partitions=body.partitions() if backfill else [partition_window((body.partition_date or datetime.now(timezone.utc).date()).isoformat())]
            pending=con.execute("SELECT count(*) FROM flow_runs WHERE status IN ('queued','running')").fetchone()[0]
            if pending+len(partitions)>64:raise HTTPException(429,'Workspace workflow queue is full (64 runs).')
            snapshot=self.freeze(con,flow,backfill)
            ids=self.insert_runs(con,flow,snapshot,partitions,backfill,'backfill' if backfill else 'schedule' if scheduled else 'manual')
            if flow['enabled'] and flow['catch_up']=='none':
                con.execute('UPDATE flows SET next_run=? WHERE id=?',((datetime.now(timezone.utc)+timedelta(seconds=flow['interval_seconds'])).isoformat(),ident))
            if key:con.execute('INSERT INTO flow_request_keys VALUES(?,?,?,?)',(key,ident,fingerprint,json.dumps(ids)))
        return {'run_ids':ids,'already_queued':False}

    def insert_runs(self,con,flow,snapshot,partitions,detached,trigger,generation=None):
        ids=[]
        for partition in partitions:
            run=uuid.uuid4().hex;ids.append(run)
            con.execute('INSERT INTO flow_runs(id,flow_id,revision,snapshot,partition_date,detached,status,created_at,partition_granularity,partition_start,partition_end,trigger,schedule_generation) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (run,flow['id'],flow['revision'],json.dumps({**snapshot,'partition':partition,'trigger':trigger,'schedule_generation':generation}),partition['date'],int(detached),'queued',now(),partition['granularity'],partition['start'],partition['end'],trigger,generation))
            con.executemany("INSERT INTO flow_step_runs(run_id,step_id,status) VALUES(?,?,'pending')",[(run,step['id']) for step in flow['steps']])
        return ids

    def configure_schedule(self,con,ident,body,current=None):
        con.execute('INSERT OR IGNORE INTO flow_schedule_state(flow_id,next_partition) VALUES(?,?)',(ident,body.catch_up_start))
        if current and (current['catch_up'],current['catch_up_start'])!=(body.catch_up,body.catch_up_start):
            con.execute('UPDATE flow_schedule_state SET next_partition=?,generation=generation+1,last_checked=NULL,last_error=NULL WHERE flow_id=?',(body.catch_up_start,ident))

    def schedule_status(self,con,ident,current=None):
        flow=self.definition(con,ident)
        row=con.execute('SELECT * FROM flow_schedule_state WHERE flow_id=?',(ident,)).fetchone()
        state=dict(row) if row else {'next_partition':flow['catch_up_start'],'last_checked':None,'last_error':None,'generation':0}
        windows=[];due=0
        if flow['catch_up']!='none':windows,due=catchup_windows(state['next_partition'] or flow['catch_up_start'],flow['catch_up'],current or datetime.now(timezone.utc))
        blocked=[dict(row) for row in con.execute("SELECT id,status,error FROM flow_runs WHERE flow_id=? AND trigger='catch_up' AND status IN ('failed','interrupted','cancelled') AND schedule_generation=? ORDER BY created_at LIMIT 20",(ident,state['generation']))]
        return {**state,'catch_up':flow['catch_up'],'enabled':flow['enabled'],'revision':flow['revision'],'next_run':flow['next_run'],
                'has_active_runs':flow['has_active_runs'],'due_partitions':due,'next_batch':windows,'blocked_runs':blocked}

    def schedule(self,ident,body,key=None):
        with connection(self.metadata) as con:flow=self.definition(con,ident)
        if flow['catch_up']=='none':
            try:return self.enqueue(ident,body,key,scheduled=True)
            except HTTPException as exc:
                if exc.status_code in (400,429) or (exc.status_code==409 and flow['has_active_runs']):
                    with connection(self.metadata) as con:
                        con.execute('UPDATE flows SET next_run=? WHERE id=? AND revision=? AND enabled=1',((datetime.now(timezone.utc)+timedelta(seconds=flow['interval_seconds'])).isoformat(),ident,body.revision))
                raise
        fingerprint=hashlib.sha256(json.dumps({'trigger':'catch_up','revision':body.revision},sort_keys=True).encode()).hexdigest()
        try:
            with connection(self.metadata) as con:
                con.execute('BEGIN IMMEDIATE')
                if key:
                    cached=con.execute('SELECT * FROM flow_request_keys WHERE key=?',(key,)).fetchone()
                    if cached:
                        if cached['flow_id']!=ident or cached['fingerprint']!=fingerprint:raise HTTPException(409,'Idempotency key belongs to a different request.')
                        return {'run_ids':json.loads(cached['run_ids']),'already_queued':True}
                flow=self.definition(con,ident)
                if flow['revision']!=body.revision:raise HTTPException(409,'Workflow changed. Reload before scheduling.')
                current=datetime.now(timezone.utc);timestamp=current.isoformat()
                if not flow['enabled'] or not flow['next_run'] or flow['next_run']>timestamp:return {'run_ids':[],'skipped':True}
                status=self.schedule_status(con,ident,current)
                reason='Waiting for active runs.' if flow['has_active_runs'] else 'Retry unsuccessful catch-up runs before continuing.' if status['blocked_runs'] else None
                pending=con.execute("SELECT count(*) FROM flow_runs WHERE status IN ('queued','running')").fetchone()[0]
                windows=status['next_batch'][:max(0,64-pending)] if not reason else []
                if not reason and status['next_batch'] and not windows:reason='Workspace workflow queue is full (64 runs).'
                ids=[]
                if windows:
                    snapshot=self.freeze(con,flow,True)
                    ids=self.insert_runs(con,flow,snapshot,windows,True,'catch_up',status['generation'])
                    cursor=windows[-1]['end'][:10] if flow['catch_up']=='day' else windows[-1]['end']
                    con.execute('UPDATE flow_schedule_state SET next_partition=? WHERE flow_id=?',(cursor,ident))
                con.execute('UPDATE flow_schedule_state SET last_checked=?,last_error=? WHERE flow_id=?',(timestamp,reason,ident))
                con.execute('UPDATE flows SET next_run=? WHERE id=?',((current+timedelta(seconds=flow['interval_seconds'])).isoformat(),ident))
                if key:con.execute('INSERT INTO flow_request_keys VALUES(?,?,?,?)',(key,ident,fingerprint,json.dumps(ids)))
                return {'run_ids':ids,'already_queued':False,'skipped':not bool(ids),'reason':reason}
        except HTTPException as exc:
            # Back off failed schedule attempts without losing the unqueued cursor.
            # A changed next_run also lets external sensors emit a fresh run key.
            if exc.status_code in (400,429):
                with connection(self.metadata) as con:
                    con.execute('BEGIN IMMEDIATE')
                    current=self.definition(con,ident)
                    if current['revision']==body.revision and current['enabled']:
                        con.execute('UPDATE flow_schedule_state SET last_checked=?,last_error=? WHERE flow_id=?',(now(),str(exc.detail)[:1500],ident))
                        con.execute('UPDATE flows SET next_run=? WHERE id=?',((datetime.now(timezone.utc)+timedelta(seconds=flow['interval_seconds'])).isoformat(),ident))
            raise

    def tick(self):
        with connection(self.metadata) as con:
            due=con.execute('SELECT id,revision,next_run FROM flows WHERE enabled=1 AND next_run<=? ORDER BY next_run',(now(),)).fetchall()
        for row in due:
            try:self.schedule(row['id'],Request(revision=row['revision']),key='schedule:'+row['id']+':'+row['next_run'])
            except HTTPException:pass

    def result(self,con,run):
        row=con.execute('SELECT * FROM flow_runs WHERE id=?',(run,)).fetchone()
        if not row:raise HTTPException(404,'Workflow run not found')
        result={key:row[key] for key in row.keys() if key!='snapshot'}
        partition=partition_window(result['partition_start'] if result['partition_granularity']=='hour' else result['partition_date'],result['partition_granularity'])
        result.update(partition_key=partition['key'],partition_start=partition['start'],partition_end=partition['end'])
        result['steps']=[{**dict(step),'result':json.loads(step['result']) if step['result'] else None}
                         for step in con.execute('SELECT * FROM flow_step_runs WHERE run_id=? ORDER BY rowid',(run,))]
        return result

    def execute(self,run):
        with self.workflows.lock:
            with connection(self.metadata) as con:
                changed=con.execute("UPDATE flow_runs SET status='running',started_at=?,error=NULL WHERE id=? AND status='queued'",(now(),run)).rowcount
                if not changed:return
                record=con.execute('SELECT * FROM flow_runs WHERE id=?',(run,)).fetchone()
            snapshot=json.loads(record['snapshot']);steps={step['id']:step for step in snapshot['flow']['steps']}
            final='succeeded';error=None
            try:
                for ident in snapshot['order']:
                    with connection(self.metadata) as con:
                        cancelled=con.execute('SELECT cancel_requested FROM flow_runs WHERE id=?',(run,)).fetchone()[0]
                        states={row['step_id']:dict(row) for row in con.execute('SELECT * FROM flow_step_runs WHERE run_id=?',(run,))}
                    if self.stop.is_set() or cancelled:
                        final='interrupted' if self.stop.is_set() else 'cancelled';error='Stopped at a step boundary. Accepted outputs are retained.';break
                    step=steps[ident]
                    if states[ident]['status']=='succeeded':continue
                    if any(states[dep]['status']!='succeeded' for dep in step['depends_on']):
                        with connection(self.metadata) as con:con.execute("UPDATE flow_step_runs SET status='blocked',error='A required dependency did not succeed.' WHERE run_id=? AND step_id=?",(run,ident))
                        final='failed';continue
                    with connection(self.metadata) as con:con.execute("UPDATE flow_step_runs SET status='running',started_at=?,finished_at=NULL,error=NULL WHERE run_id=? AND step_id=?",(now(),run,ident))
                    try:
                        outputs={key:json.loads(value['result']) for key,value in states.items() if value['status']=='succeeded' and value['result']}
                        if step['kind']=='pipeline':
                            definition=dict(snapshot['pipelines'][step['pipeline_id']])
                            if not definition['connector_id']:
                                physical={key:value['physical_table'] for key,value in outputs.items()}
                                tree=sql_tree(definition['sql'],physical,record['partition_start'] if record['partition_granularity']=='hour' else record['partition_date'],record['partition_granularity'])
                                pins={key:value['table_name'] for key,value in snapshot['pins'].items()}
                                ancestors=set(step['depends_on']);todo=list(ancestors)
                                while todo:
                                    for dep in steps[todo.pop()]['depends_on']:
                                        if dep not in ancestors:ancestors.add(dep);todo.append(dep)
                                for key in ancestors:
                                    if key in outputs:pins[outputs[key]['table_name']]=outputs[key]['physical_table']
                                aliases={cte.alias_or_name for cte in tree.find_all(exp.CTE)}
                                for table in tree.find_all(exp.Table):
                                    if isinstance(table.this,exp.Identifier) and table.name in pins and table.name not in aliases:
                                        original=table.name
                                        table.set('this',exp.to_identifier(pins[original],quoted=True))
                                        if not table.alias:table.set('alias',exp.TableAlias(this=exp.to_identifier(original,quoted=True)))
                                definition['sql']=tree.sql(dialect='duckdb')
                            if record['detached']:
                                partition=partition_window(record['partition_start'] if record['partition_granularity']=='hour' else record['partition_date'],record['partition_granularity'])
                                definition.update(target_id=None,name=(definition['name'][:150]+' ['+partition['key']+']'))
                            result=self.pipelines.run(step['pipeline_id'],key='flow:'+run+':'+ident,definition=definition,detached=bool(record['detached']))
                            if not result.get('version_id'):raise ValueError('Pipeline did not return an immutable dataset version.')
                            version=next(v for v in self.workflows.versions(result['id']) if v['id']==result['version_id'])
                            result.update(physical_table=version['table_name'])
                        else:
                            source=outputs[step['input_step']]
                            result=self.quality(source,step['rules']) if step['kind']=='quality' else self.publish(source)
                        with connection(self.metadata) as con:con.execute("UPDATE flow_step_runs SET status='succeeded',finished_at=?,result=?,error=NULL WHERE run_id=? AND step_id=?",(now(),json.dumps(result),run,ident))
                    except Exception as exc:
                        detail=str(getattr(exc,'detail',exc))[:1500]
                        with connection(self.metadata) as con:con.execute("UPDATE flow_step_runs SET status='failed',finished_at=?,error=? WHERE run_id=? AND step_id=?",(now(),detail,run,ident))
                        final='failed';error='One or more steps failed. Dependent steps were blocked.'
            except Exception:
                final='failed';error='Workflow coordination failed. Inspect step history and retry explicitly.'
            finally:
                with connection(self.metadata) as con:
                    con.execute('UPDATE flow_runs SET status=?,finished_at=?,error=? WHERE id=?',(final,now(),error,run))
                    if final in ('interrupted','cancelled'):
                        con.execute("UPDATE flow_step_runs SET status=?,finished_at=? WHERE run_id=? AND status IN ('pending','running')",(final,now(),run))


def register(app,manager):
    meta=lambda:connection(manager.metadata)

    @app.get('/api/flows')
    def list_flows():
        with meta() as con:return [manager.definition(con,row['id']) for row in con.execute('SELECT id FROM flows ORDER BY updated_at DESC')]

    @app.post('/api/flows',status_code=201)
    def create(body:Flow):
        ident=uuid.uuid4().hex
        with meta() as con:
            con.execute('BEGIN IMMEDIATE');manager.freeze(con,body.model_dump(),body.catch_up!='none')
            con.execute('INSERT INTO flows VALUES(?,?,?,?,?,?,?,?)',(ident,body.name,1,json.dumps(body.model_dump()),int(body.enabled),body.interval_seconds,
                (datetime.now(timezone.utc)+timedelta(seconds=0 if body.catch_up!='none' else body.interval_seconds)).isoformat() if body.enabled else None,now()))
            manager.configure_schedule(con,ident,body)
            return manager.definition(con,ident)

    @app.put('/api/flows/{ident}')
    def edit(ident:str,body:Flow):
        with meta() as con:
            con.execute('BEGIN IMMEDIATE');current=manager.definition(con,ident)
            if current['revision']!=body.revision:raise HTTPException(409,'Workflow changed. Reload before saving.')
            if con.execute("SELECT 1 FROM flow_runs WHERE flow_id=? AND status IN ('queued','running')",(ident,)).fetchone():raise HTTPException(409,'Wait for queued/running work before editing.')
            manager.freeze(con,body.model_dump(),body.catch_up!='none')
            con.execute('UPDATE flows SET name=?,revision=revision+1,content=?,enabled=?,interval_seconds=?,next_run=?,updated_at=? WHERE id=?',
                (body.name,json.dumps(body.model_dump()),int(body.enabled),body.interval_seconds,(datetime.now(timezone.utc)+timedelta(seconds=0 if body.catch_up!='none' else body.interval_seconds)).isoformat() if body.enabled else None,now(),ident))
            manager.configure_schedule(con,ident,body,current)
            return manager.definition(con,ident)

    @app.delete('/api/flows/{ident}')
    def delete(ident:str,body:Delete):
        with meta() as con:
            con.execute('BEGIN IMMEDIATE');current=manager.definition(con,ident)
            if current['revision']!=body.revision or current['name']!=body.confirmation_name:raise HTTPException(409,'Workflow name or revision changed.')
            if con.execute("SELECT 1 FROM flow_runs WHERE flow_id=? AND status IN ('queued','running')",(ident,)).fetchone():raise HTTPException(409,'Cancel or finish queued/running work first.')
            con.execute('DELETE FROM flow_step_runs WHERE run_id IN (SELECT id FROM flow_runs WHERE flow_id=?)',(ident,))
            con.execute('DELETE FROM flow_schedule_state WHERE flow_id=?',(ident,))
            con.execute('DELETE FROM flow_runs WHERE flow_id=?',(ident,));con.execute('DELETE FROM flow_request_keys WHERE flow_id=?',(ident,));con.execute('DELETE FROM flows WHERE id=?',(ident,))
        return {'deleted':True}

    @app.post('/api/flows/{ident}/pause')
    def pause(ident:str,body:Request):
        with meta() as con:
            con.execute('BEGIN IMMEDIATE');current=manager.definition(con,ident)
            if current['revision']!=body.revision:raise HTTPException(409,'Workflow changed. Reload before pausing.')
            if current['enabled']:
                current.update(enabled=False,revision=current['revision']+1)
                con.execute('UPDATE flows SET enabled=0,next_run=NULL,revision=revision+1,content=?,updated_at=? WHERE id=?',
                            (json.dumps(Flow(**current).model_dump()),now(),ident))
            return manager.definition(con,ident)

    @app.post('/api/flows/{ident}/run',status_code=202)
    def run(ident:str,body:Request,idempotency_key:str|None=Header(default=None,max_length=128)):
        return manager.enqueue(ident,body,idempotency_key)

    @app.post('/api/flows/{ident}/schedule/reset')
    def reset_schedule(ident:str,body:Delete):
        with meta() as con:
            con.execute('BEGIN IMMEDIATE');current=manager.definition(con,ident)
            if current['revision']!=body.revision or current['name']!=body.confirmation_name:raise HTTPException(409,'Workflow name or revision changed.')
            if current['catch_up']=='none':raise HTTPException(400,'Configure daily or hourly catch-up first.')
            if current['has_active_runs']:raise HTTPException(409,'Wait for or cancel queued/running work before restarting catch-up.')
            con.execute('UPDATE flow_schedule_state SET next_partition=?,generation=generation+1,last_checked=NULL,last_error=NULL WHERE flow_id=?',(current['catch_up_start'],ident))
            con.execute('UPDATE flows SET next_run=? WHERE id=?',(now() if current['enabled'] else None,ident))
            return manager.schedule_status(con,ident)

    @app.get('/api/flows/{ident}/schedule')
    def schedule_status(ident:str):
        with meta() as con:
            con.execute('BEGIN')
            return manager.schedule_status(con,ident)

    @app.post('/api/flows/{ident}/schedule',status_code=202)
    def schedule(ident:str,body:ScheduleRequest,idempotency_key:str|None=Header(default=None,max_length=128)):
        return manager.schedule(ident,Request(revision=body.revision),idempotency_key)

    @app.post('/api/flows/{ident}/backfill',status_code=202)
    def backfill(ident:str,body:Backfill,idempotency_key:str|None=Header(default=None,max_length=128)):
        return manager.enqueue(ident,body,idempotency_key,backfill=True)

    @app.get('/api/flows/{ident}/runs')
    def runs(ident:str):
        with meta() as con:
            con.execute('BEGIN')
            manager.definition(con,ident)
            return [manager.result(con,row['id']) for row in con.execute('SELECT id FROM flow_runs WHERE flow_id=? ORDER BY created_at DESC LIMIT 100',(ident,))]

    @app.get('/api/flows/{ident}/runs/{run_id}')
    def get_run(ident:str,run_id:str):
        with meta() as con:
            con.execute('BEGIN')
            result=manager.result(con,run_id)
            if result['flow_id']!=ident:raise HTTPException(404,'Workflow run not found')
            result['snapshot']=json.loads(con.execute('SELECT snapshot FROM flow_runs WHERE id=?',(run_id,)).fetchone()[0])
            return result

    @app.post('/api/flows/{ident}/runs/{run_id}/{action}')
    def control(ident:str,run_id:str,action:Literal['retry','cancel']):
        with meta() as con:
            con.execute('BEGIN IMMEDIATE');result=manager.result(con,run_id)
            if result['flow_id']!=ident:raise HTTPException(404,'Workflow run not found')
            if action=='cancel':
                if result['status'] in ('queued','running'):
                    con.execute('UPDATE flow_runs SET cancel_requested=1 WHERE id=?',(run_id,))
                    if result['status']=='queued':
                        con.execute("UPDATE flow_runs SET status='cancelled',finished_at=? WHERE id=?",(now(),run_id))
                        con.execute("UPDATE flow_step_runs SET status='cancelled',finished_at=? WHERE run_id=? AND status='pending'",(now(),run_id))
            else:
                if result['status'] not in ('failed','interrupted','cancelled'):raise HTTPException(409,'Only unsuccessful runs can be retried.')
                if con.execute("SELECT count(*) FROM flow_runs WHERE status IN ('queued','running')").fetchone()[0]>=64:raise HTTPException(429,'Workflow queue is full.')
                con.execute("UPDATE flow_runs SET status='queued',cancel_requested=0,attempt=attempt+1,finished_at=NULL,error=NULL WHERE id=?",(run_id,))
                con.execute("UPDATE flow_step_runs SET status='pending',error=NULL WHERE run_id=? AND status!='succeeded'",(run_id,))
        return {'run_id':run_id,'action':action}
