"""Persisted SQL/Python notebooks, metric definitions and bounded dashboard widgets."""
import json
from typing import Annotated, Literal, Union
import uuid

from fastapi import Body, HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator
from app.workflows import now
from app.semantics import Filter as BusinessFilter,ID as BusinessID,Ordering,Query as BusinessQuery


class Cell(BaseModel):
    kind: Literal['sql','python','note'] = 'sql'
    text: str = Field(max_length=20000)
    dataset_ids: list[str] = Field(default_factory=list, max_length=5)

    @field_validator('dataset_ids')
    @classmethod
    def valid_inputs(cls, values):
        import re
        if any(not re.fullmatch(r'[a-f0-9]{32}', value) for value in values) or len(set(values)) != len(values):
            raise ValueError('Select distinct workspace dataset IDs.')
        return values


class Widget(BaseModel):
    title: str = Field(min_length=1,max_length=200)
    kind: Literal['table','metric','semantic'] = 'table'
    sql: str = Field(default='',max_length=20000)
    metric_id: str | None = None
    model_id: str | None = None
    model_revision: int | None = Field(default=None,ge=1)
    measures: list[BusinessID] = Field(default_factory=list,max_length=12)
    group_by: list[BusinessID] = Field(default_factory=list,max_length=6)
    filters: list[BusinessFilter] = Field(default_factory=list,max_length=12)
    order_by: list[Ordering] = Field(default_factory=list,max_length=3)
    display: Literal['table','metric'] = 'table'

    @model_validator(mode='after')
    def meaningful(self):
        if self.kind=='table' and not self.sql.strip():
            raise ValueError('Table widgets require SELECT SQL.')
        if self.kind=='metric' and not self.metric_id:
            raise ValueError('Metric widgets require a saved metric definition.')
        if self.kind=='semantic':
            if not self.model_id or not self.model_revision or not self.measures:raise ValueError('Choose a business model, its revision and measures.')
            if self.display=='metric' and (len(self.measures)!=1 or self.group_by):raise ValueError('A scalar widget needs one measure and no grouping.')
        return self


class Document(BaseModel):
    name: str = Field(min_length=1,max_length=200)
    revision: int = Field(default=0,ge=0)

    @field_validator('name')
    @classmethod
    def valid_name(cls,value):
        value=value.strip()
        if not value or any(ord(c)<32 for c in value):
            raise ValueError('Use a non-empty name without control characters.')
        return value


class Notebook(Document):
    kind: Literal['notebook']
    cells: list[Cell] = Field(max_length=20)


class Dashboard(Document):
    kind: Literal['dashboard']
    widgets: list[Widget] = Field(max_length=8)


class Metric(Document):
    kind: Literal['metric']
    engine: Literal['sql','semantic'] = 'sql'
    sql: str = Field(default='',max_length=20000)
    value_column: str = Field(default='value',min_length=1,max_length=200)
    unit: str = Field(default='',max_length=30)
    description: str = Field(default='',max_length=2000)
    model_id: str | None = None
    model_revision: int | None = Field(default=None,ge=1)
    measure_id: BusinessID | None = None
    filters: list[BusinessFilter] = Field(default_factory=list,max_length=12)

    @model_validator(mode='after')
    def meaningful(self):
        if self.engine=='sql' and not self.sql.strip():raise ValueError('Enter metric SQL.')
        if self.engine=='semantic' and (not self.model_id or not self.model_revision or not self.measure_id):raise ValueError('Choose a business model, its revision and a measure.')
        return self


Definition=Annotated[Union[Notebook,Dashboard,Metric],Body(discriminator='kind')]


class Revision(BaseModel):
    revision: int = Field(ge=1)


class Deletion(Revision):
    confirmation_name: str


def register(app, meta, execute, python,semantic):
    python.recover()
    def decode(row):
        if not row:
            raise HTTPException(404,'Saved analysis not found')
        return {**json.loads(row['content']),'id':row['id'],'revision':row['revision'],'updated_at':row['updated_at']}

    def fetch(ident):
        with meta() as con:
            return decode(con.execute('SELECT * FROM analysis_documents WHERE id=?',(ident,)).fetchone())

    def check_bindings(body):
        if body.kind=='metric' and body.engine=='semantic':
            semantic.binding(body.model_id,BusinessQuery(revision=body.model_revision,measures=[body.measure_id],filters=body.filters))
        if body.kind=='dashboard':
            for widget in body.widgets:
                if widget.kind=='semantic':semantic.binding(widget.model_id,BusinessQuery(revision=widget.model_revision,measures=widget.measures,group_by=widget.group_by,filters=widget.filters,order_by=widget.order_by))

    @app.get('/api/notebook-runtime')
    def notebook_runtime():
        return python.status()

    @app.get('/api/documents/{ident}/runs')
    def notebook_runs(ident: str):
        document=fetch(ident)
        if document['kind']!='notebook':
            raise HTTPException(400,'Execution history is available for notebooks.')
        with meta() as con:
            return [{**dict(row),'indexes':json.loads(row['indexes']),'inputs':json.loads(row['inputs'])}
                    for row in con.execute('''SELECT id,revision,indexes,status,started_at,finished_at,elapsed_ms,inputs,error
                        FROM notebook_runs WHERE document_id=? ORDER BY started_at DESC LIMIT 100''',(ident,))]

    @app.get('/api/documents/{ident}/runs/{run_id}')
    def notebook_run(ident: str,run_id: str):
        fetch(ident)
        with meta() as con:
            row=con.execute('SELECT result,code FROM notebook_runs WHERE document_id=? AND id=?',(ident,run_id)).fetchone()
        if not row:
            raise HTTPException(404,'Notebook run not found')
        return json.loads(row['result']) if row['result'] else {'detail':'This run has no saved output. Check its history status.',
                                                             'source_cells':json.loads(row['code'])}

    @app.post('/api/documents/{ident}/python/run')
    def run_python_cells(ident: str,body: Revision):
        document=fetch(ident)
        if document['revision']!=body.revision:
            raise HTTPException(409,'This definition changed. Reload before running it.')
        if document['kind']!='notebook':
            raise HTTPException(400,'Python runs require a notebook.')
        indexes=[index for index,cell in enumerate(document['cells']) if cell['kind']=='python']
        if not indexes:
            raise HTTPException(400,'Add Python cells before running them.')
        return python.run(document,indexes)

    @app.get('/api/documents')
    def list_documents(kind: Literal['notebook','dashboard','metric']):
        with meta() as con:
            return [decode(row) for row in con.execute('SELECT * FROM analysis_documents WHERE kind=? ORDER BY updated_at DESC',(kind,))]

    @app.get('/api/documents/{ident}')
    def get_document(ident: str):
        return fetch(ident)

    @app.post('/api/documents',status_code=201)
    def create_document(body: Definition):
        ident=uuid.uuid4().hex
        with meta() as con:
            con.execute('BEGIN IMMEDIATE')
            check_bindings(body)
            con.execute('INSERT INTO analysis_documents VALUES(?,?,?,?,?,?)',
                        (ident,body.kind,body.name,json.dumps(body.model_dump()),1,now()))
        return fetch(ident)

    @app.put('/api/documents/{ident}')
    def update_document(ident: str,body: Definition):
        with meta() as con:
            con.execute('BEGIN IMMEDIATE')
            check_bindings(body)
            updated=con.execute('UPDATE analysis_documents SET name=?,content=?,revision=revision+1,updated_at=? WHERE id=? AND kind=? AND revision=?',
                (body.name,json.dumps(body.model_dump()),now(),ident,body.kind,body.revision)).rowcount
            if not updated:
                if not con.execute('SELECT 1 FROM analysis_documents WHERE id=?',(ident,)).fetchone():
                    raise HTTPException(404,'Saved analysis not found')
                raise HTTPException(409,'This definition changed. Reload it before saving.')
        return fetch(ident)

    @app.delete('/api/documents/{ident}')
    def delete_document(ident: str,body: Deletion):
        with meta() as con:
            con.execute('BEGIN IMMEDIATE')
            current=decode(con.execute('SELECT * FROM analysis_documents WHERE id=?',(ident,)).fetchone())
            if con.execute("SELECT 1 FROM notebook_runs WHERE document_id=? AND status='running'",(ident,)).fetchone():
                raise HTTPException(409,'Wait for Python execution and cleanup before deleting this notebook.')
            if current['name']!=body.confirmation_name or current['revision']!=body.revision:
                raise HTTPException(409,'Definition name or revision changed. Reload before deleting.')
            if current['kind']=='metric':
                dashboards=con.execute("SELECT content FROM analysis_documents WHERE kind='dashboard'").fetchall()
                if any(widget.get('metric_id')==ident for row in dashboards for widget in json.loads(row['content'])['widgets']):
                    raise HTTPException(409,'Remove this metric from dashboard widgets before deleting it.')
            con.execute('DELETE FROM analysis_documents WHERE id=?',(ident,))
            con.execute('DELETE FROM notebook_runs WHERE document_id=?',(ident,))
        return {'deleted':True}

    def metric_value(metric):
        if metric.get('engine')=='semantic':
            result=semantic.run(metric['model_id'],BusinessQuery(revision=metric['model_revision'],measures=[metric['measure_id']],filters=metric.get('filters',[])))
            return {'kind':'metric','value':result['rows'][0][0],'unit':result['fields'][0]['unit'],
                    'sql':result['sql'],'metric_revision':metric['revision'],'query_id':result['query_id'],
                    'model_revision':result['model_revision'],'input_versions':result['input_versions'],'semantic_run_id':result['run_id']}
        result=execute(metric['sql'])
        if len(result['rows'])!=1 or metric['value_column'] not in result['columns']:
            raise HTTPException(400,'Metric SQL must return one row and the configured value column.')
        return {'kind':'metric','value':result['rows'][0][result['columns'].index(metric['value_column'])],
                'unit':metric['unit'],'sql':metric['sql'],'metric_revision':metric['revision'],
                'query_id':result['query_id']}

    @app.post('/api/documents/{ident}/items/{index}/run')
    def run_item(ident: str,index: int,body: Revision):
        document=fetch(ident)
        if document['revision']!=body.revision:
            raise HTTPException(409,'This definition changed. Reload before running it.')
        if document['kind']=='notebook':
            items=document['cells']
        elif document['kind']=='dashboard':
            items=document['widgets']
        else:
            if index!=0:
                raise HTTPException(404,'Metric item not found')
            return {**metric_value(document),'refreshed_at':now(),'revision':document['revision']}
        if not 0<=index<len(items):
            raise HTTPException(404,'Analysis item not found')
        item=items[index]
        if item['kind']=='python':
            return python.run(document,[index])
        if item['kind']=='semantic':
            result=semantic.run(item['model_id'],BusinessQuery(revision=item['model_revision'],measures=item['measures'],group_by=item['group_by'],filters=item['filters'],order_by=item['order_by']))
            if item['display']=='metric':
                result={**result,'kind':'metric','value':result['rows'][0][0],'unit':result['fields'][0]['unit']}
            return {**result,'revision':document['revision']}
        if item['kind']=='note':
            return {'kind':'note','text':item['text'],'revision':document['revision'],'refreshed_at':now()}
        if item['kind']=='metric':
            metric=fetch(item['metric_id'])
            if metric['kind']!='metric':
                raise HTTPException(400,'Widget reference is not a metric definition.')
            result=metric_value(metric)
        else:
            statement=item['text'] if document['kind']=='notebook' else item['sql']
            if not statement.strip():
                raise HTTPException(400,'Enter SELECT SQL before running this cell.')
            result={'kind':'table','sql':statement,**execute(statement)}
        return {**result,'refreshed_at':now(),'revision':document['revision']}
