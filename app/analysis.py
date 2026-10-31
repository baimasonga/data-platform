"""Persisted SQL notebooks, metric definitions and bounded dashboard widgets."""
import json
from typing import Annotated, Literal, Union
import uuid

from fastapi import Body, HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator
from app.workflows import now


class Cell(BaseModel):
    kind: Literal['sql','note'] = 'sql'
    text: str = Field(max_length=20000)


class Widget(BaseModel):
    title: str = Field(min_length=1,max_length=200)
    kind: Literal['table','metric'] = 'table'
    sql: str = Field(default='',max_length=20000)
    metric_id: str | None = None

    @model_validator(mode='after')
    def meaningful(self):
        if self.kind=='table' and not self.sql.strip():
            raise ValueError('Table widgets require SELECT SQL.')
        if self.kind=='metric' and not self.metric_id:
            raise ValueError('Metric widgets require a saved metric definition.')
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
    sql: str = Field(min_length=1,max_length=20000)
    value_column: str = Field(min_length=1,max_length=200)
    unit: str = Field(default='',max_length=30)
    description: str = Field(default='',max_length=2000)


Definition=Annotated[Union[Notebook,Dashboard,Metric],Body(discriminator='kind')]


class Revision(BaseModel):
    revision: int = Field(ge=1)


class Deletion(Revision):
    confirmation_name: str


def register(app, meta, execute):
    def decode(row):
        if not row:
            raise HTTPException(404,'Saved analysis not found')
        return {**json.loads(row['content']),'id':row['id'],'revision':row['revision'],'updated_at':row['updated_at']}

    def fetch(ident):
        with meta() as con:
            return decode(con.execute('SELECT * FROM analysis_documents WHERE id=?',(ident,)).fetchone())

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
            con.execute('INSERT INTO analysis_documents VALUES(?,?,?,?,?,?)',
                        (ident,body.kind,body.name,json.dumps(body.model_dump()),1,now()))
        return fetch(ident)

    @app.put('/api/documents/{ident}')
    def update_document(ident: str,body: Definition):
        with meta() as con:
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
            current=decode(con.execute('SELECT * FROM analysis_documents WHERE id=?',(ident,)).fetchone())
            if current['name']!=body.confirmation_name or current['revision']!=body.revision:
                raise HTTPException(409,'Definition name or revision changed. Reload before deleting.')
            if current['kind']=='metric':
                dashboards=con.execute("SELECT content FROM analysis_documents WHERE kind='dashboard'").fetchall()
                if any(widget.get('metric_id')==ident for row in dashboards for widget in json.loads(row['content'])['widgets']):
                    raise HTTPException(409,'Remove this metric from dashboard widgets before deleting it.')
            con.execute('DELETE FROM analysis_documents WHERE id=?',(ident,))
        return {'deleted':True}

    def metric_value(metric):
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
