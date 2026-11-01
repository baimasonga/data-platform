"""Revisioned star models and safe, fanout-checked aggregate queries."""
import json
import re
import uuid
from typing import Literal, Annotated

from fastapi import HTTPException
from pydantic import BaseModel, Field, model_validator
from app.workflows import quote, now
from app.calculations import DERIVED,calculation_plan,expression_sql

ID=Annotated[str,Field(pattern=r'^[a-z][a-z0-9_]{0,39}$')]
DatasetID=Annotated[str,Field(pattern=r'^[a-f0-9]{32}$')]
NUMERIC=r'(?:U?(?:TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT)|FLOAT|DOUBLE|DECIMAL\(\d+,\d+\))'
SCALAR=NUMERIC+r'|BOOLEAN|VARCHAR|UUID|DATE|TIME|TIMESTAMP(?:_S|_MS|_NS)?|TIMESTAMP WITH TIME ZONE'


class Relationship(BaseModel):
    id: ID
    name: str = Field(min_length=1,max_length=100)
    dataset_id: DatasetID
    fact_key: str = Field(min_length=1,max_length=500)
    dimension_key: str = Field(min_length=1,max_length=500)


class Attribute(BaseModel):
    id: ID
    name: str = Field(min_length=1,max_length=100)
    table: str = Field(default='fact',max_length=40)
    column: str = Field(min_length=1,max_length=500)


class Measure(BaseModel):
    id: ID
    name: str = Field(min_length=1,max_length=100)
    aggregate: Literal['sum','count','count_distinct','avg','min','max','ratio','expression']
    column: str | None = Field(default=None,max_length=500)
    expression: str = Field(default='',max_length=1000)
    numerator: str | None = None
    denominator: str | None = None
    scale: Literal[1,100] = 1
    unit: str = Field(default='',max_length=30)
    description: str = Field(default='',max_length=2000)


class Model(BaseModel):
    name: str = Field(min_length=1,max_length=200)
    description: str = Field(default='',max_length=2000)
    revision: int = Field(default=0,ge=0)
    fact_dataset_id: DatasetID
    relationships: list[Relationship] = Field(default_factory=list,max_length=8)
    attributes: list[Attribute] = Field(default_factory=list,max_length=40)
    measures: list[Measure] = Field(min_length=1,max_length=30)

    @model_validator(mode='after')
    def valid(self):
        self.name=self.name.strip()
        if not self.name or any(ord(c)<32 for c in self.name):raise ValueError('Use a meaningful model name without control characters.')
        rel={r.id:r for r in self.relationships};fields=[item.id for item in [*self.attributes,*self.measures]]
        if len(rel)!=len(self.relationships) or 'fact' in rel or len(set(fields))!=len(fields):
            raise ValueError('Relationship IDs and field IDs must be unique; fact is reserved.')
        if any(a.table!='fact' and a.table not in rel for a in self.attributes):raise ValueError('Attribute refers to an unknown model table.')
        measures={m.id:m for m in self.measures}
        for measure in self.measures:
            if measure.aggregate=='ratio':
                if any(key not in measures or measures[key].aggregate in DERIVED for key in (measure.numerator,measure.denominator)):
                    raise ValueError('Ratios require two existing base measures; nested ratios are not supported.')
            elif measure.aggregate not in ('count','expression') and not measure.column:raise ValueError('Choose a fact column for this aggregate.')
        calculation_plan({key:value.model_dump() for key,value in measures.items()})
        return self


Value=Annotated[str,Field(max_length=1000)] | int | Annotated[float,Field(allow_inf_nan=False)] | bool


class Filter(BaseModel):
    attribute: ID
    op: Literal['eq','neq','in','not_in','gt','gte','lt','lte','is_null','not_null']
    values: list[Value] = Field(default_factory=list,max_length=50)

    @model_validator(mode='after')
    def arity(self):
        if self.op in ('is_null','not_null'):
            if self.values:raise ValueError('Null filters do not take values.')
        elif self.op in ('in','not_in'):
            if not self.values:raise ValueError('Choose at least one filter value.')
        elif len(self.values)!=1:raise ValueError('This filter requires exactly one value.')
        return self


class Ordering(BaseModel):
    field: ID
    descending: bool = False


class Query(BaseModel):
    revision: int = Field(ge=1)
    measures: list[ID] = Field(min_length=1,max_length=12)
    group_by: list[ID] = Field(default_factory=list,max_length=6)
    filters: list[Filter] = Field(default_factory=list,max_length=12)
    order_by: list[Ordering] = Field(default_factory=list,max_length=3)
    limit: int = Field(default=1000,ge=1,le=1000)


class Deletion(BaseModel):
    revision: int = Field(ge=1)
    confirmation_name: str


def literal(value):
    if isinstance(value,bool):return 'TRUE' if value else 'FALSE'
    return "'"+str(value).replace("'","''")+"'"


class Semantics:
    def __init__(self,workflows,execute):
        self.workflows,self.execute=workflows,execute
        with self.meta() as con:
            con.execute("UPDATE semantic_runs SET status='interrupted',finished_at=?,error='Server stopped before semantic query completion.' WHERE status='running'",(now(),))

    def meta(self):return self.workflows.catalog()

    def fetch(self,con,ident):
        row=con.execute('SELECT * FROM semantic_models WHERE id=?',(ident,)).fetchone()
        if not row:raise HTTPException(404,'Business model not found')
        return {**json.loads(row['content']),'id':ident,'revision':row['revision'],'updated_at':row['updated_at']},json.loads(row['contract'])

    def resolve(self,model):
        tables={'fact':model['fact_dataset_id'],**{r['id']:r['dataset_id'] for r in model['relationships']}}
        pins={};schemas={}
        for alias,ident in tables.items():
            try:dataset=self.workflows.dataset(ident)
            except (LookupError,ValueError) as exc:raise HTTPException(409,'A model dataset is unavailable or pending deletion.') from exc
            version=self.workflows.versions(ident)[0]
            schemas[alias]=[{key:column[key] for key in ('name','type')} for column in version['profile']]
            pins[alias]={'dataset_id':ident,'version_id':version['id'],'number':version['number'],
                         'table_name':version['table_name'],'name':dataset['name']}
        return pins,schemas

    def columns(self,schemas):return {table:{c['name']:c['type'] for c in columns} for table,columns in schemas.items()}

    def validate_schema(self,model,schemas):
        columns=self.columns(schemas)
        def get(table,column):
            dtype=columns.get(table,{}).get(column)
            if not dtype or not re.fullmatch(SCALAR,dtype):raise HTTPException(400,'A model field is missing or uses an unsupported scalar type.')
            return dtype
        for relation in model['relationships']:
            if get('fact',relation['fact_key'])!=get(relation['id'],relation['dimension_key']):
                raise HTTPException(400,'Relationship keys must have the same data type. Cast keys in an upstream dataset first.')
        for attribute in model['attributes']:get(attribute['table'],attribute['column'])
        measures={m['id']:m for m in model['measures']}
        for measure in model['measures']:
            if measure['aggregate'] in DERIVED:continue
            dtype=get('fact',measure['column']) if measure['column'] else None
            if measure['aggregate'] in ('sum','avg') and not re.fullmatch(NUMERIC,dtype or ''):
                raise HTTPException(400,'Sum and average require numeric fact columns.')
        _,dependencies,order=calculation_plan(measures)
        numeric={}
        for key in order:
            measure=measures[key]
            if measure['aggregate'] in DERIVED:
                if not all(numeric[dep] for dep in dependencies[key]):
                    raise HTTPException(400,'Calculated measures require numeric measure inputs.')
                numeric[key]=True
            else:
                numeric[key]=measure['aggregate'] in ('count','count_distinct') or bool(re.fullmatch(NUMERIC,get('fact',measure['column'])))

    def validate_query(self,model,body):
        measures={m['id']:m for m in model['measures']};attributes={a['id']:a for a in model['attributes']}
        if body.revision!=model['revision']:raise HTTPException(409,'Business model changed. Review and update its binding before refreshing.')
        if len(set(body.measures))!=len(body.measures) or len(set(body.group_by))!=len(body.group_by):raise HTTPException(400,'Choose each measure/group field only once.')
        if any(key not in measures for key in body.measures) or any(key not in attributes for key in [*body.group_by,*[f.attribute for f in body.filters]]):
            raise HTTPException(400,'A requested measure or grouping/filter field is not in this model.')
        if any(order.field not in [*body.group_by,*body.measures] for order in body.order_by):raise HTTPException(400,'Sort fields must be selected output fields.')

    def binding(self,ident,body):
        with self.meta() as con:model,_=self.fetch(con,ident)
        self.validate_query(model,body)
        return model

    def references(self,con,ident):
        refs=[]
        for row in con.execute('SELECT id,content FROM analysis_documents'):
            document=json.loads(row['content'])
            if document.get('engine')=='semantic' and document.get('model_id')==ident:refs.append(document)
            refs.extend(widget for widget in document.get('widgets',[]) if widget.get('kind')=='semantic' and widget.get('model_id')==ident)
        return refs

    def compile(self,model,body,pins,schemas):
        attrs={a['id']:a for a in model['attributes']};measures={m['id']:m for m in model['measures']}
        aliases={'fact':'f',**{r['id']:'d'+str(i) for i,r in enumerate(model['relationships'])}}
        columns=self.columns(schemas)
        def field(key):
            attribute=attrs[key];return quote(aliases[attribute['table']])+'.'+quote(attribute['column'])
        used={attrs[key]['table'] for key in [*body.group_by,*[f.attribute for f in body.filters]]}
        joins=''
        for relation in model['relationships']:
            if relation['id'] in used:
                joins+=' LEFT JOIN '+quote(pins[relation['id']]['table_name'])+' AS '+quote(aliases[relation['id']])+' ON '+quote('f')+'.'+quote(relation['fact_key'])+'='+quote(aliases[relation['id']])+'.'+quote(relation['dimension_key'])
        predicates=[]
        for condition in body.filters:
            expression=field(condition.attribute);attribute=attrs[condition.attribute];dtype=columns[attribute['table']][attribute['column']]
            values=['CAST('+literal(value)+' AS '+dtype+')' for value in condition.values]
            if condition.op in ('is_null','not_null'):predicate=expression+(' IS NULL' if condition.op=='is_null' else ' IS NOT NULL')
            elif condition.op in ('in','not_in'):predicate=expression+(' IN (' if condition.op=='in' else ' NOT IN (')+','.join(values)+')'
            else:predicate=expression+{'eq':'=','neq':'<>','gt':'>','gte':'>=','lt':'<','lte':'<='}[condition.op]+values[0]
            predicates.append(predicate)
        trees,dependencies,order=calculation_plan(measures)
        needed=set()
        def include(key):
            if key in needed:return
            needed.add(key)
            for dep in dependencies[key]:include(dep)
        for key in body.measures:include(key)
        aggregate=[]
        for key in sorted(needed):
            measure=measures[key];kind=measure['aggregate']
            if kind in DERIVED:continue
            value=quote('f')+'.'+quote(measure['column']) if measure['column'] else '*'
            aggregate.append(('COUNT(DISTINCT '+value+')' if kind=='count_distinct' else kind.upper()+'('+value+')')+' AS '+quote('_m_'+key))
        groups=[field(key)+' AS '+quote(key) for key in body.group_by]
        inner='SELECT '+','.join([*groups,*aggregate])+' FROM '+quote(pins['fact']['table_name'])+' AS '+quote('f')+joins
        if predicates:inner+=' WHERE '+' AND '.join('('+p+')' for p in predicates)
        if groups:inner+=' GROUP BY '+','.join(str(i+1) for i in range(len(groups)))
        # Each derived measure gets one SQL layer, so shared/nested dependencies
        # are emitted once instead of recursively expanding expression strings.
        for index,key in enumerate(order):
            if key not in needed or measures[key]['aggregate'] not in DERIVED:continue
            measure=measures[key]
            if measure['aggregate']=='expression':value=expression_sql(trees[key])
            else:value='(CAST('+quote('_m_'+measure['numerator'])+' AS DOUBLE)/NULLIF(CAST('+quote('_m_'+measure['denominator'])+' AS DOUBLE),0))*'+str(measure['scale'])
            inner='SELECT *, '+value+' AS '+quote('_m_'+key)+' FROM ('+inner+') AS calc_'+str(index)
        selected=[quote(key) for key in body.group_by]
        selected.extend(quote('_m_'+key)+' AS '+quote(key) for key in body.measures)
        statement='SELECT '+','.join(selected)+' FROM ('+inner+') AS semantic_values'
        orders=body.order_by or [Ordering(field=key) for key in body.group_by]
        if orders:statement+=' ORDER BY '+','.join(quote(o.field)+(' DESC' if o.descending else ' ASC')+' NULLS LAST' for o in orders)
        statement+=' LIMIT '+str(body.limit+1)
        if len(statement)>20000:raise HTTPException(400,'Generated query is too large; use fewer filter values or fields.')
        checks=[]
        for relation in model['relationships']:
            key=quote(relation['dimension_key'])
            checks.append('SELECT '+literal(relation['id'])+' AS relationship,COUNT(*) AS violations FROM (SELECT '+key+' FROM '+quote(pins[relation['id']]['table_name'])+' GROUP BY '+key+' HAVING COUNT(*)>1 OR '+key+' IS NULL) AS bad_keys')
        return statement,' UNION ALL '.join(checks)

    def run(self,ident,body):
        with self.workflows.lock:
            self.workflows.recover()
            with self.meta() as con:model,contract=self.fetch(con,ident)
            self.validate_query(model,body)
            run=uuid.uuid4().hex
            with self.meta() as con:con.execute('INSERT INTO semantic_runs(id,model_id,model_revision,request,snapshot,status,started_at,inputs) VALUES(?,?,?,?,?,?,?,?)',
                (run,ident,model['revision'],json.dumps(body.model_dump(mode='json')),json.dumps(model),'running',now(),'{}'))
            try:
                pins,schemas=self.resolve(model)
                with self.meta() as con:con.execute('UPDATE semantic_runs SET inputs=? WHERE id=?',(json.dumps(pins),run))
                if schemas!=contract:raise HTTPException(409,'Dataset schema changed. Review and save the business model before querying.')
                self.validate_schema(model,schemas)
                statement,validation=self.compile(model,body,pins,schemas)
                with self.meta() as con:con.execute('UPDATE semantic_runs SET sql=? WHERE id=?',(statement,run))
                if validation:
                    checked=self.execute(validation)
                    with self.meta() as con:con.execute('UPDATE semantic_runs SET validation_query_id=? WHERE id=?',(checked['query_id'],run))
                    if any(int(row[1])>0 for row in checked['rows']):
                        raise HTTPException(409,'Lookup relationship keys must be unique and non-null. Query stopped to prevent inflated totals.')
                result=self.execute(statement)
                attributes={a['id']:a for a in model['attributes']};measures={m['id']:m for m in model['measures']}
                fields=[{**attributes[key],'kind':'attribute'} for key in body.group_by]+[{**measures[key],'kind':'measure'} for key in body.measures]
                result={**result,'kind':'table','rows':result['rows'][:body.limit],
                    'truncated':result['truncated'] or len(result['rows'])>body.limit,'sql':statement,
                    'model_id':ident,'model_revision':model['revision'],'model_name':model['name'],'run_id':run,
                    'input_versions':pins,'fields':fields,'column_labels':[field['name'] for field in fields],'refreshed_at':now()}
                with self.meta() as con:con.execute("UPDATE semantic_runs SET status='succeeded',finished_at=?,query_id=?,result=? WHERE id=?",(now(),result['query_id'],json.dumps(result),run))
                return result
            except Exception as exc:
                detail=str(getattr(exc,'detail',exc))[:1500]
                with self.meta() as con:con.execute("UPDATE semantic_runs SET status='failed',finished_at=?,error=? WHERE id=?",(now(),detail,run))
                if isinstance(exc,HTTPException):raise
                raise HTTPException(400,'Business query failed. Inspect its run history.') from exc


def register(app,service):
    @app.get('/api/semantic-models')
    def models():
        with service.meta() as con:return [service.fetch(con,row['id'])[0] for row in con.execute('SELECT id FROM semantic_models ORDER BY updated_at DESC')]

    def save(ident,body,create=False):
        with service.workflows.lock,service.meta() as con:
            service.workflows.recover();con.execute('BEGIN IMMEDIATE')
            if not create:
                current,_=service.fetch(con,ident)
                if current['revision']!=body.revision:raise HTTPException(409,'Business model changed. Reload before saving.')
                for ref in service.references(con,ident):
                    requested=([ref['measure_id']] if ref.get('engine')=='semantic' else ref['measures'])
                    group=ref.get('group_by',[]);filters=ref.get('filters',[])
                    if any(key not in {m.id for m in body.measures} for key in requested) or any(key not in {a.id for a in body.attributes} for key in [*group,*[f['attribute'] for f in filters]]):
                        raise HTTPException(409,'Update saved metric/dashboard bindings before removing referenced model fields.')
            content=body.model_dump();_,contract=service.resolve(content);service.validate_schema(content,contract)
            revision=1 if create else body.revision+1;content['revision']=revision;timestamp=now()
            con.execute('INSERT OR REPLACE INTO semantic_models VALUES(?,?,?,?,?,?)',(ident,body.name,revision,json.dumps(content),json.dumps(contract),timestamp))
            con.execute('INSERT INTO semantic_model_versions VALUES(?,?,?,?,?)',(ident,revision,json.dumps(content),json.dumps(contract),timestamp))
            return service.fetch(con,ident)[0]

    @app.post('/api/semantic-models',status_code=201)
    def create(body:Model):return save(uuid.uuid4().hex,body,True)

    @app.put('/api/semantic-models/{ident}')
    def update(ident:str,body:Model):return save(ident,body)

    @app.get('/api/semantic-models/{ident}')
    def get(ident:str):
        with service.meta() as con:return service.fetch(con,ident)[0]

    @app.delete('/api/semantic-models/{ident}')
    def delete(ident:str,body:Deletion):
        with service.workflows.lock,service.meta() as con:
            con.execute('BEGIN IMMEDIATE');current,_=service.fetch(con,ident)
            if current['name']!=body.confirmation_name or current['revision']!=body.revision:raise HTTPException(409,'Model name or revision changed.')
            if service.references(con,ident):raise HTTPException(409,'Remove saved metric and dashboard bindings before deleting this business model.')
            con.execute('DELETE FROM semantic_runs WHERE model_id=?',(ident,));con.execute('DELETE FROM semantic_model_versions WHERE model_id=?',(ident,));con.execute('DELETE FROM semantic_models WHERE id=?',(ident,))
        return {'deleted':True}

    @app.get('/api/semantic-models/{ident}/versions')
    def versions(ident:str):
        with service.meta() as con:
            service.fetch(con,ident)
            return [{**json.loads(row['content']),'created_at':row['created_at']} for row in con.execute('SELECT * FROM semantic_model_versions WHERE model_id=? ORDER BY revision DESC',(ident,))]

    @app.post('/api/semantic-models/{ident}/query')
    def query(ident:str,body:Query):return service.run(ident,body)

    @app.get('/api/semantic-models/{ident}/runs')
    def runs(ident:str):
        with service.meta() as con:
            service.fetch(con,ident)
            return [dict(row) for row in con.execute('SELECT id,model_revision,status,started_at,finished_at,query_id,validation_query_id,error FROM semantic_runs WHERE model_id=? ORDER BY started_at DESC LIMIT 100',(ident,))]

    @app.get('/api/semantic-models/{ident}/runs/{run_id}')
    def run(ident:str,run_id:str):
        with service.meta() as con:row=con.execute('SELECT * FROM semantic_runs WHERE id=? AND model_id=?',(run_id,ident)).fetchone()
        if not row:raise HTTPException(404,'Business query run not found')
        return {**dict(row),**{key:json.loads(row[key]) if row[key] else None for key in ('snapshot','inputs','request','result')}}
