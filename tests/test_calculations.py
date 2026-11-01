import pytest
from pydantic import ValidationError
from fastapi.testclient import TestClient
from app.main import create_local_app
from app.calculations import parse_expression,calculation_plan
from app.semantics import Model


@pytest.mark.parametrize('source',[
    'sum(revenue)','revenue.__class__','revenue[0]','revenue ** 2','revenue // 2',
    'revenue % 2','revenue > 0','revenue if revenue else 0','revenue + True',
    'revenue + "1"','__import__("os")','revenue; DROP TABLE sales','revenue + 1j',
    '(lambda: revenue)()','[revenue for revenue in x]','revenue + 1e309',
    'revenue + '+ '9'*900,'revenue + 1000000000000001','42','','revenue +',
    'revenue + '+ ' + '.join(['revenue']*40),'-'*20+'revenue','revenue'*200,
])
def test_expression_grammar_rejects_code_and_unbounded_inputs(source):
    with pytest.raises(ValueError):parse_expression(source)


def test_expression_dependency_graph_validates_unknown_ids_and_cycles():
    base={'id':'revenue','name':'Revenue','aggregate':'sum','column':'amount'}
    derived=lambda id,expr:{'id':id,'name':id,'aggregate':'expression','expression':expr}
    def model(measures):return Model(name='Sales',fact_dataset_id='a'*32,measures=measures)
    assert model([base,derived('profit','revenue * 0.8'),derived('margin','profit / revenue * 100')])
    for measures in ([base,derived('profit','missing - revenue')],
                     [base,derived('profit','profit + revenue')],
                     [base,derived('profit','margin + revenue'),derived('margin','profit / revenue')]):
        with pytest.raises(ValidationError):model(measures)
    # Compatibility: the simpler ratio form still requires base inputs.
    with pytest.raises(ValidationError):model([base,derived('profit','revenue * 0.8'),
        {'id':'ratio','name':'Ratio','aggregate':'ratio','numerator':'profit','denominator':'revenue'}])


@pytest.fixture
def client(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:yield client


def create_model(client):
    response=client.post('/api/datasets',files={'file':('sales.csv',b'region,revenue,cost\nNorth,100,60\nNorth,50,30\nSouth,0,5\n')})
    assert response.status_code==201,response.text
    fact=response.json()
    # Out-of-order definitions exercise dependency sorting, including ratio input.
    body={'name':'Profitability','fact_dataset_id':fact['id'],
      'attributes':[{'id':'region','name':'Region','column':'region'}],
      'measures':[{'id':'margin','name':'Margin','aggregate':'expression','expression':'profit / revenue * 100','unit':'%'},
                  {'id':'revenue','name':'Revenue','aggregate':'sum','column':'revenue'},
                  {'id':'profit','name':'Profit','aggregate':'expression','expression':'revenue - cost','unit':'USD'},
                  {'id':'cost','name':'Cost','aggregate':'sum','column':'cost'},
                  {'id':'orders','name':'Orders','aggregate':'count'},
                  {'id':'aov','name':'Average order','aggregate':'ratio','numerator':'revenue','denominator':'orders'},
                  {'id':'adjusted','name':'Adjusted AOV','aggregate':'expression','expression':'-(aov - 2) + +1.5'}]}
    response=client.post('/api/semantic-models',json=body)
    assert response.status_code==201,response.text
    return response.json()


def query(client,model,**options):
    response=client.post('/api/semantic-models/'+model['id']+'/query',json={'revision':model['revision'],'measures':['profit','margin'],**options})
    assert response.status_code==200,response.text
    return response.json()


def test_nested_calculations_use_grouped_aggregates_filters_and_safe_division(client):
    model=create_model(client)
    assert client.get('/api/query-history').json()==[]
    total=query(client,model)
    assert total['rows'][0][0]==55
    assert total['rows'][0][1]==pytest.approx(55/150*100)
    grouped=query(client,model,group_by=['region'],order_by=[{'field':'profit','descending':True}])
    assert grouped['rows']==[['North',60,40.0],['South',-5,None]]
    filtered=query(client,model,filters=[{'attribute':'region','op':'eq','values':['North']}],measures=['adjusted'])
    assert filtered['rows']==[[-71.5]]
    empty=query(client,model,filters=[{'attribute':'region','op':'eq','values':['Missing']}])
    assert empty['rows']==[[None,None]]
    detail=client.get('/api/semantic-models/'+model['id']+'/runs/'+grouped['run_id']).json()
    assert detail['snapshot']['measures'][0]['expression']=='profit / revenue * 100'
    assert detail['result']['rows']==grouped['rows'] and detail['inputs']==grouped['input_versions']


def test_expression_references_must_resolve_to_numeric_measures(client):
    model=create_model(client)
    model['measures'].extend([{'id':'first_region','name':'First region','aggregate':'min','column':'region'},
        {'id':'bad','name':'Bad','aggregate':'expression','expression':'first_region + 1'}])
    response=client.put('/api/semantic-models/'+model['id'],json=model)
    assert response.status_code==400 and 'numeric' in response.text
    # Failed saves do not create a new revision or execute a query.
    assert client.get('/api/semantic-models/'+model['id']).json()['revision']==1
    assert client.get('/api/query-history').json()==[]


def test_calculated_metrics_and_scalar_widgets_require_explicit_rebinding(client):
    model=create_model(client)
    metric={'kind':'metric','name':'Profit','engine':'semantic','model_id':model['id'],'model_revision':1,'measure_id':'profit'}
    response=client.post('/api/documents',json=metric);assert response.status_code==201,response.text
    metric=response.json()
    dashboard=client.post('/api/documents',json={'kind':'dashboard','name':'Margin','widgets':[
        {'kind':'semantic','title':'Margin','model_id':model['id'],'model_revision':1,'measures':['margin'],'display':'metric'}]}).json()
    value=client.post('/api/documents/'+metric['id']+'/items/0/run',json={'revision':1}).json()
    assert value['value']==55 and value['unit']=='USD'
    scalar=client.post('/api/documents/'+dashboard['id']+'/items/0/run',json={'revision':1}).json()
    assert scalar['kind']=='metric' and scalar['value']==pytest.approx(55/150*100)
    model['measures'][2]['expression']='revenue - cost - 10'
    saved=client.put('/api/semantic-models/'+model['id'],json=model)
    assert saved.status_code==200,saved.text
    assert client.post('/api/documents/'+metric['id']+'/items/0/run',json={'revision':1}).status_code==409
    metric['model_revision']=2
    response=client.put('/api/documents/'+metric['id'],json=metric);assert response.status_code==200,response.text
    assert client.post('/api/documents/'+metric['id']+'/items/0/run',json={'revision':2}).json()['value']==45


def test_shared_dependencies_do_not_expand_exponentially(client):
    # All 30 measures stay within the model bound; each layer references aliases.
    from app.semantics import Semantics,Query
    measures=[{'id':'m0','name':'Base','aggregate':'sum','column':'amount'}]
    measures.extend({'id':f'm{i}','name':f'Level {i}','aggregate':'expression','expression':f'm{i-1} + m{i-1}'} for i in range(1,30))
    model=Model(name='Shared graph',fact_dataset_id='a'*32,measures=measures).model_dump()
    service=object.__new__(Semantics)
    sql,_=service.compile(model,Query(revision=1,measures=['m29']),{'fact':{'table_name':'ver_test'}},{'fact':[{'name':'amount','type':'BIGINT'}]})
    assert len(sql)<5000 and sql.count('SUM(')==1 and sql.count(' AS calc_')==29

    fact=client.post('/api/datasets',files={'file':('one.csv',b'amount\n1\n')}).json()
    model['fact_dataset_id']=fact['id']
    response=client.post('/api/semantic-models',json=model)
    assert response.status_code==201,response.text
    assert query(client,response.json(),measures=['m29'])['rows']==[[2**29]]


def test_integer_calculations_preserve_values_above_browser_precision(client):
    fact=client.post('/api/datasets',files={'file':('large.csv',b'amount\n9007199254740993\n')}).json()
    response=client.post('/api/semantic-models',json={'name':'Exact subtraction','fact_dataset_id':fact['id'],
        'measures':[{'id':'total','name':'Total','aggregate':'sum','column':'amount'},
                    {'id':'profit','name':'Profit','aggregate':'expression','expression':'total - 1'}]})
    assert response.status_code==201,response.text
    assert query(client,response.json(),measures=['profit'])['rows']==[['9007199254740992']]
