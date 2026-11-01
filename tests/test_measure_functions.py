import pytest
from fastapi.testclient import TestClient
from app.main import create_local_app
from app.calculations import parse_expression,calculation_plan


@pytest.mark.parametrize('source',[
    'abs()', 'abs(revenue, 2)', 'round()', 'round(revenue, -1)', 'round(revenue, 9)',
    'round(revenue, 2.0)', 'round(revenue, orders)', 'round(revenue, digits=2)',
    'round(revenue, 2, 3)', 'coalesce(revenue)', 'coalesce()', 'coalesce('+','.join(['revenue']*9)+')',
    'if_else(revenue, 1, 0)', 'if_else(revenue > 0, 1)', 'if_else(0 < revenue < 100, 1, 0)',
    'if_else(revenue is None, 1, 0)', 'if_else(revenue in (1, 2), 1, 0)',
    'if_else(revenue > 0 and orders, 1, 0)', 'if_else(True, 1, 0)',
    'abs.__call__(revenue)', 'getattr(revenue, cost)', 'coalesce(*revenue)',
    'if_else(revenue > 0, sum(revenue), 0)', 'round(revenue, **orders)',
    'if_else(revenue > 0, "paid", "free")',
    'coalesce('+ 'abs('*15+'revenue'+')'*15+', 0)',
    'if_else('+ ' and '.join(['revenue > 0']*21)+', 1, 0)',
])
def test_only_bounded_numeric_allowlisted_function_calls_are_accepted(source):
    with pytest.raises(ValueError):parse_expression(source)


def test_function_names_are_not_dependencies_but_conditions_and_branches_are():
    _,refs=parse_expression('round(if_else(revenue > 0 and not (orders == 0), abs(profit), coalesce(fallback, 0)), 2)')
    assert refs=={'revenue','orders','profit','fallback'}
    measures={'revenue':{'aggregate':'sum','column':'amount'},'profit':{'aggregate':'expression','expression':'coalesce(abs(profit), revenue)'}}
    with pytest.raises(ValueError,match='circular'):calculation_plan(measures)
    measures['profit']['expression']='if_else(missing > 0, revenue, 0)'
    with pytest.raises(ValueError,match='missing'):calculation_plan(measures)


@pytest.fixture
def client(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:yield client


def model(client):
    response=client.post('/api/datasets',files={'file':('sales.csv',b'region,revenue,cost\nNorth,100,60\nNorth,50,30\nSouth,0,5\nUnknown,,\n')})
    assert response.status_code==201,response.text
    body={'name':'Function measures','fact_dataset_id':response.json()['id'],
      'attributes':[{'id':'region','name':'Region','column':'region'}],
      'measures':[{'id':'revenue','name':'Revenue','aggregate':'sum','column':'revenue'},
                  {'id':'cost','name':'Cost','aggregate':'sum','column':'cost'},
                  {'id':'profit','name':'Profit','aggregate':'expression','expression':'revenue - cost'},
                  {'id':'margin','name':'Margin','aggregate':'expression','expression':'round(coalesce(profit / revenue * 100, 0), 2)','unit':'%'},
                  {'id':'absolute','name':'Absolute profit','aggregate':'expression','expression':'abs(profit)'},
                  {'id':'tier','name':'Selected profit','aggregate':'expression','expression':'if_else(revenue > 100 and not (profit <= 0), profit, 0)'},
                  {'id':'fallback','name':'Fallback','aggregate':'expression','expression':'coalesce(profit / 0, revenue, 7)'},
                  {'id':'branch','name':'Null condition','aggregate':'expression','expression':'if_else(revenue == 0 or revenue < 0, 1, 2)'}]}
    response=client.post('/api/semantic-models',json=body)
    assert response.status_code==201,response.text
    return response.json()


def query(client,model,**options):
    response=client.post('/api/semantic-models/'+model['id']+'/query',json={'revision':model['revision'],'measures':['margin','absolute','tier','fallback','branch'],**options})
    assert response.status_code==200,response.text
    return response.json()


def test_functions_follow_grouping_filters_and_sql_null_conditions(client):
    saved=model(client)
    assert client.get('/api/query-history').json()==[]
    result=query(client,saved,group_by=['region'])
    assert result['rows']==[['North',40.0,60,60,150.0,2],['South',0.0,5,0,0.0,1],['Unknown',0.0,None,0,7.0,2]]
    scalar=query(client,saved,filters=[{'attribute':'region','op':'eq','values':['South']}])
    assert scalar['rows']==[[0.0,5,0,0.0,1]]
    empty=query(client,saved,filters=[{'attribute':'region','op':'eq','values':['Missing']}])
    assert empty['rows']==[[0.0,None,0,7.0,2]]
    detail=client.get('/api/semantic-models/'+saved['id']+'/runs/'+result['run_id']).json()
    assert detail['result']['rows']==result['rows'] and detail['inputs']==result['input_versions']
    assert detail['snapshot']['measures'][3]['expression']=='round(coalesce(profit / revenue * 100, 0), 2)'


def test_catalog_and_invalid_saves_are_metadata_only(client):
    catalog=client.get('/api/semantic-functions')
    assert catalog.status_code==200
    assert {f['name'] for f in catalog.json()}=={'abs','round','coalesce','if_else'}
    saved=model(client)
    for expression in ('coalesce(profit, missing)', 'if_else(revenue > 0, margin, 0)', 'read_csv(revenue)'):
        edited={**saved,'measures':[dict(m) for m in saved['measures']]}
        edited['measures'][3]['expression']=expression
        response=client.put('/api/semantic-models/'+saved['id'],json=edited)
        assert response.status_code==422,response.text
        assert client.get('/api/semantic-models/'+saved['id']).json()['revision']==1
    assert client.get('/api/query-history').json()==[]


def test_condition_inputs_must_be_numeric(client):
    saved=model(client)
    saved['measures'].append({'id':'first_region','name':'First region','aggregate':'min','column':'region'})
    saved['measures'][3]['expression']='if_else(first_region > 0, revenue, 0)'
    response=client.put('/api/semantic-models/'+saved['id'],json=saved)
    assert response.status_code==400 and 'numeric' in response.text


def test_rounding_preserves_integer_precision_and_uses_half_away_from_zero(client):
    fact=client.post('/api/datasets',files={'file':('precise.csv',b'value\n9007199254740993\n')}).json()
    response=client.post('/api/semantic-models',json={'name':'Function precision','fact_dataset_id':fact['id'],'measures':[
      {'id':'total','name':'Total','aggregate':'sum','column':'value'},
      {'id':'rounded','name':'Rounded','aggregate':'expression','expression':'round(abs(-total), 2)'},
      {'id':'positive','name':'Positive half','aggregate':'expression','expression':'round(total - total + 2.5)'},
      {'id':'negative','name':'Negative half','aggregate':'expression','expression':'round(total - total - 2.5)'},
      {'id':'conditional','name':'Conditional','aggregate':'expression','expression':'if_else(total != 0, coalesce(total, 0), 0)'}]})
    assert response.status_code==201,response.text
    result=query(client,response.json(),measures=['rounded','positive','negative','conditional'])
    assert [str(value) for value in result['rows'][0]]==['9007199254740993','3','-3','9007199254740993']


def test_shared_metric_and_dashboard_use_function_measures(client):
    saved=model(client)
    response=client.post('/api/documents',json={'kind':'metric','name':'Margin','engine':'semantic','model_id':saved['id'],'model_revision':1,'measure_id':'margin'})
    assert response.status_code==201,response.text
    metric=response.json()
    result=client.post('/api/documents/'+metric['id']+'/items/0/run',json={'revision':1})
    assert result.status_code==200 and result.json()['value']==36.67 and result.json()['unit']=='%'
    response=client.post('/api/documents',json={'kind':'dashboard','name':'Grouped functions','widgets':[
      {'kind':'semantic','title':'Margin','model_id':saved['id'],'model_revision':1,'measures':['margin'],'group_by':['region']} ]})
    assert response.status_code==201,response.text
    dashboard=response.json()
    result=client.post('/api/documents/'+dashboard['id']+'/items/0/run',json={'revision':1})
    assert result.status_code==200 and result.json()['rows']==[['North',40.0],['South',0.0],['Unknown',0.0]]


def test_function_definitions_and_recorded_outputs_survive_restart(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:
        saved=model(client)
        first=query(client,saved,group_by=['region'])
    with TestClient(create_local_app(tmp_path)) as client:
        restored=client.get('/api/semantic-models/'+saved['id']).json()
        assert restored['revision']==1 and restored['measures'][3]['expression']==saved['measures'][3]['expression']
        recorded=client.get('/api/semantic-models/'+saved['id']+'/runs/'+first['run_id']).json()
        assert recorded['result']['rows']==first['rows']
        assert query(client,restored,group_by=['region'])['rows']==first['rows']
