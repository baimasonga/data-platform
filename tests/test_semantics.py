import json
import pytest
from fastapi.testclient import TestClient
from app.main import create_local_app


@pytest.fixture
def client(tmp_path):
    with TestClient(create_local_app(tmp_path)) as instance:yield instance


def upload(client,name,data):
    result=client.post('/api/datasets',files={'file':(name,data,'text/csv')})
    assert result.status_code==201,result.text
    return result.json()


def setup(client):
    fact=upload(client,'sales.csv',b'id,customer_id,amount,units\n1,10,10.50,1\n2,10,20.25,2\n3,20,5.25,1\n4,999,4.00,0\n5,,,0\n')
    dimension=upload(client,'customers.csv',b'id,region\n10,North\n20,South\n')
    body={'name':'Sales model','fact_dataset_id':fact['id'],
        'relationships':[{'id':'customer','name':'Customer','dataset_id':dimension['id'],'fact_key':'customer_id','dimension_key':'id'}],
        'attributes':[{'id':'region','name':'Region','table':'customer','column':'region'}, {'id':'order','name':'Order','column':'id'}],
        'measures':[{'id':'revenue','name':'Revenue','aggregate':'sum','column':'amount','unit':'USD'},
                    {'id':'orders','name':'Orders','aggregate':'count'},
                    {'id':'units','name':'Units','aggregate':'sum','column':'units'},
                    {'id':'aov','name':'Average order value','aggregate':'ratio','numerator':'revenue','denominator':'orders','unit':'USD'}]}
    response=client.post('/api/semantic-models',json=body)
    assert response.status_code==201,response.text
    return response.json(),fact,dimension


def query(client,model,**options):
    return client.post('/api/semantic-models/'+model['id']+'/query',json={'revision':model['revision'],'measures':['revenue','orders','aov'],**options})


def test_model_save_does_not_execute_and_star_totals_are_not_inflated(client):
    model,fact,dimension=setup(client)
    assert client.get('/api/query-history').json()==[]
    result=query(client,model)
    assert result.status_code==200,result.text
    assert result.json()['rows']==[[40.0,5,8.0]]
    assert result.json()['model_revision']==1
    assert result.json()['input_versions']['fact']['version_id']==fact['version_id']
    grouped=query(client,model,group_by=['region']).json()
    assert grouped['columns']==['region','revenue','orders','aov']
    assert grouped['column_labels']==['Region','Revenue','Orders','Average order value']
    assert grouped['rows']==[['North',30.75,2,15.375],['South',5.25,1,5.25],[None,4.0,2,2.0]]
    detail=client.get('/api/semantic-models/'+model['id']+'/runs/'+grouped['run_id']).json()
    assert detail['snapshot']['revision']==1 and detail['validation_query_id']
    assert detail['result']['rows']==grouped['rows']


def test_duplicate_or_null_lookup_keys_stop_queries_before_aggregation(client):
    model,_,dimension=setup(client)
    replacement=client.post('/api/datasets/'+dimension['id']+'/versions',files={'file':('customers.csv',b'id,region\n10,North\n10,Duplicate\n20,South\n','text/csv')})
    assert replacement.status_code==201
    result=query(client,model)
    assert result.status_code==409 and 'inflated totals' in result.text
    history=client.get('/api/semantic-models/'+model['id']+'/runs').json()
    assert history[0]['status']=='failed' and history[0]['query_id'] is None
    assert history[0]['validation_query_id']
    assert client.post('/api/datasets/'+dimension['id']+'/versions',files={'file':('customers.csv',b'id,region\n10,North\n,Unknown\n20,South\n','text/csv')}).status_code==201
    assert query(client,model).status_code==409


def test_filters_are_typed_escaped_and_null_groups_are_explicit(client):
    model,_,_=setup(client)
    selected=query(client,model,filters=[{'attribute':'region','op':'eq','values':['North']}]).json()
    assert selected['rows']==[[30.75,2,15.375]]
    unknown=query(client,model,filters=[{'attribute':'region','op':'is_null'}]).json()
    assert unknown['rows']==[[4.0,2,2.0]]
    injection=query(client,model,filters=[{'attribute':'region','op':'eq','values':["North' OR 1=1 --"]}])
    assert injection.status_code==200,injection.text
    assert injection.json()['rows']==[[None,0,None]]
    ordered=query(client,model,group_by=['region'],order_by=[{'field':'revenue','descending':True}],limit=1).json()
    assert ordered['rows'][0][0]=='North' and ordered['truncated']
    assert query(client,model,filters=[{'attribute':'region','op':'eq','values':[None]}]).status_code==422
    assert query(client,model,measures=['revenue','revenue']).status_code==400
    assert query(client,model,order_by=[{'field':'region'}]).status_code==400


def test_schema_drift_requires_review_and_revision_bindings_fail_explicitly(client):
    model,fact,_=setup(client)
    assert client.post('/api/datasets/'+fact['id']+'/versions',files={'file':('sales.csv',b'id,customer_id,amount,units,extra\n1,10,10.50,1,1\n','text/csv')}).status_code==201
    drift=query(client,model)
    assert drift.status_code==409 and 'schema changed' in drift.text
    updated=client.put('/api/semantic-models/'+model['id'],json=model)
    assert updated.status_code==200,updated.text
    assert updated.json()['revision']==2
    assert query(client,model).status_code==409
    assert query(client,updated.json()).status_code==200
    assert client.put('/api/semantic-models/'+model['id'],json=model).status_code==409
    assert len(client.get('/api/semantic-models/'+model['id']+'/versions').json())==2


def test_shared_measures_work_in_metrics_and_grouped_dashboards(client):
    model,_,_=setup(client)
    metric=client.post('/api/documents',json={'kind':'metric','name':'Shared revenue','engine':'semantic','model_id':model['id'],
        'model_revision':1,'measure_id':'revenue','unit':'ignored-by-semantic-binding'}).json()
    value=client.post('/api/documents/'+metric['id']+'/items/0/run',json={'revision':1})
    assert value.status_code==200,value.text
    assert value.json()['value']==40.0 and value.json()['unit']=='USD'
    dashboard=client.post('/api/documents',json={'kind':'dashboard','name':'Shared dashboard','widgets':[
        {'kind':'semantic','title':'Revenue by region','model_id':model['id'],'model_revision':1,'measures':['revenue'],'group_by':['region']},
        {'kind':'metric','title':'Revenue','metric_id':metric['id']}]}).json()
    result=client.post('/api/documents/'+dashboard['id']+'/items/0/run',json={'revision':1})
    assert result.status_code==200 and len(result.json()['rows'])==3
    assert client.request('DELETE','/api/semantic-models/'+model['id'],json={'revision':1,'confirmation_name':model['name']}).status_code==409
    missing={**model,'measures':[measure for measure in model['measures'] if measure['id'] not in ('revenue','aov')]}
    assert client.put('/api/semantic-models/'+model['id'],json=missing).status_code==409
    updated=client.put('/api/semantic-models/'+model['id'],json=model).json()
    assert updated['revision']==2
    assert client.post('/api/documents/'+metric['id']+'/items/0/run',json={'revision':1}).status_code==409
    metric['model_revision']=2
    assert client.put('/api/documents/'+metric['id'],json=metric).status_code==200
    assert client.post('/api/documents/'+metric['id']+'/items/0/run',json={'revision':2}).json()['value']==40.0


def test_bound_datasets_are_protected_and_model_history_survives_restart(tmp_path):
    with TestClient(create_local_app(tmp_path)) as client:
        model,fact,dimension=setup(client)
        result=query(client,model).json()
        assert client.request('DELETE','/api/datasets/'+fact['id'],json={'confirmation_name':'sales.csv'}).status_code==409
    with TestClient(create_local_app(tmp_path)) as client:
        assert client.get('/api/semantic-models/'+model['id']+'/runs/'+result['run_id']).json()['status']=='succeeded'
        assert query(client,model).json()['rows']==[[40.0,5,8.0]]
        assert client.request('DELETE','/api/semantic-models/'+model['id'],json={'revision':1,'confirmation_name':model['name']}).status_code==200
        assert client.request('DELETE','/api/datasets/'+dimension['id'],json={'confirmation_name':'customers.csv'}).status_code==200


def test_large_integer_totals_and_zero_denominator_are_not_silently_rounded(client):
    fact=upload(client,'precise.csv',b'id,amount,units\n1,9007199254740993,0\n')
    model=client.post('/api/semantic-models',json={'name':'Precise','fact_dataset_id':fact['id'],'measures':[
        {'id':'total','name':'Total','aggregate':'sum','column':'amount'},
        {'id':'units','name':'Units','aggregate':'sum','column':'units'},
        {'id':'ratio','name':'Ratio','aggregate':'ratio','numerator':'total','denominator':'units'}]}).json()
    result=query(client,model,measures=['total','ratio'])
    assert result.status_code==200,result.text
    assert result.json()['rows']==[['9007199254740993',None]]


def test_invalid_relationships_measures_and_scopes_are_rejected(client):
    model,_,_=setup(client)
    invalid={**model,'relationships':[{**model['relationships'][0],'fact_key':'amount'}]}
    assert client.post('/api/semantic-models',json=invalid).status_code==400
    invalid={**model,'measures':[{'id':'bad','name':'Bad','aggregate':'sum','column':'customer.region'}]}
    assert client.post('/api/semantic-models',json=invalid).status_code==400
    invalid={**model,'measures':[{'id':'bad','name':'Bad','aggregate':'ratio','numerator':'bad','denominator':'bad'}]}
    assert client.post('/api/semantic-models',json=invalid).status_code==422
    assert query(client,model,measures=['not_in_model']).status_code==400


def test_same_schema_refresh_uses_new_version_and_other_aggregates(client):
    fact=upload(client,'events.csv',b'category,amount\na,10\na,20\nb,30\n')
    model=client.post('/api/semantic-models',json={'name':'Aggregates','fact_dataset_id':fact['id'],
        'attributes':[{'id':'category','name':'Category','column':'category'}],
        'measures':[{'id':'average','name':'Average','aggregate':'avg','column':'amount'},
                    {'id':'minimum','name':'Minimum','aggregate':'min','column':'amount'},
                    {'id':'maximum','name':'Maximum','aggregate':'max','column':'amount'},
                    {'id':'distinct','name':'Distinct categories','aggregate':'count_distinct','column':'category'}]}).json()
    options={'measures':['average','minimum','maximum','distinct'],'filters':[{'attribute':'category','op':'in','values':['a','b']}]}
    first=query(client,model,**options).json()
    assert first['rows']==[[20.0,10,30,2]]
    replacement=client.post('/api/datasets/'+fact['id']+'/versions',files={'file':('new.csv',b'category,amount\na,40\nb,60\n')})
    assert replacement.status_code==201,replacement.text
    refreshed=query(client,model,**options).json()
    assert refreshed['rows']==[[50.0,40,60,2]]
    assert refreshed['model_revision']==1
    assert refreshed['input_versions']['fact']['version_id']!=first['input_versions']['fact']['version_id']
    stored=client.get('/api/semantic-models/'+model['id']+'/runs/'+first['run_id']).json()
    assert stored['result']['rows']==first['rows']
    assert stored['inputs']==first['input_versions']


def test_invalid_saved_binding_ids_return_validation_error(client):
    model,_,_=setup(client)
    response=client.post('/api/documents',json={'kind':'metric','name':'Invalid','engine':'semantic',
        'model_id':model['id'],'model_revision':1,'measure_id':'unsafe field'})
    assert response.status_code==422,response.text
    response=client.post('/api/documents',json={'kind':'dashboard','name':'Invalid','widgets':[
        {'kind':'semantic','title':'Bad','model_id':model['id'],'model_revision':1,'measures':['unsafe field']}]})
    assert response.status_code==422,response.text
