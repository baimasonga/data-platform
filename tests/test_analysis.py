import pytest
from fastapi.testclient import TestClient
from app.main import create_app


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path)) as instance:
        yield instance


def test_notebook_cells_are_explicit_read_only_and_versioned(client):
    response=client.post('/api/documents',json={'kind':'notebook','name':'Exploration','cells':[
        {'kind':'note','text':'Dataset notes <script>not executable</script>'},
        {'kind':'sql','text':'SELECT 42 AS answer'}]})
    assert response.status_code==201,response.text
    notebook=response.json()
    assert client.get('/api/query-history').json()==[]
    note=client.post('/api/documents/'+notebook['id']+'/items/0/run',json={'revision':1})
    assert note.status_code==200 and note.json()['kind']=='note'
    assert client.get('/api/query-history').json()==[]
    result=client.post('/api/documents/'+notebook['id']+'/items/1/run',json={'revision':1})
    assert result.status_code==200 and result.json()['rows']==[[42]]
    notebook['cells'][1]['text']='SELECT 99 AS answer'
    updated=client.put('/api/documents/'+notebook['id'],json=notebook)
    assert updated.status_code==200 and updated.json()['revision']==2
    assert client.put('/api/documents/'+notebook['id'],json=notebook).status_code==409
    assert client.post('/api/documents/'+notebook['id']+'/items/1/run',json={'revision':1}).status_code==409
    current=updated.json()
    current['cells'][1]['text']='CREATE TABLE forbidden(x INT)'
    current=client.put('/api/documents/'+notebook['id'],json=current).json()
    assert client.post('/api/documents/'+notebook['id']+'/items/1/run',json={'revision':current['revision']}).status_code==400


def test_metrics_dashboards_and_referenced_definition_safety(client):
    metric=client.post('/api/documents',json={'kind':'metric','name':'Answer','sql':'SELECT 42 AS amount',
        'value_column':'amount','unit':'units','description':'A defined example'}).json()
    dashboard=client.post('/api/documents',json={'kind':'dashboard','name':'Overview','widgets':[
        {'kind':'metric','title':'Answer','metric_id':metric['id']},
        {'kind':'table','title':'Rows','sql':'SELECT * FROM range(1005)'}]}).json()
    endpoint='/api/documents/'+dashboard['id']+'/items/'
    result=client.post(endpoint+'0/run',json={'revision':1})
    assert result.status_code==200,result.text
    assert result.json()['value']==42 and result.json()['unit']=='units'
    table=client.post(endpoint+'1/run',json={'revision':1}).json()
    assert len(table['rows'])==1000 and table['truncated']
    assert client.request('DELETE','/api/documents/'+metric['id'],json={'revision':1,'confirmation_name':'Answer'}).status_code==409
    assert client.request('DELETE','/api/documents/'+dashboard['id'],json={'revision':1,'confirmation_name':'Overview'}).status_code==200
    assert client.request('DELETE','/api/documents/'+metric['id'],json={'revision':1,'confirmation_name':'Answer'}).status_code==200


def test_invalid_metrics_and_limits_are_visible(client):
    metric=client.post('/api/documents',json={'kind':'metric','name':'Not scalar','sql':'SELECT * FROM range(2)',
        'value_column':'range','unit':''}).json()
    assert client.post('/api/documents/'+metric['id']+'/items/0/run',json={'revision':1}).status_code==400
    assert client.post('/api/documents',json={'kind':'dashboard','name':'Too many','widgets':[
        {'kind':'table','title':'Many','sql':'SELECT 1'}]*9}).status_code==422
    notebook=client.post('/api/documents',json={'kind':'notebook','name':'Draft','cells':[{'kind':'sql','text':''}]}).json()
    assert client.post('/api/documents/'+notebook['id']+'/items/0/run',json={'revision':1}).status_code==400
    assert client.post('/api/documents/'+notebook['id']+'/items/3/run',json={'revision':1}).status_code==404


def test_analysis_definitions_survive_restart(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        notebook=client.post('/api/documents',json={'kind':'notebook','name':'Persistent','cells':[]}).json()
    with TestClient(create_app(tmp_path)) as client:
        assert client.get('/api/documents?kind=notebook').json()[0]['id']==notebook['id']
        assert client.get('/api/documents/'+notebook['id']).json()['revision']==1
