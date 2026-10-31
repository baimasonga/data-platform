"""Offline contracts and optional real-container isolation checks (no host exec of user code)."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess

from fastapi.testclient import TestClient
import pytest

from app.main import create_local_app, clean
from app.notebooks import DOCKER, PythonRuntime
from app.workflows import Workflows, now
import threading


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.delenv('DATA_PLATFORM_PYTHON_IMAGE', raising=False)
    with TestClient(create_local_app(tmp_path)) as instance:
        yield instance


def notebook(client, cells=None):
    response=client.post('/api/documents',json={'kind':'notebook','name':'Python exploration','cells':cells or [
        {'kind':'python','text':'print(42)','dataset_ids':[]}]})
    assert response.status_code==201,response.text
    return response.json()


def test_python_drafts_persist_without_runtime_or_automatic_execution(client):
    document=notebook(client)
    endpoint='/api/documents/'+document['id']
    assert client.get('/api/notebook-runtime').json()['available'] is False
    assert client.get(endpoint).json()['cells'][0]['kind']=='python'
    assert client.get(endpoint+'/runs').json()==[]
    result=client.post(endpoint+'/items/0/run',json={'revision':1})
    assert result.status_code==503 and 'not enabled' in result.json()['detail']
    assert client.get(endpoint+'/runs').json()==[]
    assert client.post(endpoint+'/python/run',json={'revision':2}).status_code==409


def test_python_input_identifiers_and_cell_limits(client):
    for ids in (['../../other'], ['a'*32]*2, ['a'*32]*6):
        response=client.post('/api/documents',json={'kind':'notebook','name':'Invalid','cells':[
            {'kind':'python','text':'print(1)','dataset_ids':ids}]})
        assert response.status_code==422
    metric=client.post('/api/documents',json={'kind':'metric','name':'Metric','sql':'SELECT 1 AS value','value_column':'value'}).json()
    assert client.post('/api/documents/'+metric['id']+'/python/run',json={'revision':1}).status_code==400


def runner():
    spec=importlib.util.spec_from_file_location('notebook_runner',Path(__file__).resolve().parents[1]/'deploy/python/runner.py')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_python_scope_output_precision_and_failure_stop():
    result=runner().execute({'datasets':[{'id':'sales','columns':['value'],'rows':[[2],[3]]}],
        'cells':[{'index':0,'text':'answer = datasets["sales"]["value"].sum()\nprint("computed")\nanswer'},
                 {'index':2,'text':'pd.DataFrame({"total": [answer], "precise": [9007199254740993]})'},
                 {'index':3,'text':'raise ValueError("expected failure")'},
                 {'index':4,'text':'print("must not run")'}]})
    assert [cell['index'] for cell in result['cells']]==[0,2,3]
    assert result['cells'][0]['stdout']=='computed\n'
    assert result['cells'][1]['outputs'][0]['rows']==[[5,'9007199254740993']]
    assert 'expected failure' in result['cells'][2]['error']


def test_python_output_limits_are_explicit():
    result=runner().execute({'datasets':[],'cells':[
        {'index':0,'text':'pd.DataFrame({"value": range(1005)})'},
        {'index':1,'text':'print("x" * 21000)'}]})
    display=result['cells'][0]['outputs'][0]
    assert len(display['rows'])==1000 and display['truncated']
    assert '20,000' in result['cells'][1]['error']


def test_interrupted_runs_recover_and_active_notebooks_cannot_be_deleted(tmp_path,client,monkeypatch):
    document=notebook(client)
    from app.catalog import connection
    with connection(tmp_path/'catalog.sqlite3') as con:
        con.execute('INSERT INTO notebook_runs(id,document_id,revision,indexes,status,started_at,inputs) VALUES(?,?,?,?,?,?,?)',
                    ('a'*32,document['id'],1,'[0]','running',now(),'[]'))
    endpoint='/api/documents/'+document['id']
    assert client.request('DELETE',endpoint,json={'revision':1,'confirmation_name':document['name']}).status_code==409
    calls=[]
    monkeypatch.setattr(PythonRuntime,'remove_container',lambda self,name:calls.append(name) or True)
    runtime=PythonRuntime(Workflows(tmp_path,threading.RLock()),clean)
    runtime.recover()
    assert calls==['platform-python-'+'a'*32]
    assert client.get(endpoint+'/runs').json()[0]['status']=='interrupted'
    assert client.request('DELETE',endpoint,json={'revision':1,'confirmation_name':document['name']}).status_code==200


def test_runner_requires_immutable_image_id(tmp_path,monkeypatch):
    monkeypatch.setenv('DATA_PLATFORM_PYTHON_IMAGE','some-mutable-tag')
    with pytest.raises(ValueError,match='immutable'):
        create_local_app(tmp_path)


def test_busy_runs_and_unconfirmed_cleanup_are_visible(tmp_path,client,monkeypatch):
    from fastapi import HTTPException
    from app.notebooks import SLOTS
    document=notebook(client)
    runtime=PythonRuntime(Workflows(tmp_path,threading.RLock()),clean)
    monkeypatch.setattr(runtime,'status',lambda:{'available':True})
    SLOTS.acquire()
    try:
        with pytest.raises(HTTPException) as busy:
            runtime.run(document,[0])
        assert busy.value.status_code==429
    finally:
        SLOTS.release()
    def cleanup_failure(*args):
        raise HTTPException(503,'Python container cleanup could not be confirmed. Check Docker before running again.')
    monkeypatch.setattr(runtime,'container',cleanup_failure)
    with pytest.raises(HTTPException) as cleanup:
        runtime.run(document,[0])
    assert cleanup.value.status_code==503
    assert client.get('/api/documents/'+document['id']+'/runs').json()[0]['status']=='running'
    with pytest.raises(HTTPException) as blocked:
        runtime.run(document,[0])
    assert blocked.value.status_code==409


def test_a_fetched_notebook_cannot_start_after_concurrent_edit_or_deletion(tmp_path,client,monkeypatch):
    from fastapi import HTTPException
    document=notebook(client)
    runtime=PythonRuntime(Workflows(tmp_path,threading.RLock()),clean)
    monkeypatch.setattr(runtime,'status',lambda:{'available':True})
    changed={**document,'name':'Edited'}
    assert client.put('/api/documents/'+document['id'],json=changed).status_code==200
    with pytest.raises(HTTPException) as conflict:
        runtime.run(document,[0])
    assert conflict.value.status_code==409
    assert client.request('DELETE','/api/documents/'+document['id'],json={'revision':2,'confirmation_name':'Edited'}).status_code==200
    with pytest.raises(HTTPException) as missing:
        runtime.run(document,[0])
    assert missing.value.status_code==404


def test_daemon_create_timeout_retains_cleanup_uncertainty(tmp_path,client,monkeypatch):
    from fastapi import HTTPException
    runtime=PythonRuntime(Workflows(tmp_path,threading.RLock()),clean)
    def delayed_create(*args,**kwargs):
        raise subprocess.TimeoutExpired('docker create',30)
    monkeypatch.setattr('app.notebooks.subprocess.run',delayed_create)
    removed=[]
    monkeypatch.setattr(runtime,'remove_container',lambda name:removed.append(name) or True)
    with pytest.raises(HTTPException) as pending:
        runtime.container('platform-python-late-create',{'cells':[],'datasets':[]})
    assert pending.value.status_code==503
    assert 'cleanup could not be confirmed' in pending.value.detail
    assert removed==['platform-python-late-create']


@pytest.fixture
def live_client(tmp_path,monkeypatch):
    image=os.environ.get('DATA_PLATFORM_TEST_PYTHON_IMAGE')
    if not image:
        pytest.skip('Set DATA_PLATFORM_TEST_PYTHON_IMAGE to a built runner image ID for real Docker checks.')
    monkeypatch.setenv('DATA_PLATFORM_PYTHON_IMAGE',image)
    with TestClient(create_local_app(tmp_path)) as instance:
        yield instance


def test_live_python_selected_versions_shared_variables_and_history(live_client,tmp_path):
    client=live_client
    dataset=client.post('/api/datasets',files={'file':('numbers.csv',b'value\n2\n3\n','text/csv')}).json()
    document=notebook(client,[{'kind':'python','dataset_ids':[dataset['id']],
        'text':f'total = datasets["{dataset["id"]}"]["value"].sum()\nprint("computed", total)'},
        {'kind':'sql','text':'SELECT 99'}, {'kind':'python','text':'pd.DataFrame({"total": [total]})'}])
    endpoint='/api/documents/'+document['id']
    assert client.get('/api/notebook-runtime').json()['available']
    result=client.post(endpoint+'/python/run',json={'revision':1})
    assert result.status_code==200,result.text
    body=result.json()
    assert body['status']=='succeeded'
    assert body['cells'][1]['outputs'][0]['rows']==[[5]]
    assert body['inputs'][0]['version_id']==dataset['version_id']
    assert client.get('/api/query-history').json()==[],'Run all Python must skip SQL cells'
    history=client.get(endpoint+'/runs').json()
    assert history[0]['revision']==1 and history[0]['status']=='succeeded'
    assert client.get(endpoint+'/runs/'+body['run_id']).json()==body
    isolated=client.post(endpoint+'/items/2/run',json={'revision':1}).json()
    assert isolated['status']=='failed' and 'NameError' in isolated['cells'][0]['error']
    document['cells'][0]['text']='print("edited after execution")'
    assert client.put(endpoint,json=document).status_code==200
    prior=client.get(endpoint+'/runs/'+body['run_id']).json()
    assert prior['source_cells'][0]['text'].startswith('total = datasets[')
    assert prior['revision']==1
    missing=notebook(client,[{'kind':'python','text':'print(1)','dataset_ids':['a'*32]}])
    assert client.post('/api/documents/'+missing['id']+'/python/run',json={'revision':1}).status_code==400
    assert client.get('/api/documents/'+missing['id']+'/runs').json()[0]['status']=='failed'


def test_live_python_has_no_host_files_credentials_network_or_root_write(live_client,monkeypatch):
    monkeypatch.setenv('PLATFORM_NOTEBOOK_TEST_SECRET','not-for-runner')
    code='''import os, socket
assert os.getuid() == 65534
assert "PLATFORM_NOTEBOOK_TEST_SECRET" not in os.environ
assert not os.path.exists("/var/run/docker.sock")
assert not os.path.exists("/workspace")
assert not os.environ.get("HTTP_PROXY")
try:
    open("/root-write-probe", "w")
except OSError:
    pass
else:
    raise AssertionError("root filesystem was writable")
try:
    socket.create_connection(("1.1.1.1",443), timeout=0.2)
except OSError:
    pass
else:
    raise AssertionError("external network was accessible")
print("isolation verified")'''
    document=notebook(live_client,[{'kind':'python','text':code}])
    result=live_client.post('/api/documents/'+document['id']+'/python/run',json={'revision':1})
    assert result.status_code==200,result.text
    assert result.json()['status']=='succeeded',result.text
    assert 'isolation verified' in result.json()['cells'][0]['stdout']
    containers=subprocess.run(DOCKER+['ps','--all','--filter','label=data-platform.python=1','--format','{{.Names}}'],capture_output=True,text=True,check=True)
    assert not containers.stdout.strip(),'Disposable runner containers must be removed'


def test_live_python_timeout_and_raw_output_flood_cleanup(live_client,monkeypatch):
    import app.notebooks as module
    monkeypatch.setattr(module,'RUN_SECONDS',2)
    document=notebook(live_client,[{'kind':'python','text':'import time\ntime.sleep(60)'}])
    endpoint='/api/documents/'+document['id']
    assert live_client.post(endpoint+'/python/run',json={'revision':1}).status_code==408
    assert live_client.get(endpoint+'/runs').json()[0]['status']=='failed'
    document['cells'][0]['text']='import os\nwhile True: os.write(1, b"x" * 65536)'
    document=live_client.put(endpoint,json=document).json()
    response=live_client.post(endpoint+'/python/run',json={'revision':2})
    assert response.status_code==400,response.text
    assert 'transport limit' in response.json()['detail']
    containers=subprocess.run(DOCKER+['ps','--all','--filter','label=data-platform.python=1','--format','{{.Names}}'],capture_output=True,text=True,check=True)
    assert not containers.stdout.strip()


def test_live_python_memory_limit_kills_runner_and_records_failure(live_client):
    document=notebook(live_client,[{'kind':'python','text':'data = bytearray(800 * 1024 * 1024)\nprint(len(data))'}])
    endpoint='/api/documents/'+document['id']
    response=live_client.post(endpoint+'/python/run',json={'revision':1})
    assert response.status_code==400,response.text
    assert 'memory' in response.json()['detail']
    assert live_client.get(endpoint+'/runs').json()[0]['status']=='failed'
