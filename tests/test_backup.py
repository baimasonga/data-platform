import importlib.util
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidTag
from fastapi.testclient import TestClient
from app.main import create_app

spec=importlib.util.spec_from_file_location('backup_script',Path(__file__).parents[1]/'scripts/backup.py')
backup=importlib.util.module_from_spec(spec);spec.loader.exec_module(backup)


def test_encrypted_backup_restore_and_integrity(tmp_path):
    source=tmp_path/'data';destination=tmp_path/'restored';archive=tmp_path/'backup.dpb';keyfile=tmp_path/'key'
    backup.key(keyfile,create=True)
    with TestClient(create_app(source)) as client:
        dataset=client.post('/api/datasets',files={'file':('x.csv',b'x\n1\n2\n')}).json()
        with pytest.raises(RuntimeError): backup.backup(source,archive,keyfile)
    assert backup.backup(source,archive,keyfile)['encrypted']
    assert archive.read_bytes().startswith(b'DPB1')
    assert b'x.csv' not in archive.read_bytes()
    assert backup.restore(archive,destination,keyfile)['restored']
    import sqlite3
    with sqlite3.connect(source/'catalog.sqlite3') as old,sqlite3.connect(destination/'catalog.sqlite3') as restored:
        assert old.execute("SELECT value FROM workspace_settings WHERE key='lakehouse_schema'").fetchone()==restored.execute("SELECT value FROM workspace_settings WHERE key='lakehouse_schema'").fetchone()
    with TestClient(create_app(destination)) as client:
        assert client.get('/api/datasets').json()[0]['id']==dataset['id']
        assert client.get('/api/datasets/'+dataset['id']+'/source').content==b'x\n1\n2\n'
    with pytest.raises(ValueError): backup.restore(archive,destination,keyfile)
    corrupted=tmp_path/'corrupt.dpb'
    content=bytearray(archive.read_bytes());content[-1]^=1;corrupted.write_bytes(content)
    with pytest.raises(InvalidTag): backup.restore(corrupted,tmp_path/'rejected',keyfile)
    assert not (tmp_path/'rejected').exists()


def test_backup_rejects_symlinks_and_overwrite(tmp_path):
    source=tmp_path/'data';source.mkdir();outside=tmp_path/'private';outside.write_text('do not include')
    keyfile=tmp_path/'key';backup.key(keyfile,create=True)
    (source/'link').symlink_to(outside)
    with pytest.raises(ValueError): backup.backup(source,tmp_path/'archive',keyfile)
    (source/'link').unlink();(source/'file').write_text('safe')
    archive=tmp_path/'archive';archive.write_text('preserve existing user file')
    with pytest.raises(FileExistsError): backup.backup(source,archive,keyfile)
    assert archive.read_text()=='preserve existing user file'
