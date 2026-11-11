"""Safety boundaries for the disposable coordinated recovery drill."""
import json
import os

import pytest

from scripts.verify_recovery import Stack, configure, private_json, transfer_volume


def test_source_and_restore_configs_use_distinct_resources_and_loopback(tmp_path):
    template = {'volumes': {'postgres': {'name': 'existing_postgres'}},
                'networks': {'default': {'name': 'existing_default'}},
                'services': {'keycloak': {'command': [], 'volumes': [
                    {'type': 'volume', 'source': 'postgres', 'target': '/data'}], 'ports': [
                    {'host_ip': '0.0.0.0', 'published': '8081', 'target': 8080}]}}}
    source = configure(template, 'drill-source', {'keycloak': 18081}, tmp_path / 'source.json')
    restored = configure(template, 'drill-restored', {'keycloak': 18081}, tmp_path / 'restored.json')
    assert source['volumes']['postgres']['name'] != restored['volumes']['postgres']['name']
    assert source['networks']['default']['name'] != restored['networks']['default']['name']
    assert source['services']['keycloak']['ports'][0]['host_ip'] == '127.0.0.1'
    assert source['services']['keycloak']['ports'][0]['published'] == '18081'
    assert restored['services']['keycloak']['volumes'][0]['volume']['nocopy'] is True
    assert template['volumes']['postgres']['name'] == 'existing_postgres'


def test_drill_rejects_external_resources_before_writing_configuration(tmp_path):
    for key in ('volumes', 'networks'):
        template = {'volumes': {}, 'networks': {}, 'services': {}, key: {'shared': {'external': True}}}
        with pytest.raises(ValueError, match='external'):
            configure(template, 'drill-test', {}, tmp_path / 'config.json')
        assert not (tmp_path / 'config.json').exists()


def test_drill_refuses_live_volume_capture_before_docker_helper(monkeypatch, tmp_path):
    stack = Stack('drill-test', tmp_path / 'config.json', {})
    monkeypatch.setattr(stack, 'compose', lambda *args: 'running-container-id')
    monkeypatch.setattr(stack, 'volume', lambda name: pytest.fail('Must not inspect or mount live volumes.'))
    with pytest.raises(RuntimeError, match='Stop all'):
        transfer_volume(stack, 'postgres', tmp_path, 'unused-image')


def test_drill_refuses_foreign_volume_ownership(monkeypatch, tmp_path):
    from scripts import verify_recovery
    stack = Stack('drill-test', tmp_path / 'config.json', {})
    monkeypatch.setattr(verify_recovery, 'run', lambda args: json.dumps([
        {'Labels': {'com.docker.compose.project': 'existing-development'}}]))
    with pytest.raises(RuntimeError, match='outside'):
        stack.volume('postgres')


def test_private_report_never_overwrites_existing_file(tmp_path):
    path = tmp_path / 'report.json'
    private_json(path, {'status': 'passed'})
    if os.name != 'nt':
        assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        private_json(path, {'status': 'replaced'})
    assert json.loads(path.read_text()) == {'status': 'passed'}
