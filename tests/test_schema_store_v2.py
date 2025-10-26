import pytest
import json
from pathlib import Path
from conduit_core.schema_store import SchemaStore, compute_schema_hash

@pytest.fixture
def tmp_schema_store(tmp_path):
    return SchemaStore(base_dir=tmp_path / 'schemas')

@pytest.fixture
def sample_schema_v1():
    return {
        'columns': [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'name', 'type': 'STRING', 'nullable': True}
        ]
    }

@pytest.fixture
def sample_schema_v2():
    return {
        'columns': [
            {'name': 'id', 'type': 'INTEGER', 'nullable': False},
            {'name': 'name', 'type': 'STRING', 'nullable': True},
            {'name': 'email', 'type': 'STRING', 'nullable': True}
        ]
    }

def test_compute_schema_hash_deterministic(sample_schema_v1):
    hash1 = compute_schema_hash(sample_schema_v1)
    hash2 = compute_schema_hash(sample_schema_v1)
    assert hash1 == hash2
    assert len(hash1) == 16

def test_compute_schema_hash_different_for_different_schemas(sample_schema_v1, sample_schema_v2):
    hash1 = compute_schema_hash(sample_schema_v1)
    hash2 = compute_schema_hash(sample_schema_v2)
    assert hash1 != hash2

def test_save_schema_creates_version(tmp_schema_store, sample_schema_v1):
    version = tmp_schema_store.save_schema('test_resource', sample_schema_v1)
    assert version == 1
    
    latest = tmp_schema_store.load_last_schema('test_resource')
    assert latest['version'] == 1
    assert latest['schema'] == sample_schema_v1
    assert 'hash' in latest
    assert 'timestamp' in latest

def test_save_schema_increments_version(tmp_schema_store, sample_schema_v1, sample_schema_v2):
    v1 = tmp_schema_store.save_schema('test_resource', sample_schema_v1)
    v2 = tmp_schema_store.save_schema('test_resource', sample_schema_v2)
    
    assert v1 == 1
    assert v2 == 2
    
    latest = tmp_schema_store.load_last_schema('test_resource')
    assert latest['version'] == 2
    assert latest['schema'] == sample_schema_v2

def test_save_schema_archives_old_version(tmp_schema_store, sample_schema_v1, sample_schema_v2):
    tmp_schema_store.save_schema('test_resource', sample_schema_v1)
    tmp_schema_store.save_schema('test_resource', sample_schema_v2)
    
    history = tmp_schema_store.get_schema_history('test_resource')
    assert len(history) == 1
    assert history[0]['schema'] == sample_schema_v1
    assert history[0]['version'] == 1

def test_log_evolution_event(tmp_schema_store):
    changes = {
        'added': [{'name': 'email', 'type': 'STRING', 'nullable': True}],
        'removed': [],
        'type_changes': []
    }
    ddl = ['ALTER TABLE test ADD COLUMN email STRING']
    
    audit_file = tmp_schema_store.log_evolution_event(
        resource_name='test_resource',
        changes=changes,
        ddl_applied=ddl,
        old_version=1,
        new_version=2
    )
    
    assert audit_file.exists()
    
    with open(audit_file, 'r') as f:
        audit_data = json.load(f)
    
    assert audit_data['resource'] == 'test_resource'
    assert audit_data['old_version'] == 1
    assert audit_data['new_version'] == 2
    assert audit_data['changes'] == changes
    assert audit_data['ddl_executed'] == ddl

def test_load_nonexistent_schema(tmp_schema_store):
    result = tmp_schema_store.load_last_schema('nonexistent')
    assert result is None