import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from conduit_core.config import IngestConfig, Source, Destination, Resource, SchemaEvolutionConfig
from conduit_core.schema_store import SchemaStore
from conduit_core.schema_evolution import SchemaEvolutionManager


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


def test_schema_store_baseline_save(tmp_path, sample_schema_v1):
    """Test that SchemaStore saves baseline schema correctly."""
    store = SchemaStore(base_dir=tmp_path / '.conduit')
    
    version = store.save_schema('test_resource', sample_schema_v1)
    assert version == 1
    
    loaded = store.load_last_schema('test_resource')
    assert loaded['version'] == 1
    assert loaded['schema'] == sample_schema_v1


def test_schema_evolution_detects_and_logs(tmp_path, sample_schema_v1, sample_schema_v2):
    """Test that evolution manager detects changes and logs audit."""
    store = SchemaStore(base_dir=tmp_path / '.conduit')
    store.save_schema('test_resource', sample_schema_v1)
    
    manager = SchemaEvolutionManager()
    changes = manager.compare_schemas(sample_schema_v1, sample_schema_v2)
    
    assert changes.has_changes()
    assert len(changes.added_columns) == 1
    assert changes.added_columns[0].name == 'email'
    
    mock_dest = MagicMock()
    mock_dest.config.type = 'postgresql'
    
    config = SchemaEvolutionConfig(
        enabled=True,
        mode='auto',
        auto_add_columns=True,
        track_history=True
    )
    
    with patch('conduit_core.schema_evolution.TableAutoCreator.generate_add_column_sql') as mock_gen, \
         patch('conduit_core.schema_store.SchemaStore', return_value=store):
        
        mock_gen.return_value = 'ALTER TABLE test ADD COLUMN email STRING'
        
        executed_ddl = manager.apply_evolution(
            mock_dest,
            'test_table',
            changes,
            config,
            'test_resource'
        )
        
        assert len(executed_ddl) == 1
        assert 'email' in executed_ddl[0]
        
        audit_files = list((tmp_path / '.conduit' / 'schema_audit').glob('*.json'))
        assert len(audit_files) == 1


def test_schema_version_increments(tmp_path, sample_schema_v1, sample_schema_v2):
    """Test that schema version increments correctly."""
    store = SchemaStore(base_dir=tmp_path / '.conduit')
    
    v1 = store.save_schema('test_resource', sample_schema_v1)
    v2 = store.save_schema('test_resource', sample_schema_v2)
    
    assert v1 == 1
    assert v2 == 2
    
    latest = store.load_last_schema('test_resource')
    assert latest['version'] == 2
    assert latest['schema'] == sample_schema_v2
    
    history = store.get_schema_history('test_resource')
    assert len(history) == 1
    assert history[0]['version'] == 1