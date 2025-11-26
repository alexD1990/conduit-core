import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import json

from conduit_core.schema_evolution import SchemaEvolutionManager, SchemaChanges, SchemaEvolutionError
from conduit_core.config import SchemaEvolutionConfig
from conduit_core.schema import ColumnDefinition


@pytest.fixture
def schema_v1():
    return {
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "STRING", "nullable": True}
        ]
    }

@pytest.fixture
def schema_v2_added():
    return {
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "STRING", "nullable": True},
            {"name": "email", "type": "STRING", "nullable": True}
        ]
    }

@pytest.fixture
def schema_v3_removed():
    return {
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False}
        ]
    }

@pytest.fixture
def schema_v4_type_change():
    return {
        "columns": [
            {"name": "id", "type": "STRING", "nullable": False},
            {"name": "name", "type": "STRING", "nullable": True}
        ]
    }

@pytest.fixture
def mock_destination():
    dest = MagicMock()
    dest.config.type = "postgresql"
    dest.alter_table = MagicMock()
    return dest


def test_auto_mode_adds_columns(schema_v1, schema_v2_added, mock_destination, tmp_path):
    manager = SchemaEvolutionManager()
    changes = manager.compare_schemas(schema_v1, schema_v2_added)
    config = SchemaEvolutionConfig(
        mode="auto",
        auto_add_columns=True,
        track_history=False
    )
    
    with patch('conduit_core.schema_evolution.TableAutoCreator.generate_add_column_sql') as mock_gen:
        mock_gen.return_value = "ALTER TABLE test ADD COLUMN email STRING"
        
        ddl = manager.apply_evolution(mock_destination, "test", changes, config, "test_resource")
        
        assert len(ddl) == 1
        assert "ALTER TABLE" in ddl[0]
        mock_destination.alter_table.assert_called_once()


def test_removed_column_warns(schema_v1, schema_v3_removed, mock_destination, caplog):
    manager = SchemaEvolutionManager()
    changes = manager.compare_schemas(schema_v1, schema_v3_removed)
    config = SchemaEvolutionConfig(
        mode="auto",
        on_column_removed="warn",
        track_history=False
    )
    
    ddl = manager.apply_evolution(mock_destination, "test", changes, config, "test_resource")
    
    assert len(ddl) == 0
    assert "Source missing columns" in caplog.text
    assert "Inserting NULL values" in caplog.text


@pytest.mark.xfail(reason="on_column_removed=fail not yet implemented")
def test_removed_column_fails(schema_v1, schema_v3_removed, mock_destination):
    manager = SchemaEvolutionManager()
    changes = manager.compare_schemas(schema_v1, schema_v3_removed)
    config = SchemaEvolutionConfig(
        mode="auto",
        on_column_removed="fail",
        track_history=False
    )
    
    with pytest.raises(SchemaEvolutionError, match="removed from source"):
        manager.apply_evolution(mock_destination, "test", changes, config, "test_resource")


def test_type_change_warns(schema_v1, schema_v4_type_change, mock_destination, caplog):
    manager = SchemaEvolutionManager()
    changes = manager.compare_schemas(schema_v1, schema_v4_type_change)
    config = SchemaEvolutionConfig(
        mode="auto",
        on_type_change="warn",
        track_history=False
    )
    
    ddl = manager.apply_evolution(mock_destination, "test", changes, config, "test_resource")
    
    assert len(ddl) == 0
    assert "Type changed" in caplog.text


def test_type_change_fails(schema_v1, schema_v4_type_change, mock_destination):
    manager = SchemaEvolutionManager()
    changes = manager.compare_schemas(schema_v1, schema_v4_type_change)
    config = SchemaEvolutionConfig(
        mode="auto",
        on_type_change="fail",
        track_history=False
    )
    
    with pytest.raises(SchemaEvolutionError, match="type changed"):
        manager.apply_evolution(mock_destination, "test", changes, config, "test_resource")


def test_audit_trail_created(schema_v1, schema_v2_added, mock_destination, tmp_path):
    from conduit_core.schema_store import SchemaStore
    
    store = SchemaStore(base_dir=tmp_path / '.conduit')
    store.save_schema('test_resource', schema_v1)
    
    manager = SchemaEvolutionManager()
    changes = manager.compare_schemas(schema_v1, schema_v2_added)
    config = SchemaEvolutionConfig(
        mode="auto",
        auto_add_columns=True,
        track_history=True
    )
    
    with patch('conduit_core.schema_evolution.TableAutoCreator.generate_add_column_sql') as mock_gen:
        mock_gen.return_value = "ALTER TABLE test ADD COLUMN email STRING"
        with patch('conduit_core.schema_store.SchemaStore') as mock_store_class:
            mock_store_instance = MagicMock()
            mock_store_instance.load_last_schema.return_value = {'version': 1, 'schema': schema_v1}
            mock_store_instance.log_evolution_event.return_value = Path('.conduit/schema_audit/test_20251026_120000.json')
            mock_store_class.return_value = mock_store_instance
            
            ddl = manager.apply_evolution(mock_destination, "test", changes, config, "test_resource")
            
            assert len(ddl) == 1
            mock_store_instance.log_evolution_event.assert_called_once()