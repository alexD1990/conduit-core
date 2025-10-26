import pytest
from pathlib import Path
from conduit_core.config import IngestConfig, Source, Destination, Resource, SchemaEvolutionConfig
from conduit_core.engine import preflight_check
from conduit_core.schema_store import SchemaStore


@pytest.fixture
def test_config(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name,email\n1,Alice,alice@test.com")
    
    return IngestConfig(
        sources=[Source(
            name="csv_src",
            type="csv",
            path=str(csv_path),
            infer_schema=True
        )],
        destinations=[Destination(
            name="pg_dest",
            type="postgres",
            connection_string="postgresql://user:pass@localhost/db",
            table="users",
            schema_evolution=SchemaEvolutionConfig(
                enabled=True,
                mode="auto"
            )
        )],
        resources=[Resource(
            name="test_resource",
            source="csv_src",
            destination="pg_dest",
            query="n/a"
        )]
    )


def test_preflight_shows_schema_evolution_preview(test_config, tmp_path):
    """Preflight should show schema evolution preview when changes detected."""
    from unittest.mock import MagicMock, patch
    from conduit_core.connectors.postgresql import PostgresDestination
    from conduit_core.schema_store import SchemaStore
    
    # Setup baseline schema directly
    schema_store = SchemaStore(base_dir=tmp_path / '.conduit')
    baseline_schema = {
        'columns': [
            {'name': 'id', 'type': 'integer', 'nullable': False},
            {'name': 'name', 'type': 'string', 'nullable': True}
        ]
    }
    schema_store.save_schema('test_resource', baseline_schema)
    
    # Mock CSV source
    mock_source = MagicMock()
    mock_source.read.return_value = iter([[
        {'id': 1, 'name': 'Alice', 'email': 'alice@test.com'}
    ]])
    
    # Patch to make all SchemaStore instances use tmp_path
    original_init = SchemaStore.__init__
    def patched_init(self, base_dir=None):
        original_init(self, base_dir=tmp_path / '.conduit')
    
    # Patch PostgreSQL methods to avoid real DB connection
    with patch('conduit_core.engine.get_source_connector_map', return_value={'csv': lambda cfg: mock_source}), \
         patch.object(PostgresDestination, 'table_exists', return_value=True), \
         patch.object(PostgresDestination, 'get_table_schema', return_value={
             'id': {'type': 'integer', 'nullable': False},
             'name': {'type': 'string', 'nullable': True}
         }), \
         patch.object(SchemaStore, '__init__', patched_init):
        
        result = preflight_check(test_config, verbose=True)
    
    # DEBUG
    print("\n=== All checks ===")
    for check in result['checks']:
        print(f"{check['name']}: {check['status']}")
        if 'Evolution' in check['name'] or 'Drift' in check['name']:
            print(f"  Message: {check['message']}")
    
    # Verify
    evolution_check = next((c for c in result['checks'] if 'Schema Evolution' in c['name']), None)
    assert evolution_check is not None, f"Checks: {[c['name'] for c in result['checks']]}"
    assert '[+] Would execute:' in evolution_check['message']
    assert 'email' in evolution_check['message']
    assert 'Version:' in evolution_check['message']


def test_preflight_no_preview_when_no_changes(test_config, tmp_path):
    """Preflight should not show preview when schemas match."""
    from unittest.mock import MagicMock, patch
    from conduit_core.connectors.postgresql import PostgresDestination
    from conduit_core.schema_store import SchemaStore
    
    # Baseline matches current
    schema_store = SchemaStore(base_dir=tmp_path / '.conduit')
    baseline_schema = {
        'columns': [
            {'name': 'id', 'type': 'integer', 'nullable': False},
            {'name': 'name', 'type': 'string', 'nullable': True},
            {'name': 'email', 'type': 'string', 'nullable': True}
        ]
    }
    schema_store.save_schema('test_resource', baseline_schema)
    
    mock_source = MagicMock()
    mock_source.read.return_value = iter([[
        {'id': 1, 'name': 'Alice', 'email': 'alice@test.com'}
    ]])
    
    # Patch to make all SchemaStore instances use tmp_path
    original_init = SchemaStore.__init__
    def patched_init(self, base_dir=None):
        original_init(self, base_dir=tmp_path / '.conduit')
    
    with patch('conduit_core.engine.get_source_connector_map', return_value={'csv': lambda cfg: mock_source}), \
         patch.object(PostgresDestination, 'table_exists', return_value=True), \
         patch.object(PostgresDestination, 'get_table_schema', return_value={
             'id': {'type': 'integer', 'nullable': False},
             'name': {'type': 'string', 'nullable': True},
             'email': {'type': 'string', 'nullable': True}
         }), \
         patch.object(SchemaStore, '__init__', patched_init):
        
        result = preflight_check(test_config, verbose=True)
    
    # Should not have evolution preview when no changes
    evolution_check = next((c for c in result['checks'] if 'Schema Evolution' in c['name']), None)
    assert evolution_check is None, "Should not show evolution preview when no changes detected"