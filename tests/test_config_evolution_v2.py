import pytest
from conduit_core.config import SchemaEvolutionConfig

def test_schema_evolution_config_defaults():
    """Test default values for new config fields."""
    config = SchemaEvolutionConfig()
    
    assert config.enabled is False
    assert config.mode == "manual"
    assert config.auto_add_columns is True
    assert config.on_column_removed == "warn"
    assert config.on_type_change == "warn"
    assert config.update_yaml is True
    assert config.track_history is True

def test_schema_evolution_config_custom():
    """Test custom configuration."""
    config = SchemaEvolutionConfig(
        enabled=True,
        mode="auto",
        auto_add_columns=False,
        on_column_removed="fail",
        on_type_change="ignore",
        update_yaml=False,
        track_history=False
    )
    
    assert config.enabled is True
    assert config.mode == "auto"
    assert config.auto_add_columns is False
    assert config.on_column_removed == "fail"
    assert config.on_type_change == "ignore"
    assert config.update_yaml is False
    assert config.track_history is False

def test_schema_evolution_yaml_parsing(tmp_path):
    """Test parsing schema_evolution from YAML."""
    import yaml
    from conduit_core.config import load_config
    
    config_content = """
sources:
  - name: test_src
    type: csv
    path: /tmp/test.csv

destinations:
  - name: test_dest
    type: postgresql
    connection_string: "postgresql://user:pass@localhost/db"
    table: test_table
    schema_evolution:
      enabled: true
      mode: auto
      auto_add_columns: true
      on_column_removed: warn
      on_type_change: fail
      update_yaml: false
      track_history: true

resources:
  - name: test_resource
    source: test_src
    destination: test_dest
    query: "SELECT * FROM test"
"""
    
    config_path = tmp_path / "test_config.yml"
    config_path.write_text(config_content)
    
    config = load_config(str(config_path))
    
    evolution_cfg = config.destinations[0].schema_evolution
    assert evolution_cfg is not None
    assert evolution_cfg.enabled is True
    assert evolution_cfg.mode == "auto"
    assert evolution_cfg.on_type_change == "fail"
    assert evolution_cfg.update_yaml is False