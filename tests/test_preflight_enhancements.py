"""Tests for table existence checks and schema drift detection."""
import pytest
from pathlib import Path
from conduit_core.schema import compare_schemas, _types_compatible


def test_compare_schemas_no_drift():
    """Test schema comparison with no changes."""
    source = {
        "columns": [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "name", "type": "string", "nullable": True}
        ]
    }
    dest = {
        "columns": [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "name", "type": "string", "nullable": True}
        ]
    }
    
    drift = compare_schemas(source, dest)
    
    assert drift["added"] == []
    assert drift["removed"] == []
    assert drift["changed"] == {}


def test_compare_schemas_added_columns():
    """Test schema comparison with added columns."""
    source = {
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"}  # New column
        ]
    }
    dest = {
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"}
        ]
    }
    
    drift = compare_schemas(source, dest)
    
    assert "email" in drift["added"]
    assert drift["removed"] == []


def test_compare_schemas_removed_columns():
    """Test schema comparison with removed columns."""
    source = {
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"}
        ]
    }
    dest = {
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "name", "type": "string"},
            {"name": "old_column", "type": "string"}  # Removed from source
        ]
    }
    
    drift = compare_schemas(source, dest)
    
    assert drift["added"] == []
    assert "old_column" in drift["removed"]


def test_compare_schemas_type_changes():
    """Test schema comparison with type changes."""
    source = {
        "columns": [
            {"name": "id", "type": "bigint"},  # Changed type
            {"name": "name", "type": "string"}
        ]
    }
    dest = {
        "columns": [
            {"name": "id", "type": "smallint"},
            {"name": "name", "type": "string"}
        ]
    }
    
    drift = compare_schemas(source, dest)
    
    assert "id" in drift["changed"]
    assert "smallint → bigint" in drift["changed"]["id"]


def test_types_compatible_integers():
    """Test type compatibility for integer types."""
    assert _types_compatible("integer", "int") is True
    assert _types_compatible("bigint", "int64") is True
    assert _types_compatible("integer", "number") is True


def test_types_compatible_strings():
    """Test type compatibility for string types."""
    assert _types_compatible("varchar", "text") is True
    assert _types_compatible("string", "character varying") is True
    assert _types_compatible("varchar", "varchar") is True


def test_types_compatible_floats():
    """Test type compatibility for float types."""
    assert _types_compatible("float", "double") is True
    assert _types_compatible("numeric", "decimal") is True
    assert _types_compatible("float64", "float") is True


def test_types_incompatible():
    """Test incompatible types."""
    assert _types_compatible("integer", "string") is False
    assert _types_compatible("boolean", "integer") is False
    assert _types_compatible("date", "datetime") is False


def test_preflight_with_table_check_postgres(tmp_path):
    """Test preflight detects missing table in PostgreSQL."""
    # This requires a running PostgreSQL instance
    # Skip if DB not available
    pytest.skip("Requires PostgreSQL instance - tested manually")


def test_preflight_with_schema_drift_postgres(tmp_path):
    """Test preflight detects schema drift in PostgreSQL."""
    # This requires a running PostgreSQL instance
    # Skip if DB not available
    pytest.skip("Requires PostgreSQL instance - tested manually")