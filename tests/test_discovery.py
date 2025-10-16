# tests/test_discovery.py
import pytest
from conduit_core.connectors.registry import discover_connectors, get_source_connector_map, get_destination_connector_map
from conduit_core.connectors.base import BaseSource, BaseDestination


def test_discover_connectors_finds_csv():
    """Test that CSV connectors are discovered."""
    source_map, destination_map = discover_connectors()

    assert "csv" in source_map
    assert "csv" in destination_map
    assert issubclass(source_map["csv"], BaseSource)
    assert issubclass(destination_map["csv"], BaseDestination)


@pytest.mark.skip(reason="AzureSQL not in v1.0")
def test_discover_connectors_finds_azuresql():
    """Test that Azure SQL connectors are discovered."""
    source_map, destination_map = discover_connectors()

    assert "azuresql" in source_map
    assert "azuresql" in destination_map


def test_get_source_connector_map_caches():
    """Test that the connector map is cached."""
    map1 = get_source_connector_map()
    map2 = get_source_connector_map()

    # Should be the exact same object (cached)
    assert map1 is map2


def test_connector_type_derivation():
    """Test that connector types are derived correctly from class names."""
    from conduit_core.connectors.registry import _derive_connector_type

    assert _derive_connector_type("CsvSource", "Source") == "csv"
    assert _derive_connector_type("AzureSqlSource", "Source") == "azuresql"
    assert _derive_connector_type("PostgresDestination", "Destination") == "postgres"