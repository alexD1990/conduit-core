# src/conduit_core/connectors/registry.py
import importlib
import inspect
import logging
from pathlib import Path
from typing import Dict, Type
from .base import BaseSource, BaseDestination

logger = logging.getLogger(__name__)


def discover_connectors() -> tuple[Dict[str, Type[BaseSource]], Dict[str, Type[BaseDestination]]]:
    """
    Automatically discovers all connector classes in the connectors package.
    
    Returns:
        A tuple of (source_map, destination_map) where:
        - source_map: {connector_type: SourceClass}
        - destination_map: {connector_type: DestinationClass}
    """
    source_map = {}
    destination_map = {}
    
    # Get the connectors directory path
    connectors_dir = Path(__file__).parent
    
    # Scan all Python files in the connectors directory
    for file_path in connectors_dir.glob("*.py"):
        # Skip special files
        if file_path.name.startswith("_") or file_path.name == "registry.py":
            continue
        
        module_name = file_path.stem  # e.g., "csv", "azuresql"
        
        try:
            # Dynamically import the module
            module = importlib.import_module(f".{module_name}", package="conduit_core.connectors")
            
            # Inspect the module for connector classes
            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Check if it's a BaseSource subclass (but not BaseSource itself)
                if issubclass(obj, BaseSource) and obj is not BaseSource:
                    # Derive connector type from class name
                    # e.g., "CsvSource" -> "csv", "AzureSqlSource" -> "azuresql"
                    connector_type = _derive_connector_type(name, "Source")
                    source_map[connector_type] = obj
                    logger.info(f"Discovered source connector: {connector_type} -> {name}")
                
                # Check if it's a BaseDestination subclass
                elif issubclass(obj, BaseDestination) and obj is not BaseDestination:
                    connector_type = _derive_connector_type(name, "Destination")
                    destination_map[connector_type] = obj
                    logger.info(f"Discovered destination connector: {connector_type} -> {name}")
        
        except Exception as e:
            logger.warning(f"Failed to import connector module '{module_name}': {e}")
    
    return source_map, destination_map


def _derive_connector_type(class_name: str, suffix: str) -> str:
    """
    Derives the connector type string from the class name.
    
    Examples:
        CsvSource -> csv
        AzureSqlSource -> azuresql
        PostgresDestination -> postgres
    
    Args:
        class_name: The class name (e.g., "CsvSource")
        suffix: Either "Source" or "Destination"
    
    Returns:
        The connector type string in lowercase
    """
    # Remove the suffix (Source or Destination)
    if class_name.endswith(suffix):
        type_name = class_name[:-len(suffix)]
    else:
        type_name = class_name
    
    # Convert from CamelCase to lowercase
    # AzureSql -> azuresql
    return type_name.lower()


# Cache the discovered connectors (only discover once)
_SOURCE_CONNECTOR_MAP = None
_DESTINATION_CONNECTOR_MAP = None

def get_source_connector_map() -> Dict[str, Type[BaseSource]]:
    """Returns the discovered source connector map."""
    global _SOURCE_CONNECTOR_MAP
    if _SOURCE_CONNECTOR_MAP is None:
        _SOURCE_CONNECTOR_MAP, _ = discover_connectors()

        # Alias: allow both "postgres" and "postgresql"
        if "postgres" in _SOURCE_CONNECTOR_MAP:
            _SOURCE_CONNECTOR_MAP["postgresql"] = _SOURCE_CONNECTOR_MAP["postgres"]

    return _SOURCE_CONNECTOR_MAP


def get_destination_connector_map() -> Dict[str, Type[BaseDestination]]:
    """Returns the discovered destination connector map."""
    global _DESTINATION_CONNECTOR_MAP
    if _DESTINATION_CONNECTOR_MAP is None:
        _, _DESTINATION_CONNECTOR_MAP = discover_connectors()

        # Alias: allow both "postgres" and "postgresql"
        if "postgres" in _DESTINATION_CONNECTOR_MAP:
            _DESTINATION_CONNECTOR_MAP["postgresql"] = _DESTINATION_CONNECTOR_MAP["postgres"]

    return _DESTINATION_CONNECTOR_MAP
