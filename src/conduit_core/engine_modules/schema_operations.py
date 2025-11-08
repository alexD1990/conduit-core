"""
Schema operations module for inference, evolution, and table creation.
"""
from typing import Any, Optional, Dict
from ..schema import SchemaInferrer, TableAutoCreator
from ..schema_store import SchemaStore
from ..schema_evolution import SchemaEvolutionManager, SchemaEvolutionError
from ..logging_utils import ConduitLogger

logger = ConduitLogger("schema_operations")


def get_sql_type_for_column(col_type: str, dialect: str) -> str:
    """
    Map Python/inferred types to SQL dialect-specific types.
    
    Args:
        col_type: Python type name (e.g., 'int', 'str', 'datetime')
        dialect: SQL dialect ('postgresql', 'snowflake', 'bigquery')
    
    Returns:
        SQL type string
    """
    type_map = {
        'postgresql': {
            'int': 'INTEGER',
            'float': 'DOUBLE PRECISION',
            'str': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime': 'TIMESTAMP',
            'date': 'DATE',
        },
        'snowflake': {
            'int': 'NUMBER',
            'float': 'FLOAT',
            'str': 'VARCHAR',
            'bool': 'BOOLEAN',
            'datetime': 'TIMESTAMP_NTZ',
            'date': 'DATE',
        },
        'bigquery': {
            'int': 'INT64',
            'float': 'FLOAT64',
            'str': 'STRING',
            'bool': 'BOOL',
            'datetime': 'TIMESTAMP',
            'date': 'DATE',
        }
    }
    
    dialect_types = type_map.get(dialect, type_map['postgresql'])
    return dialect_types.get(col_type, 'TEXT')


def infer_schema_from_source(
    source: Any,
    source_config: Any,
    resource: Any
) -> Optional[Dict]:
    """
    Infer schema by sampling records from source.
    
    Args:
        source: Source connector instance
        source_config: Source configuration
        resource: Resource configuration
    
    Returns:
        Inferred schema dictionary or None
    """
    if not source_config.infer_schema:
        return None
    
    try:
        logger.info("Inferring schema from source data...")
        
        # Sample records
        sample_records = []
        source_iter = source.read(query=resource.query if hasattr(resource, 'query') else None)
        
        for batch in source_iter:
            if isinstance(batch, dict):
                sample_records.append(batch)
            elif isinstance(batch, list):
                sample_records.extend(batch)
            
            if len(sample_records) >= source_config.schema_sample_size:
                break
        
        if sample_records:
            logger.debug("[DEBUG] Source iterator consumed by schema sample, using sampled records.")
            inferred_schema = SchemaInferrer.infer_schema(
                sample_records, 
                source_config.schema_sample_size
            )
            logger.info(f"Schema inferred: {len(inferred_schema.get('columns', []))} columns")
            return inferred_schema
        else:
            logger.warning("No records available for schema inference")
            return None
            
    except Exception as e:
        logger.error(f"Schema inference failed: {e}")
        return None


def handle_schema_evolution(
    resource: Any,
    destination: Any,
    destination_config: Any,
    inferred_schema: Dict,
    tracker: Any,
    dry_run: bool = False
) -> None:
    """
    Handle schema evolution by comparing and applying changes.
    
    Args:
        resource: Resource configuration
        destination: Destination connector
        destination_config: Destination configuration
        inferred_schema: Current inferred schema
        tracker: Progress tracker for metadata
        dry_run: If True, don't execute DDL
    """
    # Check if schema evolution is enabled
    if not (
        destination_config.schema_evolution and
        destination_config.schema_evolution.enabled and
        inferred_schema and inferred_schema.get("columns") and
        destination_config.type in ['postgres', 'mysql', 'snowflake', 'bigquery']
    ):
        return
    
    schema_store = SchemaStore()
    last_schema_data = schema_store.load_last_schema(resource.name)
    
    if last_schema_data:
        last_schema = last_schema_data.get('schema') if isinstance(last_schema_data, dict) and 'schema' in last_schema_data else last_schema_data
        
        logger.info("Comparing inferred schema with last known schema...")
        evolution_mgr = SchemaEvolutionManager()
        changes = evolution_mgr.compare_schemas(last_schema, inferred_schema)
        
        if changes.has_changes():
            if not dry_run:
                try:
                    executed_ddl = evolution_mgr.apply_evolution(
                        destination, 
                        destination_config.table, 
                        changes, 
                        destination_config.schema_evolution,
                        resource.name
                    )
                    
                    if executed_ddl:
                        new_version = schema_store.save_schema(resource.name, inferred_schema)
                        
                        tracker.metadata['schema_evolution'] = {
                            'version': new_version,
                            'changes_summary': changes.summary(),
                            'ddl_executed': executed_ddl
                        }
                        
                except SchemaEvolutionError as e:
                    logger.error(f"Schema evolution failed: {e}. Halting pipeline.")
                    raise
            else:
                logger.info("DRY RUN: Skipping schema evolution actions.", prefix="[WARN]")
        else:
            logger.info("No schema changes detected.")
    else:
        logger.info("No previous schema found. Saving current schema as baseline.")
        schema_store.save_schema(resource.name, inferred_schema)


def auto_create_table(
    destination: Any,
    destination_config: Any,
    inferred_schema: Dict,
    dry_run: bool = False
) -> None:
    """
    Auto-create destination table if enabled.
    
    Args:
        destination: Destination connector
        destination_config: Destination configuration
        inferred_schema: Inferred schema for table creation
        dry_run: If True, don't execute DDL
    """
    if not (destination_config.auto_create_table and inferred_schema):
        return
    
    if destination_config.type not in ['postgres', 'snowflake', 'bigquery']:
        return
    
    logger.info(f"Auto-creating table in {destination_config.type}...")
    dialect_map = {'postgres': 'postgresql', 'snowflake': 'snowflake', 'bigquery': 'bigquery'}
    
    try:
        # For Snowflake, include database.schema prefix
        if destination_config.type == 'snowflake':
            full_table_name = f'{destination.database}.{destination.db_schema}.{destination_config.table}'
        else:
            full_table_name = destination_config.table
        
        create_sql = TableAutoCreator.generate_create_table_sql(
            full_table_name, 
            inferred_schema, 
            dialect_map[destination_config.type]
        )
        
        # For Snowflake, fix the quoting in generated SQL
        if destination_config.type == 'snowflake':
            quoted_full = f'"{destination.database}.{destination.db_schema}.{destination_config.table}"'
            unquoted_path = f'{destination.database}.{destination.db_schema}."{destination_config.table}"'
            create_sql = create_sql.replace(quoted_full, unquoted_path)
        
        logger.info(f"Generated SQL:\n{create_sql}")
        
        if not dry_run:
            try:
                destination.execute_ddl(create_sql)
                logger.info(f"Table '{destination_config.table}' created successfully.")
            except Exception as e:
                logger.error(f"Failed to auto-create table: {e}")
                raise
        else:
            logger.info("DRY RUN: Skipping table creation.", prefix="[WARN]")
            
    except ValueError as e:
        logger.warning(f"Could not generate CREATE TABLE SQL (schema likely empty): {e}")