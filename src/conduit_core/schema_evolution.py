# src/conduit_core/schema_evolution.py

import logging
from typing import List, Optional, Dict, Any, Set
from pydantic import BaseModel
from .schema import ColumnDefinition
from .connectors.base import BaseDestination
from .config import SchemaEvolutionConfig

logger = logging.getLogger(__name__)

class TypeChange(BaseModel):
    """Represents a change in a column's data type."""
    column: str
    old_type: str
    new_type: str

class SchemaChanges(BaseModel):
    """Holds the detected differences between two schemas."""
    added_columns: List[ColumnDefinition] = []
    removed_columns: List[ColumnDefinition] = []
    type_changes: List[TypeChange] = []

    def has_changes(self) -> bool:
        """Return True if any changes were detected."""
        return bool(self.added_columns or self.removed_columns or self.type_changes)

    def summary(self) -> str:
        """Return a human-readable summary of changes."""
        parts = []
        if self.added_columns:
            parts.append(f"Added: {[col.name for col in self.added_columns]}")
        if self.removed_columns:
            parts.append(f"Removed: {[col.name for col in self.removed_columns]}")
        if self.type_changes:
            parts.append(f"Type Changes: {[tc.column for tc in self.type_changes]}")
        return ", ".join(parts)


class SchemaEvolutionManager:
    """Detects and handles schema changes between a new and old schema."""

    @staticmethod
    def compare_schemas(old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> SchemaChanges:
        """
        Compares an old (destination) schema with a new (source) schema.

        Args:
            old_schema: The schema currently in the destination (e.g., from DB).
            new_schema: The schema inferred from the source data.
        """
        old_cols: Dict[str, ColumnDefinition] = {
            col['name'].lower(): ColumnDefinition(**col) for col in old_schema.get('columns', [])
        }
        new_cols: Dict[str, ColumnDefinition] = {
            col['name'].lower(): ColumnDefinition(**col) for col in new_schema.get('columns', [])
        }

        old_col_names = set(old_cols.keys())
        new_col_names = set(new_cols.keys())

        added_col_names = new_col_names - old_col_names
        removed_col_names = old_col_names - new_col_names
        common_col_names = new_col_names.intersection(old_col_names)

        changes = SchemaChanges()

        for col_name in added_col_names:
            changes.added_columns.append(new_cols[col_name])

        for col_name in removed_col_names:
            changes.removed_columns.append(old_cols[col_name])

        for col_name in common_col_names:
            old_col = old_cols[col_name]
            new_col = new_cols[col_name]
            
            # Simple type comparison for now.
            # TODO: Add more sophisticated type compatibility logic
            if old_col.type != new_col.type:
                changes.type_changes.append(
                    TypeChange(column=col_name, old_type=old_col.type, new_type=new_col.type)
                )
        
        return changes

    @staticmethod
    def generate_alter_table_sql(
        table_name: str, 
        changes: SchemaChanges, 
        config: SchemaEvolutionConfig, 
        dialect: str
    ) -> List[str]:
        """
        Generates the necessary ALTER TABLE SQL statements based on the changes and config.
        """
        sql_commands = []
        if not changes.has_changes():
            return sql_commands

        # 1. Handle New Columns
        if changes.added_columns:
            if config.on_new_column == "add_nullable":
                for col in changes.added_columns:
                    sql_commands.append(
                        TableAutoCreator.generate_add_column_sql(table_name, col, dialect)
                    )
            elif config.on_new_column == "fail":
                raise SchemaEvolutionError(f"New columns detected and 'on_new_column' is 'fail': {[col.name for col in changes.added_columns]}")
            # 'ignore' means do nothing

        # 2. Handle Removed Columns
        if changes.removed_columns:
            if config.on_removed_column == "fail":
                raise SchemaEvolutionError(f"Removed columns detected and 'on_removed_column' is 'fail': {[col.name for col in changes.removed_columns]}")
            elif config.on_removed_column == "warn":
                logger.warning(f"Source schema is missing columns present in destination: {[col.name for col in changes.removed_columns]}")
            # 'ignore' means do nothing

        # 3. Handle Type Changes
        if changes.type_changes:
            if config.on_type_change == "fail":
                raise SchemaEvolutionError(f"Data type changes detected and 'on_type_change' is 'fail': {changes.type_changes}")
            elif config.on_type_change == "warn":
                logger.warning(f"Data type changes detected: {changes.type_changes}")
            # 'ignore' means do nothing
        
        return sql_commands

    def apply_evolution(
        self,
        destination: BaseDestination,
        table_name: str,
        changes: SchemaChanges,
        config: SchemaEvolutionConfig,
        resource_name: str
    ) -> List[str]:
        """Apply evolution based on config policy. Returns list of executed DDL."""

        if not changes.has_changes():
            logger.info("No schema changes detected.")
            return []

        logger.info(f"[DEBUG] Config mode: {config.mode}, auto_add: {config.auto_add_columns}")
        logger.info(f"[DEBUG] Added columns: {len(changes.added_columns)}")

        executed_ddl = []
        dialect = destination.config.type

        if changes.added_columns:
            logger.info(f"[DEBUG] Checking: mode={config.mode}, auto_add={config.auto_add_columns}")
            if config.mode == "auto" and config.auto_add_columns:
                print(f"[Schema Evolution] {resource_name}")
                for col in changes.added_columns:

                    from .schema import TableAutoCreator
                    ddl = TableAutoCreator.generate_add_column_sql(table_name, col, dialect)
                    destination.alter_table(ddl)
                    executed_ddl.append(ddl)
                    print(f"  [+] Added: {col.name} {col.type}")
                    logger.info(f"  [+] Added: {col.name} {col.type}")
            elif config.mode == "strict":
                raise SchemaEvolutionError(
                    f"Schema changes detected in 'strict' mode. Halting pipeline.\n"
                    f"Added columns: {[c.name for c in changes.added_columns]}"
                )
            else:
                logger.warning(f"New columns detected but auto-evolution disabled: {[c.name for c in changes.added_columns]}")

        if changes.removed_columns:
            removed_names = [c.name for c in changes.removed_columns]

            # Always warn, never block
            logger.warning(f"[EVOLUTION] Source missing columns: {removed_names}. Inserting NULL values for removed columns.")
            
            # Record metadata for auditing
            if config.track_history:
                destination.columns_removed = removed_names  # dynamically add metadata field
            
            # No raise — pipeline continues


        if changes.type_changes:
            if config.on_type_change == "fail":
                raise SchemaEvolutionError(
                    f"Column type changed: {[(t.column, t.old_type, t.new_type) for t in changes.type_changes]}. "
                    f"Policy: fail. Pipeline halted."
                )
            elif config.on_type_change == "warn":
                for tc in changes.type_changes:
                    logger.warning(f"  [~] Type changed: {tc.column} {tc.old_type} → {tc.new_type}")

        if executed_ddl and config.track_history:
            from .schema_store import SchemaStore
            schema_store = SchemaStore()
            old_schema = schema_store.load_last_schema(resource_name)
            old_version = old_schema.get('version', 0) if old_schema else 0
            new_version = old_version + 1
            
            audit_file = schema_store.log_evolution_event(
                resource_name=resource_name,
                changes={
                    'added': [{'name': c.name, 'type': c.type, 'nullable': c.nullable} for c in changes.added_columns],
                    'removed': [{'name': c.name, 'type': c.type, 'nullable': c.nullable} for c in changes.removed_columns],
                    'type_changes': [
                        {'column': t.column, 'old_type': t.old_type, 'new_type': t.new_type}
                        for t in changes.type_changes
                    ],
                    # NEW: explicit list of removed column names for quick auditing
                    'columns_removed': [c.name for c in changes.removed_columns],
                },
                ddl_applied=executed_ddl,
                old_version=old_version,
                new_version=new_version
            )

            print(f"  Version: {old_version} → {new_version}")
            print(f"  Audit: {audit_file}")

        return executed_ddl
        
    @staticmethod
    def inject_nulls_for_removed_columns(records: List[Dict[str, Any]], removed_columns: List[str]) -> List[Dict[str, Any]]:
        """Ensure all removed columns exist as None values in outgoing records."""
        if not removed_columns:
            return records
        updated = []
        for row in records:
            for col in removed_columns:
                if col not in row:
                    row[col] = None
            updated.append(row)
        return updated


class SchemaEvolutionError(Exception):
    """Custom exception for schema evolution failures."""
    pass


# Need to import this late to avoid circular dependencies
try:
    from .schema import TableAutoCreator
except ImportError:
    # This might happen in some testing scenarios
    pass