# src/conduit_core/schema.py

import logging
import re
from typing import Dict, Any, List, Optional, Type
from datetime import datetime, UTC
from decimal import Decimal
from collections import Counter
from pydantic import BaseModel
import csv

logger = logging.getLogger(__name__)


class ColumnDefinition(BaseModel):
    """A Pydantic model representing a single column's schema."""
    name: str
    type: str
    nullable: bool


class SchemaInferrer:
    """
    Infers schema from data samples.
    Analyzes data to determine column types, nullable columns, etc.
    """
    
    @staticmethod
    def infer_schema(records: List[Dict[str, Any]], sample_size: int = 100) -> Dict[str, Any]:
        """
        Infer schema from a list of records.
        
        Args:
            records: List of dictionaries (data records)
            sample_size: Number of records to sample for inference
        
        Returns:
            Schema dictionary in the format: {"columns": [...]}
        """
        if not records:
            return {"columns": []}
        
        # Sample records if we have too many
        sample = records[:sample_size] if len(records) > sample_size else records
        
        # Get all column names, preserving order if possible (important for CSV header mapping)
        all_columns = list()
        seen_columns = set()
        for record in sample:
             for key in record.keys():
                  if key not in seen_columns:
                       all_columns.append(key)
                       seen_columns.add(key)
        
        column_definitions: List[Dict[str, Any]] = []
        
        for column in all_columns:
            # Collect all non-null values for this column
            values = [
                record.get(column) 
                for record in sample 
                if record.get(column) is not None and record.get(column) != ''
            ]
            
            if not values:
                # All values are null
                column_definitions.append({
                    'name': column,
                    'type': 'string',
                    'nullable': True
                })
                continue
            
            # Infer type from values
            inferred_type = SchemaInferrer._infer_column_type(values)
            
            column_definitions.append({
                'name': column,
                'type': inferred_type,
                'nullable': len(values) < len(sample)
            })
        
        logger.info(f"Inferred schema for {len(column_definitions)} columns from {len(sample)} records")
        return {"columns": column_definitions}
    
    @staticmethod
    def _infer_column_type(values: List[Any]) -> str:
        """
        Infer the type of a column from its values.
        
        Args:
            values: List of non-null values
        
        Returns:
            Type name as string
        """
        type_votes = Counter()
        
        for value in values:
            detected_type = SchemaInferrer._detect_value_type(value)
            type_votes[detected_type] += 1
        
        # Return the most common type
        # If there's a tie, prefer more specific types
        type_priority = ['datetime', 'date', 'decimal', 'float', 'integer', 'boolean', 'json', 'string']
        
        most_common = type_votes.most_common()
        if not most_common:
            return 'string'
        
        # If one type is clearly dominant (>80%), use it
        total = sum(type_votes.values())
        if total > 0: # Avoid division by zero for empty lists
            dominant_type = most_common[0][0]
            dominant_count = most_common[0][1]
            if dominant_count / total > 0.8:
                return dominant_type

        # Otherwise, pick based on priority
        present_types = type_votes.keys()
        for preferred_type in type_priority:
            if preferred_type in present_types:
                return preferred_type
        
        return 'string' # Fallback
    
    @staticmethod
    def _detect_value_type(value: Any) -> str:
        """Detect the type of a single value"""
        # Check Python type first
        if isinstance(value, bool):
            return 'boolean'
        elif isinstance(value, int):
            return 'integer'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, Decimal):
            return 'decimal'
        elif isinstance(value, datetime):
            # Differentiate date from datetime if time is midnight
            if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
                 return 'date'
            return 'datetime'
        elif isinstance(value, dict) or isinstance(value, list):
            return 'json'
        elif isinstance(value, str):
            # Try to parse string to more specific type
            return SchemaInferrer._parse_string_type(value)
        else:
            return 'string'
    
    @staticmethod
    def _parse_string_type(value: str) -> str:
        """Try to infer type from string value"""
        value = value.strip()
        if not value: # Treat empty string as potentially string
            return 'string'

        # Check for boolean
        if value.lower() in ('true', 'false', 'yes', 'no', 't', 'f', 'y', 'n', '1', '0'): # Added 1/0
             # Only return boolean if it's ONLY boolean values being seen
             # Needs context from _infer_column_type, for now, be specific
            if value.lower() in ('true', 'false', 'yes', 'no', 't', 'f', 'y', 'n'):
                 return 'boolean'

        # Check for integer
        if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
             try:
                  int(value)
                  return 'integer'
             except ValueError:
                  pass # Might be too large for standard int, could be float/string

        # Check for float/decimal
        if '.' in value or 'e' in value.lower():
             try:
                  float(value) # Use float for broad check
                  # Could refine later to check for decimal precision if needed
                  return 'float' 
             except ValueError:
                  pass
        
        # Check for date FIRST (YYYY-MM-DD) - more specific
        if len(value) == 10 and value.count('-') == 2:
            try:
                datetime.strptime(value, '%Y-%m-%d')
                return 'date'
            except ValueError:
                pass
        
        # Check for datetime (ISO format with time component or other common formats)
        # Try ISO format first
        if 'T' in value or ':' in value:
             try:
                  parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) # Handle Z timezone
                  if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0 and parsed.microsecond == 0 and len(value) <= 10:
                      return 'date' # Likely just YYYY-MM-DD parsed as midnight
                  return 'datetime'
             except (ValueError, TypeError):
                  pass
        
        # Add more common date/datetime formats if needed here, e.g.,
        # try: datetime.strptime(value, '%Y/%m/%d %H:%M:%S'); return 'datetime' ...

        return 'string'


class CsvDelimiterDetector:
    """Detects CSV delimiter from file content"""
    
    COMMON_DELIMITERS = [',', ';', '\t', '|', ':']
    
    @staticmethod
    def detect_delimiter(filepath: str, sample_lines: int = 5) -> str:
        """
        Detect CSV delimiter by analyzing the first few lines.
        
        Args:
            filepath: Path to CSV file
            sample_lines: Number of lines to sample
        
        Returns:
            Detected delimiter character
        """
        import csv
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                # Read sample lines
                sample = []
                for _ in range(sample_lines):
                    line = f.readline()
                    if not line:
                        break
                    sample.append(line)
        except FileNotFoundError:
             logger.error(f"File not found for delimiter detection: {filepath}")
             raise # Re-raise the error, test should handle it.
        
        if not sample:
            logger.warning("Empty file, defaulting to comma delimiter")
            return ','
        
        # Use csv.Sniffer
        try:
            sniffer = csv.Sniffer()
            # Join lines in case sniffer needs more context
            full_sample = ''.join(sample) 
            if not full_sample.strip(): # Handle files with only empty lines
                logger.warning("Sample contains only whitespace, defaulting to comma")
                return ','
            delimiter = sniffer.sniff(full_sample, delimiters=''.join(CsvDelimiterDetector.COMMON_DELIMITERS)).delimiter
            logger.info(f"Detected delimiter via sniffer: '{delimiter}'")
            return delimiter
        except csv.Error as e: # Catch specific sniffer error
            logger.warning(f"Sniffer failed: {e}, trying manual detection")
        
        # Manual detection: count occurrences of each delimiter in non-header lines
        # Check consistency across lines
        delimiter_counts = {delim: [] for delim in CsvDelimiterDetector.COMMON_DELIMITERS}
        header_counts = {delim: 0 for delim in CsvDelimiterDetector.COMMON_DELIMITERS}

        # Count header separately
        if sample:
            header = sample[0]
            for delim in CsvDelimiterDetector.COMMON_DELIMITERS:
                header_counts[delim] = header.count(delim)

        # Count data lines
        if len(sample) > 1:
            for line in sample[1:]: 
                if line.strip(): # Skip empty lines
                    line_counts = {delim: line.count(delim) for delim in CsvDelimiterDetector.COMMON_DELIMITERS}
                    for delim in CsvDelimiterDetector.COMMON_DELIMITERS:
                        delimiter_counts[delim].append(line_counts[delim])
        
        # Analyze counts - prefer delimiter with consistent non-zero counts across data lines
        best_delimiter = ',' # Default
        max_consistency = -1

        for delim, counts in delimiter_counts.items():
            if not counts: # No data lines analyzed
                 continue
            
            # Check if count is non-zero and the same in all data lines
            first_count = counts[0]
            if first_count > 0 and all(c == first_count for c in counts):
                 # Consider header count too - should ideally match
                 if header_counts[delim] == first_count:
                      consistency_score = len(counts) + 1 # +1 for header match
                 else:
                      consistency_score = len(counts) # Header mismatch slightly less likely
                 
                 if consistency_score > max_consistency:
                      max_consistency = consistency_score
                      best_delimiter = delim

        # If no perfectly consistent delimiter found, fallback to most frequent in header? Or overall?
        if max_consistency == -1:
            # Fallback: Most frequent in header (if header had any)
             if any(v > 0 for v in header_counts.values()):
                  best_delimiter = max(header_counts, key=header_counts.get)
             # If header also had none, stick with default comma
             if header_counts.get(best_delimiter, 0) == 0:
                 best_delimiter = ',' # Final fallback
                 logger.warning("Could not reliably detect delimiter, defaulting to comma")
             else:
                 logger.info(f"Detected delimiter based on header frequency: '{best_delimiter}'")
        else:
            logger.info(f"Detected delimiter based on consistency: '{best_delimiter}'")

        return best_delimiter


class TableAutoCreator:
    """Automatically creates destination tables based on inferred schema"""
    
    @staticmethod
    def generate_create_table_sql(
        table_name: str,
        schema: Dict[str, Any],
        dialect: str = "postgresql"
    ) -> str:
        """
        Generate CREATE TABLE SQL from inferred schema.
        
        Args:
            table_name: Name of table to create
            schema: Schema dictionary, expects {"columns": [...]}
            dialect: SQL dialect (postgresql, snowflake, bigquery, etc.)
        
        Returns:
            CREATE TABLE SQL statement
        """
        type_mapping = TableAutoCreator._get_type_mapping(dialect)
        
        column_defs = []
        for column_info_dict in schema.get('columns', []):
            try:
                # Ensure we handle potential extra fields gracefully if using ColumnDefinition directly
                col_data = {k: v for k, v in column_info_dict.items() if k in ColumnDefinition.model_fields}
                col = ColumnDefinition(**col_data)
                
                # Map to SQL type
                sql_type = type_mapping.get(col.type, 'VARCHAR(255)') # Default type if mapping missing
                
                # Build column definition
                null_clause = '' if col.nullable else ' NOT NULL'
                
                # Handle dialect-specific quoting
                if dialect == "bigquery":
                    # BigQuery doesn't quote standard identifiers unless needed
                    column_def = f'`{col.name}` {sql_type}{null_clause}'
                else:
                    # Use standard SQL double quotes
                    column_def = f'"{col.name}" {sql_type}{null_clause}'
                
                column_defs.append(column_def)
            except Exception as e:
                logger.error(f"Failed to parse column info: {column_info_dict}. Error: {e}")

        if not column_defs:
            raise ValueError("Schema contains no columns to create.")

        # Dialect-specific CREATE TABLE
        # Use standard quoting for table name as well, unless BigQuery
        quoted_table_name = f"`{table_name}`" if dialect == "bigquery" else f'"{table_name}"'
        sql = f'CREATE TABLE IF NOT EXISTS {quoted_table_name} (\n'

        sql += ',\n'.join(f'  {col}' for col in column_defs)
        sql += '\n);'
        
        return sql

    @staticmethod
    def generate_add_column_sql(
        table_name: str,
        column: ColumnDefinition,
        dialect: str = "postgresql"
    ) -> str:
        """
        Generate ALTER TABLE ... ADD COLUMN SQL for a single column.
        
        Args:
            table_name: Name of table to alter
            column: ColumnDefinition object for the new column
            dialect: SQL dialect
        
        Returns:
            ALTER TABLE SQL statement
        """
        type_mapping = TableAutoCreator._get_type_mapping(dialect)
        sql_type = type_mapping.get(column.type, 'VARCHAR(255)')
        
        # New columns added via ALTER should generally be nullable unless specified
        # Our config defaults to add_nullable, respect column def if provided
        # null_clause = '' if column.nullable else ' NOT NULL' # Keep original logic for now
        
        # Handle dialect-specific quoting
        quoted_table_name = f"`{table_name}`" if dialect == "bigquery" else f'"{table_name}"'
        quoted_column_name = f"`{column.name}`" if dialect == "bigquery" else f'"{column.name}"'

        # Simplified ALTER syntax for broader compatibility initially
        # Some dialects might need `ADD COLUMN`, others just `ADD`
        return f'ALTER TABLE {quoted_table_name} ADD COLUMN {quoted_column_name} {sql_type};'


    @staticmethod
    def _get_type_mapping(dialect: str) -> Dict[str, str]:
        """Get type mapping for SQL dialect"""
        if dialect == "postgresql":
            return {
                'integer': 'INTEGER',
                'float': 'DOUBLE PRECISION',
                'decimal': 'NUMERIC',
                'boolean': 'BOOLEAN',
                'date': 'DATE',
                'datetime': 'TIMESTAMP',
                'string': 'TEXT',
                'json': 'JSONB'
            }
        elif dialect == "snowflake":
            return {
                'integer': 'NUMBER(38,0)',
                'float': 'FLOAT',
                'decimal': 'NUMBER(38,9)', # Default precision
                'boolean': 'BOOLEAN',
                'date': 'DATE',
                'datetime': 'TIMESTAMP_NTZ', # Non-timezone aware is common
                'string': 'VARCHAR',
                'json': 'VARIANT'
            }
        elif dialect == "bigquery":
            return {
                'integer': 'INT64',
                'float': 'FLOAT64',
                'decimal': 'NUMERIC', # Standard precision NUMERIC
                'boolean': 'BOOL',
                'date': 'DATE',
                'datetime': 'DATETIME',
                'string': 'STRING',
                'json': 'JSON'
            }
        elif dialect == "mysql":
            return {
                'integer': 'INT',
                'float': 'DOUBLE',
                'decimal': 'DECIMAL(18,2)', # Example precision
                'boolean': 'BOOLEAN', # Or TINYINT(1)
                'date': 'DATE',
                'datetime': 'DATETIME',
                'string': 'TEXT',
                'json': 'JSON'
            }
        elif dialect == "sqlite":
            # *** FIX was applied here previously ***
            return {
                'integer': 'INTEGER',
                'float': 'REAL',
                'decimal': 'REAL', # SQLite uses REAL for decimals too
                'boolean': 'INTEGER', # Booleans stored as 0 or 1
                'date': 'TEXT', # Store dates as ISO8601 strings
                'datetime': 'TEXT', # Store datetimes as ISO8601 strings
                'string': 'TEXT',
                'json': 'TEXT' # Store JSON as text
            }
        elif dialect in ("mssql", "azuresql"):
             return {
                 'integer': 'INT',
                 'float': 'FLOAT',
                 'decimal': 'DECIMAL(18,2)',
                 'boolean': 'BIT',
                 'date': 'DATE',
                 'datetime': 'DATETIME2',
                 'string': 'NVARCHAR(MAX)',
                 'json': 'NVARCHAR(MAX)' # Often stored as text
             }
        else:
            # Default generic mapping (ANSI SQL like)
            return {
                'integer': 'INTEGER',
                'float': 'FLOAT',
                'decimal': 'DECIMAL',
                'boolean': 'BOOLEAN',
                'date': 'DATE',
                'datetime': 'TIMESTAMP',
                'string': 'VARCHAR(255)',
                'json': 'VARCHAR(8000)' # Example length
            }