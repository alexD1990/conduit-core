# src/conduit_core/schema.py

import logging
from typing import Dict, Any, List, Optional, Type
from datetime import datetime
from decimal import Decimal
from collections import Counter

logger = logging.getLogger(__name__)


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
            Schema dictionary with column types and metadata
        """
        if not records:
            return {}
        
        # Sample records if we have too many
        sample = records[:sample_size] if len(records) > sample_size else records
        
        # Get all column names
        all_columns = set()
        for record in sample:
            all_columns.update(record.keys())
        
        schema = {}
        
        for column in all_columns:
            # Collect all non-null values for this column
            values = [
                record.get(column) 
                for record in sample 
                if record.get(column) is not None and record.get(column) != ''
            ]
            
            if not values:
                # All values are null
                schema[column] = {
                    'type': 'string',
                    'nullable': True,
                    'inferred_from_samples': 0
                }
                continue
            
            # Infer type from values
            inferred_type = SchemaInferrer._infer_column_type(values)
            
            schema[column] = {
                'type': inferred_type,
                'nullable': len(values) < len(sample),
                'inferred_from_samples': len(values)
            }
        
        logger.info(f"Inferred schema for {len(schema)} columns from {len(sample)} records")
        return schema
    
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
        type_priority = ['datetime', 'date', 'decimal', 'float', 'integer', 'boolean', 'string']
        
        most_common = type_votes.most_common()
        if not most_common:
            return 'string'
        
        # If one type is clearly dominant (>80%), use it
        total = sum(type_votes.values())
        for detected_type, count in most_common:
            if count / total > 0.8:
                return detected_type
        
        # Otherwise, pick based on priority
        for preferred_type in type_priority:
            if preferred_type in type_votes:
                return preferred_type
        
        return 'string'
    
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
            return 'datetime'
        elif isinstance(value, str):
            # Try to parse string to more specific type
            return SchemaInferrer._parse_string_type(value)
        else:
            return 'string'
    
    @staticmethod
    def _parse_string_type(value: str) -> str:
        """Try to infer type from string value"""
        value = value.strip()
        
        # Check for boolean
        if value.lower() in ('true', 'false', 'yes', 'no', 't', 'f', 'y', 'n'):
            return 'boolean'
        
        # Check for integer
        try:
            int(value)
            return 'integer'
        except ValueError:
            pass
        
        # Check for float
        try:
            float(value)
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
        
        # Check for datetime (ISO format with time component)
        try:
            parsed = datetime.fromisoformat(value)
            # If it has time component (not midnight), it's datetime
            if parsed.hour != 0 or parsed.minute != 0 or parsed.second != 0:
                return 'datetime'
            # If time is midnight and format is just date, it's a date
            if len(value) == 10:
                return 'date'
            return 'datetime'
        except (ValueError, TypeError):
            pass
        
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
        
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            # Read sample lines
            sample = []
            for _ in range(sample_lines):
                line = f.readline()
                if not line:
                    break
                sample.append(line)
        
        if not sample:
            logger.warning("Empty file, defaulting to comma delimiter")
            return ','
        
        # Use csv.Sniffer
        try:
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(''.join(sample)).delimiter
            logger.info(f"Detected delimiter: '{delimiter}'")
            return delimiter
        except Exception as e:
            logger.warning(f"Sniffer failed: {e}, trying manual detection")
        
        # Manual detection: count occurrences of each delimiter
        delimiter_counts = {delim: 0 for delim in CsvDelimiterDetector.COMMON_DELIMITERS}
        
        for line in sample[1:]:  # Skip header
            for delim in CsvDelimiterDetector.COMMON_DELIMITERS:
                delimiter_counts[delim] += line.count(delim)
        
        # Pick the delimiter that appears most consistently
        best_delimiter = max(delimiter_counts, key=delimiter_counts.get)
        
        if delimiter_counts[best_delimiter] == 0:
            logger.warning("No delimiter detected, defaulting to comma")
            return ','
        
        logger.info(f"Detected delimiter: '{best_delimiter}'")
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
            schema: Schema dictionary from SchemaInferrer
            dialect: SQL dialect (postgresql, mysql, sqlite, mssql)
        
        Returns:
            CREATE TABLE SQL statement
        """
        type_mapping = TableAutoCreator._get_type_mapping(dialect)
        
        column_defs = []
        for column_name, column_info in schema.items():
            col_type = column_info['type']
            nullable = column_info['nullable']
            
            # Map to SQL type
            sql_type = type_mapping.get(col_type, 'VARCHAR(255)')
            
            # Build column definition
            null_clause = '' if nullable else ' NOT NULL'
            column_def = f'"{column_name}" {sql_type}{null_clause}'
            
            column_defs.append(column_def)
        
        sql = f'CREATE TABLE IF NOT EXISTS {table_name} (\n'
        sql += ',\n'.join(f'  {col}' for col in column_defs)
        sql += '\n);'
        
        return sql
    
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
                'string': 'TEXT'
            }
        elif dialect == "mysql":
            return {
                'integer': 'INT',
                'float': 'DOUBLE',
                'decimal': 'DECIMAL(18,2)',
                'boolean': 'BOOLEAN',
                'date': 'DATE',
                'datetime': 'DATETIME',
                'string': 'TEXT'
            }
        elif dialect == "sqlite":
            return {
                'integer': 'INTEGER',
                'float': 'REAL',
                'decimal': 'REAL',
                'boolean': 'INTEGER',
                'date': 'TEXT',
                'datetime': 'TEXT',
                'string': 'TEXT'
            }
        elif dialect in ("mssql", "azuresql"):
            return {
                'integer': 'INT',
                'float': 'FLOAT',
                'decimal': 'DECIMAL(18,2)',
                'boolean': 'BIT',
                'date': 'DATE',
                'datetime': 'DATETIME2',
                'string': 'NVARCHAR(MAX)'
            }
        else:
            # Default generic mapping
            return {
                'integer': 'INTEGER',
                'float': 'FLOAT',
                'decimal': 'DECIMAL',
                'boolean': 'BOOLEAN',
                'date': 'DATE',
                'datetime': 'TIMESTAMP',
                'string': 'VARCHAR(255)'
            }