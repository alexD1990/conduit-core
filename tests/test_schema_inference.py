# tests/test_schema_inference.py

import pytest
from pathlib import Path
from conduit_core.schema import SchemaInferrer, CsvDelimiterDetector, TableAutoCreator


def test_infer_schema_from_data():
    """Test schema inference from sample data"""
    records = [
        {"id": 1, "name": "Alice", "age": 30, "active": True, "salary": 50000.50},
        {"id": 2, "name": "Bob", "age": 25, "active": False, "salary": 45000.00},
        {"id": 3, "name": "Charlie", "age": 35, "active": True, "salary": 60000.75},
    ]
    
    schema = SchemaInferrer.infer_schema(records)
    
    assert "id" in schema
    assert schema["id"]["type"] == "integer"
    assert schema["name"]["type"] == "string"
    assert schema["age"]["type"] == "integer"
    assert schema["active"]["type"] == "boolean"
    assert schema["salary"]["type"] == "float"


def test_infer_schema_with_nulls():
    """Test schema inference with NULL values"""
    records = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": None},
        {"id": 3, "name": "Charlie", "email": ""},
    ]
    
    schema = SchemaInferrer.infer_schema(records)
    
    assert schema["email"]["nullable"] == True


def test_detect_comma_delimiter(tmp_path):
    """Test detecting comma delimiter"""
    csv_file = tmp_path / "comma.csv"
    csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")
    
    delimiter = CsvDelimiterDetector.detect_delimiter(str(csv_file))
    
    assert delimiter == ','


def test_detect_semicolon_delimiter(tmp_path):
    """Test detecting semicolon delimiter"""
    csv_file = tmp_path / "semicolon.csv"
    csv_file.write_text("id;name;age\n1;Alice;30\n2;Bob;25\n")
    
    delimiter = CsvDelimiterDetector.detect_delimiter(str(csv_file))
    
    assert delimiter == ';'


def test_detect_tab_delimiter(tmp_path):
    """Test detecting tab delimiter"""
    csv_file = tmp_path / "tab.csv"
    csv_file.write_text("id\tname\tage\n1\tAlice\t30\n2\tBob\t25\n")
    
    delimiter = CsvDelimiterDetector.detect_delimiter(str(csv_file))
    
    assert delimiter == '\t'


def test_generate_create_table_sql_postgres():
    """Test generating PostgreSQL CREATE TABLE statement"""
    schema = {
        "id": {"type": "integer", "nullable": False},
        "name": {"type": "string", "nullable": False},
        "email": {"type": "string", "nullable": True},
        "created_at": {"type": "datetime", "nullable": False},
    }
    
    sql = TableAutoCreator.generate_create_table_sql("users", schema, "postgresql")
    
    assert "CREATE TABLE" in sql
    assert "users" in sql
    assert "INTEGER" in sql
    assert "TEXT" in sql
    assert "TIMESTAMP" in sql
    assert "NOT NULL" in sql


def test_generate_create_table_sql_sqlite():
    """Test generating SQLite CREATE TABLE statement"""
    schema = {
        "id": {"type": "integer", "nullable": False},
        "active": {"type": "boolean", "nullable": False},
    }
    
    sql = TableAutoCreator.generate_create_table_sql("flags", schema, "sqlite")
    
    assert "CREATE TABLE" in sql
    assert "INTEGER" in sql


def test_infer_types_from_strings():
    """Test type inference from string values"""
    records = [
        {"count": "123", "price": "45.67", "active": "true", "created": "2025-10-11"},
    ]
    
    schema = SchemaInferrer.infer_schema(records)
    
    assert schema["count"]["type"] == "integer"
    assert schema["price"]["type"] == "float"
    assert schema["active"]["type"] == "boolean"
    assert schema["created"]["type"] == "date"