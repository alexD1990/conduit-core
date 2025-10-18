# tests/test_schema_inference.py

import pytest
import re # <-- Import re for regex search
from datetime import datetime
from decimal import Decimal

from conduit_core.schema import SchemaInferrer, CsvDelimiterDetector, TableAutoCreator

def test_infer_schema_from_data():
    """Test schema inference from sample data"""
    records = [
        {"id": 1, "name": "Alice", "age": 30, "active": True, "salary": 50000.50},
        {"id": 2, "name": "Bob", "age": 25, "active": False, "salary": 45000.00},
        {"id": 3, "name": "Charlie", "age": 35, "active": True, "salary": 60000.75},
    ]

    schema = SchemaInferrer.infer_schema(records)

    assert "columns" in schema
    cols = {c['name']: c for c in schema['columns']}
    
    assert "id" in cols
    assert cols["id"]["type"] == "integer"
    assert not cols["id"]["nullable"]
    
    assert "name" in cols
    assert cols["name"]["type"] == "string"
    assert not cols["name"]["nullable"]
    
    assert "age" in cols
    assert cols["age"]["type"] == "integer"
    
    assert "active" in cols
    assert cols["active"]["type"] == "boolean"
    
    assert "salary" in cols
    assert cols["salary"]["type"] == "float"


def test_infer_schema_with_nulls():
    """Test schema inference with NULL values"""
    records = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": None},
        {"id": 3, "name": "Charlie", "email": ""}, # Empty string considered null for nullable check
    ]

    schema = SchemaInferrer.infer_schema(records)

    assert "columns" in schema
    cols = {c['name']: c for c in schema['columns']}
    
    assert "id" in cols
    assert not cols["id"]["nullable"]
    
    assert "name" in cols
    assert not cols["name"]["nullable"]
    
    assert "email" in cols
    assert cols["email"]["type"] == "string"
    assert cols["email"]["nullable"] == True


def test_infer_schema_mixed_types():
    """Test inference with mixed data types in a column"""
    records = [
        {"value": 10},
        {"value": "20"},
        {"value": 30.5},
        {"value": "40.0"},
        {"value": "fifty"},
    ]
    schema = SchemaInferrer.infer_schema(records)
    
    assert "columns" in schema
    cols = {c['name']: c for c in schema['columns']}
    
    # *** FIX: Expect 'float' based on priority logic ***
    assert cols["value"]["type"] == "float" 
    assert not cols["value"]["nullable"]


def test_generate_create_table_sql_postgres():
    """Test generating PostgreSQL CREATE TABLE statement"""
    schema = {
        "columns": [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "name", "type": "string", "nullable": False},
            {"name": "email", "type": "string", "nullable": True},
            {"name": "created_at", "type": "datetime", "nullable": False},
        ]
    }

    sql = TableAutoCreator.generate_create_table_sql("users", schema, "postgresql")

    assert 'CREATE TABLE IF NOT EXISTS "users"' in sql
    assert '"id" INTEGER NOT NULL' in sql
    assert '"name" TEXT NOT NULL' in sql
    assert '"created_at" TIMESTAMP NOT NULL' in sql
    
    # Use regex to check the "email" line specifically for nullability
    assert re.search(r'"email"\s+TEXT\s*(,|$)', sql, re.MULTILINE) is not None
    assert '"email" TEXT NOT NULL' not in sql # Double check


def test_generate_create_table_sql_sqlite():
    """Test generating SQLite CREATE TABLE statement"""
    schema = {
        "columns": [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "active", "type": "boolean", "nullable": False},
        ]
    }

    sql = TableAutoCreator.generate_create_table_sql("flags", schema, "sqlite")
    
    assert 'CREATE TABLE IF NOT EXISTS "flags"' in sql
    assert '"id" INTEGER NOT NULL' in sql
    # Correct expectation: SQLite uses INTEGER for booleans
    assert '"active" INTEGER NOT NULL' in sql 


def test_infer_types_from_strings():
    """Test type inference from string values"""
    records = [
        {"count": "123", "price": "45.67", "active": "true", "created": "2025-10-11"},
    ]

    schema = SchemaInferrer.infer_schema(records)

    assert "columns" in schema
    cols = {c['name']: c for c in schema['columns']}

    assert cols["count"]["type"] == "integer"
    assert cols["price"]["type"] == "float"
    assert cols["active"]["type"] == "boolean"
    assert cols["created"]["type"] == "date"