# tests/fixtures/generate_edge_case_files.py

import csv
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from decimal import Decimal
import math

FIXTURES_DIR = Path(__file__).parent / "data"
FIXTURES_DIR.mkdir(exist_ok=True)

def generate_edge_case_csv():
    """Generate CSV with challenging data types"""
    
    edge_cases = [
        {
            "id": 1,
            "name": "José García",
            "email": "josé@example.com",
            "price": "99.99",
            "quantity": "",  # Empty string
            "active": "true",
            "rating": "4.5",
            "notes": "Special chars: é, ñ, ü, ç",
        },
        {
            "id": 2,
            "name": "李明",
            "email": "liming@example.com",
            "price": "NULL",  # NULL as string
            "quantity": "0",
            "active": "false",
            "rating": "NaN",  # NaN as string
            "notes": 'Contains "quotes" and commas, too',
        },
        {
            "id": 3,
            "name": "François Müller",
            "email": "",  # Empty email
            "price": "149.99",
            "quantity": "N/A",  # Another NULL representation
            "active": "yes",
            "rating": "",
            "notes": "Line\nbreak\ntest",
        },
    ]
    
    with open(FIXTURES_DIR / "edge_cases.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=edge_cases[0].keys())
        writer.writeheader()
        writer.writerows(edge_cases)
    
    print(f"[OK] Created edge_cases.csv")


def generate_types_csv():
    """Generate CSV with various data types"""
    
    with open(FIXTURES_DIR / "data_types.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "int_col", "float_col", "bool_col", "date_col", "datetime_col", 
            "decimal_col", "null_col", "empty_col"
        ])
        writer.writeheader()
        writer.writerows([
            {
                "int_col": "42",
                "float_col": "3.14159",
                "bool_col": "true",
                "date_col": "2025-10-11",
                "datetime_col": "2025-10-11T15:30:45",
                "decimal_col": "999.99",
                "null_col": "NULL",
                "empty_col": "",
            },
            {
                "int_col": "-100",
                "float_col": "2.71828",
                "bool_col": "false",
                "date_col": "2024-01-01",
                "datetime_col": "2024-01-01T00:00:00",
                "decimal_col": "0.01",
                "null_col": "None",
                "empty_col": "",
            },
            {
                "int_col": "0",
                "float_col": "NaN",
                "bool_col": "yes",
                "date_col": "2023-12-31",
                "datetime_col": "2023-12-31T23:59:59",
                "decimal_col": "1000000.50",
                "null_col": "N/A",
                "empty_col": "",
            },
        ])
    
    print(f"[OK] Created data_types.csv")


def generate_encoding_test_files():
    """Generate files with different encodings"""
    
    # UTF-8 with BOM
    with open(FIXTURES_DIR / "utf8_bom.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "text"])
        writer.writeheader()
        writer.writerows([
            {"id": 1, "name": "Test", "text": "UTF-8 with BOM"},
        ])
    
    # Latin-1
    with open(FIXTURES_DIR / "latin1.csv", "w", newline="", encoding="latin-1") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "text"])
        writer.writeheader()
        writer.writerows([
            {"id": 1, "name": "Café", "text": "Latin-1 encoding"},
        ])
    
    print(f"[OK] Created encoding test files")


def generate_edge_case_json():
    """Generate JSON with edge cases"""
    
    edge_cases = [
        {
            "id": 1,
            "name": "Test User",
            "metadata": {
                "nested": True,
                "value": 42
            },
            "tags": ["tag1", "tag2"],
            "price": 99.99,
            "quantity": None,
            "active": True,
        },
        {
            "id": 2,
            "name": "José García",
            "metadata": None,
            "tags": [],
            "price": None,
            "quantity": 0,
            "active": False,
        },
    ]
    
    with open(FIXTURES_DIR / "edge_cases.json", "w", encoding="utf-8") as f:
        json.dump(edge_cases, f, indent=2, ensure_ascii=False)
    
    print(f"[OK] Created edge_cases.json")


def generate_edge_case_parquet():
    """Generate Parquet with various types"""
    
    df = pd.DataFrame({
        "int_col": [1, 2, 3, None],
        "float_col": [1.5, 2.5, float('nan'), 3.5],
        "string_col": ["a", "b", None, "José"],
        "bool_col": [True, False, None, True],
        "datetime_col": pd.to_datetime([
            "2025-01-01",
            "2025-06-15",
            None,
            "2025-12-31"
        ]),
    })
    
    df.to_parquet(FIXTURES_DIR / "edge_cases.parquet", index=False)
    
    print(f"[OK] Created edge_cases.parquet")


if __name__ == "__main__":
    print("🔧 Generating edge case test fixtures...")
    generate_edge_case_csv()
    generate_types_csv()
    generate_encoding_test_files()
    generate_edge_case_json()
    generate_edge_case_parquet()
    print(f"\n📁 All edge case fixtures saved to: {FIXTURES_DIR}")