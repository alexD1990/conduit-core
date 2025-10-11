# tests/fixtures/generate_test_files.py
import csv
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

FIXTURES_DIR = Path(__file__).parent / "data"
FIXTURES_DIR.mkdir(exist_ok=True)

def generate_csv_files():
    """Generate CSV test files with various edge cases"""
    
    # 1. Normal CSV
    with open(FIXTURES_DIR / "normal.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "email", "age"])
        writer.writeheader()
        writer.writerows([
            {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35},
        ])
    
    # 2. CSV with NULLs
    with open(FIXTURES_DIR / "with_nulls.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "email", "age"])
        writer.writeheader()
        writer.writerows([
            {"id": 1, "name": "Alice", "email": "", "age": 30},
            {"id": 2, "name": "", "email": "bob@example.com", "age": ""},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35},
        ])
    
    # 3. CSV with special characters
    with open(FIXTURES_DIR / "special_chars.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "description"])
        writer.writeheader()
        writer.writerows([
            {"id": 1, "name": "José", "description": "Café owner, loves 'coffee'"},
            {"id": 2, "name": "François", "description": 'Said: "Bonjour!"'},
            {"id": 3, "name": "李明", "description": "Developer @ Beijing"},
        ])
    
    # 4. Empty CSV
    with open(FIXTURES_DIR / "empty.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        writer.writeheader()
    
    # 5. Large CSV (10k rows)
    with open(FIXTURES_DIR / "large.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "timestamp", "value"])
        writer.writeheader()
        base_time = datetime(2025, 1, 1)
        for i in range(10000):
            writer.writerow({
                "id": i,
                "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
                "value": i * 1.5
            })

def generate_json_files():
    """Generate JSON test files"""
    
    # 1. Normal JSON (array of objects)
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25},
    ]
    with open(FIXTURES_DIR / "normal.json", "w") as f:
        json.dump(data, f, indent=2)
    
    # 2. JSONL (newline-delimited JSON)
    with open(FIXTURES_DIR / "normal.jsonl", "w") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    
    # 3. Nested JSON
    nested_data = [
        {
            "id": 1,
            "user": {"name": "Alice", "email": "alice@example.com"},
            "metadata": {"created_at": "2025-01-01", "tags": ["admin", "active"]}
        }
    ]
    with open(FIXTURES_DIR / "nested.json", "w") as f:
        json.dump(nested_data, f, indent=2)

def generate_parquet_files():
    """Generate Parquet test files using pandas"""
    
    # 1. Normal Parquet
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "age": [30, 25, 35],
    })
    df.to_parquet(FIXTURES_DIR / "normal.parquet", index=False)
    
    # 2. Parquet with various types
    df_types = pd.DataFrame({
        "int_col": [1, 2, 3],
        "float_col": [1.5, 2.5, 3.5],
        "string_col": ["a", "b", "c"],
        "bool_col": [True, False, True],
        "datetime_col": pd.date_range("2025-01-01", periods=3),
    })
    df_types.to_parquet(FIXTURES_DIR / "various_types.parquet", index=False)

if __name__ == "__main__":
    print("🔧 Generating test fixtures...")
    generate_csv_files()
    print("✅ CSV files generated")
    generate_json_files()
    print("✅ JSON files generated")
    generate_parquet_files()
    print("✅ Parquet files generated")
    print(f"\n📁 All fixtures saved to: {FIXTURES_DIR}")