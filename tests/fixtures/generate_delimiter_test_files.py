# tests/fixtures/generate_delimiter_test_files.py

import csv
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "data"
FIXTURES_DIR.mkdir(exist_ok=True)

def generate_delimiter_files():
    """Generate CSV files with different delimiters"""
    
    data = [
        {"id": "1", "name": "Alice", "age": "30"},
        {"id": "2", "name": "Bob", "age": "25"},
        {"id": "3", "name": "Charlie", "age": "35"},
    ]
    
    # Comma-delimited
    with open(FIXTURES_DIR / "comma_delim.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age"], delimiter=",")
        writer.writeheader()
        writer.writerows(data)
    print("✅ Created comma_delim.csv")
    
    # Semicolon-delimited
    with open(FIXTURES_DIR / "semicolon_delim.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age"], delimiter=";")
        writer.writeheader()
        writer.writerows(data)
    print("✅ Created semicolon_delim.csv")
    
    # Tab-delimited
    with open(FIXTURES_DIR / "tab_delim.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age"], delimiter="\t")
        writer.writeheader()
        writer.writerows(data)
    print("✅ Created tab_delim.csv")
    
    # Pipe-delimited
    with open(FIXTURES_DIR / "pipe_delim.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age"], delimiter="|")
        writer.writeheader()
        writer.writerows(data)
    print("✅ Created pipe_delim.csv")


if __name__ == "__main__":
    print("🔧 Generating delimiter test files...")
    generate_delimiter_files()
    print(f"\n📁 All delimiter test files saved to: {FIXTURES_DIR}")