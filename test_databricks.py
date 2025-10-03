# test_databricks.py
import os
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

# Tvinger den til å lete etter .env i samme mappe som skriptet
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")

print("\n--- 🕵️ FEILSØKER KOBBING TIL DATABRICKS 🕵️ ---")
print(f"Lest HOST fra .env: {host}")
if token:
    print(f"Lest TOKEN fra .env: {token[:5]}...{token[-4:]}")
else:
    print("Lest TOKEN fra .env: None")
print("--------------------------------------------------\n")

try:
    if not all([host, token]):
        raise ValueError("HOST eller TOKEN mangler. Sjekk .env-filen.")

    ws = WorkspaceClient(host=host, token=token)

    # Community Edition bruker 'hive_metastore' som standard catalog
    schemas = ws.schemas.list(catalog_name="hive_metastore")

    print("\n✅ Vellykket tilkobling! Fant følgende schemas i 'hive_metastore':")
    for s in schemas:
        print(f"- {s.name}")

except Exception as e:
    print(f"\n❌ Tilkoblingen feilet.")
    print(e)