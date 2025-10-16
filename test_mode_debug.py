from conduit_core.config import load_config

config = load_config('ingest_full_refresh_test.yml')

for resource in config.resources:
    print(f"Resource: {resource.name}")
    print(f"  mode: {resource.mode}")
    print(f"  incremental_column: {resource.incremental_column}")
