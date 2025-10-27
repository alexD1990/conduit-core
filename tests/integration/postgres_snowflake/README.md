# Postgres → Snowflake Integration Tests

## Overview
Comprehensive end-to-end tests for Postgres to Snowflake data pipeline.

## Test Coverage

### TestBasicIngestion
- ✅ **test_simple_transfer_1000_rows**: Basic data transfer with 1K rows
- ✅ **test_parallel_extraction_10k_rows**: Parallel extraction with 10K rows (4 workers, 2.5K batch size)

### TestDataTypes
- ✅ **test_all_postgres_data_types**: All major Postgres data types (INTEGER, BIGINT, NUMERIC, FLOAT, VARCHAR, TEXT, BOOLEAN, DATE, TIMESTAMP, NULL)

### TestModes
- ✅ **test_full_refresh_mode**: Full refresh mode truncates and reloads correctly

## Requirements
- Snowflake credentials in `.env`:
```
  SNOWFLAKE_ACCOUNT=your_account
  SNOWFLAKE_USER=your_user
  SNOWFLAKE_PASSWORD=your_password
  SNOWFLAKE_WAREHOUSE=COMPUTE_WH
  SNOWFLAKE_DATABASE=DEV_DB
  SNOWFLAKE_SCHEMA=PUBLIC
```
- Running Postgres instance on localhost:5432

## Running Tests
```bash
# All tests
pytest tests/integration/postgres_snowflake/test_pg_to_snowflake.py -v

# Specific test
pytest tests/integration/postgres_snowflake/test_pg_to_snowflake.py::TestBasicIngestion::test_parallel_extraction_10k_rows -v
```

## Known Issues Fixed
1. **Snowflake case sensitivity**: Table names created without quotes become lowercase, requiring quoted identifiers in queries
2. **TRUNCATE not working**: Fixed by adding quotes to table identifiers in TRUNCATE statements
3. **Data types**: Snowflake returns all CSV-loaded data as strings; tests convert to appropriate types

## Performance
- 1K rows: ~2.5s
- 10K rows (parallel): ~2.2s
- Data types test: ~2.1s
