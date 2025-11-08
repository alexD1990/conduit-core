import pytest
import pyarrow as pa
from conduit_core.connectors.snowflake import SnowflakeDestination
from conduit_core.config import Destination
import os

TABLE_NAME = "test_auto_schema_removed"

@pytest.fixture(scope="module")
def sf_conn():
    """Snowflake connector for schema removal test."""
    config = Destination(
        name="snowflake_removed",
        type="snowflake",
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        db_schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        table=TABLE_NAME,
        mode="append",
    )

    conn = SnowflakeDestination(config)
    # Drop old test table if exists
    sf = conn._get_connection()
    cur = sf.cursor()
    cur.execute(f'DROP TABLE IF EXISTS "{config.db_schema}"."{TABLE_NAME}"')
    sf.close()
    yield conn

def test_removed_column_autonull(sf_conn):
    """Ensure Conduit handles missing columns gracefully (NULL fallback)."""
    # Step 1: Initial table creation
    batch_v1 = pa.RecordBatch.from_pydict({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "age": [30, 25]
    })
    records_v1 = [dict(zip(batch_v1.schema.names, row)) for row in zip(*batch_v1.columns)]
    sf_conn.write(records_v1)
    sf_conn.finalize()

    # Step 2: Simulate column removed (age missing)
    batch_v2 = pa.RecordBatch.from_pydict({
        "id": [3],
        "name": ["Charlie"]
    })
    records_v2 = [dict(zip(batch_v2.schema.names, row)) for row in zip(*batch_v2.columns)]
    sf_conn.write(records_v2)
    sf_conn.finalize()

    # Step 3: Verify that 'age' exists but is NULL for new record
    conn = sf_conn._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT "ID", "NAME", "AGE" FROM "{sf_conn.db_schema}"."{TABLE_NAME}" ORDER BY "ID"')
            rows = cur.fetchall()
            assert len(rows) == 3
            assert rows[-1][2] is None  # last record should have NULL in 'AGE'
    finally:
        conn.close()
