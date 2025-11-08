import os
import pytest
import pyarrow as pa
from conduit_core.connectors.snowflake import SnowflakeDestination
from conduit_core.config import Destination

TABLE_NAME = "test_auto_schema_evolution"

@pytest.fixture(scope="module")
def sf_conn():
    config = Destination(
        name="test_sf_auto_schema",
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
    connector = SnowflakeDestination(config)
    fq_table = f'"{config.db_schema}"."{config.table}"'
    connector.execute_ddl(f"DROP TABLE IF EXISTS {fq_table}")
    connector.execute_ddl(f"CREATE TABLE {fq_table} (id INTEGER, name STRING)")
    yield connector
    connector.execute_ddl(f"DROP TABLE IF EXISTS {fq_table}")

def test_auto_add_column(sf_conn):
    """Ensure Conduit auto-adds new columns during load."""
    batch = pa.RecordBatch.from_pydict({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "email": ["alice@example.com", "bob@example.com"]
    })
    records = [dict(zip(batch.schema.names, row)) for row in zip(*batch.columns)]
    sf_conn.write(records)
    sf_conn.finalize()

    # Verify new column exists and is populated
    conn = sf_conn._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT "EMAIL" FROM "{sf_conn.db_schema}"."{TABLE_NAME}"')
            rows = [r[0] for r in cur.fetchall()]
            assert "alice@example.com" in rows
    finally:
        conn.close()
