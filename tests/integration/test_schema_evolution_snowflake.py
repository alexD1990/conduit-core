import os
import pytest
import pyarrow as pa
from conduit_core.connectors.snowflake import SnowflakeDestination
from conduit_core.config import Destination  # correct config class


TABLE_NAME = "test_schema_evolution"


@pytest.fixture(scope="module")
def snowflake_connector():
    """Create Snowflake connector and ensure test table exists."""
    config = Destination(
        name="test_snowflake_destination",
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

    # Drop and recreate the table with quoted, lowercase identifiers
    connector.execute_ddl(f"DROP TABLE IF EXISTS {fq_table}")
    connector.execute_ddl(f"""
        CREATE TABLE {fq_table} (
            "id" INTEGER,
            "name" STRING,
            "age" INTEGER
        )
    """)

    yield connector

    connector.execute_ddl(f"DROP TABLE IF EXISTS {fq_table}")


# --------------------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------------------

def _fetch_results(connector, sql: str):
    """Run a SQL query via Snowflake connection and return rows as dicts."""
    conn = connector._get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0].lower() for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_all(connector, schema, table):
    fq_table = f'"{schema}"."{table}"'
    return _fetch_results(connector, f'SELECT * FROM {fq_table} ORDER BY "id"')


def _get_columns(connector):
    """Return list of column names from Snowflake via DESCRIBE TABLE."""
    conn = connector._get_connection()
    try:
        with conn.cursor() as cur:
            fq_table = f'"{connector.database}"."{connector.db_schema}"."{connector.table}"'
            cur.execute(f"DESCRIBE TABLE {fq_table}")
            return [row[0].lower() for row in cur.fetchall()]
    finally:
        conn.close()


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------

def test_initial_load(snowflake_connector):
    """Scenario 1: create table and insert initial data"""
    batch = pa.RecordBatch.from_pydict(
        {"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]}
    )

    records = [dict(zip(batch.schema.names, row)) for row in zip(*batch.columns)]
    snowflake_connector.write(records)
    snowflake_connector.finalize()

    rows = _fetch_all(snowflake_connector, snowflake_connector.db_schema, TABLE_NAME)
    assert len(rows) == 2
    assert rows[0]["name"].lower() == "alice"
    assert rows[1]["name"].lower() == "bob"

    cols = _get_columns(snowflake_connector)
    for expected_col in ["id", "name", "age"]:
        assert expected_col in cols


def test_add_column(snowflake_connector):
    """Scenario 2: add new column (email)"""
    batch = pa.RecordBatch.from_pydict(
        {
            "id": [3],
            "name": ["Charlie"],
            "age": [35],
            "email": ["charlie@example.com"],
        }
    )

    # Add missing column with quoted identifier (preserve lowercase)
    snowflake_connector.execute_ddl(
        f'ALTER TABLE "{snowflake_connector.database}"."{snowflake_connector.db_schema}"."{TABLE_NAME}" '
        f'ADD COLUMN IF NOT EXISTS "email" STRING'
    )

    records = [dict(zip(batch.schema.names, row)) for row in zip(*batch.columns)]
    snowflake_connector.write(records)
    snowflake_connector.finalize()

    fq_table = f'"{snowflake_connector.db_schema}"."{TABLE_NAME}"'
    rows = _fetch_results(snowflake_connector, f'SELECT * FROM {fq_table} WHERE "id" = 3')

    assert len(rows) == 1
    row = rows[0]
    assert row["email"].lower() == "charlie@example.com"

    cols = _get_columns(snowflake_connector)
    assert "email" in cols

def test_remove_column(snowflake_connector):
    """Scenario 3: load missing column (age) → expect NULL fallback"""

    # Ensure "email" column still exists
    snowflake_connector.execute_ddl(
        f'ALTER TABLE "{snowflake_connector.database}"."{snowflake_connector.db_schema}"."{TABLE_NAME}" '
        f'ADD COLUMN IF NOT EXISTS "email" STRING'
    )

    # ✅ Use blank string instead of None so Snowflake treats it as NULL
    batch = pa.RecordBatch.from_pydict({
        "id": [4],
        "name": ["Diana"],
        "age": [""],  # blank = NULL
        "email": ["diana@example.com"],
    })

    records = [dict(zip(batch.schema.names, row)) for row in zip(*batch.columns)]
    snowflake_connector.write(records)
    snowflake_connector.finalize()

    fq_table = f'"{snowflake_connector.db_schema}"."{TABLE_NAME}"'
    rows = _fetch_results(snowflake_connector, f'SELECT * FROM {fq_table} WHERE "id" = 4')

    assert len(rows) == 1
    row = rows[0]
    assert row["name"].lower() == "diana"
    assert row["email"].lower() == "diana@example.com"

    cols = _get_columns(snowflake_connector)
    assert "age" in cols
    assert "age" in row and row["age"] in (None, "", "NULL")
