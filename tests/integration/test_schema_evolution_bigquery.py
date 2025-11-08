import os
import pytest
import pyarrow as pa
from conduit_core.connectors.bigquery import BigQueryDestination
from conduit_core.config import Destination

TABLE_NAME = "test_schema_evolution"


@pytest.fixture(scope="module")
def bigquery_connector():
    """Create BigQueryDestination connector and ensure test table exists."""
    config = Destination(
        name="test_bigquery_destination",
        type="bigquery",
        project=os.getenv("BIGQUERY_PROJECT_ID"),
        dataset=os.getenv("BIGQUERY_DATASET"),
        credentials_path=os.getenv("BIGQUERY_CREDENTIALS_PATH"),
        table=TABLE_NAME,
        mode="append",
    )

    connector = BigQueryDestination(config)
    fq_table = f"{config.dataset}.{config.table}"

    connector.client.query(f"DROP TABLE IF EXISTS `{fq_table}`").result()
    yield connector
    connector.client.query(f"DROP TABLE IF EXISTS `{fq_table}`").result()


def _fetch_all(connector, dataset, table):
    fq_table = f"`{dataset}.{table}`"
    query = f"SELECT * FROM {fq_table} ORDER BY id"
    results = connector.client.query(query).result()
    return [dict(row) for row in results]


def _get_columns(connector, dataset, table):
    query = f"SELECT column_name FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name='{table}'"
    results = connector.client.query(query).result()
    return [row["column_name"].lower() for row in results]


def test_initial_load(bigquery_connector):
    """Scenario 1: create table and insert initial data"""
    batch = pa.RecordBatch.from_pydict(
        {"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]}
    )

    records = [dict(zip(batch.schema.names, row)) for row in zip(*batch.columns)]
    bigquery_connector.write(records)
    bigquery_connector.finalize()

    rows = _fetch_all(bigquery_connector, bigquery_connector.config.dataset, TABLE_NAME)
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "Bob"

    cols = _get_columns(bigquery_connector, bigquery_connector.config.dataset, TABLE_NAME)
    assert all(col in cols for col in ["id", "name", "age"])


def test_add_column(bigquery_connector):
    """Scenario 2: add new column (email)"""
    batch = pa.RecordBatch.from_pydict(
        {
            "id": [3],
            "name": ["Charlie"],
            "age": [35],
            "email": ["charlie@example.com"],
        }
    )

    records = [dict(zip(batch.schema.names, row)) for row in zip(*batch.columns)]
    bigquery_connector.write(records)
    bigquery_connector.finalize()

    rows = _fetch_all(bigquery_connector, bigquery_connector.config.dataset, TABLE_NAME)
    assert any(r["email"] == "charlie@example.com" for r in rows)

    cols = _get_columns(bigquery_connector, bigquery_connector.config.dataset, TABLE_NAME)
    assert "email" in cols


def test_remove_column(bigquery_connector):
    """Scenario 3: load missing column (age) → expect NULL fallback"""
    batch = pa.RecordBatch.from_pydict(
        {"id": [4], "name": ["Diana"], "email": ["diana@example.com"]}
    )

    records = [dict(zip(batch.schema.names, row)) for row in zip(*batch.columns)]
    bigquery_connector.write(records)
    bigquery_connector.finalize()

    rows = _fetch_all(bigquery_connector, bigquery_connector.config.dataset, TABLE_NAME)
    row = next((r for r in rows if r["id"] == 4), None)
    assert row is not None
    assert row["name"] == "Diana"
    assert row["email"] == "diana@example.com"
    assert "age" in row and row["age"] is None
