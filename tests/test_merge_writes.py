import pytest
from unittest.mock import MagicMock, patch, call
from conduit_core.config import Destination
from conduit_core.connectors.postgresql import PostgresDestination


def test_generate_merge_sql():
    """Test MERGE SQL generation for PostgreSQL."""
    config = Destination(
        name="test_dest",
        type="postgres",
        connection_string="postgresql://user:pass@localhost/db",
        table="users",
        write_mode="merge",
        primary_keys=["id"]
    )
    
    dest = PostgresDestination(config)
    
    columns = ["id", "name", "email"]
    sql = dest._generate_merge_sql("users", columns, ["id"])
    
    assert "INSERT INTO" in sql
    assert "ON CONFLICT" in sql
    assert '"id"' in sql
    assert '"name" = EXCLUDED."name"' in sql
    assert '"email" = EXCLUDED."email"' in sql


def test_merge_mode_in_finalize():
    """Test that finalize uses MERGE when write_mode='merge'."""
    config = Destination(
        name="test_dest",
        type="postgres",
        connection_string="postgresql://user:pass@localhost/db",
        table="users",
        db_schema="public",
        write_mode="merge",
        primary_keys=["id"]
    )
    
    dest = PostgresDestination(config)
    dest.accumulated_records = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
    
    # Mock psycopg2 connection
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock()
    
    # Track what SQL was executed
    executed_sql = []
    def mock_execute(sql, *args, **kwargs):
        executed_sql.append(str(sql))
    
    mock_cursor.execute = mock_execute
    
    with patch('psycopg2.connect', return_value=mock_conn), \
         patch('conduit_core.connectors.postgresql.execute_batch') as mock_batch:
        dest.finalize()
    
    # Verify MERGE SQL was used in execute_batch
    assert mock_batch.called
    merge_sql = str(mock_batch.call_args[0][1])
    assert "ON CONFLICT" in merge_sql, f"Expected ON CONFLICT. Got: {merge_sql}"


def test_append_mode_still_works():
    """Test that default append mode still works."""
    config = Destination(
        name="test_dest",
        type="postgres",
        connection_string="postgresql://user:pass@localhost/db",
        table="users",
        db_schema="public",
        write_mode="append"
    )
    
    dest = PostgresDestination(config)
    dest.accumulated_records = [
        {"id": 1, "name": "Alice"}
    ]
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock()
    
    with patch('psycopg2.connect', return_value=mock_conn), \
         patch('conduit_core.connectors.postgresql.execute_batch') as mock_batch:
        dest.finalize()
    
    # Verify regular INSERT was used (no ON CONFLICT)
    assert mock_batch.called
    insert_sql = str(mock_batch.call_args[0][1])
    assert "INSERT INTO" in insert_sql
    assert "ON CONFLICT" not in insert_sql


def test_merge_without_primary_keys_raises_error():
    """Test that MERGE without primary_keys raises error at config validation."""
    with pytest.raises(ValueError, match="primary_keys"):
        config = Destination(
            name="test_dest",
            type="postgres",
            connection_string="postgresql://user:pass@localhost/db",
            table="users",
            db_schema="public",
            write_mode="merge"
            # Missing primary_keys - should fail here
        )