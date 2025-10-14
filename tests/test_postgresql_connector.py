# tests/test_postgresql_connector.py

import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from conduit_core.connectors.postgresql import PostgresSource, PostgresDestination
from conduit_core.config import Source as SourceConfig
from conduit_core.config import Destination as DestinationConfig

@pytest.fixture
def mock_postgres_env(monkeypatch):
    """Mock PostgreSQL environment variables."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DATABASE", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")


@pytest.fixture
def mock_psycopg2_connect():
    """Mock psycopg2.connect to avoid real database connections."""
    with patch('conduit_core.connectors.postgresql.psycopg2.connect') as mock_connect:
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Setup cursor to return dictionaries
        mock_cursor.fetchone.side_effect = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
            None  # End of results
        ]
        
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        yield mock_connect


def test_postgres_source_with_connection_string(mock_psycopg2_connect):
    """Test PostgresSource with connection string."""
    config = SourceConfig(
        name='test_source',
        type='postgresql',
        connection_string='host=localhost dbname=testdb user=testuser password=testpass'
    )
    
    source = PostgresSource(config)
    assert source.connection_string is not None


def test_postgres_source_with_individual_params(mock_postgres_env, mock_psycopg2_connect):
    """Test PostgresSource with individual parameters."""
    config = SourceConfig(
        name='test_source',
        type='postgresql',
        host='localhost',
        port=5432,
        database='testdb',
        user='testuser',
        password='testpass'
    )
    
    source = PostgresSource(config)
    assert 'testdb' in source.connection_string


def test_postgres_source_reads_data(mock_psycopg2_connect):
    """Test that PostgresSource can read data."""
    config = SourceConfig(
        name='test_source',
        type='postgresql',
        connection_string='host=localhost dbname=testdb user=testuser password=testpass'
    )
    
    source = PostgresSource(config)
    records = list(source.read("SELECT * FROM users"))
    
    assert len(records) == 2
    assert records[0]['name'] == 'Alice'
    assert records[1]['name'] == 'Bob'


def test_postgres_source_requires_query(mock_psycopg2_connect):
    """Test that PostgresSource requires a query."""
    config = SourceConfig(
        name='test_source',
        type='postgresql',
        connection_string='host=localhost dbname=testdb user=testuser password=testpass'
    )
    
    source = PostgresSource(config)
    
    with pytest.raises(ValueError, match="requires a SQL query"):
        list(source.read("n/a"))


def test_postgres_source_missing_credentials():
    """Test that PostgresSource raises error when credentials missing."""
    config = SourceConfig(
        name='test_source',
        type='postgresql',
        host='localhost'
        # Missing database, user, password
    )
    
    with pytest.raises(ValueError, match="requires database, user, and password"):
        PostgresSource(config)


def test_postgres_destination_accumulates_records(mock_psycopg2_connect):
    """Test that PostgresDestination accumulates records."""
    config = DestinationConfig(
        name='test_dest',
        type='postgresql',
        connection_string='host=localhost dbname=testdb user=testuser password=testpass',
        table='users'
    )
    
    destination = PostgresDestination(config)
    
    records = [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]
    
    destination.write(records)
    
    assert len(destination.accumulated_records) == 2


def test_postgres_destination_finalize_writes_to_db(mock_psycopg2_connect):
    """Test that finalize() writes accumulated records to database."""
    
    # Mock execute_batch
    with patch('conduit_core.connectors.postgresql.execute_batch') as mock_execute_batch:
        config = DestinationConfig(
            name='test_dest',
            type='postgresql',
            connection_string='host=localhost dbname=testdb user=testuser password=testpass',
            table='users',
            schema='public'
        )
        
        destination = PostgresDestination(config)
        
        records = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        destination.write(records)
        destination.finalize()
        
        # Verify execute_batch was called
        assert mock_execute_batch.called
        assert mock_execute_batch.call_count == 1
        
        # Verify it was called with correct number of records
        call_args = mock_execute_batch.call_args
        data = call_args[0][2]  # Third argument is the data
        assert len(data) == 2


def test_postgres_destination_requires_table(mock_psycopg2_connect):
    """Test that PostgresDestination requires table parameter."""
    config = DestinationConfig(
        name='test_dest',
        type='postgresql',
        connection_string='host=localhost dbname=testdb user=testuser password=testpass'
        # Missing table
    )
    
    with pytest.raises(ValueError, match="requires 'table'"):
        PostgresDestination(config)


def test_postgres_destination_empty_records(mock_psycopg2_connect):
    """Test that finalize handles empty records gracefully."""
    config = DestinationConfig(
        name='test_dest',
        type='postgresql',
        connection_string='host=localhost dbname=testdb user=testuser password=testpass',
        table='users'
    )
    
    destination = PostgresDestination(config)
    destination.finalize()  # Should not crash with empty records
    
    # Verify no database operations were performed
    assert not mock_psycopg2_connect.return_value.cursor.return_value.execute.called