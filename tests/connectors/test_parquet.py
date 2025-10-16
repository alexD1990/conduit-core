"""Tests for Parquet connector."""

import tempfile
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from conduit_core.connectors.parquet import ParquetSource, ParquetDestination


@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return [
        {"id": 1, "name": "Alice", "age": 30, "active": True},
        {"id": 2, "name": "Bob", "age": 25, "active": False},
        {"id": 3, "name": "Charlie", "age": 35, "active": True},
    ]


@pytest.fixture
def parquet_file(sample_data, tmp_path):
    """Create a sample Parquet file."""
    file_path = tmp_path / "test.parquet"
    table = pa.Table.from_pylist(sample_data)
    pq.write_table(table, file_path)
    return file_path


def test_parquet_source_get_schema(parquet_file):
    """Test schema extraction."""
    config = {"file_path": str(parquet_file)}
    source = ParquetSource(config)
    
    # ParquetSource doesn't have get_schema in base contract
    # Test that we can read and infer schema from data
    records = list(source.read())
    assert len(records) > 0
    assert "id" in records[0]
    assert "name" in records[0]


def test_parquet_destination_write(sample_data, tmp_path):
    """Test writing to Parquet file."""
    file_path = tmp_path / "output.parquet"
    config = {"file_path": str(file_path)}
    
    destination = ParquetDestination(config)
    destination.write(sample_data)
    destination.finalize()
    
    assert file_path.exists()
    
    table = pq.read_table(file_path)
    records = table.to_pylist()
    
    assert len(records) == len(sample_data)
    assert records == sample_data


def test_parquet_destination_compression(sample_data, tmp_path):
    """Test compression options."""
    file_path = tmp_path / "compressed.parquet"
    config = {"file_path": str(file_path), "compression": "gzip"}
    
    destination = ParquetDestination(config)
    destination.write(sample_data)
    destination.finalize()
    
    assert file_path.exists()
    
    parquet_file = pq.ParquetFile(file_path)
    assert parquet_file.metadata.row_group(0).column(0).compression == "GZIP"


def test_parquet_destination_creates_directory(sample_data, tmp_path):
    """Test automatic directory creation."""
    file_path = tmp_path / "nested" / "dir" / "output.parquet"
    config = {"file_path": str(file_path)}
    
    destination = ParquetDestination(config)
    destination.write(sample_data)
    destination.finalize()
    
    assert file_path.exists()


def test_parquet_roundtrip(sample_data, tmp_path):
    """Test write then read."""
    file_path = tmp_path / "roundtrip.parquet"
    
    # Write
    dest_config = {"file_path": str(file_path)}
    destination = ParquetDestination(dest_config)
    destination.write(sample_data)
    destination.finalize()
    
    # Read
    source_config = {"file_path": str(file_path)}
    source = ParquetSource(source_config)
    records = list(source.read())
    
    assert records == sample_data


def test_parquet_destination_empty_flush(tmp_path):
    """Test flushing with no data."""
    file_path = tmp_path / "empty.parquet"
    config = {"file_path": str(file_path)}
    
    destination = ParquetDestination(config)
    destination.finalize()
    
    assert not file_path.exists()