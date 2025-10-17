# tests/test_connection_validation.py

import pytest
from pathlib import Path

from conduit_core.config import Source as SourceConfig
from conduit_core.config import Destination as DestinationConfig
from conduit_core.connectors.csv import CsvSource, CsvDestination
from conduit_core.errors import ConnectionError

def test_csv_source_validates_file_exists(tmp_path):
    """Test that CsvSource.test_connection() fails if the file does not exist."""
    config = SourceConfig(
        name="test",
        type="csv",
        path=str(tmp_path / "nonexistent.csv")
    )
    source = CsvSource(config)

    with pytest.raises(ConnectionError, match="not found"):
        source.test_connection()

def test_csv_source_validates_readable(tmp_path):
    """Test that CsvSource.test_connection() fails if the file is not readable."""
    # Create an unreadable file by removing all permissions
    unreadable_file = tmp_path / "unreadable.csv"
    unreadable_file.write_text("id,name\n1,test")
    unreadable_file.chmod(0o000)

    config = SourceConfig(name="test", type="csv", path=str(unreadable_file))
    source = CsvSource(config)

    try:
        with pytest.raises(ConnectionError, match="Cannot read"):
            source.test_connection()
    finally:
        # IMPORTANT: Restore permissions so pytest can clean up the temporary directory
        unreadable_file.chmod(0o644)

def test_csv_destination_validates_writable_and_creates_dir(tmp_path):
    """Test that CsvDestination.test_connection() creates a directory and confirms it's writable."""
    output_path = tmp_path / "new_subdir" / "output.csv"
    
    # The subdirectory does not exist yet
    assert not output_path.parent.exists()

    config = DestinationConfig(name="test", type="csv", path=str(output_path))
    destination = CsvDestination(config)

    # The test should succeed and create the directory
    assert destination.test_connection() is True
    assert output_path.parent.exists()

def test_connection_validation_provides_helpful_errors():
    """Test that the raised ConnectionError contains the 'Suggestions' text."""
    config = SourceConfig(
        name="test",
        type="csv",
        path="/nonexistent/path/for/testing/file.csv"
    )
    source = CsvSource(config)

    with pytest.raises(ConnectionError) as exc_info:
        source.test_connection()
    
    error_message = str(exc_info.value)
    assert "Suggestions:" in error_message
    assert "Check file path" in error_message