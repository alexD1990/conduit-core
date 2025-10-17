# tests/test_atomic_writes.py
import pytest
import csv
from conduit_core.connectors.csv import CsvDestination
from conduit_core.config import Destination as DestinationConfig
from unittest.mock import patch

def test_atomic_write_cleans_up_on_error(tmp_path, monkeypatch):
    """Test at temp fil blir ryddet opp ved feil under skriving."""
    output_file = tmp_path / "output.csv"
    temp_file = output_file.with_suffix(".csv.tmp")
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    records = [{'id': '1', 'name': 'Alice'}]

    # --- FIX: Mock the writerows method directly to simulate a failure ---
    with patch('csv.DictWriter.writerows', side_effect=IOError("Simulated write failure")):
        destination.write(records)
        with pytest.raises(IOError):
            destination.finalize()
    
    assert not temp_file.exists(), "Temp file should be cleaned up after error"
    assert not output_file.exists(), "Output file should not exist after failed write"
# ... (resten av testene i filen er uendret) ...
def test_atomic_write_creates_temp_file(tmp_path):
    output_file = tmp_path / "output.csv"
    temp_file = output_file.with_suffix(".csv.tmp")
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    records = [{'id': '1', 'name': 'Alice'}]
    destination.write(records)
    destination.finalize()
    assert not temp_file.exists()
    assert output_file.exists()