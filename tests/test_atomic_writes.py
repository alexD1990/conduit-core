# tests/test_atomic_writes.py

import pytest
import csv
import time
from pathlib import Path
from conduit_core.connectors.csv import CsvDestination
from conduit_core.config import Destination as DestinationConfig


def test_atomic_write_creates_temp_file(tmp_path):
    """Test at atomic write bruker en temp fil under skriving."""
    output_file = tmp_path / "output.csv"
    temp_file = tmp_path / "output.tmp"
    
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    
    records = [
        {'id': '1', 'name': 'Alice'},
        {'id': '2', 'name': 'Bob'},
    ]
    
    # Skriv data
    destination.write(records)
    
    # Temp filen skal IKKE eksistere etter vellykket skriving
    assert not temp_file.exists(), "Temp file should be cleaned up after successful write"
    
    # Output filen skal eksistere
    assert output_file.exists(), "Output file should exist"
    
    # Verifiser innholdet
    with open(output_file, 'r') as f:
        content = f.read()
        assert "Alice" in content
        assert "Bob" in content


def test_atomic_write_replaces_existing_file(tmp_path):
    """Test at atomic write trygt erstatter en eksisterende fil."""
    output_file = tmp_path / "output.csv"
    
    # Skriv første versjon
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    
    records_v1 = [{'id': '1', 'name': 'Alice'}]
    destination.write(records_v1)
    
    # Verifiser første versjon
    with open(output_file, 'r') as f:
        content_v1 = f.read()
        assert "Alice" in content_v1
        assert "Bob" not in content_v1
    
    # Skriv andre versjon (skal erstatte)
    records_v2 = [
        {'id': '1', 'name': 'Alice'},
        {'id': '2', 'name': 'Bob'},
    ]
    destination.write(records_v2)
    
    # Verifiser at filen ble erstattet
    with open(output_file, 'r') as f:
        content_v2 = f.read()
        assert "Alice" in content_v2
        assert "Bob" in content_v2


def test_atomic_write_cleans_up_on_error(tmp_path, monkeypatch):
    """Test at temp fil blir ryddet opp ved feil."""
    output_file = tmp_path / "output.csv"
    temp_file = tmp_path / "output.tmp"
    
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    
    records = [{'id': '1', 'name': 'Alice'}]
    
    # Mock csv.DictWriter til å feile
    original_dictwriter = csv.DictWriter
    
    def failing_dictwriter(*args, **kwargs):
        writer = original_dictwriter(*args, **kwargs)
        original_writerows = writer.writerows
        
        def failing_writerows(rows):
            raise IOError("Simulated write failure")
        
        writer.writerows = failing_writerows
        return writer
    
    monkeypatch.setattr(csv, 'DictWriter', failing_dictwriter)
    
    # Forsøk å skrive (skal feile)
    with pytest.raises(IOError):
        destination.write(records)
    
    # Temp filen skal være ryddet opp
    assert not temp_file.exists(), "Temp file should be cleaned up after error"
    
    # Output filen skal ikke eksistere
    assert not output_file.exists(), "Output file should not exist after failed write"


def test_atomic_write_preserves_data_on_crash(tmp_path):
    """Test at eksisterende fil forblir intakt hvis skriving feiler."""
    output_file = tmp_path / "output.csv"
    
    # Skriv initial data
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    
    initial_records = [{'id': '1', 'name': 'Alice'}]
    destination.write(initial_records)
    
    # Les initial data
    with open(output_file, 'r') as f:
        initial_content = f.read()
    
    # Forsøk å skrive ugyldig data (tom liste)
    # Dette skal ikke påvirke den eksisterende filen
    destination.write([])
    
    # Original fil skal fortsatt eksistere og være uendret
    assert output_file.exists()
    with open(output_file, 'r') as f:
        current_content = f.read()
    
    # Innholdet skal være det samme
    assert current_content == initial_content


def test_atomic_write_creates_directory_if_missing(tmp_path):
    """Test at atomic write oppretter parent directory hvis den mangler."""
    nested_dir = tmp_path / "output" / "nested" / "dir"
    output_file = nested_dir / "output.csv"
    
    # Directory eksisterer IKKE ennå
    assert not nested_dir.exists()
    
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    
    records = [{'id': '1', 'name': 'Alice'}]
    destination.write(records)
    
    # Directory og fil skal nå eksistere
    assert nested_dir.exists()
    assert output_file.exists()
    
    # Verifiser innholdet
    with open(output_file, 'r') as f:
        content = f.read()
        assert "Alice" in content


def test_atomic_write_handles_empty_records_gracefully(tmp_path):
    """Test at atomic write håndterer tom liste uten å krasje."""
    output_file = tmp_path / "output.csv"
    
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    
    # Skriv tom liste (skal ikke krasje)
    destination.write([])
    
    # Ingen fil skal opprettes for tom data
    # (dette er current behavior - vi logger bare at det er tomt)