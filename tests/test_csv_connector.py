# tests/test_csv_connector.py

import pytest
import csv
from conduit_core.connectors.csv import CsvSource, CsvDestination
from conduit_core.config import Source as SourceConfig
from conduit_core.config import Destination as DestinationConfig

@pytest.fixture
def sample_records():
    """En gjenbrukbar liste med test-data."""
    return [
        {'id': '1', 'name': 'Alice'},
        {'id': '2', 'name': 'Bob'},
    ]

def test_csv_destination_writes_correctly(tmp_path, sample_records):
    """Tester at CsvDestination skriver data til fil som forventet."""
    # Oppsett
    output_file = tmp_path / "output.csv"
    config = DestinationConfig(name="test", type="csv", path=str(output_file))
    destination = CsvDestination(config)
    
    # Handling
    destination.write(sample_records)
    
    # Forventning
    with open(output_file, 'r') as f:
        content = f.read()
        # Sjekker at overskrifter og rader ble skrevet riktig
        assert "id,name" in content
        assert "1,Alice" in content
        assert "2,Bob" in content

def test_csv_source_reads_correctly(tmp_path, sample_records):
    """Tester at CsvSource leser data fra fil som forventet."""
    # Oppsett
    input_file = tmp_path / "input.csv"
    # Skriver testdata til en midlertidig fil
    headers = sample_records[0].keys()
    with open(input_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(sample_records)

    config = SourceConfig(name="test", type="csv", path=str(input_file))
    source = CsvSource(config)
    
    # Handling
    read_records = list(source.read())
    
    # Forventning
    assert len(read_records) == 2
    assert read_records[0]['name'] == 'Alice'
    assert read_records[1]['id'] == '2'