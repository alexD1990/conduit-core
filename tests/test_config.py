# tests/test_config.py
import pytest
from pydantic import ValidationError
from conduit_core.config import load_config, IngestConfig

def test_load_valid_config(tmp_path):
    """
    Tester at en gyldig ingest.yml-fil blir parset korrekt.
    `tmp_path` er en magisk pytest-funksjon som gir oss en midlertidig mappe.
    """
    # 1. Oppsett: Lag en falsk ingest.yml-fil for testen
    config_content = """
    sources:
      - name: test_source
        type: dummy_source
    destinations:
      - name: test_dest
        type: dummy_destination
    resources:
      - name: test_resource
        source: test_source
        destination: test_dest
        query: "SELECT 1"
    """
    config_file = tmp_path / "ingest.yml"
    config_file.write_text(config_content)

    # 2. Handling: Kjør funksjonen vi vil teste
    config = load_config(config_file)

    # 3. Forventning: Sjekk at resultatet er som forventet
    assert isinstance(config, IngestConfig)
    assert len(config.sources) == 1
    assert config.resources[0].name == "test_resource"
    assert config.sources[0].type == "dummy_source"

def test_load_invalid_config_raises_error(tmp_path):
    """
    Tester at en ugyldig config (mangler 'type')
    kaster en Pydantic ValidationError.
    """
    # 1. Oppsett: Lag en falsk ingest.yml med en feil
    invalid_config_content = """
    sources:
      - name: test_source
        # 'type'-feltet mangler med vilje
    destinations:
      - name: test_dest
        type: dummy_destination
    resources:
      - name: test_resource
        source: test_source
        destination: test_dest
        query: "SELECT 1"
    """
    config_file = tmp_path / "ingest.yml"
    config_file.write_text(invalid_config_content)

    # 2. Handling & Forventning: Sjekk at en feil faktisk oppstår
    # Denne 'with'-blokken sier: "Jeg forventer at koden inni her
    # vil krasje med en ValidationError. Hvis den gjør det, er testen bestått."
    with pytest.raises(ValidationError):
        load_config(config_file)