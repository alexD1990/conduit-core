"""Tests for preflight check functionality."""
import pytest
from pathlib import Path
from conduit_core.engine import preflight_check, run_preflight
from conduit_core.config import load_config


def test_preflight_check_valid_config(tmp_path):
    """Test preflight passes with valid configuration."""
    config_path = tmp_path / "test_config.yml"
    output_path = tmp_path / "output.csv"

    config_path.write_text(f"""
sources:
  - name: test_source
    type: csv
    path: tests/fixtures/data/comma_delim.csv

destinations:
  - name: test_dest
    type: csv
    path: {output_path}

resources:
  - name: test_resource
    source: test_source
    destination: test_dest
    query: n/a
""")
    
    config = load_config(str(config_path))
    results = preflight_check(config)
    
    print(f"\nDEBUG: {results}")
    assert results["passed"] is True
    assert len(results["checks"]) > 0
    assert results["checks"][0]["name"] == "Config Syntax"
    assert results["checks"][0]["status"] == "pass"


def test_preflight_check_missing_source():
    """Test preflight fails when source file doesn't exist."""
    config_path = Path("tests/fixtures/invalid_source_config.yml")
    
    # Create temp config with non-existent source
    config_content = """
sources:
  - name: missing_source
    type: csv
    path: /nonexistent/file.csv

destinations:
  - name: test_dest
    type: csv
    path: /tmp/output.csv

resources:
  - name: test_resource
    source: missing_source
    destination: test_dest
    query: n/a
"""
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        config = load_config(temp_path)
        results = preflight_check(config)
        
        assert results["passed"] is False
        assert len(results["errors"]) > 0
        assert any("connection failed" in err.lower() for err in results["errors"])
    finally:
        Path(temp_path).unlink()


def test_preflight_check_missing_resource():
    """Test preflight fails when resource not found."""
    config_path = Path("tests/fixtures/basic_config.yml")
    
    config_content = """
sources:
  - name: test_source
    type: csv
    path: tests/fixtures/comma_delim.csv

destinations:
  - name: test_dest
    type: csv
    path: /tmp/output.csv

resources:
  - name: test_resource
    source: test_source
    destination: test_dest
    query: n/a
"""
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        config = load_config(temp_path)
        results = preflight_check(config, resource_name="nonexistent_resource")
        
        assert results["passed"] is False
        assert any("not found" in err for err in results["errors"])
    finally:
        Path(temp_path).unlink()


def test_preflight_includes_duration():
    """Test preflight results include timing information."""
    config_content = """
sources:
  - name: test_source
    type: csv
    path: tests/fixtures/comma_delim.csv

destinations:
  - name: test_dest
    type: csv
    path: /tmp/output.csv

resources:
  - name: test_resource
    source: test_source
    destination: test_dest
    query: n/a
"""
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write(config_content)
        temp_path = f.name
    
    try:
        config = load_config(temp_path)
        results = preflight_check(config)
        
        assert "duration_s" in results
        assert isinstance(results["duration_s"], (int, float))
        assert results["duration_s"] >= 0
    finally:
        Path(temp_path).unlink()


def test_run_preflight_cli_success(tmp_path, capsys):
    """Test run_preflight function returns correct exit status."""
    config_path = tmp_path / "test_config.yml"
    output_path = tmp_path / "output.csv"

    config_path.write_text(f"""
sources:
  - name: test_source
    type: csv
    path: tests/fixtures/data/comma_delim.csv

destinations:
  - name: test_dest
    type: csv
    path: {output_path}

resources:
  - name: test_resource
    source: test_source
    destination: test_dest
    query: n/a
""")
    
    result = run_preflight(str(config_path), verbose=False)
    assert result is True