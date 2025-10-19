import pytest
from typer.testing import CliRunner
from pathlib import Path
from conduit_core.cli import app

runner = CliRunner()

def test_validate_command_success(tmp_path):
    cfg = tmp_path / "ingest.yml"
    cfg.write_text("""
sources:
  - name: test_source
    type: csv
    path: test.csv
destinations:
  - name: test_dest
    type: json
    path: output.json
resources:
  - name: test_resource
    source: test_source
    destination: test_dest
    query: "n/a"
""")
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

    import os
    orig = os.getcwd()
    os.chdir(tmp_path)
    try:
        result = runner.invoke(app, ["validate", "test_resource", "--file", str(cfg)])
        assert result.exit_code == 0
        assert "All validations passed" in result.stdout
    finally:
        os.chdir(orig)

def test_validate_command_invalid_resource(tmp_path):
    cfg = tmp_path / "ingest.yml"
    cfg.write_text("""
sources:
  - name: s
    type: csv
    path: t.csv
destinations:
  - name: d
    type: json
    path: o.json
resources:
  - name: good_resource
    source: s
    destination: d
    query: "n/a"
""")
    result = runner.invoke(app, ["validate", "missing_resource", "--file", str(cfg)])
    assert result.exit_code == 2
    assert "not found" in result.stdout.lower()
