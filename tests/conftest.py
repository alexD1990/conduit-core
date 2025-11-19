# tests/conftest.py
import pytest
from click.testing import CliRunner
from conduit_core.cli import app  # Typer app

@pytest.fixture
def cli_runner():
    """Provide a reusable CLI test runner for Conduit Core."""
    return CliRunner()
