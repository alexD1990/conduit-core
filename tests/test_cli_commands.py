"""Tests for CLI commands."""
import pytest


def test_cli_imports():
    """Test that CLI module can be imported."""
    from conduit_core.cli import app
    assert app is not None