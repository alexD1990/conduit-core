# tests/test_schema_validation.py
import pytest
from conduit_core.schema_validator import (
    SchemaValidator,
    ValidationReport,
    ValidationError,
)


def test_validate_compatible_types():
    """Test that compatible types pass validation"""
    validator = SchemaValidator()

    source_schema = {
        'id': {'type': 'integer', 'nullable': False},
        'name': {'type': 'string', 'nullable': False},
        'price': {'type': 'float', 'nullable': True},
    }

    dest_schema = {
        'id': {'type': 'integer', 'nullable': False},
        'name': {'type': 'string', 'nullable': False},
        'price': {'type': 'decimal', 'nullable': True},  # float -> decimal is safe
    }

    report = validator.validate_type_compatibility(source_schema, dest_schema)
    assert report.is_valid
    assert len(report.errors) == 0
    assert not report.has_errors()


def test_detect_type_mismatch():
    """Test that incompatible types are detected"""
    validator = SchemaValidator()

    source_schema = {'id': {'type': 'string', 'nullable': False}}
    dest_schema = {'id': {'type': 'integer', 'nullable': False}}

    report = validator.validate_type_compatibility(source_schema, dest_schema)
    assert not report.is_valid
    assert len(report.errors) == 1
    assert report.errors[0].column == 'id'
    assert report.errors[0].issue == 'type_mismatch'


def test_detect_missing_columns():
    """Test detection of columns in dest but not source"""
    validator = SchemaValidator()

    source_schema = {'id': {'type': 'integer', 'nullable': False}}
    dest_schema = {
        'id': {'type': 'integer', 'nullable': False},
        'email': {'type': 'string', 'nullable': False},
    }

    report = validator.check_missing_columns(source_schema, dest_schema)
    missing = [e.column for e in report.errors + report.warnings]
    assert 'email' in missing


def test_required_columns_present():
    """Test that all required columns are present"""
    validator = SchemaValidator()

    source_schema = {
        'id': {'type': 'integer', 'nullable': False},
        'name': {'type': 'string', 'nullable': False},
        'email': {'type': 'string', 'nullable': False},
    }

    required = ['id', 'name', 'email']
    missing = validator.check_required_columns(source_schema, required)
    assert len(missing) == 0


def test_required_columns_missing_fails():
    """Test that missing required columns are detected"""
    validator = SchemaValidator()

    source_schema = {'id': {'type': 'integer', 'nullable': False}}
    required = ['id', 'name', 'email']

    missing = validator.check_required_columns(source_schema, required)
    assert 'name' in missing
    assert 'email' in missing


def test_strict_validation_fails_on_warnings():
    """Test that strict mode treats warnings as errors"""
    pytest.skip("Tested in integration")


def test_non_strict_allows_warnings():
    """Test that non-strict mode allows warnings"""
    validator = SchemaValidator()

    source_schema = {'id': {'type': 'integer', 'nullable': False}}
    dest_schema = {
        'id': {'type': 'integer', 'nullable': False},
        'email': {'type': 'string', 'nullable': False},  # Missing from source
    }

    report = validator.validate_type_compatibility(source_schema, dest_schema)
    assert report.is_valid  # no hard errors
    assert len(report.warnings) > 0


def test_constraint_validation():
    """Test NOT NULL constraint validation"""
    validator = SchemaValidator()

    source_schema = {'id': {'type': 'integer', 'nullable': True}}
    dest_schema = {'id': {'type': 'integer', 'nullable': False}}

    report = validator.validate_constraints(source_schema, dest_schema)
    assert not report.is_valid
    assert len(report.errors) == 1
    assert 'constraint_violation' in report.errors[0].issue


def test_validation_report_formatting():
    """Test that ValidationReport formats errors nicely"""
    error = ValidationError(
        column='age',
        issue='type_mismatch',
        expected='integer',
        actual='string',
        severity='error',
    )

    report = ValidationReport(is_valid=False, errors=[error], warnings=[])
    formatted = report.format_errors()

    assert 'age' in formatted
    assert 'integer' in formatted
    assert 'string' in formatted
    assert 'type' in formatted.lower()


def test_skip_validation_when_disabled():
    """Test that validation is skipped when validate_schema=False"""
    pytest.skip("Tested in integration")
