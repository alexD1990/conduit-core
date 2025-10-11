# tests/test_type_handling.py

import pytest
from datetime import datetime, date
from decimal import Decimal
import math
from conduit_core.types import TypeConverter, EncodingDetector, sanitize_dict


def test_datetime_conversion():
    """Test datetime to ISO string conversion"""
    converter = TypeConverter()
    
    dt = datetime(2025, 10, 11, 15, 30, 45)
    result = converter.to_safe_type(dt, "json")
    
    assert isinstance(result, str)
    assert "2025-10-11" in result


def test_decimal_conversion():
    """Test Decimal to float conversion"""
    converter = TypeConverter()
    
    dec = Decimal("123.45")
    result = converter.to_safe_type(dec, "json")
    
    assert isinstance(result, float)
    assert result == 123.45


def test_bytes_conversion():
    """Test bytes to string with encoding handling"""
    converter = TypeConverter()
    
    # UTF-8 bytes
    utf8_bytes = "Hello 世界".encode('utf-8')
    result = converter.to_safe_type(utf8_bytes)
    
    assert isinstance(result, str)
    assert "Hello" in result


def test_nan_handling():
    """Test that NaN is converted to None"""
    converter = TypeConverter()
    
    result = converter.to_safe_type(float('nan'))
    
    assert result is None


def test_infinity_handling():
    """Test that Infinity is converted to None"""
    converter = TypeConverter()
    
    result = converter.to_safe_type(float('inf'))
    assert result is None
    
    result = converter.to_safe_type(float('-inf'))
    assert result is None


def test_parse_value_int():
    """Test parsing string to int"""
    converter = TypeConverter()
    
    assert converter.parse_value("123") == 123
    assert converter.parse_value("-456") == -456


def test_parse_value_float():
    """Test parsing string to float"""
    converter = TypeConverter()
    
    assert converter.parse_value("123.45") == 123.45
    assert converter.parse_value("-456.78") == -456.78


def test_parse_value_bool():
    """Test parsing string to bool"""
    converter = TypeConverter()
    
    assert converter.parse_value("true") == True
    assert converter.parse_value("false") == False
    assert converter.parse_value("yes") == True
    assert converter.parse_value("no") == False


def test_parse_value_null():
    """Test parsing NULL representations"""
    converter = TypeConverter()
    
    assert converter.parse_value("null") is None
    assert converter.parse_value("NULL") is None
    assert converter.parse_value("None") is None
    assert converter.parse_value("N/A") is None
    assert converter.parse_value("") is None


def test_sanitize_dict():
    """Test sanitizing entire dictionary"""
    data = {
        "id": 123,
        "name": "Test",
        "created_at": datetime(2025, 1, 1),
        "price": Decimal("99.99"),
        "active": True,
        "rating": float('nan'),
    }
    
    result = sanitize_dict(data, "json")
    
    assert result["id"] == 123
    assert result["name"] == "Test"
    assert isinstance(result["created_at"], str)
    assert isinstance(result["price"], float)
    assert result["active"] == True
    assert result["rating"] is None  # NaN converted to None


def test_encoding_detector(tmp_path):
    """Test encoding detection"""
    # Create a UTF-8 file
    test_file = tmp_path / "test_utf8.txt"
    test_file.write_text("Hello 世界", encoding='utf-8')
    
    detected = EncodingDetector.detect_encoding(str(test_file))
    
    assert detected in ['utf-8', 'utf-8-sig']


def test_nested_structure_to_csv():
    """Test that nested structures are JSON-stringified for CSV"""
    converter = TypeConverter()
    
    nested = {"key": "value", "nested": {"a": 1}}
    result = converter.to_safe_type(nested, "csv")
    
    assert isinstance(result, str)
    assert "key" in result