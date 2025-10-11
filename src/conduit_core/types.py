# src/conduit_core/types.py

import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Check if pandas is available
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


class TypeConverter:
    """
    Handles conversion between different data types across sources and destinations.
    Makes data transfer robust by handling edge cases gracefully.
    """
    
    @staticmethod
    def to_safe_type(value: Any, target_format: str = "json") -> Any:
        """
        Convert value to a safe type for the target format.
        
        Args:
            value: The value to convert
            target_format: Target format - "json", "csv", "parquet", "sql"
        
        Returns:
            Converted value safe for target format
        """
        # Handle None
        if value is None:
            return None
        
        # Handle pandas Timestamp (from Parquet files)
        if HAS_PANDAS and isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            return value.isoformat()
        
        # Handle pandas NA/NaT
        if HAS_PANDAS:
            try:
                if pd.isna(value):
                    return None
            except (TypeError, ValueError):
                # pd.isna can fail on some types, ignore
                pass
        
        # Handle datetime objects
        if isinstance(value, (datetime, date)):
            return TypeConverter._convert_datetime(value, target_format)
        
        # Handle Decimal
        if isinstance(value, Decimal):
            return TypeConverter._convert_decimal(value, target_format)
        
        # Handle bytes
        if isinstance(value, bytes):
            return TypeConverter._convert_bytes(value)
        
        # Handle boolean (some formats have issues with bool)
        if isinstance(value, bool):
            return TypeConverter._convert_bool(value, target_format)
        
        # Handle lists and dicts (for JSON)
        if isinstance(value, (list, dict)):
            if target_format == "csv":
                # CSV can't handle nested structures
                import json
                return json.dumps(value)
            return value
        
        # Handle NaN and Infinity for numeric types
        if isinstance(value, float):
            return TypeConverter._convert_float(value)
        
        # Pass through other types
        return value
    
    @staticmethod
    def _convert_datetime(value: datetime | date, target_format: str) -> str:
        """Convert datetime to ISO format string"""
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, date):
            return value.isoformat()
        return str(value)
    
    @staticmethod
    def _convert_decimal(value: Decimal, target_format: str) -> float | str:
        """Convert Decimal to float or string"""
        if target_format == "json":
            return float(value)
        return str(value)
    
    @staticmethod
    def _convert_bytes(value: bytes) -> str:
        """Convert bytes to string, handling encoding errors"""
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            try:
                return value.decode('latin-1')
            except:
                # Last resort: ignore errors
                return value.decode('utf-8', errors='replace')
    
    @staticmethod
    def _convert_bool(value: bool, target_format: str) -> bool | str | int:
        """Convert boolean based on target format"""
        if target_format == "csv":
            return "true" if value else "false"
        elif target_format == "sql":
            return 1 if value else 0
        return value
    
    @staticmethod
    def _convert_float(value: float) -> Optional[float]:
        """Handle special float values (NaN, Infinity)"""
        import math
        
        if math.isnan(value):
            return None  # Convert NaN to None
        elif math.isinf(value):
            return None  # Convert Infinity to None
        return value
    
    @staticmethod
    def parse_value(value: str, hint_type: Optional[type] = None) -> Any:
        """
        Parse a string value into the appropriate Python type.
        
        Args:
            value: String value to parse
            hint_type: Optional type hint to guide conversion
        
        Returns:
            Parsed value in appropriate type
        """
        # Handle empty string
        if value == "":
            return None
        
        # Handle common NULL representations
        if value.lower() in ("null", "none", "n/a", "na", "nan"):
            return None
        
        # If type hint provided, try to convert
        if hint_type:
            try:
                if hint_type == int:
                    return int(value)
                elif hint_type == float:
                    return float(value)
                elif hint_type == bool:
                    return value.lower() in ("true", "yes", "1", "t", "y")
                elif hint_type == datetime:
                    return datetime.fromisoformat(value)
            except (ValueError, TypeError):
                logger.debug(f"Failed to convert '{value}' to {hint_type}, returning as string")
                return value
        
        # Auto-detect type
        # Try int
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Try boolean
        if value.lower() in ("true", "false", "yes", "no"):
            return value.lower() in ("true", "yes")
        
        # Try datetime (ISO format)
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            pass
        
        # Return as string
        return value


class EncodingDetector:
    """Detects and handles file encodings"""
    
    COMMON_ENCODINGS = [
        'utf-8',
        'utf-8-sig',  # UTF-8 with BOM
        'latin-1',
        'iso-8859-1',
        'windows-1252',
        'ascii',
    ]
    
    @staticmethod
    def detect_encoding(filepath: str, sample_size: int = 10000) -> str:
        """
        Attempt to detect file encoding by trying common encodings.
        
        Args:
            filepath: Path to file
            sample_size: Number of bytes to sample
        
        Returns:
            Detected encoding name
        """
        with open(filepath, 'rb') as f:
            sample = f.read(sample_size)
        
        for encoding in EncodingDetector.COMMON_ENCODINGS:
            try:
                sample.decode(encoding)
                logger.info(f"Detected encoding: {encoding}")
                return encoding
            except (UnicodeDecodeError, LookupError):
                continue
        
        logger.warning("Could not detect encoding, defaulting to utf-8 with error handling")
        return 'utf-8'
    
    @staticmethod
    def read_with_encoding(filepath: str, encoding: Optional[str] = None) -> str:
        """
        Read file with proper encoding handling.
        
        Args:
            filepath: Path to file
            encoding: Specific encoding to use, or None to auto-detect
        
        Returns:
            File contents as string
        """
        if encoding is None:
            encoding = EncodingDetector.detect_encoding(filepath)
        
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            # Fallback: try with error handling
            logger.warning(f"Encoding {encoding} failed, using utf-8 with error replacement")
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                return f.read()


def sanitize_dict(data: dict, target_format: str = "json") -> dict:
    """
    Sanitize all values in a dictionary for target format.
    
    Args:
        data: Dictionary to sanitize
        target_format: Target format for conversion
    
    Returns:
        Sanitized dictionary
    """
    converter = TypeConverter()
    return {
        key: converter.to_safe_type(value, target_format)
        for key, value in data.items()
    }