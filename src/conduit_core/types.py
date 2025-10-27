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

class ConduitType:
    """Conduit's unified type system for schema mapping."""
    
    # Map of source type names to Conduit types
    TYPE_ALIASES = {
        # String types
        'string': 'string',
        'str': 'string',
        'text': 'string',
        'varchar': 'string',
        'char': 'string',
        
        # Integer types
        'integer': 'integer',
        'int': 'integer',
        'bigint': 'integer',
        'smallint': 'integer',
        
        # Float types
        'float': 'float',
        'double': 'float',
        'real': 'float',
        
        # Decimal types
        'decimal': 'decimal',
        'numeric': 'decimal',
        
        # Boolean
        'boolean': 'boolean',
        'bool': 'boolean',
        
        # Date/Time
        'datetime': 'datetime',
        'timestamp': 'datetime',
        'date': 'date',
        'time': 'time',
    }
    
    @classmethod
    def normalize_type(cls, type_name: str) -> str:
        """Normalize a type name to Conduit's type system."""
        return cls.TYPE_ALIASES.get(type_name.lower(), 'string')


class TypeCoercionError(Exception):
    """Raised when type coercion fails in strict mode."""
    pass


class TypeCoercer:
    """
    Handles intelligent type coercion for incoming data.
    Complements TypeConverter (output) with input validation and casting.
    """
    
    def __init__(self, strict_mode: bool = False, null_values: list = None):
        """
        Initialize type coercer.
        
        Args:
            strict_mode: If True, raise error on coercion failure. If False, log warning and return None.
            null_values: List of string values to treat as NULL (e.g., ['', 'N/A', 'NULL'])
        """
        self.strict_mode = strict_mode
        self.null_values = null_values or ['', 'N/A', 'NULL', 'null', 'None', 'none', 'nan', 'NaN']
    
    def is_null_value(self, value: Any) -> bool:
        """Check if a value should be treated as NULL."""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() in self.null_values:
            return True
        return False
    
    def coerce(self, value: Any, target_type: str, column_name: str = None) -> Any:
        """
        Coerce a value to the target type with intelligent casting.
        
        Args:
            value: Value to coerce
            target_type: Target type (normalized Conduit type)
            column_name: Optional column name for error messages
        
        Returns:
            Coerced value or None if coercion fails (in non-strict mode)
        
        Raises:
            TypeCoercionError: If coercion fails in strict mode
        """
        # Handle NULL values
        if self.is_null_value(value):
            return None
        
        # Normalize target type
        target_type = ConduitType.normalize_type(target_type)
        
        try:
            if target_type == 'string':
                return self._to_string(value)
            elif target_type == 'integer':
                return self._to_integer(value)
            elif target_type == 'float':
                return self._to_float(value)
            elif target_type == 'decimal':
                return self._to_decimal(value)
            elif target_type == 'boolean':
                return self._to_boolean(value)
            elif target_type == 'datetime':
                return self._to_datetime(value)
            elif target_type == 'date':
                return self._to_date(value)
            else:
                # Unknown type, return as-is
                return value
                
        except Exception as e:
            error_msg = f"Failed to coerce value '{value}' (type: {type(value).__name__}) to {target_type}"
            if column_name:
                error_msg += f" for column '{column_name}'"
            
            if self.strict_mode:
                raise TypeCoercionError(error_msg) from e
            else:
                logger.warning(f"{error_msg}. Setting to NULL.")
                return None
    
    def _to_string(self, value: Any) -> str:
        """Convert value to string."""
        return str(value)
    
    def _to_integer(self, value: Any) -> int:
        """Convert value to integer with smart casting."""
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            # Handle strings like "123.0" or "  456  "
            value = value.strip()
            # Remove common formatting (commas, dollar signs)
            value = value.replace(',', '').replace('$', '')
            if '.' in value:
                # Try parsing as float first, then convert to int
                return int(float(value))
            return int(value)
        raise ValueError(f"Cannot convert {type(value).__name__} to integer")
    
    def _to_float(self, value: Any) -> float:
        """Convert value to float."""
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.strip().replace(',', '').replace('$', '')
            return float(value)
        raise ValueError(f"Cannot convert {type(value).__name__} to float")
    
    def _to_decimal(self, value: Any) -> Decimal:
        """Convert value to decimal."""
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        if isinstance(value, str):
            value = value.strip().replace(',', '').replace('$', '')
            return Decimal(value)
        raise ValueError(f"Cannot convert {type(value).__name__} to decimal")
    
    def _to_boolean(self, value: Any) -> bool:
        """Convert value to boolean with flexible parsing."""
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            value = value.strip().lower()
            if value in ('true', 't', 'yes', 'y', '1', 'on'):
                return True
            if value in ('false', 'f', 'no', 'n', '0', 'off'):
                return False
            raise ValueError(f"Cannot interpret '{value}' as boolean")
        raise ValueError(f"Cannot convert {type(value).__name__} to boolean")
    
    def _to_datetime(self, value: Any) -> datetime:
        """Convert value to datetime with multiple format support."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            value = value.strip()
            # Try common formats
            for fmt in [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%d',
                '%m/%d/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M:%S',
            ]:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            # Try ISO format as fallback
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                pass
        raise ValueError(f"Cannot convert {type(value).__name__} to datetime")
    
    def _to_date(self, value: Any) -> date:
        """Convert value to date."""
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            value = value.strip()
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y%m%d']:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
            raise ValueError(f"Cannot parse date from '{value}'")
        raise ValueError(f"Cannot convert {type(value).__name__} to date")


def coerce_record(
    record: dict,
    schema: dict,
    strict_mode: bool = False,
    null_values: list = None
) -> dict:
    """
    Coerce all values in a record according to schema.
    
    Args:
        record: Dictionary record to coerce
        schema: Schema dict with 'columns' containing type info
        strict_mode: If True, fail on any coercion error
        null_values: Custom NULL value representations
    
    Returns:
        Record with coerced values
    
    Example:
        schema = {
            'columns': [
                {'name': 'age', 'type': 'integer'},
                {'name': 'price', 'type': 'float'}
            ]
        }
        record = {'age': '25', 'price': '$19.99'}
        coerced = coerce_record(record, schema)
        # {'age': 25, 'price': 19.99}
    """
    coercer = TypeCoercer(strict_mode=strict_mode, null_values=null_values)
    coerced = {}
    
    # Build type map from schema
    type_map = {}
    if 'columns' in schema:
        for col in schema['columns']:
            type_map[col['name']] = col.get('type', 'string')
    
    # Coerce each value
    for key, value in record.items():
        target_type = type_map.get(key, 'string')
        coerced[key] = coercer.coerce(value, target_type, column_name=key)
    
    return coerced