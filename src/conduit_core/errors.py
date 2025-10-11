# src/conduit_core/errors.py

class ConduitError(Exception):
    """Base exception for all Conduit errors"""
    def __init__(self, message: str, suggestions: list[str] = None):
        self.message = message
        self.suggestions = suggestions or []
        super().__init__(self.format_message())
    
    def format_message(self) -> str:
        """Format error message with helpful suggestions"""
        msg = f"\n❌ {self.message}\n"
        if self.suggestions:
            msg += "\n💡 Suggestions:\n"
            for i, suggestion in enumerate(self.suggestions, 1):
                msg += f"   {i}. {suggestion}\n"
        return msg


class ConnectionError(ConduitError):
    """Raised when cannot connect to source or destination"""
    pass


class ConfigurationError(ConduitError):
    """Raised when configuration is invalid"""
    pass


class DataValidationError(ConduitError):
    """Raised when data fails validation"""
    pass


class SchemaError(ConduitError):
    """Raised when schema mismatch occurs"""
    pass