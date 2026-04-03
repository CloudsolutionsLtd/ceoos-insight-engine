"""Custom exceptions for the insight-engine"""

class InsightEngineError(Exception):
    """Base exception for insight-engine"""
    pass

class GenerationError(InsightEngineError):
    """Raised when generation fails"""
    pass

class InsufficientDataError(InsightEngineError):
    """Raised when there's not enough data to generate insights"""
    pass

class CacheError(InsightEngineError):
    """Raised when cache operations fail"""
    pass

class ValidationError(InsightEngineError):
    """Raised when data validation fails"""
    pass

class ConfigurationError(InsightEngineError):
    """Raised when configuration is invalid"""
    pass