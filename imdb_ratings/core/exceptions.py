"""Custom exceptions for IMDB ratings application."""

class IMDBRatingsError(Exception):
    """Base exception for all application errors."""
    pass

class DatabaseError(IMDBRatingsError):
    """Base exception for database-related errors."""
    pass

class DatabaseConnectionError(DatabaseError):
    """Raised when a database connection fails."""
    pass

class DatabaseOperationError(DatabaseError):
    """Raised when a database operation fails."""
    pass

class ScrapingError(IMDBRatingsError):
    """Base exception for scraping-related errors."""
    pass

class RateLimitError(ScrapingError):
    """Raised when we hit rate limits."""
    pass

class DataValidationError(ScrapingError):
    """Raised when scraped data doesn't match expected format."""
    pass

class NetworkError(ScrapingError):
    """Raised for network-related issues."""
    pass

class DataProcessingError(IMDBRatingsError):
    """Base exception for data processing errors."""
    pass

class FileOperationError(DataProcessingError):
    """Raised when file operations fail."""
    pass

class DataIntegrityError(DataProcessingError):
    """Raised when data integrity checks fail."""
    pass

class ConfigurationError(IMDBRatingsError):
    """Raised when configuration is invalid or missing."""
    pass