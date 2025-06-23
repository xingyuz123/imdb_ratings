"""
Database connection management for IMDB Ratings application.

This module provides a singleton connection manager to ensure efficient
database connection reuse throughout the application lifecycle.
"""

from typing import Self, Any
from threading import Lock
from supabase import Client, create_client
from imdb_ratings import logger
from imdb_ratings.config import get_settings
from imdb_ratings.exceptions import DatabaseConnectionError, ConfigurationError
import time

class DatabaseConnectionManager:
    """
    Singleton manager for database connections.

    This class ensures that only one Supabase client is created and reused
    throughout the application's lifecycle, preventing connection exhaustion
    and improving performance.
    """

    _instance: Self | None = None
    _lock: Lock = Lock()
    _client: Client | None = None
    _max_retries: int = 3
    _retry_delay: float = 1.0

    def __new__(cls) -> Self:
        """Ensure only one instance of the connection manager exists."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """Initialize the connection manager."""
        if hasattr(self, "_initialized"):
            return
        
        self._initialized = True
        try:
            self._settings = get_settings()
            self._validate_configuration()
        except Exception as e:
            logger.error(f"Failed to initialize database connection manager: {e}")
            raise ConfigurationError(f"Invalid configuration: {e}")
        
    def _validate_configuration(self) -> None:
        """Validate that required configuration is present."""
        if not self._settings.supabase.project_url:
            raise ConfigurationError("Supabase project URL is not configured")
        if not self._settings.supabase.secret_key:
            raise ConfigurationError("Supabase secret key is not configured")
        if not self._settings.supabase.project_url.startswith(('http://', 'https://')):
            raise ConfigurationError(f"Invalid Supabase URL format: {self._settings.supabase.project_url}")
        
    def get_client(self) -> Client:
        """
        Get the Supabase client instance, creating it if necessary.

        Returns:
            Supabase client instance
        """
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = self._create_client_with_retry()

        try:
            # check connection exists
            self._verify_connection()
        except Exception:
            logger.warning("Connection verification failed. Attempting to reconnect...")
            with self._lock:
                self._client = None
                self._client = self._create_client_with_retry()
        return self._client
    
    def _create_client_with_retry(self) -> Client:
        """Create a Supabase client with retry logic."""
        last_error = None

        for attempt in range(self._max_retries):
            try:
                logger.info(f"Creating Supabase client (attempt {attempt + 1}/{self._max_retries})")
                client = create_client(
                    self._settings.supabase.project_url,
                    self._settings.supabase.secret_key
                )
                
                # Test the connection
                self._test_connection(client)
                
                logger.info("Supabase client created and verified successfully")
                return client
                
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Failed to create Supabase client (attempt {attempt + 1}/{self._max_retries}): {e}"
                )
                
                if attempt < self._max_retries - 1:
                    delay = self._retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
        
        error_msg = f"Failed to create Supabase client after {self._max_retries} attempts"
        logger.error(f"{error_msg}: {last_error}")
        raise DatabaseConnectionError(f"{error_msg}: {last_error}")

    def _test_connection(self, client: Client) -> None:
        """Test if the client can connect to the database."""
        try:
            # Simple query to test connection
            client.table(self._settings.supabase.titles_table).select("id").limit(1).execute()
        except Exception as e:
            raise DatabaseConnectionError(f"Connection test failed: {e}")

    def _verify_connection(self) -> None:
        """Verify that the existing connection is still valid."""
        if self._client is None:
            raise DatabaseConnectionError("No client connection exists")
        
        self._test_connection(self._client)

    def close(self) -> None:
        """
        Close the database connection.

        This should be called when shutting down the application.
        """
        if self._client is not None:
            with self._lock:
                try:
                    logger.info("Closing Supabase client")
                    self._client = None
                    logger.info("Supabase client closed successfully")
                except Exception as e:
                    logger.error(f"Error closing Supabase client: {e}")
                    self._client = None

    def reset(self) -> None:
        """
        Reset the connection manager.

        This is mainly used for testing purposes.
        """
        with self._lock:
            self._client = None
            DatabaseConnectionManager._instance = None
            if hasattr(self, "_initialized"):
                delattr(self, "_initialized")

    def health_check(self) -> dict[str, Any]:
        """
        Perform a health check on the database connection.
        
        Returns:
            Dictionary with health status information
        """
        health_status = {
            "status": "unknown",
            "connected": False,
            "error": None,
            "project_url": self._settings.supabase.project_url.split('.')[0] + ".supabase.co"
        }
        
        try:
            self._verify_connection()
            health_status["status"] = "healthy"
            health_status["connected"] = True
        except Exception as e:
            health_status["status"] = "unhealthy"
            health_status["error"] = str(e)
            logger.error(f"Database health check failed: {e}")
        
        return health_status

def get_database_client() -> Client:
    """
    Get the shared database client.
    
    This is a convenience function that uses the connection manager
    to provide a consistent interface for getting the database client.
    
    Returns:
        Supabase client instance

    Raises:
        DatabaseConnectionError: If the connection fails
        ConfigurationError: If the configuration is invalid
    """
    try:
        manager = DatabaseConnectionManager()
        return manager.get_client()
    except Exception as e:
        logger.error(f"Failed to get database client: {e}")
        raise

# Create a module-level instance for easy cleanup
_connection_manager = DatabaseConnectionManager()


def close_database_connection() -> None:
    """Close the database connection when shutting down."""
    try:
        _connection_manager.close()
    except Exception as e:
        logger.error(f"Error during database connection cleanup: {e}")

def get_database_health() -> dict[str, Any]:
    """Get the health status of the database connection."""
    try:
        return _connection_manager.health_check()
    except Exception as e:
        return {
            "status": "error",
            "connected": False,
            "error": f"Health check failed: {str(e)}"
        }