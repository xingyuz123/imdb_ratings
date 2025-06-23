"""
Database connection management for IMDB Ratings application.

This module provides a singleton connection manager to ensure efficient
database connection reuse throughout the application lifecycle.
"""

from typing import Self
from threading import Lock
from supabase import Client, create_client
from imdb_ratings import logger
from imdb_ratings.config import get_settings

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
        self._settings = get_settings()
        
    def get_client(self) -> Client:
        """
        Get the Supabase client instance, creating it if necessary.

        Returns:
            Supabase client instance
        """
        if self._client is None:
            with self._lock:
                if self._client is None:
                    logger.info("Creating new Supabase client")
                    try:
                        self._client = create_client(
                            self._settings.supabase.project_url,
                            self._settings.supabase.secret_key
                        )
                        logger.info("Supabase client created successfully")
                    except Exception as e:
                        logger.error(f"Failed to create Supabase client: {e}")
                        raise
        return self._client
    
    def close(self) -> None:
        """
        Close the database connection.

        This should be called when shutting down the application.
        """
        if self._client is not None:
            with self._lock:
                logger.info("Closing Supabase client")
                self._client = None
                logger.info("Supabase client closed successfully")

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


def get_database_client() -> Client:
    """
    Get the shared database client.
    
    This is a convenience function that uses the connection manager
    to provide a consistent interface for getting the database client.
    
    Returns:
        Supabase client instance
    """
    manager = DatabaseConnectionManager()
    return manager.get_client()


# Create a module-level instance for easy cleanup
_connection_manager = DatabaseConnectionManager()


def close_database_connection() -> None:
    """Close the database connection when shutting down."""
    _connection_manager.close()