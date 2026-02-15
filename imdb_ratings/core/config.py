"""
Configuration management for IMDB Ratings application.

This module centralizes all configuration settings using Pydantic for 
validation and type safety. Configuration can be loaded from environment 
variables or a .env file.
"""

from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class IMDBDataConfig(BaseModel):
    """Configuration for IMDB data processing."""
    
    # Dataset URLs
    basics_url: str = "https://datasets.imdbws.com/title.basics.tsv.gz"
    ratings_url: str = "https://datasets.imdbws.com/title.ratings.tsv.gz"
    
    # Filtering thresholds
    min_votes: int = Field(default=15_000, ge=0)
    valid_title_types: tuple[str, ...] = ("movie", "tvSeries", "tvMiniSeries")
    
    # Data processing
    null_value: str = "\\N"
    genre_separator: str = ","

class SupabaseConfig(BaseModel):
    """Configuration for Supabase connection."""
    
    project_url: str
    secret_key: str
    
    # Table names
    titles_table: str = "titles"
    reviews_table: str = "reviews"
    weighted_ratings_table: str = "weighted_ratings"
    
    # Batch processing
    batch_size: int = Field(default=1000, ge=1, le=10000)


class ScrapingConfig(BaseModel):
    """Configuration for web scraping."""
    
    # Request settings
    request_timeout: int = Field(default=10, ge=1)
    retry_total: int = Field(default=3, ge=0)
    retry_backoff_factor: float = Field(default=1.0, ge=0)
    retry_status_codes: list[int] = [500, 502, 503, 504]
    
    # Rate limiting
    request_delay: float = Field(default=0.3, ge=0)
    
    # Review filtering
    min_helpful_votes: int = Field(default=1, ge=0)
    min_review_words: int = Field(default=100, ge=0)
    
    # GraphQL endpoint
    graphql_url: str = "https://caching.graphql.imdb.com/"
    
    # User agent (should be configurable, not hardcoded)
    user_agent: str = Field(
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

class LoggingConfig(BaseModel):
    """Configuration for logging."""
    
    log_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    log_file: Optional[Path] = None
    console_level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    file_level: str = Field(default="DEBUG", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")

class Settings(BaseSettings):
    """Main settings class that combines all configuration sections."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Supabase settings (from environment)
    project_url: str = Field(..., alias="PROJECT_URL")
    secret_key: str = Field(..., alias="SECRET_KEY")

    # OMDB API
    omdb_api_key: str = Field(..., alias="OMDB_API")
    
    # Sub-configurations
    imdb: IMDBDataConfig = Field(default_factory=IMDBDataConfig)
    supabase: SupabaseConfig = Field(default_factory=lambda: SupabaseConfig(
        project_url="",  # Will be set in __init__
        secret_key=""    # Will be set in __init__
    ))
    scraping: ScrapingConfig = Field(default_factory=ScrapingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    # Paths
    project_root: Path = Field(default_factory=lambda: Path(__file__).parent.parent)
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Set Supabase config from environment variables
        self.supabase.project_url = self.project_url
        self.supabase.secret_key = self.secret_key
    
    @property
    def log_file_path(self) -> Optional[Path]:
        """Get the full log file path."""
        if self.logging.log_file:
            if self.logging.log_file.is_absolute():
                return self.logging.log_file
            return self.project_root / self.logging.log_file
        return self.project_root / "logs" / "imdb_scraper.log"
    
    @property
    def export_file_path(self) -> Path:
        """Get the default export file path."""
        return self.project_root / "titles.xlsx"


# Global settings instance (singleton pattern)
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Reset settings (useful for testing)."""
    global _settings
    _settings = None