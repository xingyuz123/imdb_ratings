"""
IMDB Title Database Module

This module handles downloading and processing IMDB's public datasets to create
a curated database of movies and TV shows with significant user engagement.

The module downloads data from IMDB's public dataset repository and filters
for high-quality content based on vote count thresholds.
"""

import polars as pl
from imdb_ratings import logger

# Configuration constants
class IMDBConfig:
    """Configuration for IMDB data processing."""
    
    # Dataset URLs
    BASICS_URL: str = "https://datasets.imdbws.com/title.basics.tsv.gz"
    RATINGS_URL: str = "https://datasets.imdbws.com/title.ratings.tsv.gz"
    
    # Filtering thresholds
    MIN_VOTES: int = 15_000
    VALID_TITLE_TYPES: tuple[str, ...] = ("movie", "tvSeries", "tvMiniSeries")
    
    # Column configurations
    BASICS_COLUMNS: list[str] = [
        "tconst", "titleType", "primaryTitle", "isAdult", 
        "startYear", "endYear", "runtimeMinutes", "genres"
    ]
    RATINGS_COLUMNS: list[str] = ["tconst", "averageRating", "numVotes"]
    
    # Data processing
    NULL_VALUE: str = "\\N"
    GENRE_SEPARATOR: str = ","


class IMDBDataProcessor:
    """Handles downloading and processing of IMDB datasets."""
    
    def __init__(self, config: IMDBConfig | None = None):
        """
        Initialize the data processor.
        
        Args:
            config: Configuration object. If None, uses default configuration.
        """
        self.config = config or IMDBConfig()
    
    def download_title_df(self) -> pl.DataFrame:
        """
        Download and process IMDB title data with ratings.
        
        This method:
        1. Downloads basic title information from IMDB
        2. Filters for movies and TV series with sufficient engagement
        3. Downloads and joins rating information
        4. Returns a cleaned, processed DataFrame
        
        Returns:
            pl.DataFrame: Processed title data with the following columns:
                - id (Int64): IMDB title ID (without 'tt' prefix)
                - isMovie (Boolean): True for movies, False for TV series
                - primaryTitle (String): Title name
                - genres (List[String]): List of genres
                - startYear (Int16): Release/start year
                - endYear (Int16 | None): End year (for TV series)
                - imdb_rating (Float64): Average IMDB rating
                - num_votes (Int32): Number of votes
        
        Raises:
            pl.ComputeError: If data download or processing fails
            ValueError: If required columns are missing from datasets
        """
        logger.info("Starting IMDB data download and processing")
        
        try:
            basics_df = self._download_basics_data()
            basics_df = self._process_basics_data(basics_df)
            
            ratings_df = self._download_ratings_data()
            ratings_df = self._process_ratings_data(ratings_df)
            
            result_df = self._join_title_and_ratings(basics_df, ratings_df)
            
            logger.info(f"Successfully processed {len(result_df):,} titles")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to download and process IMDB data: {e}")
            raise
    
    def _download_basics_data(self) -> pl.DataFrame:
        """Download raw title basics data from IMDB."""
        logger.info("Fetching basic title data from IMDB")
        
        return pl.read_csv(
            self.config.BASICS_URL,
            separator="\t",
            quote_char=None,
            columns=self.config.BASICS_COLUMNS,
            null_values=[self.config.NULL_VALUE],
            use_pyarrow=True,
        )
    
    def _process_basics_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Process and filter basic title data.
        
        Args:
            df: Raw basics DataFrame
            
        Returns:
            Processed DataFrame with standardized columns and filtered content
        """
        logger.info("Processing basic title data")
        
        return (
            df
            # Filter for relevant title types and adult content
            .filter(
                pl.col("titleType").is_in(self.config.VALID_TITLE_TYPES)
                & (pl.col("isAdult") == 0)
                & pl.col("startYear").is_not_null()
                & pl.col("runtimeMinutes").is_not_null()
            )
            # Drop unnecessary columns
            .drop(["isAdult", "runtimeMinutes"])
            # Convert and rename columns
            .with_columns([
                # Extract numeric ID from tconst (remove 'tt' prefix)
                pl.col("tconst").str.replace("tt", "").cast(pl.Int64),
                # Convert titleType to boolean isMovie flag
                pl.col("titleType").eq("movie"),
                # Cast year columns
                pl.col("startYear").cast(pl.Int16),
                pl.col("endYear").cast(pl.Int16),
                # Split genres string into list
                pl.col("genres").str.split(self.config.GENRE_SEPARATOR)
            ])
            .rename({
                "tconst": "id",
                "titleType": "isMovie"
            })
        )
    
    def _download_ratings_data(self) -> pl.DataFrame:
        """Download raw ratings data from IMDB."""
        logger.info("Fetching ratings data from IMDB")
        
        return pl.read_csv(
            self.config.RATINGS_URL,
            separator="\t",
            quote_char=None,
            columns=self.config.RATINGS_COLUMNS,
            null_values=[self.config.NULL_VALUE],
            use_pyarrow=True,
        )
    
    def _process_ratings_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Process and filter ratings data.
        
        Args:
            df: Raw ratings DataFrame
            
        Returns:
            Processed DataFrame with vote threshold applied
        """
        logger.info(f"Processing ratings data (min votes: {self.config.MIN_VOTES:,})")
        
        return (
            df
            # Convert ID and apply vote threshold
            .with_columns(
                pl.col("tconst").str.replace("tt", "").cast(pl.Int64),
                pl.col("averageRating").mul(10).cast(pl.Int64)
            )
            .filter(pl.col("numVotes") >= self.config.MIN_VOTES)
            .rename({
                "tconst": "id",
                "averageRating": "imdb_rating",
                "numVotes": "num_votes"
            })
        )
    
    def _join_title_and_ratings(
        self, 
        basics_df: pl.DataFrame, 
        ratings_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Join title and ratings data.
        
        Args:
            basics_df: Processed basics DataFrame
            ratings_df: Processed ratings DataFrame
            
        Returns:
            Joined DataFrame containing only titles with ratings
        """
        logger.info("Joining title and ratings data")
        
        # Inner join ensures we only keep titles that have ratings
        # with sufficient votes
        return basics_df.join(ratings_df, on="id", how="inner")


def download_title_df() -> pl.DataFrame:
    """
    Convenience function to download IMDB title data using default configuration.
    
    Returns:
        pl.DataFrame: Processed IMDB title data
        
    Example:
        >>> df = download_title_df()
        >>> print(f"Downloaded {len(df)} titles")
        >>> print(df.select(["primaryTitle", "imdb_rating"]).head())
    """
    processor = IMDBDataProcessor()
    return processor.download_title_df()