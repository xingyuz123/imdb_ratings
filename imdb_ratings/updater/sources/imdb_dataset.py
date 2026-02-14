"""
IMDB Dataset Module

This module handles downloading and processing IMDB's public datasets to create
a curated database of movies and TV shows with significant user engagement.
"""

import polars as pl
from imdb_ratings import logger
from imdb_ratings.core.config import get_settings, IMDBDataConfig
from imdb_ratings.core.constants import IMDB_TITLE_ID_PREFIX, IMDB_RATING_MULTIPLIER


class IMDBDataProcessor:
    """Handles downloading and processing of IMDB datasets."""

    def __init__(self, config: IMDBDataConfig | None = None):
        """
        Initialize the data processor.

        Args:
            config: Configuration object. If None, uses default configuration.
        """
        if config is None:
            settings = get_settings()
            config = settings.imdb
        self.config = config

    def download_title_df(self) -> pl.DataFrame:
        """
        Download and process IMDB title data with ratings.
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
        logger.info(f"Fetching basic title data from {self.config.basics_url}")

        columns = [
            "tconst", "titleType", "primaryTitle", "isAdult",
            "startYear", "endYear", "runtimeMinutes", "genres"
        ]

        return pl.read_csv(
            self.config.basics_url,
            separator="\t",
            quote_char=None,
            columns=columns,
            null_values=[self.config.null_value],
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
                pl.col("titleType").is_in(self.config.valid_title_types)
                & (pl.col("isAdult") == 0)
                & pl.col("startYear").is_not_null()
                & pl.col("runtimeMinutes").is_not_null()
            )
            # Drop unnecessary columns
            .drop(["isAdult", "runtimeMinutes"])
            # Convert and rename columns
            .with_columns([
                pl.col("tconst").str.replace(IMDB_TITLE_ID_PREFIX, "").cast(pl.Int64),
                pl.col("titleType").eq("movie"),
                pl.col("startYear").cast(pl.Int16),
                pl.col("endYear").cast(pl.Int16),
                pl.col("genres").str.split(self.config.genre_separator)
            ])
            .rename({
                "tconst": "id",
                "titleType": "isMovie"
            })
        )

    def _download_ratings_data(self) -> pl.DataFrame:
        """Download raw ratings data from IMDB."""
        logger.info(f"Fetching ratings data from {self.config.ratings_url}")

        columns = ["tconst", "averageRating", "numVotes"]

        return pl.read_csv(
            self.config.ratings_url,
            separator="\t",
            quote_char=None,
            columns=columns,
            null_values=[self.config.null_value],
            use_pyarrow=True,
        )

    def _process_ratings_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Process and filter ratings data.
        """
        logger.info(f"Processing ratings data (min votes: {self.config.min_votes:,})")

        return (
            df
            .with_columns(
                pl.col("tconst").str.replace(IMDB_TITLE_ID_PREFIX, "").cast(pl.Int64),
                pl.col("averageRating").mul(IMDB_RATING_MULTIPLIER).cast(pl.Int64)
            )
            .filter(pl.col("numVotes") >= self.config.min_votes)
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
        """
        logger.info("Joining title and ratings data")
        return basics_df.join(ratings_df, on="id", how="inner")


def download_titles_from_imdb() -> pl.DataFrame:
    """
    Convenience function to download IMDB title data using default configuration.

    Returns:
        pl.DataFrame: Processed IMDB title data
    """
    processor = IMDBDataProcessor()
    return processor.download_title_df()
