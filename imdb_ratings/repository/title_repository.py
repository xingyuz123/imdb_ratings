"""
Repository for title-related database operations.
"""
import polars as pl
from imdb_ratings.repository.base import BaseRepository
from imdb_ratings import logger

class TitleRepository(BaseRepository):
    """Repository for managing title data in Supabase."""

    @property
    def table_name(self) -> str:
        return self.config.titles_table
    
    def get_all_as_dataframe(self) -> pl.DataFrame:
        """
        Get all titles as a Polars DataFrame.

        Returns:
            DataFrame containing all titles.
        """
        data = self.fetch_all()
        return pl.DataFrame(data, infer_schema_length=None)
    
    def get_titles_needing_update(self) -> list[int]:
        """
        Get IDs of titles that need review updates.

        Returns:
            List of title IDs marked as needing update.
        """
        # Supabase columns argument is a string of comma-separated column names
        data = self.fetch_all(columns="id, needsUpdate, firstWorld")
        df = pl.DataFrame(data, infer_schema_length=None)

        if df.is_empty():
            return []
        
        return (
            df.filter(pl.col("needsUpdate") & pl.col("firstWorld"))
            .select("id")
            .to_series()
            .to_list()
        )
    
    def get_titles_needing_first_world_update(self) -> list[int]:
        """
        Get IDs of titles that need firstWorld updates.
        """
        data = self.fetch_all(columns="id, firstWorld")
        df = pl.DataFrame(data, infer_schema_length=None)

        if df.is_empty():
            return []
        
        return sorted(
            df.filter(pl.col("firstWorld").is_null())
            .select("id")
            .to_series()
            .to_list()
        )
    
    def mark_titles_updated(self, title_id: int) -> None:
        """
        Mark a title as no longer needing update.

        Args:
            title_id: ID of the title to mark as updated.
        """
        self.update(
            data={"needsUpdate": False},
            filters={"id": title_id}
        )
        logger.debug(f"Marked title {title_id} as updated")

    def upsert_titles(self, titles_df: pl.DataFrame) -> None:
        """
        Upsert titles from a DataFrame.

        Args:
            titles_df: DataFrame containing title data.
        """
        data = titles_df.to_dicts()
        self.upsert_batch(data)