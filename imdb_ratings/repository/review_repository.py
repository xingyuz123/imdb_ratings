"""
Repository for review-related database operations.
"""
import polars as pl
from imdb_ratings.repository.base import BaseRepository


class ReviewRepository(BaseRepository):
    """Repository for managing review data in Supabase."""
    
    @property
    def table_name(self) -> str:
        return self.config.reviews_table
    
    def upsert_reviews(self, reviews_df: pl.DataFrame) -> None:
        """
        Upsert reviews from a DataFrame.
        
        Args:
            reviews_df: DataFrame containing review data
        """
        if reviews_df.is_empty():
            return
        
        data = reviews_df.to_dicts()
        self.upsert_batch(data)