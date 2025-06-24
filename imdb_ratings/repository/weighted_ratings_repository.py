"""
Repository for weighted ratings database operations.
"""
import polars as pl
from imdb_ratings.repository.base import BaseRepository


class WeightedRatingsRepository(BaseRepository):
    """Repository for managing weighted ratings data in Supabase."""
    
    @property
    def table_name(self) -> str:
        return self.config.weighted_ratings_table
    
    def get_all_as_dataframe(self) -> pl.DataFrame:
        """
        Get all weighted ratings as a Polars DataFrame.
        
        Returns:
            DataFrame with proper schema for weighted ratings
        """
        data = self.fetch_all()
        
        return pl.DataFrame(
            data, 
            schema=pl.Schema({
                "id": pl.Int64,
                "weighted_rating": pl.Int16,
            })
        )