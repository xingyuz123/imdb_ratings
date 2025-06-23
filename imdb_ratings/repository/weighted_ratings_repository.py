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
                "isMovie": pl.Boolean,
                "primaryTitle": pl.String,
                "genres": pl.List(pl.String),
                "startYear": pl.Int16,
                "endYear": pl.Int16,
                "num_votes": pl.Int32,
                "imdb_rating": pl.Float32,
                "weighted_rating": pl.Float32,
                "total_weight": pl.Float32,
                "bayesian_rating": pl.Float32,
            })
        )