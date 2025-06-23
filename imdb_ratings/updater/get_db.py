import polars as pl
from supabase import Client
from imdb_ratings.repository import WeightedRatingsRepository, TitleRepository

def get_weighted_ratings_df(supabase_client: Client) -> pl.DataFrame:
    """
    Fetch all weighted ratings from Supabase.
    
    Args:
        supabase_client: Supabase client instance
        
    Returns:
        DataFrame with weighted ratings
    """
    weighted_ratings_repo = WeightedRatingsRepository(supabase_client)
    weighted_ratings_df = weighted_ratings_repo.get_all_as_dataframe()
    return weighted_ratings_df

def get_title_df(supabase_client: Client) -> pl.DataFrame:
    """
    Fetch all titles from Supabase.
    
    Args:
        supabase_client: Supabase client instance
        
    Returns:
        DataFrame with titles
    """
    title_repo = TitleRepository(supabase_client)
    return title_repo.get_all_as_dataframe()