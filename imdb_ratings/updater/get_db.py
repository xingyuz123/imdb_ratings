import polars as pl
from supabase import Client
from imdb_ratings.config import get_settings
from imdb_ratings import logger

def get_weighted_ratings_df(supabase_client: Client) -> pl.DataFrame:
    """
    Fetch all weighted ratings from Supabase.
    
    Args:
        supabase_client: Supabase client instance
        
    Returns:
        DataFrame with weighted ratings
    """
    settings = get_settings()
    config = settings.supabase

    weighted_ratings = []
    offset: int = 0

    logger.info(f"Fetching data from {config.weighted_ratings_table}")

    while True:
        new_weighted_ratings = (
            supabase_client.table(config.weighted_ratings_table)
            .select("*")
            .offset(offset)
            .limit(config.batch_size)
            .execute()
            .data
        )
        weighted_ratings.extend(new_weighted_ratings)
        offset += config.batch_size

        logger.debug(f"Fetched {len(weighted_ratings)} records so far")

        if len(new_weighted_ratings) < config.batch_size:
            break

    logger.info(f"Total records fetched: {len(weighted_ratings)}")
    
    weighted_ratings_df = pl.DataFrame(
        weighted_ratings, 
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
    return weighted_ratings_df

def get_title_df(supabase_client: Client) -> pl.DataFrame:
    """
    Fetch all titles from Supabase.
    
    Args:
        supabase_client: Supabase client instance
        
    Returns:
        DataFrame with titles
    """
    settings = get_settings()
    config = settings.supabase

    titles = []
    offset: int = 0         

    logger.info(f"Fetching data from {config.titles_table}")

    while True:
        new_titles = (
            supabase_client.table(config.titles_table)
            .select("*")
            .offset(offset)
            .limit(config.batch_size)
            .execute()
            .data
        )
        titles.extend(new_titles)
        offset += config.batch_size

        logger.debug(f"Fetched {len(titles)} records so far")

        if len(new_titles) < config.batch_size:
            break

    logger.info(f"Total titles fetched: {len(titles)}")
    
    titles_df = pl.DataFrame(titles)
    return titles_df