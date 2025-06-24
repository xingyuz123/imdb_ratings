"""
Module to handle weighted ratings table updates.
"""

from imdb_ratings import logger
from imdb_ratings.database import get_database_client
from imdb_ratings.exceptions import DatabaseOperationError
from supabase import Client

def update_weighted_ratings_table(supabase_client: Client | None = None):
    """
    Update the weighted ratings table by calling the Supabase stored procedure.

    This recalculates all weighted ratings based on current reviews and stores
    them in the weighted_ratings table for fast access.

    Args:
        supabase_client: Existing Supabase client or None to create a new one.
    """
    logger.info("Starting weighted ratings table update")
    
    if supabase_client is None:
        supabase_client = get_database_client()

    try:
        result = supabase_client.rpc("update_weighted_ratings").execute()

        if hasattr(result, "data"):
            logger.info("Weighted ratings table update completed successfully")
        else:
            logger.error("No response from update_weighted_ratings function")
            raise DatabaseOperationError("No response from update_weighted_ratings function")
    except Exception as e:
        logger.error(f"Failed to update weighted ratings: {e}")
        raise DatabaseOperationError(f"Failed to update weighted ratings: {e}")