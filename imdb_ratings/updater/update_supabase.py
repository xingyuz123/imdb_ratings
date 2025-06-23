from imdb_ratings.updater.movie_database import download_titles_from_imdb
from imdb_ratings.updater.scrape_reviews import create_requests_session, get_reviews_from_title_code
from imdb_ratings.config import get_settings
from supabase import Client, create_client
from imdb_ratings import logger
from imdb_ratings.repository import TitleRepository, ReviewRepository
import time
import polars as pl

def create_supabase_client() -> Client:
    """Create a Supabase client using configuration."""
    settings = get_settings()
    config = settings.supabase
    return create_client(config.project_url, config.secret_key)


def update_title_table(supabase_client: Client | None = None) -> None:
    """
    Update the title table with the latest data from IMDB.
    
    Args:
        supabase_client: Existing Supabase client or None to create a new one
    """
    logger.info("Starting title table update")
    title_df_from_imdb = download_titles_from_imdb()

    if supabase_client is None:
        supabase_client = create_supabase_client()

    title_repo = TitleRepository(supabase_client)
    title_df_from_supabase = title_repo.get_all_as_dataframe()
    titles_to_update = title_df_from_imdb

    if len(title_df_from_supabase) > 0:
        titles_to_update = title_df_from_imdb.join(
            title_df_from_supabase.select(["id", "num_votes"]).rename({"num_votes": "num_votes_supabase"}),
            on="id",
            how="left"
        ).filter(
            (pl.col("num_votes_supabase").is_null()) |  # ID absent from Supabase
            (pl.col("num_votes") >= pl.col("num_votes_supabase") * 1.05)  # At least 5% more votes
        ).select(title_df_from_imdb.columns)

    titles_to_update = titles_to_update.with_columns(
        pl.lit(True).alias("needsUpdate")
    )

    title_repo.upsert_titles(titles_to_update)
    logger.info("Title table update completed successfully")

def update_reviews_table(supabase_client: Client | None = None, titles_to_update: list[int] | None = None) -> None:
    """
    Updates reviews table.
    
    Args:
        supabase_client: Existing Supabase client or None to create a new one
        titles_to_update: List of title IDs to update, or None to update all missing ones
    """
    logger.info("Starting reviews table update")

    if supabase_client is None:
        supabase_client = create_supabase_client()
    
    title_repo = TitleRepository(supabase_client)
    review_repo = ReviewRepository(supabase_client)

    requests_session = create_requests_session()

    if titles_to_update is None:
        titles_to_update = title_repo.get_titles_needing_update()
        logger.info(f"Found {len(titles_to_update)} titles to process")

    try:
        for i, title_id in enumerate(titles_to_update):
            title_code = f"tt{title_id:07d}"
            logger.info(f"Processing reviews for {i+1}/{len(titles_to_update)} titles: {title_code}")

            try:
                review_df = get_reviews_from_title_code(title_code, requests_session)
                if not review_df.is_empty():
                    review_repo.upsert_reviews(review_df) 
                else:
                    logger.debug(f"No reviews found for {title_code}")
                title_repo.mark_titles_updated(title_id)
            except Exception as e:
                logger.error(f"Error processing {title_code}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error updating reviews: {str(e)}")
        raise
    finally:
        requests_session.close()

    logger.info("Reviews table update completed")
