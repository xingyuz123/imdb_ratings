"""
Pipeline Step 3: Scrape and update reviews for titles needing updates.
"""

from imdb_ratings.updater.sources.scrape_reviews import create_requests_session, get_reviews_from_title_code
from imdb_ratings.utils import format_imdb_id
from supabase import Client
from imdb_ratings import logger
from imdb_ratings.core.database import get_database_client
from imdb_ratings.repository import TitleRepository, ReviewRepository


def update_reviews_table(supabase_client: Client | None = None, titles_to_update: list[int] | None = None) -> None:
    """
    Updates reviews table by scraping IMDB for review data.

    Args:
        supabase_client: Existing Supabase client or None to create a new one
        titles_to_update: List of title IDs to update, or None to update all missing ones
    """
    logger.info("Starting reviews table update")

    if supabase_client is None:
        supabase_client = get_database_client()

    title_repo = TitleRepository(supabase_client)
    review_repo = ReviewRepository(supabase_client)

    requests_session = create_requests_session()

    if titles_to_update is None:
        titles_to_update = title_repo.get_titles_needing_update()
        logger.info(f"Found {len(titles_to_update)} titles to process")

    try:
        for i, title_id in enumerate(titles_to_update):
            title_code = format_imdb_id(title_id)
            logger.info(f"Processing reviews for {i+1}/{len(titles_to_update)} titles: {title_code}")

            try:
                review_df = get_reviews_from_title_code(title_code, requests_session)
                if not review_df.is_empty():
                    review_repo.upsert_reviews(review_df)
                    title_repo.mark_titles_updated(title_id)
                else:
                    logger.debug(f"No reviews found for {title_code}")
            except Exception as e:
                logger.error(f"Error processing {title_code}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Error updating reviews: {str(e)}")
        raise
    finally:
        requests_session.close()

    logger.info("Reviews table update completed")
