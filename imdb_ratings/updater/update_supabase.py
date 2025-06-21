from imdb_ratings.updater.movie_database import download_title_df
from imdb_ratings.updater.scrape_reviews import create_requests_session, get_reviews_from_title_code
from imdb_ratings.config import get_settings
from supabase import Client, create_client
from enum import Enum
from imdb_ratings import logger
from typing import Any
import time
import polars as pl

class TableName(Enum):
    TITLES = "titles"
    REVIEWS = "reviews"

def create_supabase_client() -> Client:
    """Create a Supabase client using configuration."""
    settings = get_settings()
    config = settings.supabase
    return create_client(config.project_url, config.secret_key)


def upsert_rows_into_table(
    table_name: TableName, 
    df: pl.DataFrame, 
    supabase_client: Client, 
    batch_size: int | None = None
) -> None:
    """
    Insert the dataframe into the database in batches.
    
    Args:
        table_name: The table to insert into
        df: DataFrame to insert
        supabase_client: Supabase client instance
        batch_size: Batch size for insertion (uses config default if None)
    """
    settings = get_settings()

    if batch_size is None:
        batch_size = settings.supabase.batch_size

    list_of_dicts: list[dict[str, Any]] = df.to_dicts()
    total_records: int = len(list_of_dicts)

    for i in range(0, total_records, batch_size):
        batch_dicts: list[dict[str, Any]] = list_of_dicts[i:i+batch_size]
        try:
            supabase_client.table(table_name.value).upsert(batch_dicts).execute()
            logger.info(f"Successfully inserted batch {i//batch_size + 1}/{(total_records + batch_size - 1)//batch_size}")
            time.sleep(settings.scraping.request_delay)
        except Exception as e:
            logger.error(f"{table_name.value}: Error inserting batch {i//batch_size + 1}: {str(e)}")
            raise

def update_title_table(supabase_client: Client | None = None) -> None:
    """
    Update the title table with the latest data from IMDB.
    
    Args:
        supabase_client: Existing Supabase client or None to create a new one
    """
    logger.info("Starting title table update")
    df = download_title_df()

    if supabase_client is None:
        supabase_client = create_supabase_client()

    upsert_rows_into_table(TableName.TITLES, df, supabase_client)
    logger.info("Title table update completed successfully")

def extract_title_ids(table_name: TableName, supabase_client: Client) -> set[int]:
    """
    Extract all title IDs from a given table.
    
    Args:
        table_name: The table to extract IDs from
        supabase_client: Supabase client instance
        
    Returns:
        Set of title IDs
    """
    settings = get_settings()
    id_name: str = "id" if table_name == TableName.TITLES else "title_id"
    table_name_str: str = "reviews" if table_name == TableName.REVIEWS else "titles"
    offset: int = 0
    title_ids: set[int] = set()
    batch_size: int = settings.supabase.batch_size
    
    logger.info(f"Extracting title IDs from {table_name_str}")

    while True:
        new_ids = [
            int(id_dict[id_name])
            for id_dict in supabase_client.table(table_name_str)
            .select(id_name)
            .offset(offset)
            .limit(batch_size)
            .execute()
            .data
        ]
        title_ids.update(new_ids)
        offset += batch_size
        if len(new_ids) < batch_size:
            break

    return title_ids

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
    
    requests_session = create_requests_session()

    if titles_to_update is None:
        all_title_ids = extract_title_ids(TableName.TITLES, supabase_client)
        processed_title_ids = extract_title_ids(TableName.REVIEWS, supabase_client)
        titles_to_update = sorted(all_title_ids - processed_title_ids)
        logger.info(f"Found {len(titles_to_update)} titles to process")

    try:
        for i, title_id in enumerate(titles_to_update):
            title_code = f"tt{title_id:07d}"
            logger.info(f"Processing reviews for {i+1}/{len(titles_to_update)} titles: {title_code}")

            try:
                review_df = get_reviews_from_title_code(title_code, requests_session)
                if not review_df.is_empty():
                    upsert_rows_into_table(TableName.REVIEWS, review_df, supabase_client)
                    logger.info(f"Inserted {len(review_df)} reviews for {title_code}")
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
