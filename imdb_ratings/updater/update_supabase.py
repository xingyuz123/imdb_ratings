from imdb_ratings.updater.movie_database import download_title_df
from imdb_ratings.updater.scrape_reviews import create_requests_session, get_reviews_from_title_code
from supabase import Client, create_client
from enum import Enum
from imdb_ratings import logger
from dotenv import load_dotenv
from typing import Any
import os
import time
import polars as pl

load_dotenv()

class TableName(Enum):
    TITLES = "titles"
    REVIEWS = "reviews"

def create_supabase_client() -> Client:
    project_url: str = os.getenv("PROJECT_URL", "")
    secret_key: str = os.getenv("SECRET_KEY", "")
    if not project_url or not secret_key:
        logger.error("Missing required environment variables")
        raise ValueError("Missing required environment variables")
    return create_client(project_url, secret_key)


def upsert_rows_into_table(table_name: TableName, df: pl.DataFrame, supabase_client: Client, batch_size: int = 1000) -> None:
    """
    This function inserts the dataframe into the database in batches.
    """
    list_of_dicts: list[dict[str, Any]] = df.to_dicts()
    total_records: int = len(list_of_dicts)

    for i in range(0, total_records, batch_size):
        batch_dicts: list[dict[str, Any]] = list_of_dicts[i:i+batch_size]
        try:
            supabase_client.table(table_name.value).upsert(batch_dicts).execute()
            time.sleep(1)
        except Exception as e:
            logger.error(f"{table_name.value}: Error inserting batch {i//batch_size + 1}: {str(e)}")

def update_title_table(supabase_client: Client | None = None) -> None:
    """
    Update the title table with the latest data from IMDB.
    """
    df = download_title_df()

    if supabase_client is None:
        supabase_client = create_supabase_client()

    upsert_rows_into_table(TableName.TITLES, df, supabase_client)
    logger.info("Data update completed successfully")

def extract_title_ids(table_name: TableName, supabase_client: Client) -> set[int]:
    id_name: str = "id" if table_name == TableName.TITLES else "title_id"
    table_name_str: str = "distinct_title_id_from_review" if table_name == TableName.REVIEWS else "distinct_title_id"
    offset: int = 0
    title_ids: set[int] = set()
    
    while True:
        new_ids = [
            int(id_dict[id_name])
            for id_dict in supabase_client.table(table_name_str)
            .select(id_name)
            .offset(offset)
            .limit(1000)
            .execute()
            .data
        ]
        title_ids.update(new_ids)
        offset += 1000
        if len(new_ids) < 1000:
            break

    return title_ids

def update_reviews_table(supabase_client: Client | None = None, titles_to_update: list[int] | None = None) -> None:
    """
    Updates reviews table
    """
    if supabase_client is None:
        supabase_client = create_supabase_client()
    
    requests_session = create_requests_session()

    if titles_to_update is None:
        all_title_ids = extract_title_ids(TableName.TITLES, supabase_client)
        processed_title_ids = extract_title_ids(TableName.REVIEWS, supabase_client)
        titles_to_update = sorted(all_title_ids - processed_title_ids)

    try:
        for i, title_id in enumerate(titles_to_update):
            title_code = f"tt{title_id:07d}"
            logger.info(f"Extracting reviews for {i+1}/{len(titles_to_update)} titles: {title_code}")
            review_df = get_reviews_from_title_code(title_code, requests_session)
            upsert_rows_into_table(TableName.REVIEWS, review_df, supabase_client)

    except Exception as e:
        logger.error(f"Error updating reviews: {str(e)}")
        raise
    finally:
        requests_session.close()
