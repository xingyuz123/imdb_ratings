from imdb_ratings.movie_database import get_title_df
from imdb_ratings.scrape_reviews import create_requests_session, extract_reviews
import polars as pl
from supabase import Client, create_client
import time
from enum import Enum
import os
from imdb_ratings import logger
from dotenv import load_dotenv
from datetime import date, timedelta, datetime

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
    list_of_dicts: list[dict] = df.to_dicts()
    total_records: int = len(list_of_dicts)

    for i in range(0, total_records, batch_size):
        batch_dicts = list_of_dicts[i:i+batch_size]
        try:
            supabase_client.table(table_name.value).upsert(batch_dicts).execute()
            logger.info(f"{table_name.value}: Inserted batch {i//batch_size + 1} of {(total_records + batch_size - 1)//batch_size}")
            time.sleep(0.1)
        except Exception as e:
            logger.error(f"{table_name.value}: Error inserting batch {i//batch_size + 1}: {str(e)}")

def update_title_table(supabase_client: Client | None = None) -> None:
    """
    Update the title table with the latest data from IMDB.
    """
    df = get_title_df()

    if supabase_client is None:
        supabase_client = create_supabase_client()

    upsert_rows_into_table(TableName.TITLES, df, supabase_client)
    logger.info("Data update completed successfully")

def extract_title_ids(supabase_client: Client) -> set[int]:
    return set([id_dict["id"] for id_dict in supabase_client.table("titles").select("id").execute().data])

def extract_max_review_id(supabase_client: Client) -> int:
    review_ids = [review_dict["review_id"] for review_dict in supabase_client.table("reviews").select("review_id").execute().data]
    if not review_ids:
        return 0
    return max(review_ids)

def update_reviews_table(supabase_client: Client | None = None, batch_size: int = 100) -> None:
    """
    Updates reviews table
    """
    if supabase_client is None:
        supabase_client = create_supabase_client()
    
    requests_session = create_requests_session()
    review_date_threshold = date.today() - timedelta(days=30)
    
    start_id = extract_max_review_id(supabase_client) + 1
    logger.info(f"Starting from review id {start_id}")

    try:
        while True:
            reviews = extract_reviews(start_id=start_id, batch_size=batch_size, requests_session=requests_session)
            start_id += batch_size

            if not reviews:
                continue

            latest_review_date = max([datetime.strptime(review.date, "%Y-%m-%d").date() for review in reviews])
            logger.info(f"Latest review date: {latest_review_date}")
            if latest_review_date > review_date_threshold:
                logger.info(f"Reached reviews newer than threshold date {review_date_threshold}")
                break
            filtered_reviews = [
                review.model_dump() 
                for review in reviews 
                if review.rating is not None
            ]
            if filtered_reviews:
                logger.info(f"Found {len(filtered_reviews)} relevant reviews")
                df = pl.DataFrame(filtered_reviews)
                upsert_rows_into_table(TableName.REVIEWS, df, supabase_client)
    except Exception as e:
        logger.error(f"Error updating reviews: {str(e)}")
        raise
    finally:
        requests_session.close()
