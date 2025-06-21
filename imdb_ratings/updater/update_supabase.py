from imdb_ratings.updater.movie_database import download_titles_from_imdb
from imdb_ratings.updater.scrape_reviews import create_requests_session, get_reviews_from_title_code
from imdb_ratings.config import get_settings
from supabase import Client, create_client
from imdb_ratings import logger
from typing import Any
import time
import polars as pl

def create_supabase_client() -> Client:
    """Create a Supabase client using configuration."""
    settings = get_settings()
    config = settings.supabase
    return create_client(config.project_url, config.secret_key)


def upsert_rows_into_table(
    table_name: str, 
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
            supabase_client.table(table_name).upsert(batch_dicts).execute()
            logger.info(f"Successfully inserted batch {i//batch_size + 1}/{(total_records + batch_size - 1)//batch_size}")
            time.sleep(settings.scraping.request_delay)
        except Exception as e:
            logger.error(f"{table_name}: Error inserting batch {i//batch_size + 1}: {str(e)}")
            raise

def download_titles_from_database(supabase_client: Client) -> pl.DataFrame:
    """
    Download the entire titles database from Supabase.
    
    Args:
        supabase_client: Supabase client instance
        
    Returns:
        DataFrame containing all titles from the database
    """
    settings = get_settings()
    offset: int = 0
    all_data: list[dict[str, Any]] = []
    batch_size: int = settings.supabase.batch_size
    
    logger.info("Downloading entire titles database from Supabase")

    while True:
        batch_data = supabase_client.table("titles") \
            .select("*") \
            .offset(offset) \
            .limit(batch_size) \
            .execute() \
            .data
        
        all_data.extend(batch_data)
        offset += batch_size
        
        if len(batch_data) < batch_size:
            break

    return pl.DataFrame(all_data, infer_schema_length=None)

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

    title_df_from_supabase = download_titles_from_database(supabase_client)

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

    upsert_rows_into_table("titles", titles_to_update, supabase_client)
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
    
    requests_session = create_requests_session()

    if titles_to_update is None:
        titles_df_from_supabase = download_titles_from_database(supabase_client)
        titles_to_update = titles_df_from_supabase.filter(
            pl.col("needsUpdate")
        ).select("id").to_series().to_list()
        logger.info(f"Found {len(titles_to_update)} titles to process")

    try:
        for i, title_id in enumerate(titles_to_update):
            title_code = f"tt{title_id:07d}"
            logger.info(f"Processing reviews for {i+1}/{len(titles_to_update)} titles: {title_code}")

            try:
                review_df = get_reviews_from_title_code(title_code, requests_session)
                if not review_df.is_empty():
                    upsert_rows_into_table("reviews", review_df, supabase_client)
                    logger.info(f"Inserted {len(review_df)} reviews for {title_code}")
                    
                    # Update needsUpdate to False in titles table after successful review insertion
                    try:
                        supabase_client.table("titles").update({"needsUpdate": False}).eq("id", title_id).execute()
                        logger.debug(f"Updated needsUpdate to False for title {title_id}")
                    except Exception as update_error:
                        logger.error(f"Error updating needsUpdate for title {title_id}: {str(update_error)}")
                        # Continue processing other titles even if this update fails
                        
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
