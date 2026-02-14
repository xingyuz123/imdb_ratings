"""
Pipeline Step 1: Update titles table with latest data from IMDB.
"""

from imdb_ratings.updater.sources.imdb_dataset import download_titles_from_imdb
from supabase import Client
from imdb_ratings import logger
from imdb_ratings.core.constants import VOTE_INCREASE_THRESHOLD
from imdb_ratings.core.database import get_database_client
from imdb_ratings.repository import TitleRepository
import polars as pl


def update_title_table(supabase_client: Client | None = None) -> None:
    """
    Update the title table with the latest data from IMDB.

    Downloads current IMDB datasets, compares with existing Supabase data,
    and upserts titles that are new or have significantly more votes.

    Args:
        supabase_client: Existing Supabase client or None to create a new one
    """
    logger.info("Starting title table update")
    title_df_from_imdb = download_titles_from_imdb()

    if supabase_client is None:
        supabase_client = get_database_client()

    title_repo = TitleRepository(supabase_client)
    title_df_from_supabase = title_repo.get_all_as_dataframe()
    titles_to_update = title_df_from_imdb

    if len(title_df_from_supabase) > 0:
        titles_to_update = titles_to_update.join(
            title_df_from_supabase.select(["id", "num_votes"]).rename({"num_votes": "num_votes_supabase"}),
            on="id",
            how="left"
        ).filter(
            (pl.col("num_votes_supabase").is_null()) |  # ID absent from Supabase
            (pl.col("num_votes") >= pl.col("num_votes_supabase") * VOTE_INCREASE_THRESHOLD)  # At least 5% more votes
        ).select(title_df_from_imdb.columns)

    titles_to_update = titles_to_update.with_columns(
        pl.lit(True).alias("needsUpdate")
    )

    title_repo.upsert_titles(titles_to_update)

    logger.info("Title table update completed successfully")
