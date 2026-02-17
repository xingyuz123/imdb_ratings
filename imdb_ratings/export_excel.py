"""
Pipeline Step 5: Export ratings data to Excel.
"""

import polars as pl
import xlsxwriter
from pathlib import Path
from imdb_ratings.core.database import get_database_client
from imdb_ratings.core.config import get_settings
from imdb_ratings import logger
from imdb_ratings.repository import WeightedRatingsRepository, TitleRepository
from supabase import Client

def export_to_excel(file_path: Path | None = None, supabase_client: Client | None = None):
    """
    Export IMDB ratings data to Excel file.

    Args:
        file_path: Output file path (uses config default if None)
        supabase_client: Supabase client (creates new if None)
    """
    settings = get_settings()

    if file_path is None:
        file_path = settings.export_file_path

    # Ensure file directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting Excel export to {file_path}")

    if supabase_client is None:
        supabase_client = get_database_client()

    weighted_ratings_repo = WeightedRatingsRepository(supabase_client)
    weighted_ratings_df = weighted_ratings_repo.get_all_as_dataframe()

    title_repo = TitleRepository(supabase_client)
    titles_df = title_repo.get_all_as_dataframe()

    combined_df = (
        titles_df
        .filter(pl.col("firstWorld") == True)
        .join(weighted_ratings_df, on="id", how="inner")
        .sort('weighted_rating', descending=True)
        .drop_nulls("weighted_rating")
    )

    logger.info(f"Processing {len(combined_df)} titles for export")

    export_columns = ["id", "primaryTitle", "genres", "startYear", "endYear", "imdb_rating", "isMovie", "weighted_rating"]

    movies_df = (
        combined_df
        .select(export_columns)
        .filter(pl.col("isMovie"))
        .drop(["isMovie", "endYear", "id"])
        .rename({"startYear": "year"})
    )

    shows_df = (
        combined_df
        .select(export_columns)
        .filter(~pl.col("isMovie"))
        .drop(["isMovie", "id"])
    )

    logger.info(f"Exporting {len(movies_df)} movies and {len(shows_df)} shows")

    with xlsxwriter.Workbook(file_path) as writer:
        movies_df.write_excel(worksheet="movies", workbook=writer, float_precision=1)
        shows_df.write_excel(worksheet="shows", workbook=writer, float_precision=1)

    logger.info(f"Excel export completed successfully to {file_path}")

if __name__ == "__main__":
    export_to_excel()
