
import polars as pl
import xlsxwriter
from pathlib import Path
from imdb_ratings.database import get_database_client
from imdb_ratings.config import get_settings
from imdb_ratings import logger
from imdb_ratings.repository import WeightedRatingsRepository
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

    titles_df = (
        weighted_ratings_df
        .sort('bayesian_rating', descending=True)
        .drop_nulls("bayesian_rating")
        .with_columns(pl.col("bayesian_rating").round(1))
        .drop(["weighted_rating", "total_weight"])
        .rename({"bayesian_rating": "weighted_rating"})
    )

    logger.info(f"Processing {len(titles_df)} titles for export")

    movies_df = (
        titles_df.filter(pl.col("isMovie"))
        .drop(["isMovie", "endYear"])
        .rename({"startYear": "year"})
    )

    shows_df = (
        titles_df.filter(~pl.col("isMovie"))
        .drop(["isMovie"])
    )

    logger.info(f"Exporting {len(movies_df)} movies and {len(shows_df)} shows")

    with xlsxwriter.Workbook(file_path) as writer:
        movies_df.write_excel(worksheet="movies", workbook=writer, float_precision=1)
        shows_df.write_excel(worksheet="shows", workbook=writer, float_precision=1)

    logger.info(f"Excel export completed successfully to {file_path}")

if __name__ == "__main__":
    export_to_excel()
