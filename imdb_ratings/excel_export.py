
import polars as pl
import xlsxwriter
from pathlib import Path
from imdb_ratings.updater.update_supabase import create_supabase_client
from imdb_ratings.updater.get_db import get_weighted_ratings_df
from supabase import Client

def export_to_excel(file_path: Path, supabase_client: Client):
    titles_df = get_weighted_ratings_df(supabase_client
        ).sort('bayesian_rating', descending=True
        ).drop_nulls("bayesian_rating"
        ).with_columns(pl.col("bayesian_rating").round(1)
        ).drop(["weighted_rating", "total_weight"]
        ).rename({"bayesian_rating": "weighted_rating"})

    movies_df = titles_df.filter(pl.col("isMovie")
        ).drop(["isMovie", "endYear"]
        ).rename({"startYear": "year"})

    shows_df = titles_df.filter(pl.col("isMovie") == False
        ).drop(["isMovie"])

    with xlsxwriter.Workbook(file_path) as writer:
        movies_df.write_excel(worksheet="movies", workbook=writer, float_precision=1)
        shows_df.write_excel(worksheet="shows", workbook=writer, float_precision=1)


if __name__ == "__main__":
    directory = Path(__file__).parent.parent
    supabase_client = create_supabase_client()
    export_to_excel(directory / "titles.xlsx", supabase_client)
