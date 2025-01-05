import polars as pl
from supabase import Client

def get_weighted_ratings_df(supabase_client: Client) -> pl.DataFrame:
    weighted_ratings = []
    offset: int = 0
    while True:
        new_weighted_ratings = supabase_client.table("weighted_ratings").select("*").offset(offset).limit(1000).execute().data
        weighted_ratings.extend(new_weighted_ratings)
        offset += 1000
        if len(new_weighted_ratings) < 1000:
            break
    weighted_ratings_df = pl.DataFrame(weighted_ratings, schema=
        pl.Schema({
            "id": pl.Int64,
            "isMovie": pl.Boolean,
            "primaryTitle": pl.String,
            "startYear": pl.Int16,
            "endYear": pl.Int16,
            "num_votes": pl.Int32,
            "imdb_rating": pl.Float32,
            "weighted_rating": pl.Float32,
            "total_weight": pl.Float32,
            "bayesian_rating": pl.Float32,
        })
    )
    return weighted_ratings_df

def get_title_df(supabase_client: Client) -> pl.DataFrame:
    titles = []
    offset: int = 0         
    while True:
        new_titles = supabase_client.table("titles").select("*").offset(offset).limit(1000).execute().data
        titles.extend(new_titles)
        offset += 1000
        if len(new_titles) < 1000:
            break
    titles_df = pl.DataFrame(titles)
    return titles_df