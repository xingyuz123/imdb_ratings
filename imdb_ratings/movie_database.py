import polars as pl
from imdb_ratings import logger


def get_title_df():
    """
    Fetches and filters IMDB title data.
    Returns a dataframe with show/movie information, excluding adult titles and titles with null startYear or runtimeMinutes.
    """

    logger.info("Fetching basic title data")

    basics_df = pl.read_csv(
        "https://datasets.imdbws.com/title.basics.tsv.gz",
        separator="\t",
        quote_char=None,
        columns=["tconst", "titleType", "primaryTitle", "isAdult", "startYear", "endYear", "runtimeMinutes"],
        null_values=["\\N"],
        use_pyarrow=True,
    ).filter((pl.col("titleType").is_in(["movie", "tvSeries"])) & 
             (pl.col("isAdult") == 0) & 
             (pl.col("startYear").is_not_null()) & 
             (pl.col("runtimeMinutes").is_not_null())
    ).drop(["isAdult", "runtimeMinutes"]
    ).with_columns(
        pl.col("tconst").str.replace("tt", "").cast(pl.Int64()),
        pl.col("titleType").eq("movie"),
        pl.col("startYear").cast(pl.Int16()),
        pl.col("endYear").cast(pl.Int16())
    ).rename({"tconst": "id", "titleType": "isMovie"})

    logger.info("Fetching ratings data")

    ratings_df = pl.read_csv(
        "https://datasets.imdbws.com/title.ratings.tsv.gz",
        separator="\t",
        quote_char=None,
        columns=["tconst", "averageRating", "numVotes"],
        null_values=["\\N"],
        use_pyarrow=True,
    ).with_columns(
        pl.col("tconst").str.replace("tt", "").cast(pl.Int64()),
    ).filter(pl.col("numVotes") > 1000
    ).rename({"tconst": "id", "averageRating": "imdb_rating"}
    ).drop(["numVotes"])

    return basics_df.join(ratings_df, on="id", how="inner")
