"""
Pipeline Step 2: Update firstWorld column in titles table using OMDB API.
"""

import time
from imdb_ratings import logger
from imdb_ratings.core.config import get_settings
from imdb_ratings.repository import TitleRepository
from imdb_ratings.core.exceptions import NetworkError
from imdb_ratings.core.database import get_database_client
from imdb_ratings.updater.sources.omdb_client import OMDBClient
from imdb_ratings.utils import format_imdb_id
from supabase import Client

NON_FIRST_WORLD_COUNTRIES = {
    "Argentina", "Bangladesh", "Brazil", "Bulgaria", "Chile",
    "Colombia", "Egypt", "Federal Republic of Yugoslavia", "India",
    "Indonesia", "Iran", "Kazakhstan", "Mexico",
    "Occupied Palestinian Territory", "Pakistan", "Philippines",
    "Romania", "Russia", "Saudi Arabia", "Serbia", "South Africa",
    "Soviet Union", "Sri Lanka", "Thailand", "Turkey", "Yugoslavia"
}


def determine_first_world_status(country_string: str | None) -> bool | None:
    """
    Determine if a title is from a first world country.

    Args:
        country_string: Comma-separated string of countries or None

    Returns:
        True if any country is first world, False if all are non-first world,
        None if no data
    """
    if not country_string:
        return None

    countries = [country.strip() for country in country_string.split(',')]

    all_non_first_world = all(
        country in NON_FIRST_WORLD_COUNTRIES
        for country in countries
    )

    return not all_non_first_world


def update_first_world_status(
    supabase_client: Client | None = None,
    delay_between_calls: float = 0.1
) -> None:
    """
    Update firstWorld column in titles table using OMDB API.

    Args:
        supabase_client: Existing Supabase client or None to create a new one
        delay_between_calls: Delay between API calls in seconds
    """
    logger.info("Starting firstWorld column update")

    api_key = get_settings().omdb_api_key
    if not api_key:
        logger.warning("OMDB API key not found. Skipping firstWorld update.")
        raise ValueError("OMDB API key not found")

    if supabase_client is None:
        supabase_client = get_database_client()

    title_repo = TitleRepository(supabase_client)
    omdb_client = OMDBClient(api_key)

    try:
        logger.info("Fetching all titles with missing firstWorld data")
        titles_to_update = title_repo.get_titles_needing_first_world_update()

        logger.info(f"Found {len(titles_to_update)} titles to update")

        if not titles_to_update:
            logger.info("No titles need firstWorld update")
            return

        updated_count = 0
        error_count = 0

        total = len(titles_to_update)

        for i, title_id in enumerate(titles_to_update, 1):
            imdb_id = format_imdb_id(title_id)

            try:
                movie_data = omdb_client.get_movie_data(imdb_id)

                if movie_data:
                    country_string = movie_data.get('Country')
                    first_world_status = determine_first_world_status(country_string)

                    if first_world_status is not None:
                        title_repo.update(
                            data={"firstWorld": first_world_status},
                            filters={"id": title_id}
                        )
                        updated_count += 1
                        logger.info(f"({i} / {total}) Updated {imdb_id} firstWorld={first_world_status}")
                    else:
                        logger.warning(f"({i} / {total}) No country data for {imdb_id}")
                        error_count += 1
                else:
                    error_count += 1

            except NetworkError:
                error_count += 1
                logger.error(f"({i} / {total}) Skipping {imdb_id} due to network error")
                continue

            time.sleep(delay_between_calls)

        logger.info(f"firstWorld update completed. Updated: {updated_count}, Errors: {error_count}")

    finally:
        omdb_client.close()
