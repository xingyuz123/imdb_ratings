"""
Module to update firstWorld column in titles table using OMDB API.
"""

import os
import requests
import time
from typing import Any
from imdb_ratings import logger
from imdb_ratings.repository import TitleRepository
from imdb_ratings.exceptions import NetworkError

# Non-first world countries list
NON_FIRST_WORLD_COUNTRIES = {
    "Argentina", "Bangladesh", "Brazil", "Bulgaria", "Chile", "China",
    "Colombia", "Egypt", "Federal Republic of Yugoslavia", "India",
    "Indonesia", "Iran", "Kazakhstan", "Mexico", 
    "Occupied Palestinian Territory", "Pakistan", "Philippines",
    "Romania", "Russia", "Saudi Arabia", "Serbia", "South Africa",
    "Soviet Union", "Sri Lanka", "Thailand", "Turkey", "Yugoslavia"
}

class OMDBClient:
    """Client for interacting with OMDB API."""
    
    def __init__(self, api_key: str):
        """
        Initialize OMDB client.
        
        Args:
            api_key: OMDB API key
        """
        self.api_key = api_key
        self.base_url = "http://www.omdbapi.com/"
        self.session = requests.Session()
        
    def get_movie_data(self, imdb_id: str) -> dict[str, Any] | None:
        """
        Get movie data from OMDB API.
        
        Args:
            imdb_id: IMDB ID in format "tt1234567"
            
        Returns:
            Movie data dict or None if error
        """
        params = {
            'apikey': self.api_key,
            'i': imdb_id
        }
        
        try:
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('Response') == 'False':
                logger.warning(f"OMDB API error for {imdb_id}: {data.get('Error', 'Unknown error')}")
                raise NetworkError(f"OMDB API error for {imdb_id}: {data.get('Error', 'Unknown error')}")
                
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching OMDB data for {imdb_id}: {e}")
            raise NetworkError(f"Failed to fetch OMDB data: {e}")
        except ValueError as e:
            logger.error(f"Invalid JSON response for {imdb_id}: {e}")
            raise NetworkError(f"Invalid JSON response for {imdb_id}: {e}")
    
    def close(self):
        """Close the session."""
        self.session.close()


def format_imdb_id(title_id: int) -> str:
    """
    Format title ID to IMDB format.
    
    Args:
        title_id: Numeric title ID
        
    Returns:
        Formatted IMDB ID (e.g., "tt00123456")
    """
    # Convert to string and pad with zeros to make it 7 digits
    id_str = str(title_id).zfill(8)
    return f"tt{id_str}"


def determine_first_world_status(country_string: str | None) -> bool | None:
    """
    Determine if a title is from a first world country.
    
    Args:
        country_string: Comma-separated string of countries or None
        
    Returns:
        True if any country is first world, False if all are non-first world, None if no data
    """
    if not country_string:
        return None
        
    # Split countries and clean them
    countries = [country.strip() for country in country_string.split(',')]
    
    # Check if all countries are non-first world
    all_non_first_world = all(
        country in NON_FIRST_WORLD_COUNTRIES 
        for country in countries
    )
    
    # Return False if all are non-first world, True otherwise
    return not all_non_first_world


def update_first_world_status(
    title_repo: TitleRepository,
    delay_between_calls: float = 0.1
) -> None:
    """
    Update firstWorld column in titles table using OMDB API.
    
    Args:
        supabase_client: Existing Supabase client or None to create a new one
        api_key: OMDB API key (uses environment variable if not provided)
        batch_size: Number of titles to process in each batch
        delay_between_calls: Delay between API calls in seconds
        title_ids: Specific title IDs to update (if None, updates all titles with null firstWorld)
    """
    logger.info("Starting firstWorld column update")
    
    api_key = os.getenv("OMDB_API")
    if not api_key:
        logger.warning("OMDB API key not found. Skipping firstWorld update.")
        raise ValueError("OMDB API key not found")
    
    # Initialize OMDB client
    omdb_client = OMDBClient(api_key)
    
    try:
        logger.info("Fetching all titles with missing firstWorld data")
        titles_to_update = title_repo.get_titles_needing_first_world_update()

        logger.info(f"Found {len(titles_to_update)} titles to update")
        
        if not titles_to_update:
            logger.info("No titles need firstWorld update")
            return
        
        # Process titles in batches
        updated_count = 0
        error_count = 0
        
        for title_id in titles_to_update:
            imdb_id = format_imdb_id(title_id)
                
            try:
                # Get movie data from OMDB
                movie_data = omdb_client.get_movie_data(imdb_id)
                
                if movie_data:
                    country_string = movie_data.get('Country')
                    first_world_status = determine_first_world_status(country_string)
                    
                    if first_world_status is not None:
                        title_repo.update(
                            data={"firstWorld": first_world_status},
                            filters={"id": title_id}
                        )
                        logger.info(f"Updated {imdb_id} firstWorld={first_world_status}")
                    else:
                        logger.warning(f"No country data for {imdb_id}")
                        error_count += 1
                else:
                    error_count += 1
                    
            except NetworkError:
                error_count += 1
                logger.error(f"Skipping {imdb_id} due to network error")
                continue
            
            # Rate limiting
            time.sleep(delay_between_calls)
        
        logger.info(f"firstWorld update completed. Updated: {updated_count}, Errors: {error_count}")
        
    finally:
        omdb_client.close()