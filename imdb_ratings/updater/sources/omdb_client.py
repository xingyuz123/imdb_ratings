"""
Client for the OMDB (Open Movie Database) API.
"""

import requests
from typing import Any
from imdb_ratings import logger
from imdb_ratings.core.constants import OMDB_BASE_URL, OMDB_REQUEST_TIMEOUT
from imdb_ratings.core.exceptions import NetworkError


class OMDBClient:
    """Client for interacting with OMDB API."""

    def __init__(self, api_key: str):
        """
        Initialize OMDB client.

        Args:
            api_key: OMDB API key
        """
        self.api_key = api_key
        self.base_url = OMDB_BASE_URL
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
            response = self.session.get(self.base_url, params=params, timeout=OMDB_REQUEST_TIMEOUT)
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
