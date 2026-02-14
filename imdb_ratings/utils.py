"""
Shared utility functions for the updater module.
"""

from imdb_ratings.core.constants import IMDB_TITLE_ID_FORMAT


def format_imdb_id(title_id: int) -> str:
    """
    Format a numeric title ID to standard IMDB format.

    The standard IMDB ID format is "tt" followed by at least 7 digits,
    zero-padded (e.g., "tt0000001", "tt12345678").

    Args:
        title_id: Numeric title ID

    Returns:
        Formatted IMDB ID string (e.g., "tt0111161")
    """
    return IMDB_TITLE_ID_FORMAT.format(title_id)
