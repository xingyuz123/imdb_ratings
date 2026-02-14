"""
IMDB ratings updater module.
"""
from .update_titles import update_title_table
from .update_first_world import update_first_world_status
from .update_reviews import update_reviews_table
from .update_weighted_ratings import update_weighted_ratings_table

__all__ = [
    "update_title_table",
    "update_first_world_status",
    "update_reviews_table",
    "update_weighted_ratings_table",
]
