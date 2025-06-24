"""
IMDB ratings updater module.
"""
from .update_supabase import update_title_table, update_reviews_table
from .update_weighted_ratings import update_weighted_ratings_table
from .main_update import main

__all__ = [
    "update_title_table",
    "update_reviews_table", 
    "update_weighted_ratings_table",
    "main"
]