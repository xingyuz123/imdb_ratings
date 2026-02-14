"""
Main entry point for updating IMDB ratings database.

This script orchestrates the complete update pipeline:
1. Update titles from IMDB datasets
2. Classify new titles by country (first world)
3. Scrape reviews for titles needing updates
4. Recalculate weighted ratings
5. Export results to Excel
"""

import argparse
import sys
import atexit
from imdb_ratings.updater.update_titles import update_title_table
from imdb_ratings.updater.update_first_world import update_first_world_status
from imdb_ratings.updater.update_reviews import update_reviews_table
from imdb_ratings.updater.update_weighted_ratings import update_weighted_ratings_table
from imdb_ratings.export_excel import export_to_excel
from imdb_ratings.core.database import close_database_connection, get_database_client
from imdb_ratings import logger


atexit.register(close_database_connection)


def main(skip_titles: bool=False, skip_reviews: bool=False, skip_ratings: bool=False, skip_export: bool=False) -> None:
    """
    Main update process.

    Args:
        skip_titles: Skip updating titles table (also skips first world classification)
        skip_reviews: Skip updating reviews table
        skip_ratings: Skip updating weighted ratings table
        skip_export: Skip Excel export
    """
    logger.info("Starting IMDB ratings update process")

    try:
        supabase_client = get_database_client()

        # Step 1: Download and update titles from IMDB
        if not skip_titles:
            logger.info("Step 1: Updating titles table")
            update_title_table(supabase_client)
        else:
            logger.info("Step 1: Skipping titles update")

        # Step 2: Classify new titles by country
        if not skip_titles:
            logger.info("Step 2: Updating first world classification")
            update_first_world_status(supabase_client)
        else:
            logger.info("Step 2: Skipping first world classification")

        # Step 3: Scrape and update reviews
        if not skip_reviews:
            logger.info("Step 3: Updating reviews table")
            update_reviews_table(supabase_client)
        else:
            logger.info("Step 3: Skipping reviews update")

        # Step 4: Recalculate weighted ratings
        if not skip_ratings:
            logger.info("Step 4: Updating weighted ratings table")
            update_weighted_ratings_table(supabase_client)
        else:
            logger.info("Step 4: Skipping weighted ratings update")

        # Step 5: Export to Excel
        if not skip_export:
            logger.info("Step 5: Exporting to Excel")
            export_to_excel()
        else:
            logger.info("Step 5: Skipping Excel export")

        logger.info("Update process completed successfully")

    except Exception as e:
        logger.error(f"Update process failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        close_database_connection()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Update IMDB ratings database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            # Run full update
            python -m imdb_ratings.main

            # Skip titles update (only update reviews for existing titles)
            python -m imdb_ratings.main --skip-titles

            # Only export to Excel (no updates)
            python -m imdb_ratings.main --skip-titles --skip-reviews --skip-ratings
            """
    )

    parser.add_argument(
        "--skip-titles",
        action="store_true",
        help="Skip updating titles table"
    )

    parser.add_argument(
        "--skip-reviews",
        action="store_true",
        help="Skip updating reviews table"
    )

    parser.add_argument(
        "--skip-ratings",
        action="store_true",
        help="Skip updating weighted ratings table"
    )

    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip Excel export"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        skip_titles=args.skip_titles,
        skip_reviews=args.skip_reviews,
        skip_ratings=args.skip_ratings,
        skip_export=args.skip_export
    )
