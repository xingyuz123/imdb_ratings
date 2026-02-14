"""
Main entry point for updating IMDB ratings database.

This script orchestrates the complete update process:
1. Update titles from IMDB
2. Scrape reviews for new titles
3. Export results to Excel
"""

import argparse
import sys
import atexit
from imdb_ratings.updater.update_supabase import update_reviews_table, update_title_table
from imdb_ratings.updater.update_weighted_ratings import update_weighted_ratings_table
from imdb_ratings.excel_export import export_to_excel
from imdb_ratings.database import close_database_connection, get_database_client
from imdb_ratings import logger


atexit.register(close_database_connection)


def main(skip_titles: bool=False, skip_reviews: bool=False, skip_ratings: bool=False, skip_export: bool=False) -> None:
    """
    Main update process.
    
    Args:
        skip_titles: Skip updating titles table
        skip_reviews: Skip updating reviews table
        skip_ratings: Skip updating weighted ratings table
        skip_export: Skip Excel export
    """
    logger.info("Starting IMDB ratings update process")

    try:
        supabase_client = get_database_client()

        # Step 1: Update titles
        if not skip_titles:
            logger.info("Step 1: Updating titles table")
            update_title_table()
        else:
            logger.info("Step 1: Skipping titles update")
        
        # Step 2: Update reviews
        if not skip_reviews:
            logger.info("Step 2: Updating reviews table")
            update_reviews_table()
        else:
            logger.info("Step 2: Skipping reviews update")
        
        # Step 3: Update weighted ratings
        if not skip_ratings:
            logger.info("Step 3: Updating weighted ratings table")
            update_weighted_ratings_table(supabase_client)
        else:
            logger.info("Step 3: Skipping weighted ratings update")

        # Step 4: Export to Excel
        if not skip_export:
            logger.info("Step 3: Exporting to Excel")
            export_to_excel()
        else:
            logger.info("Step 3: Skipping Excel export")
        
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
            python -m imdb_ratings.updater.main_update
            
            # Skip titles update (only update reviews for existing titles)
            python -m imdb_ratings.updater.main_update --skip-titles
            
            # Only export to Excel (no updates)
            python -m imdb_ratings.updater.main_update --skip-titles --skip-reviews --skip-ratings
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