"""
Main entry point for updating IMDB ratings database.

This script orchestrates the complete update process:
1. Update titles from IMDB
2. Scrape reviews for new titles
3. Export results to Excel
"""

import argparse
import sys
from imdb_ratings.updater.update_supabase import update_reviews_table, update_title_table
from imdb_ratings.excel_export import export_to_excel
from imdb_ratings import logger

def main(skip_titles: bool=False, skip_reviews: bool=False, skip_export: bool=False) -> None:
    """
    Main update process.
    
    Args:
        skip_titles: Skip updating titles table
        skip_reviews: Skip updating reviews table
        skip_export: Skip Excel export
    """
    logger.info("Starting IMDB ratings update process")

    try:
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
        
        # Step 3: Export to Excel
        if not skip_export:
            logger.info("Step 3: Exporting to Excel")
            export_to_excel()
        else:
            logger.info("Step 3: Skipping Excel export")
        
        logger.info("Update process completed successfully")
        
    except Exception as e:
        logger.error(f"Update process failed: {str(e)}", exc_info=True)
        sys.exit(1)


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
            python -m imdb_ratings.updater.main_update --skip-titles --skip-reviews
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
        skip_export=args.skip_export
    )