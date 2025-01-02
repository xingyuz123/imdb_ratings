from .logging_config import setup_logging
from pathlib import Path

logger = setup_logging(
    log_file=Path(__file__).parents[1] / "logs" / "imdb_scraper.log",
)
