import logging
from pathlib import Path

from imdb_ratings.config import get_settings

def setup_logging(
    log_file: Path | None = None,
    console_level: int | None = None,
    file_level: int | None = None
) -> logging.Logger:
    """
    Configure logging for the IMDB scraper application.
    
    This function creates a logger that can write to both console and file,
    with different logging levels for each if desired.
    
    Args:
        log_file: Path to log file (uses config default if None)
        console_level: Console logging level (uses config default if None)
        file_level: File logging level (uses config default if None)
    
    Returns:
        Configured logger instance
    """
    settings = get_settings()

    if log_file is None:
        log_file = settings.log_file_path
    if console_level is None:
        console_level = getattr(logging, settings.logging.console_level)
    if file_level is None:
        file_level = getattr(logging, settings.logging.file_level)

    logger = logging.getLogger("imdb_scraper")
    logger.setLevel(logging.DEBUG)
    
    # Prevent adding handlers multiple times
    if logger.handlers:
        return logger
        
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file:            
        # Ensure log directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
    
    return logger