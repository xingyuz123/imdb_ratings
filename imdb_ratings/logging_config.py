import logging
from pathlib import Path

def setup_logging(
    log_file: Path | None = None,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG
) -> logging.Logger:
    """
    Configure logging for the IMDB scraper application.
    
    This function creates a logger that can write to both console and file,
    with different logging levels for each if desired.
    """

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
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
    
    return logger