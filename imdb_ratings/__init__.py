from .logging_config import setup_logging
from .config import get_settings

_settings = get_settings()
logger = setup_logging()
