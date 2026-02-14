"""
Application-wide constants for IMDB Ratings.

This module contains hardcoded literal values that act as constants across the
codebase. These are NOT configurable settings (those belong in config.py) but
rather fixed protocol values, format strings, retry parameters, and business
rule thresholds.
"""

# =============================================================================
# Logging
# =============================================================================

LOGGER_NAME = "imdb_scraper"

LOG_FORMAT_DETAILED = (
    "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
)
LOG_FORMAT_CONSOLE = "%(asctime)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# =============================================================================
# Database Connection
# =============================================================================

DB_MAX_RETRIES = 3
DB_RETRY_DELAY = 1.0  # seconds, base delay for exponential backoff

# =============================================================================
# IMDB ID Formatting
# =============================================================================

IMDB_TITLE_ID_PREFIX = "tt"
IMDB_TITLE_ID_FORMAT = "tt{:07d}"
IMDB_REVIEW_ID_PREFIX = "rw"

# =============================================================================
# IMDB GraphQL API
# =============================================================================

IMDB_GRAPHQL_HEADERS = {
    "x-imdb-client-name": "imdb-web-next-localized",
    "x-imdb-client-rid": "T2VXW4Z3H4Q856A770FW",
    "x-imdb-user-country": "US",
    "x-imdb-user-language": "en-US",
}

IMDB_GRAPHQL_OPERATION_NAME = "TitleReviewsRefine"
IMDB_GRAPHQL_PERSISTED_QUERY_HASH = "fb58a77d474033025bf28e1fe68f9b998111d3df58e08cd8405bd9265b1a9aff"
IMDB_GRAPHQL_PAGE_SIZE = 50
IMDB_GRAPHQL_LOCALE = "en-US"
IMDB_GRAPHQL_SORT_BY = "HELPFULNESS_SCORE"
IMDB_GRAPHQL_SORT_ORDER = "DESC"

# =============================================================================
# HTTP
# =============================================================================

HTTP_STATUS_RATE_LIMITED = 429

# =============================================================================
# Scraping Retry Policy
# =============================================================================

SCRAPER_MAX_RETRIES = 3
SCRAPER_MAX_CONSECUTIVE_FAILURES = 3
RATE_LIMIT_BASE_WAIT_SECONDS = 60
RATE_LIMIT_MAX_WAIT_SECONDS = 300

# =============================================================================
# OMDB API
# =============================================================================

OMDB_BASE_URL = "http://www.omdbapi.com/"
OMDB_REQUEST_TIMEOUT = 10  # seconds

# =============================================================================
# IMDB Data Processing
# =============================================================================

IMDB_RATING_MULTIPLIER = 10  # converts 0.0-10.0 scale to 0-100 integer scale

# =============================================================================
# Title Update Thresholds
# =============================================================================

VOTE_INCREASE_THRESHOLD = 1.05  # 5% increase triggers update

# =============================================================================
# Genre Enum Values
# =============================================================================

VALID_GENRES = (
    "Action",
    "Adventure",
    "Animation",
    "Biography",
    "Comedy",
    "Crime",
    "Documentary",
    "Drama",
    "Family",
    "Fantasy",
    "Film-Noir",
    "Game-Show",
    "History",
    "Horror",
    "Music",
    "Musical",
    "Mystery",
    "News",
    "Reality-TV",
    "Romance",
    "Sci-Fi",
    "Short",
    "Sport",
    "Talk-Show",
    "Thriller",
    "War",
    "Western",
)
