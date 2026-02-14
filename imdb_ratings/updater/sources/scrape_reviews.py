import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException, Timeout, ConnectionError
from urllib3.util.retry import Retry
from pydantic import BaseModel
from imdb_ratings import logger
from imdb_ratings.core.config import get_settings
from imdb_ratings.core.constants import (
    IMDB_GRAPHQL_HEADERS,
    IMDB_GRAPHQL_OPERATION_NAME,
    IMDB_GRAPHQL_PERSISTED_QUERY_HASH,
    IMDB_GRAPHQL_PAGE_SIZE,
    IMDB_GRAPHQL_LOCALE,
    IMDB_GRAPHQL_SORT_BY,
    IMDB_GRAPHQL_SORT_ORDER,
    IMDB_REVIEW_ID_PREFIX,
    IMDB_TITLE_ID_PREFIX,
    HTTP_STATUS_RATE_LIMITED,
    SCRAPER_MAX_RETRIES,
    SCRAPER_MAX_CONSECUTIVE_FAILURES,
    RATE_LIMIT_BASE_WAIT_SECONDS,
    RATE_LIMIT_MAX_WAIT_SECONDS,
)
from imdb_ratings.core.exceptions import RateLimitError, DataValidationError, NetworkError
import polars as pl
from typing import TypedDict
import time

class ReviewData(BaseModel):
    review_id: int
    title_id: int
    rating: int | None
    num_helpful: int
    num_unhelpful: int
    num_words: int

# Define the structure of the GraphQL response
class ReviewPageInfo(TypedDict):
    hasNextPage: bool
    endCursor: str

class ReviewHelpfulness(TypedDict):
    upVotes: int
    downVotes: int

class ReviewNode(TypedDict):
    id: str
    authorRating: int | None
    helpfulness: ReviewHelpfulness

class ReviewEdge(TypedDict):
    node: ReviewNode

class ReviewsData(TypedDict):
    edges: list[ReviewEdge]
    pageInfo: ReviewPageInfo

def create_requests_session() -> requests.Session:
    """Creates a requests session with retry logic and timeouts from config."""
    settings = get_settings()
    config = settings.scraping

    session = requests.Session()
    
    retries = Retry(
        total=config.retry_total,
        backoff_factor=config.retry_backoff_factor,  # Each retry will wait {backoff_factor * (2 ** (retry - 1))} seconds
        status_forcelist=config.retry_status_codes,  # HTTP status codes to retry on
        allowed_methods=["GET", "POST"],
        raise_on_status=False, # Handle status codes ourselves
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

def get_json_reviews(cursor: str, title_code: str, session: requests.Session) -> ReviewsData | None:
    """
    Fetch reviews from IMDB GraphQL API.
    
    Args:
        cursor: Pagination cursor
        title_code: IMDB title code (e.g., 'tt1234567')
        session: Requests session
        
    Returns:
        Review data or None if error

    Raises:
        RateLimitError: If the request is rate limited
        ScrapingError: For unrecoverable errors
    """
    settings = get_settings()
    config = settings.scraping

    headers = {
        "accept": "application/graphql+json, application/json",
        "content-type": "application/json",
        "user-agent": config.user_agent,
        **IMDB_GRAPHQL_HEADERS,
    }
    sort_config = f'{{"by":"{IMDB_GRAPHQL_SORT_BY}","order":"{IMDB_GRAPHQL_SORT_ORDER}"}}'
    querystring = {
        "operationName": IMDB_GRAPHQL_OPERATION_NAME,
        "variables": f'{{"after":"{cursor}","const":"{title_code}","filter":{{}},"first":{IMDB_GRAPHQL_PAGE_SIZE},"locale":"{IMDB_GRAPHQL_LOCALE}","sort":{sort_config}}}',
        "extensions": f'{{"persistedQuery":{{"sha256Hash":"{IMDB_GRAPHQL_PERSISTED_QUERY_HASH}","version":1}}}}'
    }

    try:
        response = session.get(
            config.graphql_url, 
            headers=headers, 
            params=querystring, 
            timeout=config.request_timeout
        )

        if response.status_code == HTTP_STATUS_RATE_LIMITED:
            retry_after = response.headers.get("Retry-After", str(RATE_LIMIT_BASE_WAIT_SECONDS))
            logger.warning(f"Rate limit for {title_code} exceeded. Retry after {retry_after} seconds")
            raise RateLimitError(f"Rate limited. Retry after {retry_after} seconds")

        # Raise for other HTTP errors
        response.raise_for_status()

        try: 
            json_data = response.json()
        except ValueError as e:
            logger.error(f"Invalid JSON response for {title_code}: {e}")
            raise DataValidationError(f"Invalid JSON response: {e}")

        if "data" not in json_data:
            if "errors" in json_data:
                error_msg = json_data["errors"][0].get("message", "Unknown error")
                logger.error(f"GraphQL error for {title_code}: {error_msg}")
                raise DataValidationError(f"GraphQL error: {error_msg}")
            else:
                logger.error(f"No data in response for {title_code}")
                raise DataValidationError(f"No data in response: {json_data}")
            
        try:
            return json_data["data"]["title"]["reviews"]
        except (KeyError, TypeError) as e:
            logger.error(f"Unexpected response structure for {title_code}: {e}")
            raise DataValidationError(f"Unexpected response structure: {e}")

    except Timeout:
        logger.warning(f"Timeout fetching reviews for {title_code}")
        raise NetworkError(f"Timeout fetching reviews for {title_code}")
    except ConnectionError as e:
        logger.error(f"Connection error for {title_code}: {e}")
        raise NetworkError(f"Connection error: {e}")
    except HTTPError as e:
        if e.response.status_code >= 500:
            logger.warning(f"Server error for {title_code}: {e}")
            return None
        else:
            logger.error(f"Client error for {title_code}: {e}")
            raise NetworkError(f"HTTP error: {e}")
    except RequestException as e:
        logger.error(f"Request failed for {title_code}: {e}")
        raise NetworkError(f"Request failed: {e}")

def extract_reviews_from_json(response_dict: ReviewsData, title_code: str) -> list[ReviewData]:
    """
    Extract review data from GraphQL response.
    
    Args:
        response_dict: GraphQL response data
        title_code: IMDB title code
        
    Returns:
        List of ReviewData objects

    Raises:
        DataValidationError: If data validation fails
    """
    reviews: list[ReviewData] = []
    errors_count = 0

    for edge in response_dict["edges"]:
        try:
            node = edge["node"]

            review_text = node.get("text", {}).get("originalText", {}).get("plaidHtml", "")
            word_count = len(review_text.split()) if review_text else 0

            review_id_str = node.get("id", "")
            if not review_id_str.startswith(IMDB_REVIEW_ID_PREFIX):
                logger.debug(f"Invalid review ID format: {review_id_str}")
                errors_count += 1
                continue

            try:
                review_id = int(review_id_str[len(IMDB_REVIEW_ID_PREFIX):])
                title_id = int(title_code[len(IMDB_TITLE_ID_PREFIX):])
            except ValueError:
                logger.debug(f"Cannot parse IDs: review={review_id_str}, title={title_code}")
                errors_count += 1
                continue

            reviews.append(
                ReviewData(
                    review_id=review_id,
                    title_id=title_id,
                    rating=node.get("authorRating"),
                    num_helpful=node.get("helpfulness", {}).get("upVotes", 0),
                    num_unhelpful=node.get("helpfulness", {}).get("downVotes", 0),
                    num_words=word_count
                )
            )
        except (KeyError, TypeError) as e:
            logger.debug(f"Error extracting review from edge: {e}")
            errors_count += 1
            continue

    if errors_count > 0:
        logger.warning(f"Skipped {errors_count} reviews due to errors")
    return reviews

def get_reviews_from_title_code(
    title_code: str, 
    requests_session: requests.Session,
    max_retries: int = SCRAPER_MAX_RETRIES
    ) -> pl.DataFrame:
    """
    Extract all reviews for a given title.
    
    Args:
        title_code: IMDB title code (e.g., 'tt1234567')
        requests_session: Requests session to use
        
    Returns:
        DataFrame with filtered reviews

    Raises: 
        NetworkError: If there's a network error
    """
    settings = get_settings()
    config = settings.scraping

    has_next_page: bool = True
    cursor: str = ""
    reviews: list[ReviewData] = []
    consecutive_failures = 0
    max_consecutive_failures = SCRAPER_MAX_CONSECUTIVE_FAILURES

    logger.debug(f"Starting review extraction for {title_code}")

    while has_next_page:
        retry_count = 0
        while retry_count < max_retries:
            try: 
                response_dict = get_json_reviews(
                    cursor=cursor, 
                    title_code=title_code, 
                    session=requests_session
                )
                
                if response_dict is None:
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        logger.warning(
                            f"Too many consecutive failures ({consecutive_failures}) for {title_code}. "
                            "Stopping extraction."
                        )
                        has_next_page = False
                    break

                consecutive_failures = 0
                has_next_page = bool(response_dict.get("pageInfo", {}).get("hasNextPage", False))
                cursor = response_dict.get("pageInfo", {}).get("endCursor", "")
                extracted = extract_reviews_from_json(response_dict, title_code)
                reviews.extend(extracted)

                logger.debug(f"Fetched {len(reviews)} reviews so far for {title_code}")
                time.sleep(config.request_delay)
                break

            except RateLimitError:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = min(RATE_LIMIT_BASE_WAIT_SECONDS * retry_count, RATE_LIMIT_MAX_WAIT_SECONDS)
                    logger.info(f"Rate limited. Waiting {wait_time} seconds before retry {retry_count}/{max_retries}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries exceeded for {title_code} due to rate limiting")
                    raise
            except NetworkError:
                raise

    # Convert to DataFrame
    df = pl.DataFrame([review.model_dump() for review in reviews])

    if df.is_empty():
        return df
    
    filtered_df = df.filter(
        pl.col("rating").is_not_null() &
        (pl.col("num_helpful") > config.min_helpful_votes) &
        (pl.col("num_words") >= config.min_review_words)
    )

    logger.debug(f"Extracted {len(filtered_df)}/{len(df)} reviews for {title_code} after filtering")

    return filtered_df
