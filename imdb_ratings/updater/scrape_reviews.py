import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry
from pydantic import BaseModel
from imdb_ratings import logger
from imdb_ratings.config import get_settings
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
        status_forcelist=config.retry_status_codes  # HTTP status codes to retry on
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
    """
    settings = get_settings()
    config = settings.scraping

    headers = {
        "accept": "application/graphql+json, application/json",
        "content-type": "application/json",
        "user-agent": config.user_agent,
        "x-imdb-client-name": "imdb-web-next-localized",
        "x-imdb-client-rid": "T2VXW4Z3H4Q856A770FW",
        "x-imdb-user-country": "US",
        "x-imdb-user-language": "en-US",
    }
    querystring = {
        "operationName": "TitleReviewsRefine",
        "variables": f"{{\"after\":\"{cursor}\",\"const\":\"{title_code}\",\"filter\":{{}},\"first\":50,\"locale\":\"en-US\",\"sort\":{{\"by\":\"HELPFULNESS_SCORE\",\"order\":\"DESC\"}}}}",
        "extensions": "{\"persistedQuery\":{\"sha256Hash\":\"8e851a269025170d18a33b296a5ced533529abb4e7bc3d6b96d1f36636e7f685\",\"version\":1}}"
    }

    try:
        response = session.get(
            config.graphql_url, 
            headers=headers, 
            params=querystring, 
            timeout=config.request_timeout
        )
        response.raise_for_status()
        json_data = response.json()
        if "data" not in json_data:
            logger.warning(f"No data in response for {title_code}")
            return None
        
        return json_data["data"]["title"]["reviews"]

    except HTTPError as e:
        logger.error(f"Error getting json reviews: {e}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error getting json reviews: {e}")
        raise

def extract_reviews_from_json(response_dict: ReviewsData, title_code: str) -> list[ReviewData]:
    """
    Extract review data from GraphQL response.
    
    Args:
        response_dict: GraphQL response data
        title_code: IMDB title code
        
    Returns:
        List of ReviewData objects
    """

    reviews: list[ReviewData] = []

    for edge in response_dict["edges"]:
        node = edge["node"]

        review_text = node.get("text", {}).get("originalText", {}).get("plaidHtml", "")
        word_count = len(review_text.split())

        reviews.append(
            ReviewData(
                review_id=int(node["id"][2:]),
                title_id=int(title_code[2:]),
                rating=node["authorRating"],
                num_helpful=node["helpfulness"]["upVotes"],
                num_unhelpful=node["helpfulness"]["downVotes"],
                num_words=word_count
            )
        )
    return reviews

def get_reviews_from_title_code(title_code: str, requests_session: requests.Session) -> pl.DataFrame:
    """
    Extract all reviews for a given title.
    
    Args:
        title_code: IMDB title code (e.g., 'tt1234567')
        requests_session: Requests session to use
        
    Returns:
        DataFrame with filtered reviews
    """
    settings = get_settings()
    config = settings.scraping

    has_next_page: bool = True
    cursor: str = ""
    reviews: list[ReviewData] = []

    logger.debug(f"Starting review extraction for {title_code}")

    while has_next_page:
        response_dict = get_json_reviews(cursor=cursor, title_code=title_code, session=requests_session)
        if response_dict is None:
            break

        has_next_page = bool(response_dict["pageInfo"]["hasNextPage"])
        cursor = response_dict["pageInfo"]["endCursor"]
        reviews.extend(extract_reviews_from_json(response_dict, title_code))

        logger.debug(f"Fetched {len(reviews)} reviews so far for {title_code}")
        time.sleep(config.request_delay)

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
