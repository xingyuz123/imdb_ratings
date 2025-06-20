import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry
from pydantic import BaseModel
from imdb_ratings import logger
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
    """Creates a requests session with retry logic and timeouts"""
    session = requests.Session()
    
    retries = Retry(
        total=3,
        backoff_factor=1,  # Each retry will wait {backoff_factor * (2 ** (retry - 1))} seconds
        status_forcelist=[500, 502, 503, 504]  # HTTP status codes to retry on
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

def get_json_reviews(cursor: str, title_code: str, session: requests.Session) -> ReviewsData | None:
    url = "https://caching.graphql.imdb.com/"
    headers = {
        "accept": "application/graphql+json, application/json",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
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

    response = session.request("GET", url, headers=headers, params=querystring, timeout=10)
    try:
        response.raise_for_status()
    except HTTPError as e:
        logger.error(f"Error getting json reviews: {e}")
        raise e
    if "data" not in response.json():
        return None
    return response.json()["data"]["title"]["reviews"]

def extract_reviews_from_json(response_dict: ReviewsData, title_code: str) -> list[ReviewData]:
    reviews: list[ReviewData] = []
    for edge in response_dict["edges"]:
        node = edge["node"]
        reviews.append(
            ReviewData(
                review_id=int(node["id"][2:]),
                title_id=int(title_code[2:]),
                rating=node["authorRating"],
                num_helpful=node["helpfulness"]["upVotes"],
                num_unhelpful=node["helpfulness"]["downVotes"],
                num_words=len(node["text"]["originalText"]["plaidHtml"].split(" "))
            )
        )
    return reviews

def get_reviews_from_title_code(title_code: str, requests_session: requests.Session) -> pl.DataFrame:
    """Extracts review data from the given review id"""
    has_next_page: bool = True
    cursor: str = ""
    reviews: list[ReviewData] = []

    while has_next_page:
        response_dict = get_json_reviews(cursor=cursor, title_code=title_code, session=requests_session)
        if response_dict is None:
            break
        has_next_page = bool(response_dict["pageInfo"]["hasNextPage"])
        cursor = response_dict["pageInfo"]["endCursor"]
        reviews.extend(extract_reviews_from_json(response_dict, title_code))
        time.sleep(1)

    return pl.DataFrame([review.model_dump() for review in reviews if (review.rating is not None) and (review.num_helpful > 1) and (review.num_words >= 100)])
