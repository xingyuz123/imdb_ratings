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
        "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "content-type": "application/json",
        "cookie": "session-id=143-2228010-5905528; session-id-time=2082787201l; ubid-main=133-7651439-3657636; ad-oo=0; uu=eyJpZCI6InV1ZmRhMmQ2YzI4ZjdkNDE4OTlhNWUiLCJwcmVmZXJlbmNlcyI6eyJmaW5kX2luY2x1ZGVfYWR1bHQiOmZhbHNlfX0=; ci=e30; session-token=WQDRWHnhpc7sMaaAye5CgjVKZZ0uIyW6PRPnqEXghsJKDSHKKVurY9I/3DFlWSxGmlch5wS/O8M+/jHtfsmlxIfbscvVRwdCJP2d2RY3U2JRrsWI05VR2XETdbN9AIX6wMgp95eoAlDsaU9MLsXpdgjOFLIP3oOnx7b2nTd0W1/SCIBbG8bAIQEt5uyGL8YSIZgWZJ+YC0O7UdGelevrRLrsNu/XmteGDULwbYGR9DAEglqPeIuQ30vc4N1l3nyJj8zJ+J5hVUMmeJgcxxRtNtESF7kPUENLXmFnQCpTHUK4SsYx++Rg52Zwkinarw/BwmE3vzsyo73IJJdOiKNyE+fv/mXq6/Cq",
        "origin": "https://www.imdb.com",
        "priority": "u=1, i",
        "referer": "https://www.imdb.com/",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-amzn-sessionid": "143-2228010-5905528",
        "x-imdb-client-name": "imdb-web-next-localized",
        "x-imdb-client-rid": "T2VXW4Z3H4Q856A770FW",
        "x-imdb-user-country": "US",
        "x-imdb-user-language": "en-US",
        "x-imdb-weblab-treatment-overrides": '{"IMDB_SDUI_FAQS_989503":"T1"}'
    }
    querystring = {
        "operationName": "TitleReviewsRefine",
        "variables": f"{{\"after\":\"{cursor}\",\"const\":\"{title_code}\",\"filter\":{{}},\"first\":50,\"locale\":\"en-US\",\"sort\":{{\"by\":\"HELPFULNESS_SCORE\",\"order\":\"DESC\"}}}}",
        "extensions": "{\"persistedQuery\":{\"sha256Hash\":\"89aff4cd7503e060ff1dd5aba91885d8bac0f7a21aa1e1f781848a786a5bdc19\",\"version\":1}}"
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
        total_reviews = int(response_dict["total"])
        if response_dict is None:
            break
        has_next_page = bool(response_dict["pageInfo"]["hasNextPage"])
        cursor = response_dict["pageInfo"]["endCursor"]
        reviews.extend(extract_reviews_from_json(response_dict, title_code))
        time.sleep(0.1)

    assert 0.99 * total_reviews <= len(reviews), f"Total reviews: {total_reviews}, Scraped reviews: {len(reviews)}"

    return pl.DataFrame([review.model_dump() for review in reviews if (review.rating is not None) and (review.num_helpful > 1) and (review.num_words >= 100)])
