import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime, date
from pydantic import BaseModel
from imdb_ratings import logger
import time
import random

class ReviewData(BaseModel):
    review_id: int
    title_id: int
    date: str | None
    rating: int | None
    num_helpful: int
    num_unhelpful: int

def get_title_code(soup: BeautifulSoup) -> int:
    """Returns the title id for the given review page"""
    return int(soup.find("div", class_="lister-item-header").find("a").get("href").split("tt")[1].split("/")[0])

def get_rating(soup: BeautifulSoup) -> int | None:
    """Returns the rating for the given review page"""
    rating_span = soup.find("span", class_="rating-other-user-rating")
    if rating_span is None:
        return None
    return int(rating_span.text.strip().split("/")[0])

def get_review_date(soup: BeautifulSoup) -> str | None:
    """Returns the date of the review"""
    review_date_span = soup.find("span", class_ = "review-date")
    if review_date_span is None:
        return None
    return datetime.strptime(review_date_span.text.strip(), "%d %B %Y").date().strftime("%Y-%m-%d")

def get_num_helpful_unhelpful(soup: BeautifulSoup) -> tuple[int, int]:
    """Returns the number of votes for review indicating helpfulness"""

    try:
        helpful_div = soup.find("div", class_="actions text-muted")
        if not helpful_div:
            return 0, 0
        helpful_div_parts = helpful_div.text.strip().split()
        if len(helpful_div_parts) < 4:
            return 0, 0
        try:
            num_helpful = int(helpful_div_parts[0])
            num_unhelpful = int(helpful_div_parts[3]) - num_helpful
            if num_helpful < 0 or num_unhelpful < 0:
                return 0, 0
            return num_helpful, num_unhelpful
        except (ValueError, IndexError):
            return 0, 0
    except Exception as e:
        logger.error(f"Error getting number of helpful and unhelpful votes: {e}")
        return 0, 0

def create_requests_session() -> requests.Session:
    """Creates a requests session with retry logic and timeouts"""
    session = requests.Session()
    
    # Configure retry strategy
    retries = Retry(
        total=3,  # Number of total retries
        backoff_factor=1,  # Each retry will wait {backoff_factor * (2 ** (retry - 1))} seconds
        status_forcelist=[500, 502, 503, 504]  # HTTP status codes to retry on
    )
    
    # Mount the retry adapter to both HTTP and HTTPS requests
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

def extract_review_data(review_id: int, session: requests.Session | None = None) -> ReviewData | None:
    """Extracts review data from the given review id"""
    url = f"https://www.imdb.com/review/rw{review_id:08d}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }

    should_close_session = False
    if session is None:
        session = create_requests_session()
        should_close_session = True
    
    try:
        response = requests.get(url, headers=headers, timeout=10)

        try:
            response.raise_for_status()
        except HTTPError as e:
            if response.status_code == 404:
                return None
            logger.error(f"Error extracting review data: {e}")
            raise e

        parser = BeautifulSoup(response.content, 'html.parser')
        title_id = get_title_code(parser)
        rating = get_rating(parser)
        review_date = get_review_date(parser)
        num_helpful, num_unhelpful = get_num_helpful_unhelpful(parser)

        return ReviewData(
            review_id=review_id,
            title_id=title_id,
            date=review_date,
            rating=rating,
            num_helpful=num_helpful,
            num_unhelpful=num_unhelpful
        )
    finally:
        if should_close_session:
            session.close()

def extract_reviews(start_id: int, batch_size: int = 1000, requests_session: requests.Session | None = None) -> list[ReviewData]:
    """Extracts review data starting from start_id until we get batch_size reviews"""

    logger.info(f"Extracting reviews from {start_id} to {start_id + batch_size - 1}")

    reviews: list[ReviewData] = []

    should_close_session = False
    if requests_session is None:
        requests_session = create_requests_session()
        should_close_session = True
    try:
        for i in range(start_id, start_id + batch_size):
            review = extract_review_data(i, requests_session)
            if review is None:
                logger.info(f"Review {i} not found")
            else:
                reviews.append(review)
                logger.info(f"Extracted review {i}")
            # time.sleep(random.uniform(0.1, 1))
    finally:
        if should_close_session:
            requests_session.close()

    return reviews
