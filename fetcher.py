"""Enhanced HTTP fetcher with error handling, retries, proxy support, and robots.txt checking."""
import logging
import time
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from requests.exceptions import RequestException, Timeout, HTTPError, ConnectionError

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DataScraper/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Cache for robots.txt parsers keyed by scheme+netloc
_robots_cache: dict[str, RobotFileParser] = {}


def check_robots_txt(url: str, user_agent: str = "DataScraper") -> bool:
    """Check whether the URL is allowed by the site's robots.txt.

    Results are cached per domain so robots.txt is only fetched once.

    Args:
        url: URL to check
        user_agent: User-Agent string to match against robots.txt rules

    Returns:
        True if fetching is allowed (or robots.txt cannot be retrieved)
    """
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    if base not in _robots_cache:
        rp = RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        try:
            rp.read()
        except Exception:
            logger.debug(f"Could not fetch robots.txt for {base}, assuming allowed")
            return True
        _robots_cache[base] = rp

    return _robots_cache[base].can_fetch(user_agent, url)


def fetch_html(
    url: str,
    retries: int = 3,
    timeout: int = 10,
    backoff_factor: float = 0.3,
    proxies: Optional[dict[str, str]] = None,
    respect_robots: bool = True,
) -> Optional[str]:
    """Fetch HTML content from a URL with retry logic and exponential backoff.

    Args:
        url: Target URL to scrape
        retries: Number of retry attempts (default: 3)
        timeout: Request timeout in seconds (default: 10)
        backoff_factor: Exponential backoff multiplier (default: 0.3)
        proxies: Optional proxy mapping (e.g. {"https": "http://proxy:8080"})
        respect_robots: Check robots.txt before fetching (default: True)

    Returns:
        HTML content as string, or None if all attempts fail
    """
    if respect_robots and not check_robots_txt(url):
        logger.warning(f"Blocked by robots.txt: {url}")
        return None

    for attempt in range(retries):
        try:
            logger.info(f"Fetching {url} (attempt {attempt + 1}/{retries})")

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=timeout,
                verify=True,
                allow_redirects=True,
                proxies=proxies,
            )

            response.raise_for_status()

            logger.info(f"Successfully fetched {url} ({response.status_code})")
            return response.text

        except Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1} for {url}")
            if attempt < retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Failed to fetch {url} after {retries} attempts (Timeout)")
                return None

        except HTTPError as e:
            logger.error(f"HTTP error for {url}: {e.response.status_code} - {e}")
            if 400 <= e.response.status_code < 500:
                return None
            if attempt < retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                time.sleep(sleep_time)
            else:
                return None

        except ConnectionError:
            logger.warning(f"Connection error on attempt {attempt + 1} for {url}")
            if attempt < retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                time.sleep(sleep_time)
            else:
                logger.error(f"Failed to connect to {url} after {retries} attempts")
                return None

        except RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None

    return None


def fetch_html_with_session(
    session: requests.Session,
    url: str,
    retries: int = 3,
    timeout: int = 10,
    proxies: Optional[dict[str, str]] = None,
    respect_robots: bool = True,
) -> Optional[str]:
    """Fetch HTML using a persistent session for connection pooling.

    Args:
        session: Requests session object
        url: Target URL to scrape
        retries: Number of retry attempts
        timeout: Request timeout in seconds
        proxies: Optional proxy mapping
        respect_robots: Check robots.txt before fetching (default: True)

    Returns:
        HTML content as string, or None if failed
    """
    if respect_robots and not check_robots_txt(url):
        logger.warning(f"Blocked by robots.txt: {url}")
        return None

    session.headers.update(HEADERS)
    if proxies:
        session.proxies.update(proxies)

    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt == retries - 1:
                return None
            time.sleep(0.3 * (2 ** attempt))

    return None
