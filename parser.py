"""HTML parser with configurable CSS selectors and multiple extraction strategies."""
import logging
from typing import Optional
from dataclasses import dataclass, field

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


@dataclass
class SelectorConfig:
    """Configuration for CSS selector-based extraction.

    Args:
        container: CSS selector for the item container elements
        fields: Mapping of field name to CSS selector within each container
        attrs: Mapping of field name to HTML attribute to extract (defaults to text)
    """
    container: str = ".item"
    fields: dict[str, str] = field(default_factory=lambda: {
        "title": ".title",
        "price": ".price",
    })
    attrs: dict[str, str] = field(default_factory=dict)


def _extract_field(element: Optional[Tag], attr: Optional[str] = None) -> Optional[str]:
    """Extract text or attribute value from a BeautifulSoup element."""
    if element is None:
        return None
    if attr:
        return element.get(attr)
    return element.get_text(strip=True)


def parse_items(html: str, config: Optional[SelectorConfig] = None) -> list[dict]:
    """Parse HTML and extract items using CSS selectors.

    Args:
        html: Raw HTML string to parse
        config: Selector configuration. Uses default (.item/.title/.price) if None.

    Returns:
        List of dictionaries with extracted field values
    """
    if config is None:
        config = SelectorConfig()

    soup = BeautifulSoup(html, "lxml")
    items = []

    for card in soup.select(config.container):
        item = {}
        for field_name, selector in config.fields.items():
            element = card.select_one(selector)
            attr = config.attrs.get(field_name)
            item[field_name] = _extract_field(element, attr)
        items.append(item)

    logger.debug(f"Parsed {len(items)} items from HTML")
    return items


def parse_links(html: str, selector: str = "a") -> list[dict]:
    """Extract all links matching a CSS selector.

    Args:
        html: Raw HTML string
        selector: CSS selector for anchor elements

    Returns:
        List of dicts with 'text' and 'href' keys
    """
    soup = BeautifulSoup(html, "lxml")
    links = []
    for anchor in soup.select(selector):
        href = anchor.get("href")
        if href:
            links.append({
                "text": anchor.get_text(strip=True),
                "href": href,
            })
    return links


def parse_table(html: str, selector: str = "table") -> list[dict]:
    """Extract data from the first HTML table matching the selector.

    Args:
        html: Raw HTML string
        selector: CSS selector for the table element

    Returns:
        List of dicts keyed by header column text
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one(selector)
    if not table:
        return []

    headers = [th.get_text(strip=True) for th in table.select("th")]
    if not headers:
        first_row = table.select_one("tr")
        if first_row:
            headers = [td.get_text(strip=True) for td in first_row.select("td")]

    rows = []
    for tr in table.select("tr"):
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if cells and len(cells) == len(headers):
            rows.append(dict(zip(headers, cells)))

    logger.debug(f"Parsed table with {len(rows)} rows and {len(headers)} columns")
    return rows


def parse_meta(html: str) -> dict[str, Optional[str]]:
    """Extract common meta tags from HTML.

    Returns:
        Dict with keys: title, description, keywords, author, og_title, og_description, og_image
    """
    soup = BeautifulSoup(html, "lxml")
    meta = {
        "title": soup.title.get_text(strip=True) if soup.title else None,
    }

    meta_tags = {
        "description": {"name": "description"},
        "keywords": {"name": "keywords"},
        "author": {"name": "author"},
        "og_title": {"property": "og:title"},
        "og_description": {"property": "og:description"},
        "og_image": {"property": "og:image"},
    }

    for key, attrs in meta_tags.items():
        tag = soup.find("meta", attrs=attrs)
        meta[key] = tag.get("content") if tag else None

    return meta
