"""Comprehensive tests for web scraping modules."""
import pytest
import asyncio
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

def test_parse_items_default():
    """Test parse_items with default selectors."""
    from parser import parse_items

    html = """
    <div class="item">
        <span class="title">Widget</span>
        <span class="price">$9.99</span>
    </div>
    <div class="item">
        <span class="title">Gadget</span>
        <span class="price">$19.99</span>
    </div>
    """
    results = parse_items(html)
    assert len(results) == 2
    assert results[0]["title"] == "Widget"
    assert results[1]["price"] == "$19.99"


def test_parse_items_custom_selectors():
    """Test parse_items with custom SelectorConfig."""
    from parser import parse_items, SelectorConfig

    html = """
    <article class="product">
        <h2 class="name">Laptop</h2>
        <span class="cost">$999</span>
    </article>
    """
    config = SelectorConfig(
        container=".product",
        fields={"name": ".name", "cost": ".cost"},
    )
    results = parse_items(html, config=config)
    assert len(results) == 1
    assert results[0]["name"] == "Laptop"
    assert results[0]["cost"] == "$999"


def test_parse_items_attr_extraction():
    """Test extracting HTML attributes instead of text."""
    from parser import parse_items, SelectorConfig

    html = """
    <div class="card">
        <a class="link" href="/products/1">Product 1</a>
    </div>
    """
    config = SelectorConfig(
        container=".card",
        fields={"link": ".link"},
        attrs={"link": "href"},
    )
    results = parse_items(html, config=config)
    assert results[0]["link"] == "/products/1"


def test_parse_items_missing_elements():
    """Test handling of missing elements within containers."""
    from parser import parse_items

    html = '<div class="item"><span class="title">Only Title</span></div>'
    results = parse_items(html)
    assert results[0]["title"] == "Only Title"
    assert results[0]["price"] is None


def test_parse_links():
    """Test link extraction."""
    from parser import parse_links

    html = """
    <nav>
        <a href="/home">Home</a>
        <a href="/about">About</a>
        <span>Not a link</span>
    </nav>
    """
    links = parse_links(html, selector="nav a")
    assert len(links) == 2
    assert links[0]["href"] == "/home"
    assert links[1]["text"] == "About"


def test_parse_table():
    """Test HTML table parsing."""
    from parser import parse_table

    html = """
    <table id="data">
        <tr><th>Name</th><th>Age</th></tr>
        <tr><td>Alice</td><td>30</td></tr>
        <tr><td>Bob</td><td>25</td></tr>
    </table>
    """
    rows = parse_table(html, selector="#data")
    assert len(rows) == 2
    assert rows[0]["Name"] == "Alice"
    assert rows[1]["Age"] == "25"


def test_parse_table_empty():
    """Test table parsing with no matching table."""
    from parser import parse_table

    rows = parse_table("<div>No table here</div>")
    assert rows == []


def test_parse_meta():
    """Test meta tag extraction."""
    from parser import parse_meta

    html = """
    <html>
    <head>
        <title>Test Page</title>
        <meta name="description" content="A test page">
        <meta property="og:title" content="OG Title">
    </head>
    <body></body>
    </html>
    """
    meta = parse_meta(html)
    assert meta["title"] == "Test Page"
    assert meta["description"] == "A test page"
    assert meta["og_title"] == "OG Title"
    assert meta["keywords"] is None


# ---------------------------------------------------------------------------
# Async scraper tests
# ---------------------------------------------------------------------------

def test_async_scraper_import():
    """Test async scraper can be imported."""
    try:
        from async_scraper import AsyncScraper, ScrapingResult
        assert AsyncScraper is not None
        assert ScrapingResult is not None
    except ImportError as e:
        pytest.skip(f"async_scraper not available: {e}")


@pytest.mark.asyncio
async def test_scraping_result():
    """Test ScrapingResult dataclass."""
    try:
        from async_scraper import ScrapingResult

        result = ScrapingResult(
            url="https://example.com",
            data={"test": "data"},
            status_code=200,
            success=True,
        )

        assert result.url == "https://example.com"
        assert result.success is True
        assert result.status_code == 200
    except ImportError:
        pytest.skip("async_scraper not available")


# ---------------------------------------------------------------------------
# Rate limiter tests
# ---------------------------------------------------------------------------

def test_rate_limiter_import():
    """Test rate limiter can be imported."""
    try:
        from rate_limiter import RateLimiter, RateLimitConfig
        assert RateLimiter is not None
        assert RateLimitConfig is not None
    except ImportError as e:
        pytest.skip(f"rate_limiter not available: {e}")


def test_rate_limit_config():
    """Test rate limit configuration."""
    from rate_limiter import RateLimitConfig

    config = RateLimitConfig(
        requests_per_second=2.0,
        requests_per_minute=120,
        burst_size=10,
    )

    assert config.requests_per_second == 2.0
    assert config.requests_per_minute == 120
    assert config.burst_size == 10


def test_rate_limiter_acquire():
    """Test rate limiter acquire method."""
    from rate_limiter import RateLimiter, RateLimitConfig

    config = RateLimitConfig(requests_per_second=100, min_delay=0)
    limiter = RateLimiter(config)

    assert limiter.acquire(wait=True) is True
    assert limiter.total_requests == 1


def test_rate_limiter_stats():
    """Test rate limiter statistics."""
    from rate_limiter import RateLimiter, RateLimitConfig

    config = RateLimitConfig(requests_per_second=100, min_delay=0)
    limiter = RateLimiter(config)
    limiter.acquire(wait=True)

    stats = limiter.get_stats()
    assert stats["total_requests"] == 1
    assert "current_tokens" in stats


def test_rate_limiter_reset():
    """Test rate limiter reset."""
    from rate_limiter import RateLimiter, RateLimitConfig

    config = RateLimitConfig(requests_per_second=100, min_delay=0)
    limiter = RateLimiter(config)
    limiter.acquire(wait=True)
    limiter.reset()

    assert limiter.total_requests == 0
    assert limiter.total_wait_time == 0.0


def test_domain_rate_limiter():
    """Test domain-based rate limiter."""
    from rate_limiter import DomainRateLimiter, RateLimitConfig

    config = RateLimitConfig(requests_per_second=100, min_delay=0)
    limiter = DomainRateLimiter(default_config=config)

    assert limiter.acquire("https://example.com/page1") is True
    assert limiter.acquire("https://other.com/page1") is True

    stats = limiter.get_stats()
    assert "example.com" in stats
    assert "other.com" in stats


# ---------------------------------------------------------------------------
# Exporter tests
# ---------------------------------------------------------------------------

def test_exporter_import():
    """Test exporter can be imported."""
    from exporter import DataExporter, ExportFormat
    assert DataExporter is not None
    assert ExportFormat is not None


def test_export_format_enum():
    """Test export format enum."""
    from exporter import ExportFormat

    assert ExportFormat.CSV.value == "csv"
    assert ExportFormat.JSON.value == "json"
    assert ExportFormat.EXCEL.value == "xlsx"
    assert ExportFormat.PARQUET.value == "parquet"


def test_data_exporter_initialization():
    """Test data exporter initialization."""
    from exporter import DataExporter

    with tempfile.TemporaryDirectory() as tmpdir:
        exporter = DataExporter(output_dir=tmpdir)
        assert exporter.output_dir.exists()


def test_csv_export():
    """Test CSV export functionality."""
    from exporter import DataExporter, ExportFormat

    test_data = [
        {"name": "Test1", "value": 100},
        {"name": "Test2", "value": 200},
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        exporter = DataExporter(output_dir=tmpdir)
        result = exporter.export(test_data, "test", ExportFormat.CSV)

        assert result is not None
        assert os.path.exists(result)
        assert result.endswith(".csv")


def test_json_export():
    """Test JSON export functionality."""
    import json as json_mod
    from exporter import DataExporter, ExportFormat

    test_data = [{"key": "value"}]

    with tempfile.TemporaryDirectory() as tmpdir:
        exporter = DataExporter(output_dir=tmpdir)
        result = exporter.export(test_data, "test_json", ExportFormat.JSON)

        assert result is not None
        with open(result) as f:
            loaded = json_mod.load(f)
        assert loaded == test_data


def test_export_empty_data():
    """Test exporting empty data returns None."""
    from exporter import DataExporter, ExportFormat

    with tempfile.TemporaryDirectory() as tmpdir:
        exporter = DataExporter(output_dir=tmpdir)
        result = exporter.export([], "empty", ExportFormat.CSV)
        assert result is None


# ---------------------------------------------------------------------------
# Validator tests
# ---------------------------------------------------------------------------

def test_validators_import():
    """Test validators can be imported."""
    try:
        from validators import DataValidator, ScrapedDataModel
        assert DataValidator is not None
        assert ScrapedDataModel is not None
    except ImportError as e:
        pytest.skip(f"validators not available: {e}")


def test_scraped_data_model():
    """Test scraped data model validation."""
    try:
        from validators import ScrapedDataModel

        data = {
            "url": "https://example.com",
            "title": "Test Title",
            "content": "Test content",
        }

        model = ScrapedDataModel(**data)
        assert model.title == "Test Title"
        assert str(model.url) == "https://example.com/"
    except ImportError:
        pytest.skip("validators/pydantic not available")


def test_data_validator():
    """Test data validator class."""
    try:
        from validators import DataValidator, ScrapedDataModel

        validator = DataValidator(ScrapedDataModel)

        valid_data = {
            "url": "https://example.com",
            "title": "Valid Title",
        }

        result = validator.validate(valid_data)
        assert result is not None
        assert validator.valid_count == 1
    except ImportError:
        pytest.skip("validators/pydantic not available")


def test_data_validator_invalid_data():
    """Test validator rejects invalid data."""
    try:
        from validators import DataValidator, ScrapedDataModel

        validator = DataValidator(ScrapedDataModel)
        result = validator.validate({"url": "not-a-url"})
        assert result is None
        assert validator.invalid_count == 1
    except ImportError:
        pytest.skip("validators/pydantic not available")


def test_validation_report():
    """Test validation report generation."""
    try:
        from validators import DataValidator, ScrapedDataModel

        validator = DataValidator(ScrapedDataModel)
        validator.validate({"url": "https://example.com", "title": "Good"})
        validator.validate({"url": "bad"})

        report = validator.get_validation_report()
        assert report["valid_count"] == 1
        assert report["invalid_count"] == 1
        assert 0 < report["success_rate"] < 1
    except ImportError:
        pytest.skip("validators/pydantic not available")


# ---------------------------------------------------------------------------
# Fetcher tests
# ---------------------------------------------------------------------------

def test_fetcher_import():
    """Test fetcher can be imported."""
    from fetcher import fetch_html, check_robots_txt
    assert fetch_html is not None
    assert check_robots_txt is not None


@patch("fetcher.requests.get")
@patch("fetcher.check_robots_txt", return_value=True)
def test_fetch_html_success(mock_robots, mock_get):
    """Test successful HTML fetch."""
    from fetcher import fetch_html

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "<html>Test</html>"
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    result = fetch_html("https://example.com")
    assert result is not None
    assert "Test" in result


@patch("fetcher.requests.get")
@patch("fetcher.check_robots_txt", return_value=False)
def test_fetch_html_blocked_by_robots(mock_robots, mock_get):
    """Test that fetch returns None when blocked by robots.txt."""
    from fetcher import fetch_html

    result = fetch_html("https://example.com/secret")
    assert result is None
    mock_get.assert_not_called()


@patch("fetcher.requests.get")
@patch("fetcher.check_robots_txt", return_value=True)
def test_fetch_html_skip_robots(mock_robots, mock_get):
    """Test fetch with robots.txt checking disabled."""
    from fetcher import fetch_html

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "<html>OK</html>"
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    result = fetch_html("https://example.com", respect_robots=False)
    assert result is not None
    mock_robots.assert_not_called()


@patch("fetcher.requests.get")
@patch("fetcher.check_robots_txt", return_value=True)
def test_fetch_html_with_proxy(mock_robots, mock_get):
    """Test fetch passes proxy config to requests."""
    from fetcher import fetch_html

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = "<html>Proxied</html>"
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response

    proxies = {"https": "http://proxy:8080"}
    fetch_html("https://example.com", proxies=proxies)
    _, kwargs = mock_get.call_args
    assert kwargs["proxies"] == proxies


# ---------------------------------------------------------------------------
# Scraper orchestrator tests
# ---------------------------------------------------------------------------

@patch("scraper.fetch_html")
def test_scraper_run_no_html(mock_fetch):
    """Test scraper.run returns None when fetch fails."""
    from scraper import run

    mock_fetch.return_value = None
    result = run("https://example.com")
    assert result is None


@patch("scraper.fetch_html")
def test_scraper_run_no_items(mock_fetch):
    """Test scraper.run returns None when no items parsed."""
    from scraper import run

    mock_fetch.return_value = "<html><body>No items here</body></html>"
    result = run("https://example.com")
    assert result is None


@patch("scraper.fetch_html")
def test_scraper_run_success(mock_fetch):
    """Test scraper.run exports parsed data."""
    from scraper import run

    mock_fetch.return_value = """
    <div class="item">
        <span class="title">Test</span>
        <span class="price">$5</span>
    </div>
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        result = run("https://example.com", output=tmpdir)
        assert result is not None
        assert os.path.exists(result)


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_full_scraping_workflow():
    """Test complete scraping workflow."""
    from exporter import DataExporter, ExportFormat

    test_data = [
        {"url": "https://example.com", "title": "Test", "price": 10.0}
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        exporter = DataExporter(output_dir=tmpdir)
        result = exporter.export(test_data, "integration_test", ExportFormat.JSON)
        assert result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
