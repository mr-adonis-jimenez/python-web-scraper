"""Comprehensive tests for web scraping modules."""
import pytest
import asyncio
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Test async_scraper
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
            success=True
        )
        
        assert result.url == "https://example.com"
        assert result.success is True
        assert result.status_code == 200
    except ImportError:
        pytest.skip("async_scraper not available")


# Test rate_limiter
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
    try:
        from rate_limiter import RateLimitConfig
        
        config = RateLimitConfig(
            requests_per_second=2.0,
            requests_per_minute=120,
            burst_size=10
        )
        
        assert config.requests_per_second == 2.0
        assert config.requests_per_minute == 120
        assert config.burst_size == 10
    except ImportError:
        pytest.skip("rate_limiter not available")


def test_rate_limiter_acquire():
    """Test rate limiter acquire method."""
    try:
        from rate_limiter import RateLimiter, RateLimitConfig
        
        config = RateLimitConfig(requests_per_second=100, min_delay=0)
        limiter = RateLimiter(config)
        
        # Should acquire immediately with high rate
        assert limiter.acquire(wait=True) is True
        assert limiter.total_requests == 1
    except ImportError:
        pytest.skip("rate_limiter not available")


# Test exporter
def test_exporter_import():
    """Test exporter can be imported."""
    try:
        from exporter import DataExporter, ExportFormat
        assert DataExporter is not None
        assert ExportFormat is not None
    except ImportError as e:
        pytest.skip(f"exporter not available: {e}")


def test_export_format_enum():
    """Test export format enum."""
    try:
        from exporter import ExportFormat
        
        assert ExportFormat.CSV.value == "csv"
        assert ExportFormat.JSON.value == "json"
        assert ExportFormat.EXCEL.value == "xlsx"
        assert ExportFormat.PARQUET.value == "parquet"
    except ImportError:
        pytest.skip("exporter not available")


def test_data_exporter_initialization():
    """Test data exporter initialization."""
    try:
        from exporter import DataExporter
        import tempfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            exporter = DataExporter(output_dir=tmpdir)
            assert exporter.output_dir.exists()
    except ImportError:
        pytest.skip("exporter not available")


def test_csv_export():
    """Test CSV export functionality."""
    try:
        from exporter import DataExporter, ExportFormat
        import tempfile
        import os
        
        test_data = [
            {"name": "Test1", "value": 100},
            {"name": "Test2", "value": 200}
        ]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            exporter = DataExporter(output_dir=tmpdir)
            result = exporter.export(test_data, "test", ExportFormat.CSV)
            
            assert result is not None
            assert os.path.exists(result)
            assert result.endswith(".csv")
    except ImportError:
        pytest.skip("exporter not available")


# Test validators
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
            "content": "Test content"
        }
        
        model = ScrapedDataModel(**data)
        assert model.url == "https://example.com"
        assert model.title == "Test Title"
    except ImportError:
        pytest.skip("validators/pydantic not available")


def test_data_validator():
    """Test data validator class."""
    try:
        from validators import DataValidator, ScrapedDataModel
        
        validator = DataValidator(ScrapedDataModel)
        
        valid_data = {
            "url": "https://example.com",
            "title": "Valid Title"
        }
        
        result = validator.validate(valid_data)
        assert result is not None
        assert validator.valid_count == 1
    except ImportError:
        pytest.skip("validators/pydantic not available")


# Test fetcher
def test_fetcher_import():
    """Test fetcher can be imported."""
    try:
        from fetcher import fetch_html
        assert fetch_html is not None
    except ImportError as e:
        pytest.skip(f"fetcher not available: {e}")


@patch('fetcher.requests.Session')
def test_fetch_html_success(mock_session):
    """Test successful HTML fetch."""
    try:
        from fetcher import fetch_html
        
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<html>Test</html>"
        mock_session.return_value.__enter__.return_value.get.return_value = mock_response
        
        result = fetch_html("https://example.com")
        assert result is not None
        assert "Test" in result
    except ImportError:
        pytest.skip("fetcher not available")


# Integration tests
def test_full_scraping_workflow():
    """Test complete scraping workflow."""
    test_data = [
        {"url": "https://example.com", "title": "Test", "price": 10.0}
    ]
    
    try:
        from exporter import DataExporter, ExportFormat
        import tempfile
        
        with tempfile.TemporaryDirectory() as tmpdir:
            exporter = DataExporter(output_dir=tmpdir)
            result = exporter.export(test_data, "integration_test", ExportFormat.JSON)
            
            assert result is not None
    except ImportError:
        pytest.skip("Required modules not available")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
