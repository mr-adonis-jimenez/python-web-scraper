"""Data validation using Pydantic models for type safety and validation."""
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from urllib.parse import urlparse

try:
    from pydantic import (
        BaseModel,
        ConfigDict,
        Field,
        HttpUrl,
        ValidationError,
        field_validator,
    )
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    logging.warning("pydantic not available. Data validation disabled.")

    # Fallback placeholder classes
    class BaseModel:  # type: ignore[no-redef]
        pass

    class Field:  # type: ignore[no-redef]
        @staticmethod
        def __call__(*args, **kwargs):
            return None

    HttpUrl = str  # type: ignore[misc]


class ScrapedDataModel(BaseModel):
    """Base model for scraped data with common fields."""

    model_config = ConfigDict(extra="allow", validate_assignment=True)

    url: HttpUrl = Field(..., description="Source URL")
    title: Optional[str] = Field(None, max_length=500)
    content: Optional[str] = Field(None)
    timestamp: datetime = Field(default_factory=datetime.now)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("url")
    @classmethod
    def validate_url(cls, v):
        """Validate URL format."""
        if isinstance(v, str):
            parsed = urlparse(v)
            if not all([parsed.scheme, parsed.netloc]):
                raise ValueError("Invalid URL format")
        return v

    @field_validator("title")
    @classmethod
    def validate_title(cls, v):
        """Clean and validate title."""
        if v:
            v = v.strip()
            if not v:
                return None
        return v


class ProductModel(ScrapedDataModel):
    """Model for product data."""

    name: str = Field(..., min_length=1, max_length=500)
    price: Optional[float] = Field(None, ge=0)
    currency: Optional[str] = Field(None, max_length=3)
    description: Optional[str] = None
    image_url: Optional[HttpUrl] = None
    availability: Optional[bool] = None
    rating: Optional[float] = Field(None, ge=0, le=5)
    review_count: Optional[int] = Field(None, ge=0)
    sku: Optional[str] = None

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v):
        """Validate currency code."""
        if v:
            v = v.upper()
            if len(v) != 3:
                raise ValueError("Currency code must be 3 characters")
        return v


class ArticleModel(ScrapedDataModel):
    """Model for article/news data."""

    headline: str = Field(..., min_length=1)
    author: Optional[str] = None
    publish_date: Optional[datetime] = None
    body: Optional[str] = None
    summary: Optional[str] = Field(None, max_length=1000)
    tags: List[str] = Field(default_factory=list)
    category: Optional[str] = None
    image_url: Optional[HttpUrl] = None
    source: Optional[str] = None


class ContactModel(ScrapedDataModel):
    """Model for contact information."""

    name: Optional[str] = None
    email: Optional[str] = Field(None, pattern=r"[^@]+@[^@]+\.[^@]+")
    phone: Optional[str] = None
    address: Optional[str] = None
    company: Optional[str] = None
    position: Optional[str] = None


class DataValidator:
    """Validator for scraped data using Pydantic models."""
    
    def __init__(self, model_class: type = ScrapedDataModel):
        """Initialize validator with a Pydantic model.
        
        Args:
            model_class: Pydantic model class to use for validation
        """
        if not PYDANTIC_AVAILABLE:
            raise ImportError("pydantic required for data validation")
        
        self.model_class = model_class
        self.logger = logging.getLogger(__name__)
        self.validation_errors = []
        self.valid_count = 0
        self.invalid_count = 0
    
    def validate(self, data: Dict[str, Any]) -> Optional[BaseModel]:
        """Validate a single data item.
        
        Args:
            data: Dictionary of data to validate
            
        Returns:
            Validated Pydantic model instance, or None if invalid
        """
        try:
            validated = self.model_class(**data)
            self.valid_count += 1
            return validated
        except ValidationError as e:
            self.invalid_count += 1
            self.validation_errors.append({
                "data": data,
                "errors": e.errors()
            })
            self.logger.warning(f"Validation failed: {e}")
            return None
        except Exception as e:
            self.invalid_count += 1
            self.logger.error(f"Unexpected validation error: {e}")
            return None
    
    def validate_batch(self, data_list: List[Dict[str, Any]]) -> List[BaseModel]:
        """Validate multiple data items.
        
        Args:
            data_list: List of dictionaries to validate
            
        Returns:
            List of validated model instances (excludes invalid items)
        """
        validated_items = []
        
        for data in data_list:
            validated = self.validate(data)
            if validated:
                validated_items.append(validated)
        
        self.logger.info(
            f"Validated {len(data_list)} items: "
            f"{self.valid_count} valid, {self.invalid_count} invalid"
        )
        
        return validated_items
    
    def get_validation_report(self) -> Dict[str, Any]:
        """Get validation statistics and error report.
        
        Returns:
            Dictionary with validation stats
        """
        return {
            "valid_count": self.valid_count,
            "invalid_count": self.invalid_count,
            "success_rate": (
                self.valid_count / (self.valid_count + self.invalid_count)
                if (self.valid_count + self.invalid_count) > 0 else 0
            ),
            "errors": self.validation_errors
        }
    
    def reset_stats(self) -> None:
        """Reset validation statistics."""
        self.validation_errors = []
        self.valid_count = 0
        self.invalid_count = 0


def validate_scraped_data(data: List[Dict[str, Any]], 
                         model_class: type = ScrapedDataModel) -> tuple:
    """Convenience function to validate scraped data.
    
    Args:
        data: List of dictionaries to validate
        model_class: Pydantic model class to use
        
    Returns:
        Tuple of (validated_items, validation_report)
    """
    validator = DataValidator(model_class)
    validated = validator.validate_batch(data)
    report = validator.get_validation_report()
    
    return validated, report
