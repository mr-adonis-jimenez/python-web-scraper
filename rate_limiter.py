"""Rate limiter for respectful web scraping with configurable limits."""
import time
import logging
from typing import Optional, Dict
from collections import deque
from threading import Lock
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    requests_per_second: float = 1.0
    requests_per_minute: int = 60
    requests_per_hour: int = 1000
    burst_size: int = 5
    min_delay: float = 0.1
    

class RateLimiter:
    """Token bucket rate limiter with multiple time windows."""
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        """Initialize rate limiter.
        
        Args:
            config: Rate limit configuration
        """
        self.config = config or RateLimitConfig()
        self.logger = logging.getLogger(__name__)
        
        # Token bucket for burst handling
        self.tokens = self.config.burst_size
        self.max_tokens = self.config.burst_size
        self.last_refill = time.time()
        
        # Request timestamps for time windows
        self.requests_minute = deque(maxlen=self.config.requests_per_minute)
        self.requests_hour = deque(maxlen=self.config.requests_per_hour)
        
        # Thread safety
        self.lock = Lock()
        
        # Stats
        self.total_requests = 0
        self.total_wait_time = 0.0
        self.rejections = 0
    
    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        
        tokens_to_add = elapsed * self.config.requests_per_second
        self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def _clean_old_requests(self) -> None:
        """Remove old request timestamps outside time windows."""
        now = datetime.now()
        
        # Clean minute window
        while (self.requests_minute and 
               now - self.requests_minute[0] > timedelta(minutes=1)):
            self.requests_minute.popleft()
        
        # Clean hour window
        while (self.requests_hour and 
               now - self.requests_hour[0] > timedelta(hours=1)):
            self.requests_hour.popleft()
    
    def _calculate_wait_time(self) -> float:
        """Calculate required wait time before next request.
        
        Returns:
            Wait time in seconds
        """
        wait_time = 0.0
        
        # Check token bucket
        if self.tokens < 1:
            tokens_needed = 1 - self.tokens
            wait_time = max(wait_time, 
                          tokens_needed / self.config.requests_per_second)
        
        # Check minute limit
        if len(self.requests_minute) >= self.config.requests_per_minute:
            oldest = self.requests_minute[0]
            time_since_oldest = (datetime.now() - oldest).total_seconds()
            if time_since_oldest < 60:
                wait_time = max(wait_time, 60 - time_since_oldest)
        
        # Check hour limit
        if len(self.requests_hour) >= self.config.requests_per_hour:
            oldest = self.requests_hour[0]
            time_since_oldest = (datetime.now() - oldest).total_seconds()
            if time_since_oldest < 3600:
                wait_time = max(wait_time, 3600 - time_since_oldest)
        
        # Ensure minimum delay
        wait_time = max(wait_time, self.config.min_delay)
        
        return wait_time
    
    def acquire(self, wait: bool = True) -> bool:
        """Acquire permission to make a request.
        
        Args:
            wait: Whether to wait for permission or return immediately
            
        Returns:
            True if permission granted, False if denied (when wait=False)
        """
        with self.lock:
            self._refill_tokens()
            self._clean_old_requests()
            
            wait_time = self._calculate_wait_time()
            
            if wait_time > 0:
                if not wait:
                    self.rejections += 1
                    self.logger.debug(
                        f"Rate limit would require {wait_time:.2f}s wait"
                    )
                    return False
                
                self.logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                self.total_wait_time += wait_time
                time.sleep(wait_time)
                
                # Refill after waiting
                self._refill_tokens()
            
            # Consume token and record request
            self.tokens -= 1
            now = datetime.now()
            self.requests_minute.append(now)
            self.requests_hour.append(now)
            self.total_requests += 1
            
            return True
    
    def get_stats(self) -> Dict[str, any]:
        """Get rate limiter statistics.
        
        Returns:
            Dictionary with stats
        """
        with self.lock:
            return {
                "total_requests": self.total_requests,
                "total_wait_time": self.total_wait_time,
                "rejections": self.rejections,
                "avg_wait_time": (
                    self.total_wait_time / self.total_requests 
                    if self.total_requests > 0 else 0
                ),
                "requests_last_minute": len(self.requests_minute),
                "requests_last_hour": len(self.requests_hour),
                "current_tokens": self.tokens
            }
    
    def reset(self) -> None:
        """Reset rate limiter state."""
        with self.lock:
            self.tokens = self.max_tokens
            self.last_refill = time.time()
            self.requests_minute.clear()
            self.requests_hour.clear()
            self.total_requests = 0
            self.total_wait_time = 0.0
            self.rejections = 0
            self.logger.info("Rate limiter reset")


class DomainRateLimiter:
    """Rate limiter that tracks limits per domain."""
    
    def __init__(self, default_config: Optional[RateLimitConfig] = None):
        """Initialize domain-based rate limiter.
        
        Args:
            default_config: Default rate limit configuration for all domains
        """
        self.default_config = default_config or RateLimitConfig()
        self.limiters: Dict[str, RateLimiter] = {}
        self.lock = Lock()
        self.logger = logging.getLogger(__name__)
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL.
        
        Args:
            url: URL string
            
        Returns:
            Domain name
        """
        from urllib.parse import urlparse
        return urlparse(url).netloc
    
    def acquire(self, url: str, wait: bool = True) -> bool:
        """Acquire permission for a request to a specific domain.
        
        Args:
            url: URL to request
            wait: Whether to wait for permission
            
        Returns:
            True if permission granted, False otherwise
        """
        domain = self._get_domain(url)
        
        with self.lock:
            if domain not in self.limiters:
                self.limiters[domain] = RateLimiter(self.default_config)
                self.logger.info(f"Created rate limiter for {domain}")
        
        return self.limiters[domain].acquire(wait)
    
    def get_stats(self, domain: Optional[str] = None) -> Dict:
        """Get statistics for a specific domain or all domains.
        
        Args:
            domain: Specific domain, or None for all domains
            
        Returns:
            Dictionary with statistics
        """
        with self.lock:
            if domain:
                if domain in self.limiters:
                    return {domain: self.limiters[domain].get_stats()}
                return {domain: "No stats available"}
            
            return {
                domain: limiter.get_stats() 
                for domain, limiter in self.limiters.items()
            }
