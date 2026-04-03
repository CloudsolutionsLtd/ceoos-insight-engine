import functools
import time
from typing import Any, Callable, Optional

_cache = {}
_cache_times = {}

def cache_result(func=None, *, ttl_seconds: Optional[int] = None):
    """
    Decorator to cache function results with optional TTL
    
    Can be used as:
    @cache_result
    or
    @cache_result(ttl_seconds=3600)
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args, **kwargs) -> Any:
            # Create cache key from function name and arguments
            cache_key = f"{f.__name__}:{str(args)}:{str(kwargs)}"
            
            # Check if cache exists and is still valid
            current_time = time.time()
            if cache_key in _cache:
                if ttl_seconds is None:
                    # No TTL, return cached value
                    return _cache[cache_key]
                elif current_time - _cache_times.get(cache_key, 0) < ttl_seconds:
                    # TTL not expired
                    return _cache[cache_key]
            
            # Cache miss or expired, call the function
            result = f(*args, **kwargs)
            _cache[cache_key] = result
            _cache_times[cache_key] = current_time
            return result
        return wrapper
    
    if func is None:
        # Called with parameters like @cache_result(ttl_seconds=3600)
        return decorator
    else:
        # Called without parameters like @cache_result
        return decorator(func)

def cache_response(func=None, *, ttl_seconds: Optional[int] = None):
    """
    Decorator to cache API responses with optional TTL
    
    Can be used as:
    @cache_response
    or
    @cache_response(ttl_seconds=3600)
    """
    return cache_result(func=func, ttl_seconds=ttl_seconds)