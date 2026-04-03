import functools
import time
from typing import Any, Callable, Optional

def setup_metrics():
    pass

def track_request(func):
    return func

def track_generation_time(metric_name: Optional[str] = None):
    """
    Decorator to track generation time
    
    Can be used as:
    @track_generation_time
    or
    @track_generation_time('metric_name')
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            
            # Use provided metric name or function name
            name = metric_name or func.__name__
            print(f"Generation time for {name}: {duration:.2f} seconds")
            
            # You could also log this to a metrics system
            return result
        return wrapper
    
    # Handle both @track_generation_time and @track_generation_time('name')
    if callable(metric_name):
        # Called as @track_generation_time without parentheses
        func = metric_name
        metric_name = None
        return decorator(func)
    
    # Called as @track_generation_time('name') or @track_generation_time()
    return decorator

# For backward compatibility, also support the old way
def track_generation_time_simple(func):
    """Simple version without metric name support"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        print(f"Generation time for {func.__name__}: {duration:.2f} seconds")
        return result
    return wrapper