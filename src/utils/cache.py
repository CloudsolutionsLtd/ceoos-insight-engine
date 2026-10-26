"""
In-process TTL cache for async API handlers.

The previous decorator was synchronous: on an async function it cached the
coroutine object, so a cache hit returned an already-awaited coroutine
("cannot reuse already awaited coroutine"), and because the key included
repr(request) it also grew without bound. This version awaits the function,
bounds the cache, expires entries and leaves per-request objects out of the
key. The caller's data scope (api_key / tenant) stays in the key, so tenants
never see each other's cached responses.
"""

import functools
import hashlib
import time
from collections import OrderedDict
from typing import Any, Callable, Optional

_SKIP = ("Request", "BackgroundTasks", "Response")
MAX_ENTRIES = 4096


def _part(v: Any) -> str:
    return "" if type(v).__name__ in _SKIP else repr(v)


def cache_result(func=None, *, ttl_seconds: Optional[int] = None):
    ttl = ttl_seconds or 300

    def decorator(f: Callable) -> Callable:
        store: "OrderedDict[str, tuple]" = OrderedDict()

        @functools.wraps(f)
        async def wrapper(*args, **kwargs) -> Any:
            raw = f.__qualname__ + "|" + "|".join(_part(a) for a in args) + "|" + "|".join(
                f"{k}={_part(v)}" for k, v in sorted(kwargs.items()))
            key = hashlib.sha256(raw.encode()).hexdigest()
            now = time.monotonic()
            hit = store.get(key)
            if hit and now - hit[1] < ttl:
                store.move_to_end(key)
                return hit[0]
            result = await f(*args, **kwargs)
            store[key] = (result, now)
            store.move_to_end(key)
            while len(store) > MAX_ENTRIES:
                store.popitem(last=False)
            return result

        return wrapper

    return decorator if func is None else decorator(func)


def cache_response(func=None, *, ttl_seconds: Optional[int] = None):
    return cache_result(func=func, ttl_seconds=ttl_seconds)
