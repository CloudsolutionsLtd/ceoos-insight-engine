"""
Current tenant for generator queries and cache keys.

Account numbers are only unique within one institution. Entry points (API
requests, Kafka handlers) wrap work in `use_tenant(company_id)`. Every pooled
connection gets the Postgres setting `ceoos.tenant` on acquire (see the pool
`setup` hook in main.py), and queries filter with `tenant_ok(company_id)`.
None means unscoped (admins / internal jobs): no filter.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional

_current: ContextVar[Optional[str]] = ContextVar("tenant", default=None)
UNSCOPED = "*"


@contextmanager
def use_tenant(company_id: Optional[str]):
    token = _current.set(company_id)
    try:
        yield
    finally:
        _current.reset(token)


def company() -> Optional[str]:
    return _current.get()


def setting_value() -> str:
    c = _current.get()
    return UNSCOPED if c is None else c


def scoped(key: str) -> str:
    return f"t:{_current.get() or '_'}:{key}"


TENANT_SQL = """
-- True when the row belongs to the tenant of the current request
-- (setting ceoos.tenant), or when the request is unscoped ('*').
CREATE OR REPLACE FUNCTION tenant_ok(row_company text) RETURNS boolean
LANGUAGE sql STABLE AS $fn$
    SELECT COALESCE(current_setting('ceoos.tenant', true), '*') = '*'
        OR COALESCE(row_company, '') = current_setting('ceoos.tenant', true)
$fn$;
"""
