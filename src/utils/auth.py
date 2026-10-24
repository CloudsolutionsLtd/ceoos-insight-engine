"""
Caller authentication for the insight engine.

Returns the caller's data scope: their company id, or ALL_COMPANIES ("*")
for admins and internal service calls.

Accepted:
  1. Requests forwarded by the api-gateway: valid x-internal-token plus the
     x-company-id / x-user-role headers it sets.
  2. Direct calls with a Bearer JWT signed by auth-service (HS256).

(Previously any request without a token was let in as "anonymous".)
"""
import hmac
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.config import settings

ALL_COMPANIES = "*"

security = HTTPBearer(auto_error=False)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def verify_api_key(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> str:
    if not settings.enable_auth:
        return ALL_COMPANIES

    internal = request.headers.get("x-internal-token")
    if internal and settings.internal_service_token and hmac.compare_digest(
        internal, settings.internal_service_token
    ):
        if request.headers.get("x-user-role") == "admin":
            return ALL_COMPANIES
        return request.headers.get("x-company-id") or ALL_COMPANIES

    if credentials is None or not settings.jwt_secret:
        raise _unauthorized("Authentication required")
    try:
        payload = jwt.decode(credentials.credentials, settings.jwt_secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise _unauthorized("Invalid or expired token")

    if payload.get("role") == "admin":
        return ALL_COMPANIES
    return str(payload.get("companyId") or payload.get("userId") or payload.get("sub"))


def company_of(scope: str) -> Optional[str]:
    return None if scope == ALL_COMPANIES else scope
