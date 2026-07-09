"""
core/middleware.py
──────────────────
Production-grade FastAPI middleware for SPV Quantum AI.

Provides:
  - CorrelationIDMiddleware  — injects X-Correlation-ID on every request/response
  - RequestLoggingMiddleware — structured access log (method, path, status, ms)
"""

import base64
import secrets
import time
import uuid
from typing import Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

from core.config import settings
from core.logging import get_logger

logger = get_logger("http_middleware")


_warned_unconfigured = False

def check_basic_auth_header(auth_header: Optional[str]) -> bool:
    """
    Shared credential check used by both BasicAuthMiddleware (HTTP routes) and
    the /ws WebSocket handler — Starlette's BaseHTTPMiddleware does not run for
    websocket-scope connections, so that route must call this directly.

    Fails closed: returns False (deny) if DASHBOARD_PASSWORD isn't configured,
    rather than silently letting traffic through unauthenticated.
    """
    global _warned_unconfigured
    if not settings.DASHBOARD_PASSWORD:
        if not _warned_unconfigured:
            logger.error(
                "DASHBOARD_PASSWORD is not set — refusing all requests. "
                "Set DASHBOARD_USERNAME and DASHBOARD_PASSWORD in .env."
            )
            _warned_unconfigured = True
        return False

    if not auth_header or not auth_header.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
        username, _, password = decoded.partition(":")
    except Exception:
        return False

    valid_user = secrets.compare_digest(username, settings.DASHBOARD_USERNAME)
    valid_pass = secrets.compare_digest(password, settings.DASHBOARD_PASSWORD)
    return valid_user and valid_pass


class BasicAuthMiddleware(BaseHTTPMiddleware):
    """
    Gates every HTTP request behind Basic Auth. This dashboard can place and
    cancel real orders on a live broker and has no other auth layer, so it
    must never be reachable without credentials — including once deployed
    to a public IP.

    Does NOT cover the /ws WebSocket route (Starlette skips BaseHTTPMiddleware
    for websocket-scope connections) — that route checks check_basic_auth_header()
    directly.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if not check_basic_auth_header(request.headers.get("Authorization")):
            if not settings.DASHBOARD_PASSWORD:
                return JSONResponse(
                    status_code=503,
                    content={"error": "SERVICE_UNAVAILABLE", "message": "Dashboard authentication is not configured."},
                )
            return Response(
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="SPV Quantum AI"'},
                content="Authentication required.",
            )
        return await call_next(request)

# Context-variable so handlers can read the current correlation ID
from contextvars import ContextVar

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    """
    Reads the X-Correlation-ID header from incoming requests.
    If absent, generates a fresh UUID4 correlation ID.
    Stores it in a ContextVar for downstream access, and echoes it back
    in the response header.
    """

    HEADER = "X-Correlation-ID"

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        corr_id = request.headers.get(self.HEADER) or str(uuid.uuid4())
        token = correlation_id_var.set(corr_id)
        try:
            response: Response = await call_next(request)
            response.headers[self.HEADER] = corr_id
            return response
        finally:
            correlation_id_var.reset(token)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Emits a structured log line for every HTTP request:
      method, path, status_code, duration_ms, correlation_id
    Skips health-check noise (e.g. /api/health/status polling).
    """

    _SKIP_PATHS = {"/api/health/status", "/api/status"}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path in self._SKIP_PATHS:
            return await call_next(request)

        t0 = time.perf_counter()
        response: Response = await call_next(request)
        duration_ms = round((time.perf_counter() - t0) * 1000, 2)

        logger.info(
            f"{request.method} {request.url.path} → {response.status_code} ({duration_ms}ms)",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=duration_ms,
            correlation_id=correlation_id_var.get(""),
        )
        return response


def get_correlation_id() -> str:
    """Helper callable usable from any async handler to read the current correlation ID."""
    return correlation_id_var.get("")
