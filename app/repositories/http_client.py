"""Shared HTTP retry utility for repository clients."""

from __future__ import annotations

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

# Longest single wait between attempts, so a page request never stalls for long.
MAX_RETRY_DELAY_SECONDS = 5.0


def _retry_delay(attempt: int, response: httpx.Response | None = None) -> float:
    """Seconds to wait before the next attempt: Retry-After if given, else exponential backoff."""

    if response is not None:
        retry_after = response.headers.get("Retry-After", "")
        try:
            return min(max(float(retry_after), 0.0), MAX_RETRY_DELAY_SECONDS)
        except ValueError:
            pass
    return min(0.5 * 2**attempt, MAX_RETRY_DELAY_SECONDS)


async def get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    params: dict | None = None,
    retries: int = 2,
    error_cls: type[RuntimeError] = RuntimeError,
    label: str = "HTTP",
) -> httpx.Response:
    """GET a URL with retry handling, raising error_cls on final failure.

    Handles 404 immediately (no retry). Waits between attempts, honoring
    Retry-After on rate-limit responses. Logs warnings on transient failures.
    """

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        if attempt:
            failed_response = last_error.response if isinstance(last_error, httpx.HTTPStatusError) else None
            await asyncio.sleep(_retry_delay(attempt - 1, failed_response))
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response
        except httpx.TimeoutException as exc:
            last_error = exc
            logger.warning("%s timeout on attempt %s for %s", label, attempt + 1, url)
        except httpx.HTTPStatusError as exc:
            last_error = exc
            status = exc.response.status_code
            if status == 404:
                raise error_cls(f"{label} resource not found") from exc
            if 400 <= status < 500 and status != 429:
                raise error_cls(f"{label} request rejected ({status})") from exc
            logger.warning("%s HTTP error on attempt %s: %s", label, attempt + 1, exc)
        except httpx.HTTPError as exc:
            last_error = exc
            logger.warning("%s request failed on attempt %s: %s", label, attempt + 1, exc)
    raise error_cls(f"{label} request failed") from last_error
