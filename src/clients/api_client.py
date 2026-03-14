"""External API client wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from email.utils import parsedate_to_datetime
import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from src.utils.retries import with_default_retry


@dataclass(slots=True)
class APIClient:
    """Minimal HTTP client wrapper for public JSON APIs used by the pipeline."""

    base_url: str
    token: str = ""
    timeout_seconds: int = 30

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    @with_default_retry
    def get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """Perform a GET request with retry-safe handling for transient provider failures."""
        query = urlencode(params, doseq=True)
        url = f"{self.base_url.rstrip('/')}{endpoint}?{query}"
        request = Request(url=url, headers=self._headers(), method="GET")
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 429:
                _sleep_for_retry_after(exc)
                raise ConnectionError(f"rate limited by upstream API: {url}") from exc
            if exc.code >= 500:
                raise ConnectionError(f"upstream API server error: {url}") from exc
            raise
        except URLError as exc:
            raise ConnectionError(f"request failed for {url}") from exc


def _sleep_for_retry_after(error: HTTPError) -> None:
    """Honor `Retry-After` when an upstream API provides a concrete delay."""
    retry_after = error.headers.get("Retry-After")
    if not retry_after:
        return

    try:
        time.sleep(max(0, int(retry_after)))
        return
    except ValueError:
        retry_at = parsedate_to_datetime(retry_after)
        delay_seconds = retry_at.timestamp() - time.time()
        if delay_seconds > 0:
            time.sleep(delay_seconds)
