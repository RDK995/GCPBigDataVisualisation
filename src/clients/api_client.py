"""External API client wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json

from src.utils.retries import with_default_retry


@dataclass(slots=True)
class APIClient:
    base_url: str
    token: str
    timeout_seconds: int = 30

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

    @with_default_retry
    def get(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """Perform GET request with default retries.

        TODO(auth): implement token refresh flow for expiring credentials.
        """
        query = urlencode(params)
        url = f"{self.base_url.rstrip('/')}{endpoint}?{query}"
        request = Request(url=url, headers=self._headers(), method="GET")
        with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))
