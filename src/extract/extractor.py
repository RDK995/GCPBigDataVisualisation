"""Incremental data extractor with pagination and rate-limit-aware retries."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from src.clients.api_client import APIClient
from src.extract.paginator import PageState, build_page_params

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ExtractResult:
    records: list[dict[str, Any]]
    max_updated_at: str | None


class IncrementalExtractor:
    def __init__(
        self,
        api_client: APIClient,
        endpoint: str,
        page_size: int,
        incremental_field: str,
    ) -> None:
        self.api_client = api_client
        self.endpoint = endpoint
        self.page_size = page_size
        self.incremental_field = incremental_field

    def fetch(self, updated_since: str | None) -> ExtractResult:
        all_records: list[dict[str, Any]] = []
        max_updated_at: str | None = updated_since
        page_state = PageState()

        while True:
            params = build_page_params(
                page_state=page_state,
                page_size=self.page_size,
                incremental_field=self.incremental_field,
                updated_since=updated_since,
            )
            payload = self.api_client.get(self.endpoint, params)

            # TODO(api): map these keys to your API response schema.
            records = payload.get("data", [])
            next_cursor = payload.get("next_cursor")
            has_more = payload.get("has_more", False)

            if not records:
                break

            all_records.extend(records)
            for record in records:
                record_ts = record.get(self.incremental_field)
                if record_ts and (max_updated_at is None or record_ts > max_updated_at):
                    max_updated_at = record_ts

            logger.info(
                "Fetched page",
                extra={"context": {"count": len(records), "next_cursor": next_cursor}},
            )

            if next_cursor:
                page_state.cursor = next_cursor
            elif has_more:
                page_state.page_number += 1
            else:
                break

        return ExtractResult(records=all_records, max_updated_at=max_updated_at)
