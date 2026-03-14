"""Source-specific extractors for World Bank and Open-Meteo."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone as dt_timezone
from typing import Any

from src.clients.api_client import APIClient
from src.config import OpenMeteoLocation
from src.extract.paginator import build_world_bank_page_params


@dataclass(slots=True)
class ExtractResult:
    """Records returned from an extraction call plus a source-specific high-water mark."""

    records: list[dict[str, Any]]
    max_cursor_value: str | None


class WorldBankExtractor:
    """Fetch paginated country-level indicator records from the World Bank API."""

    def __init__(self, api_client: APIClient, page_size: int) -> None:
        self.api_client = api_client
        self.page_size = page_size

    def fetch_indicator(self, indicator_id: str) -> ExtractResult:
        page_number = 1
        all_records: list[dict[str, Any]] = []
        max_year: str | None = None

        while True:
            payload = self.api_client.get(
                f"/country/all/indicator/{indicator_id}",
                build_world_bank_page_params(page_number, self.page_size),
            )
            metadata, records = _parse_world_bank_response(payload)
            if not records:
                break

            all_records.extend(records)
            max_year = _max_world_bank_year(records, max_year)

            if page_number >= int(metadata.get("pages", page_number)):
                break
            page_number += 1

        return ExtractResult(records=all_records, max_cursor_value=max_year)


class OpenMeteoExtractor:
    """Fetch forecast and archive payloads for configured Open-Meteo locations."""

    def __init__(self, forecast_client: APIClient, archive_client: APIClient) -> None:
        self.forecast_client = forecast_client
        self.archive_client = archive_client

    def fetch_forecast(
        self,
        location: OpenMeteoLocation,
        hourly_variables: list[str],
        timezone: str = "UTC",
        forecast_days: int = 3,
    ) -> ExtractResult:
        payload = self.forecast_client.get(
            "/forecast",
            _build_open_meteo_params(
                location=location,
                hourly_variables=hourly_variables,
                timezone=timezone,
                extra_params={"forecast_days": forecast_days},
            ),
        )
        return ExtractResult([payload], _max_open_meteo_timestamp(payload))

    def fetch_archive(
        self,
        location: OpenMeteoLocation,
        hourly_variables: list[str],
        archive_days: int = 7,
        timezone: str = "UTC",
        end_date: date | None = None,
    ) -> ExtractResult:
        archive_end = end_date or datetime.now(dt_timezone.utc).date()
        archive_start = archive_end - timedelta(days=max(archive_days - 1, 0))
        payload = self.archive_client.get(
            "/archive",
            _build_open_meteo_params(
                location=location,
                hourly_variables=hourly_variables,
                timezone=timezone,
                extra_params={
                    "start_date": archive_start.isoformat(),
                    "end_date": archive_end.isoformat(),
                },
            ),
        )
        return ExtractResult([payload], _max_open_meteo_timestamp(payload))


def _build_open_meteo_params(
    location: OpenMeteoLocation,
    hourly_variables: list[str],
    timezone: str,
    extra_params: dict[str, Any],
) -> dict[str, Any]:
    """Centralize request shaping so archive and forecast stay in lockstep."""
    return {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "hourly": ",".join(hourly_variables),
        "timezone": timezone,
        **extra_params,
    }


def _parse_world_bank_response(
    payload: dict[str, Any] | list[Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """World Bank returns a two-element JSON array: metadata then records."""
    if not isinstance(payload, list) or len(payload) < 2:
        return {}, []

    metadata = payload[0] if isinstance(payload[0], dict) else {}
    raw_records = payload[1] if isinstance(payload[1], list) else []
    return metadata, [record for record in raw_records if isinstance(record, dict)]


def _max_world_bank_year(records: list[dict[str, Any]], current_max: str | None) -> str | None:
    max_year = current_max
    for record in records:
        record_year = record.get("date")
        if record_year and (max_year is None or record_year > max_year):
            max_year = record_year
    return max_year


def _max_open_meteo_timestamp(payload: dict[str, Any]) -> str | None:
    hourly = payload.get("hourly", {})
    if not isinstance(hourly, dict):
        return None

    timestamps = hourly.get("time", [])
    if not isinstance(timestamps, list) or not timestamps:
        return None
    return str(timestamps[-1])
