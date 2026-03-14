"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class OpenMeteoLocation:
    """A stable reporting location for Open-Meteo requests."""

    location_id: str
    latitude: float
    longitude: float

    @classmethod
    def parse(cls, raw_location: str) -> "OpenMeteoLocation":
        """Parse `location_id:latitude:longitude` into a typed location object."""
        location_id, latitude, longitude = raw_location.split(":", maxsplit=2)
        return cls(
            location_id=location_id,
            latitude=float(latitude),
            longitude=float(longitude),
        )


@dataclass(slots=True)
class Settings:
    """All runtime configuration for the ingestion job."""

    env: str = os.getenv("ENV", "dev")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    local_data_dir: Path = Path(os.getenv("LOCAL_DATA_DIR", "./data"))
    dead_letter_dir: Path = Path(os.getenv("DEAD_LETTER_DIR", "./data/dead_letter"))

    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    gcp_location: str = os.getenv("GCP_LOCATION", "US")
    gcs_bucket: str = os.getenv("GCS_BUCKET", "")
    gcs_raw_prefix: str = os.getenv("GCS_RAW_PREFIX", "raw")

    bq_raw_dataset: str = os.getenv("BQ_RAW_DATASET", "analytics_raw")
    bq_staging_dataset: str = os.getenv("BQ_STAGING_DATASET", "analytics_staging")
    bq_mart_dataset: str = os.getenv("BQ_MART_DATASET", "analytics_mart")
    world_bank_raw_table: str = os.getenv("WORLD_BANK_RAW_TABLE", "world_bank_indicator")
    open_meteo_raw_table: str = os.getenv("OPEN_METEO_RAW_TABLE", "open_meteo_response")

    api_token: str = os.getenv("API_TOKEN", "")
    api_timeout_seconds: int = int(os.getenv("API_TIMEOUT_SECONDS", "30"))

    world_bank_base_url: str = os.getenv("WORLD_BANK_BASE_URL", "https://api.worldbank.org/v2")
    world_bank_indicator_ids_raw: str = os.getenv("WORLD_BANK_INDICATOR_IDS", "SP.POP.TOTL")
    world_bank_page_size: int = int(os.getenv("WORLD_BANK_PAGE_SIZE", "20000"))

    open_meteo_forecast_base_url: str = os.getenv(
        "OPEN_METEO_FORECAST_BASE_URL",
        "https://api.open-meteo.com/v1",
    )
    open_meteo_archive_base_url: str = os.getenv(
        "OPEN_METEO_ARCHIVE_BASE_URL",
        "https://archive-api.open-meteo.com/v1",
    )
    open_meteo_hourly_variables_raw: str = os.getenv(
        "OPEN_METEO_HOURLY_VARIABLES",
        "temperature_2m,precipitation,wind_speed_10m",
    )
    open_meteo_locations_raw: str = os.getenv(
        "OPEN_METEO_LOCATIONS",
        "london:51.5072:-0.1276",
    )
    open_meteo_timezone: str = os.getenv("OPEN_METEO_TIMEZONE", "UTC")
    open_meteo_archive_days: int = int(os.getenv("OPEN_METEO_ARCHIVE_DAYS", "7"))
    open_meteo_forecast_days: int = int(os.getenv("OPEN_METEO_FORECAST_DAYS", "3"))
    open_meteo_enable_forecast: bool = (
        os.getenv("OPEN_METEO_ENABLE_FORECAST", "true").lower() == "true"
    )
    open_meteo_enable_archive: bool = (
        os.getenv("OPEN_METEO_ENABLE_ARCHIVE", "true").lower() == "true"
    )

    @property
    def world_bank_indicator_ids(self) -> list[str]:
        return [
            indicator_id.strip()
            for indicator_id in self.world_bank_indicator_ids_raw.split(",")
            if indicator_id.strip()
        ]

    @property
    def open_meteo_hourly_variables(self) -> list[str]:
        return [
            variable.strip()
            for variable in self.open_meteo_hourly_variables_raw.split(",")
            if variable.strip()
        ]

    @property
    def open_meteo_locations(self) -> list[OpenMeteoLocation]:
        return [
            OpenMeteoLocation.parse(raw_location.strip())
            for raw_location in self.open_meteo_locations_raw.split(";")
            if raw_location.strip()
        ]

    def validate(self) -> None:
        """Fail fast on configuration errors before any network calls happen."""
        if not self.gcp_project_id:
            raise ValueError("GCP_PROJECT_ID must be set")
        if not self.gcs_bucket:
            raise ValueError("GCS_BUCKET must be set")
        if not self.world_bank_indicator_ids:
            raise ValueError("WORLD_BANK_INDICATOR_IDS must include at least one indicator")
        if not self.open_meteo_locations:
            raise ValueError("OPEN_METEO_LOCATIONS must include at least one location")
        if self.world_bank_page_size < 1:
            raise ValueError("WORLD_BANK_PAGE_SIZE must be >= 1")
        if self.open_meteo_archive_days < 1:
            raise ValueError("OPEN_METEO_ARCHIVE_DAYS must be >= 1")
        if self.open_meteo_forecast_days < 1:
            raise ValueError("OPEN_METEO_FORECAST_DAYS must be >= 1")


settings = Settings()
