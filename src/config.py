"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    env: str = os.getenv("ENV", "dev")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    api_base_url: str = os.getenv("API_BASE_URL", "https://api.example.com")
    api_token: str = os.getenv("API_TOKEN", "")
    api_page_size: int = int(os.getenv("API_PAGE_SIZE", "500"))
    api_timeout_seconds: int = int(os.getenv("API_TIMEOUT_SECONDS", "30"))
    api_incremental_field: str = os.getenv("API_INCREMENTAL_FIELD", "updated_at")

    checkpoint_file: Path = Path(os.getenv("CHECKPOINT_FILE", "./state/checkpoint.json"))
    local_data_dir: Path = Path(os.getenv("LOCAL_DATA_DIR", "./data"))
    dead_letter_dir: Path = Path(os.getenv("DEAD_LETTER_DIR", "./data/dead_letter"))

    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    gcp_location: str = os.getenv("GCP_LOCATION", "US")
    gcs_bucket: str = os.getenv("GCS_BUCKET", "")
    gcs_raw_prefix: str = os.getenv("GCS_RAW_PREFIX", "raw/api_entities")

    bq_raw_dataset: str = os.getenv("BQ_RAW_DATASET", "raw")
    bq_staging_dataset: str = os.getenv("BQ_STAGING_DATASET", "staging")
    bq_mart_dataset: str = os.getenv("BQ_MART_DATASET", "mart")
    bq_raw_table: str = os.getenv("BQ_RAW_TABLE", "api_entities_raw")

    source_name: str = os.getenv("SOURCE_NAME", "example_api")


settings = Settings()
