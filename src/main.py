"""Pipeline orchestration entrypoint."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.clients.api_client import APIClient
from src.clients.bigquery_client import BigQueryClient
from src.clients.gcs_client import GCSClient
from src.config import OpenMeteoLocation, Settings, settings
from src.extract.extractor import OpenMeteoExtractor, WorldBankExtractor
from src.load.bq_loader import BigQueryLoader
from src.load.file_writer import write_jsonl
from src.load.gcs_uploader import GCSUploader
from src.transform.runner import TransformRunner
from src.utils.dates import utc_now_iso
from src.utils.logging import configure_logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RawBatch:
    """A set of raw rows that share one landing file and one destination table."""

    filename_prefix: str
    table_ref: str
    records: list[dict[str, Any]]
    dead_letters: list[dict[str, Any]]


def run(run_settings: Settings = settings) -> None:
    """Execute one end-to-end extraction, load, and transform run."""
    run_settings.validate()
    configure_logging(run_settings.log_level)
    _configure_google_project(run_settings)

    extract_run_id = uuid4().hex
    ingestion_ts = utc_now_iso()
    pipeline = _build_pipeline_context(run_settings)

    pipeline.transform_runner.run_files(["001_create_raw.sql"])

    raw_batches = [
        batch
        for batch in (
            _build_world_bank_batch(run_settings, ingestion_ts, extract_run_id),
            _build_open_meteo_batch(run_settings, ingestion_ts, extract_run_id),
        )
        if batch.records or batch.dead_letters
    ]

    if not raw_batches:
        logger.info("No source data fetched")
        return

    for batch in raw_batches:
        _load_raw_batch(batch, extract_run_id, pipeline.uploader, pipeline.bq_client, run_settings)

    if any(batch.records for batch in raw_batches):
        pipeline.transform_runner.run_all(exclude_files={"001_create_raw.sql"})
    logger.info("Pipeline run completed", extra={"context": {"extract_run_id": extract_run_id}})


@dataclass(frozen=True, slots=True)
class PipelineContext:
    gcs_client: GCSClient
    uploader: GCSUploader
    bq_client: BigQueryClient
    transform_runner: TransformRunner


def _configure_google_project(run_settings: Settings) -> None:
    if run_settings.gcp_project_id:
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", run_settings.gcp_project_id)


def _build_pipeline_context(run_settings: Settings) -> PipelineContext:
    """Build shared clients once so the run reuses the same cloud sessions."""
    gcs_client = GCSClient(run_settings.gcp_project_id, run_settings.gcs_bucket)
    bq_client = BigQueryClient(run_settings.gcp_project_id, run_settings.gcp_location)
    return PipelineContext(
        gcs_client=gcs_client,
        uploader=GCSUploader(gcs_client, run_settings.gcs_raw_prefix),
        bq_client=bq_client,
        transform_runner=TransformRunner(
            bq_client=bq_client,
            sql_dir=Path("src/transform/sql"),
            context={
                "GCP_PROJECT_ID": run_settings.gcp_project_id,
                "BQ_RAW_DATASET": run_settings.bq_raw_dataset,
                "BQ_STAGING_DATASET": run_settings.bq_staging_dataset,
                "BQ_MART_DATASET": run_settings.bq_mart_dataset,
                "WORLD_BANK_RAW_TABLE": run_settings.world_bank_raw_table,
                "OPEN_METEO_RAW_TABLE": run_settings.open_meteo_raw_table,
                "OPEN_METEO_TIMEZONE": run_settings.open_meteo_timezone,
            },
        ),
    )


def _build_world_bank_batch(
    run_settings: Settings,
    ingestion_ts: str,
    extract_run_id: str,
) -> RawBatch:
    extractor = WorldBankExtractor(
        api_client=APIClient(
            base_url=run_settings.world_bank_base_url,
            token=run_settings.api_token,
            timeout_seconds=run_settings.api_timeout_seconds,
        ),
        page_size=run_settings.world_bank_page_size,
    )

    records: list[dict[str, Any]] = []
    dead_letters: list[dict[str, Any]] = []
    for indicator_id in run_settings.world_bank_indicator_ids:
        result = extractor.fetch_indicator(indicator_id)
        logger.info(
            "Fetched World Bank indicator",
            extra={
                "context": {
                    "indicator_id": indicator_id,
                    "records": len(result.records),
                    "max_year": result.max_cursor_value,
                }
            },
        )

        # Preserve the provider payload untouched and only add ingestion metadata here.
        valid_records, invalid_records = _partition_world_bank_records(
            records=result.records,
            indicator_id=indicator_id,
            ingestion_ts=ingestion_ts,
            extract_run_id=extract_run_id,
        )
        records.extend(valid_records)
        dead_letters.extend(invalid_records)

    return RawBatch(
        filename_prefix="world_bank_indicator",
        table_ref=(
            f"{run_settings.gcp_project_id}.{run_settings.bq_raw_dataset}.{run_settings.world_bank_raw_table}"
        ),
        records=records,
        dead_letters=dead_letters,
    )


def _build_open_meteo_batch(
    run_settings: Settings,
    ingestion_ts: str,
    extract_run_id: str,
) -> RawBatch:
    extractor = OpenMeteoExtractor(
        forecast_client=APIClient(
            base_url=run_settings.open_meteo_forecast_base_url,
            timeout_seconds=run_settings.api_timeout_seconds,
        ),
        archive_client=APIClient(
            base_url=run_settings.open_meteo_archive_base_url,
            timeout_seconds=run_settings.api_timeout_seconds,
        ),
    )

    records: list[dict[str, Any]] = []
    dead_letters: list[dict[str, Any]] = []
    for location in run_settings.open_meteo_locations:
        valid_records, invalid_records = _build_open_meteo_location_rows(
            run_settings=run_settings,
            extractor=extractor,
            location=location,
            ingestion_ts=ingestion_ts,
            extract_run_id=extract_run_id,
        )
        records.extend(valid_records)
        dead_letters.extend(invalid_records)

    return RawBatch(
        filename_prefix="open_meteo_response",
        table_ref=(
            f"{run_settings.gcp_project_id}.{run_settings.bq_raw_dataset}.{run_settings.open_meteo_raw_table}"
        ),
        records=records,
        dead_letters=dead_letters,
    )


def _build_open_meteo_location_rows(
    run_settings: Settings,
    extractor: OpenMeteoExtractor,
    location: OpenMeteoLocation,
    ingestion_ts: str,
    extract_run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    dead_letters: list[dict[str, Any]] = []

    if run_settings.open_meteo_enable_archive:
        archive_result = extractor.fetch_archive(
            location=location,
            hourly_variables=run_settings.open_meteo_hourly_variables,
            archive_days=run_settings.open_meteo_archive_days,
            timezone=run_settings.open_meteo_timezone,
        )
        logger.info(
            "Fetched Open-Meteo archive window",
            extra={
                "context": {
                    "location_id": location.location_id,
                    "records": len(archive_result.records),
                    "max_timestamp": archive_result.max_cursor_value,
                }
            },
        )
        valid_rows, invalid_rows = _build_open_meteo_raw_rows(
            payloads=archive_result.records,
            location=location,
            ingestion_ts=ingestion_ts,
            extract_run_id=extract_run_id,
            source_variant="archive",
        )
        rows.extend(valid_rows)
        dead_letters.extend(invalid_rows)

    if run_settings.open_meteo_enable_forecast:
        forecast_result = extractor.fetch_forecast(
            location=location,
            hourly_variables=run_settings.open_meteo_hourly_variables,
            timezone=run_settings.open_meteo_timezone,
            forecast_days=run_settings.open_meteo_forecast_days,
        )
        logger.info(
            "Fetched Open-Meteo forecast window",
            extra={
                "context": {
                    "location_id": location.location_id,
                    "records": len(forecast_result.records),
                    "max_timestamp": forecast_result.max_cursor_value,
                }
            },
        )
        valid_rows, invalid_rows = _build_open_meteo_raw_rows(
            payloads=forecast_result.records,
            location=location,
            ingestion_ts=ingestion_ts,
            extract_run_id=extract_run_id,
            source_variant="forecast",
        )
        rows.extend(valid_rows)
        dead_letters.extend(invalid_rows)

    return rows, dead_letters


def _build_open_meteo_raw_rows(
    payloads: list[dict[str, Any]],
    location: OpenMeteoLocation,
    ingestion_ts: str,
    extract_run_id: str,
    source_variant: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_records: list[dict[str, Any]] = []
    dead_letters: list[dict[str, Any]] = []

    for payload in payloads:
        if _is_valid_open_meteo_payload(payload):
            valid_records.append(
                {
                    "ingested_at": ingestion_ts,
                    "extract_run_id": extract_run_id,
                    "source_file": "",
                    "location_id": location.location_id,
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "payload": payload,
                }
            )
            continue

        dead_letters.append(
            _build_dead_letter(
                source_name="open_meteo",
                reason=f"invalid_{source_variant}_payload",
                payload=payload,
                ingestion_ts=ingestion_ts,
                extract_run_id=extract_run_id,
                extra_context={"location_id": location.location_id},
            )
        )

    return valid_records, dead_letters


def _load_raw_batch(
    batch: RawBatch,
    extract_run_id: str,
    uploader: GCSUploader,
    bq_client: BigQueryClient,
    run_settings: Settings,
) -> None:
    """Write one source batch to disk, upload it, then load it into BigQuery."""
    if batch.dead_letters:
        _write_dead_letters(batch, extract_run_id, run_settings.dead_letter_dir)
    if not batch.records:
        return

    landing_file = (
        run_settings.local_data_dir
        / f"{batch.filename_prefix}_{extract_run_id}_{utc_now_iso().replace(':', '-')}.jsonl"
    )

    # The landing filename becomes part of the raw metadata for traceability.
    for row in batch.records:
        row["source_file"] = landing_file.name

    write_jsonl(batch.records, landing_file)
    gcs_uri = uploader.upload_run_file(landing_file, extract_run_id)
    BigQueryLoader(bq_client=bq_client, table_ref=batch.table_ref).load_jsonl(gcs_uri)


def _partition_world_bank_records(
    records: list[dict[str, Any]],
    indicator_id: str,
    ingestion_ts: str,
    extract_run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_records: list[dict[str, Any]] = []
    dead_letters: list[dict[str, Any]] = []

    for record in records:
        if _is_valid_world_bank_record(record):
            valid_records.append(
                {
                    "ingested_at": ingestion_ts,
                    "extract_run_id": extract_run_id,
                    "source_file": "",
                    "indicator_id": indicator_id,
                    "payload": record,
                }
            )
            continue

        dead_letters.append(
            _build_dead_letter(
                source_name="world_bank",
                reason="invalid_indicator_record",
                payload=record,
                ingestion_ts=ingestion_ts,
                extract_run_id=extract_run_id,
                extra_context={"indicator_id": indicator_id},
            )
        )

    return valid_records, dead_letters


def _is_valid_world_bank_record(record: dict[str, Any]) -> bool:
    country = record.get("country")
    indicator = record.get("indicator")
    return bool(
        isinstance(country, dict)
        and isinstance(indicator, dict)
        and record.get("countryiso3code")
        and indicator.get("id")
        and record.get("date")
    )


def _is_valid_open_meteo_payload(payload: dict[str, Any]) -> bool:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return False

    values_by_field = [
        hourly.get("time"),
        hourly.get("temperature_2m"),
        hourly.get("precipitation"),
        hourly.get("wind_speed_10m"),
    ]
    if not all(isinstance(values, list) for values in values_by_field):
        return False

    expected_length = len(values_by_field[0])
    return expected_length > 0 and all(len(values) == expected_length for values in values_by_field)


def _build_dead_letter(
    source_name: str,
    reason: str,
    payload: dict[str, Any],
    ingestion_ts: str,
    extract_run_id: str,
    extra_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "source_name": source_name,
        "reason": reason,
        "ingested_at": ingestion_ts,
        "extract_run_id": extract_run_id,
        "context": extra_context or {},
        "payload": payload,
    }


def _write_dead_letters(batch: RawBatch, extract_run_id: str, dead_letter_dir: Path) -> None:
    dead_letter_file = (
        dead_letter_dir
        / f"{batch.filename_prefix}_{extract_run_id}_{utc_now_iso().replace(':', '-')}.jsonl"
    )
    write_jsonl(batch.dead_letters, dead_letter_file)


if __name__ == "__main__":
    run()
