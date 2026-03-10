"""Pipeline orchestration entrypoint."""

from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

from src.clients.api_client import APIClient
from src.clients.bigquery_client import BigQueryClient
from src.clients.gcs_client import GCSClient
from src.config import settings
from src.extract.checkpoint import Checkpoint, CheckpointStore
from src.extract.extractor import IncrementalExtractor
from src.load.bq_loader import BigQueryLoader
from src.load.file_writer import write_jsonl
from src.load.gcs_uploader import GCSUploader
from src.transform.runner import TransformRunner
from src.utils.dates import utc_now_iso
from src.utils.logging import configure_logging

logger = logging.getLogger(__name__)


def run() -> None:
    configure_logging(settings.log_level)
    extract_run_id = uuid4().hex

    checkpoint_store = CheckpointStore(settings.checkpoint_file)
    previous_checkpoint = checkpoint_store.read()

    api_client = APIClient(
        base_url=settings.api_base_url,
        token=settings.api_token,
        timeout_seconds=settings.api_timeout_seconds,
    )
    extractor = IncrementalExtractor(
        api_client=api_client,
        endpoint="/v1/entities",  # TODO(api): fill in endpoint.
        page_size=settings.api_page_size,
        incremental_field=settings.api_incremental_field,
    )

    extract_result = extractor.fetch(previous_checkpoint.updated_since)
    if not extract_result.records:
        logger.info("No new records found")
        return

    ingestion_ts = utc_now_iso()
    enriched_records = [
        {
            "payload": record,
            "source_file": "",
            "extract_run_id": extract_run_id,
            "ingested_at": ingestion_ts,
            "source_name": settings.source_name,
        }
        for record in extract_result.records
    ]

    landing_file = (
        settings.local_data_dir
        / f"{settings.source_name}_{extract_run_id}_{ingestion_ts.replace(':', '-')}.jsonl"
    )
    for row in enriched_records:
        row["source_file"] = landing_file.name
    write_jsonl(enriched_records, landing_file)

    gcs_client = GCSClient(settings.gcp_project_id, settings.gcs_bucket)
    uploader = GCSUploader(gcs_client, settings.gcs_raw_prefix)
    gcs_uri = uploader.upload_run_file(landing_file, extract_run_id)

    bq_client = BigQueryClient(settings.gcp_project_id, settings.gcp_location)
    raw_table_ref = (
        f"{settings.gcp_project_id}.{settings.bq_raw_dataset}.{settings.bq_raw_table}"
    )
    bq_loader = BigQueryLoader(bq_client=bq_client, table_ref=raw_table_ref)
    bq_loader.load_jsonl(gcs_uri)

    transform_runner = TransformRunner(bq_client, Path("src/transform/sql"))
    transform_runner.run_all()

    checkpoint_store.write(Checkpoint(updated_since=extract_result.max_updated_at))
    logger.info("Pipeline run completed", extra={"context": {"extract_run_id": extract_run_id}})


if __name__ == "__main__":
    run()
