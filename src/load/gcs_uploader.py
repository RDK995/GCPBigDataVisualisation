"""Upload local files to GCS raw zone."""

from __future__ import annotations

from pathlib import Path

from src.clients.gcs_client import GCSClient


class GCSUploader:
    def __init__(self, gcs_client: GCSClient, raw_prefix: str) -> None:
        self.gcs_client = gcs_client
        self.raw_prefix = raw_prefix.rstrip("/")

    def upload_run_file(self, local_file: Path, extract_run_id: str) -> str:
        blob = f"{self.raw_prefix}/extract_run_id={extract_run_id}/{local_file.name}"
        return self.gcs_client.upload_file(local_file, blob)
