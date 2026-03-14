"""Google Cloud Storage client helper."""

from __future__ import annotations

from pathlib import Path


class GCSClient:
    """Thin wrapper around a single configured GCS bucket."""

    def __init__(self, project_id: str, bucket: str) -> None:
        from google.cloud import storage

        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket)

    def upload_file(self, source: Path, destination_blob: str) -> str:
        blob = self.bucket.blob(destination_blob)
        blob.upload_from_filename(str(source))
        return f"gs://{self.bucket.name}/{destination_blob}"
