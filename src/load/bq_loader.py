"""Load raw files from GCS into BigQuery raw table."""

from __future__ import annotations

from src.clients.bigquery_client import BigQueryClient


class BigQueryLoader:
    """Load raw JSONL files into a preconfigured BigQuery table."""

    def __init__(self, bq_client: BigQueryClient, table_ref: str) -> None:
        self.bq_client = bq_client
        self.table_ref = table_ref

    def load_jsonl(self, gcs_uri: str) -> None:
        self.bq_client.load_jsonl_from_gcs(gcs_uri=gcs_uri, table_ref=self.table_ref)
