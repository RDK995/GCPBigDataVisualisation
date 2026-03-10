"""Run SQL transformations in sequence."""

from __future__ import annotations

from pathlib import Path

from src.clients.bigquery_client import BigQueryClient


class TransformRunner:
    def __init__(self, bq_client: BigQueryClient, sql_dir: Path) -> None:
        self.bq_client = bq_client
        self.sql_dir = sql_dir

    def run_all(self) -> None:
        for sql_file in sorted(self.sql_dir.glob("*.sql")):
            sql = sql_file.read_text(encoding="utf-8")
            self.bq_client.run_query(sql)
