"""BigQuery client helper operations."""

from __future__ import annotations


class BigQueryClient:
    """Thin wrapper around BigQuery operations used by the pipeline."""

    def __init__(self, project_id: str, location: str = "US") -> None:
        from google.cloud import bigquery

        self._bigquery = bigquery
        self.client = bigquery.Client(project=project_id, location=location)

    def run_query(self, sql: str) -> None:
        job = self.client.query(sql)
        job.result()

    def load_jsonl_from_gcs(
        self,
        gcs_uri: str,
        table_ref: str,
        write_disposition: str | None = None,
    ) -> None:
        config = self._bigquery.LoadJobConfig(
            source_format=self._bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition
            or self._bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[self._bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            ignore_unknown_values=True,
        )
        job = self.client.load_table_from_uri(gcs_uri, table_ref, job_config=config)
        job.result()
