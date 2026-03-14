"""Run SQL transformations in sequence."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Mapping

from src.clients.bigquery_client import BigQueryClient


class TransformRunner:
    """Execute BigQuery SQL files with simple `${VAR}` substitution."""

    def __init__(
        self,
        bq_client: BigQueryClient,
        sql_dir: Path,
        context: Mapping[str, str] | None = None,
    ) -> None:
        self.bq_client = bq_client
        self.sql_dir = sql_dir
        self.context = context or {}

    def run_all(self, exclude_files: set[str] | None = None) -> None:
        excluded = exclude_files or set()
        for sql_file in sorted(self.sql_dir.glob("*.sql")):
            if sql_file.name in excluded:
                continue
            self._run_file(sql_file)

    def run_files(self, filenames: list[str]) -> None:
        for filename in filenames:
            self._run_file(self.sql_dir / filename)

    def _run_file(self, sql_file: Path) -> None:
        sql = sql_file.read_text(encoding="utf-8")
        self.bq_client.run_query(_render_sql(sql, self.context))


def _render_sql(sql: str, context: Mapping[str, str]) -> str:
    pattern = re.compile(r"\$\{([A-Z0-9_]+)\}")

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        return context.get(key, match.group(0))

    return pattern.sub(replace, sql)
