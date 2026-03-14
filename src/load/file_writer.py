"""Write extracted records to local landing files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_jsonl(records: list[dict[str, Any]], output_path: Path) -> None:
    """Write newline-delimited JSON while creating parent directories on demand."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")
