"""File-based checkpoint store for idempotent incremental runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Checkpoint:
    updated_since: str | None


class CheckpointStore:
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> Checkpoint:
        if not self.file_path.exists():
            return Checkpoint(updated_since=None)

        payload = json.loads(self.file_path.read_text())
        return Checkpoint(updated_since=payload.get("updated_since"))

    def write(self, checkpoint: Checkpoint) -> None:
        self.file_path.write_text(
            json.dumps({"updated_since": checkpoint.updated_since}, indent=2)
        )
