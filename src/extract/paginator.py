"""Pagination helper abstractions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class PageState:
    cursor: str | None = None
    page_number: int = 1


def build_page_params(
    page_state: PageState,
    page_size: int,
    incremental_field: str,
    updated_since: str | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": page_size}

    if page_state.cursor:
        params["cursor"] = page_state.cursor
    else:
        params["page"] = page_state.page_number

    if updated_since:
        params[f"{incremental_field}_gte"] = updated_since

    return params
