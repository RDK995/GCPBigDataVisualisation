"""Helpers for source pagination parameters."""

from __future__ import annotations

from typing import Any


def build_world_bank_page_params(page_number: int, page_size: int) -> dict[str, Any]:
    """Build the exact parameter contract expected by the World Bank API."""
    return {
        "format": "json",
        "page": page_number,
        "per_page": page_size,
    }
