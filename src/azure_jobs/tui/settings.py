"""Validated dashboard limits and defaults."""

from __future__ import annotations

from azure_jobs.config.models import DEFAULT_DASHBOARD_PAGE_SIZE

DEFAULT_DASHBOARD_LAST = 50
MAX_DASHBOARD_LAST = 2_000
MAX_DASHBOARD_PAGE_SIZE = 200


def validate_last(value: int) -> int:
    if not 1 <= value <= MAX_DASHBOARD_LAST:
        raise ValueError(
            f"last must be between 1 and {MAX_DASHBOARD_LAST}, got {value}"
        )
    return value


def validate_page_size(value: int) -> int:
    if not 1 <= value <= MAX_DASHBOARD_PAGE_SIZE:
        raise ValueError(
            "page_size must be between 1 and "
            f"{MAX_DASHBOARD_PAGE_SIZE}, got {value}"
        )
    return value


__all__ = [
    "DEFAULT_DASHBOARD_LAST",
    "DEFAULT_DASHBOARD_PAGE_SIZE",
    "MAX_DASHBOARD_LAST",
    "MAX_DASHBOARD_PAGE_SIZE",
    "validate_last",
    "validate_page_size",
]
