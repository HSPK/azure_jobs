"""Description hook for daemon-side amlt compatibility submissions."""

from __future__ import annotations

from typing import Any

from azure_jobs.shared.spec import register_spec


def _load(data: dict[str, Any]) -> dict[str, Any]:
    return dict(data or {})


register_spec("amlt", load_spec_backend=_load)
