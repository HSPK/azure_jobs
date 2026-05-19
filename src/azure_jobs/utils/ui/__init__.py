"""Rich console output for the aj CLI — facade package.

Imported lazily by CLI commands so ``aj --help`` stays fast.

Submodules:

* :mod:`.console` — ``Console`` instance, theme, log helpers
  (``success`` / ``info`` / ``warning`` / ``error`` / ``dim``),
  status icon/style mappings, portal-URL shortener.
* :mod:`.tables` — local-record + cloud-job + template tables.
* :mod:`.panels` — submission preview / job status / job detail panels.

Everything below is re-exported for backward compatibility — existing
``from azure_jobs.utils.ui import ...`` callers keep working.
"""

from __future__ import annotations

from .console import (
    AZ_ICON,
    AZ_STYLE,
    console,
    dim,
    error,
    esc,
    icon_style,
    info,
    print_table,
    short_portal_url,
    status_badge,
    success,
    truncate_middle,
    warning,
)
from .panels import (
    build_job_info_lines,
    show_job_detail,
    show_job_status,
    show_submission_preview,
)
from .tables import (
    show_cloud_jobs_table,
    show_jobs_table,
    show_template_table,
)

__all__ = [
    # Console / theme / helpers
    "console",
    "print_table",
    "success",
    "info",
    "warning",
    "error",
    "dim",
    "esc",
    "AZ_ICON",
    "AZ_STYLE",
    "icon_style",
    "status_badge",
    "short_portal_url",
    "truncate_middle",
    # Tables
    "show_template_table",
    "show_jobs_table",
    "show_cloud_jobs_table",
    # Panels
    "show_submission_preview",
    "show_job_status",
    "show_job_detail",
    "build_job_info_lines",
]
