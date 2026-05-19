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
from .quota_tables import (
    show_aml_quota_table,
    show_sing_quota_table,
    show_sku_table,
)
from .render import (
    Column,
    DetailField,
    DetailView,
    TableView,
    get_output_mode,
    render_detail,
    render_table,
    set_output_mode,
)
from .tables import (
    show_auth_status,
    show_cloud_jobs_table,
    show_code_stats,
    show_datastore_detail,
    show_datastores_table,
    show_environment_versions_table,
    show_environments_table,
    show_jobs_table,
    show_sing_images_table,
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
    # Render middleware
    "Column",
    "TableView",
    "DetailField",
    "DetailView",
    "render_table",
    "render_detail",
    "get_output_mode",
    "set_output_mode",
    # Tables
    "show_template_table",
    "show_jobs_table",
    "show_cloud_jobs_table",
    "show_environments_table",
    "show_environment_versions_table",
    "show_datastores_table",
    "show_datastore_detail",
    "show_sing_images_table",
    "show_auth_status",
    "show_code_stats",
    "show_sing_quota_table",
    "show_aml_quota_table",
    "show_sku_table",
    # Panels
    "show_submission_preview",
    "show_job_status",
    "show_job_detail",
    "build_job_info_lines",
]
