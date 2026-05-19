"""Table renderers — local records, cloud jobs, templates, resources.

These helpers construct a :class:`TableView` (or :class:`DetailView`)
and delegate to :func:`render_table` / :func:`render_detail`, so passing
``--json`` to ``aj`` emits the same data as a JSON document instead of
a Rich table. Cell values stay raw; icon/style mapping lives on each
:class:`Column`.
"""

from __future__ import annotations

from typing import Any

from azure_jobs.utils.format import format_size
from azure_jobs.utils.time import format_time, time_ago

from .render import Column, DetailField, DetailView, TableView, render_detail, render_table

_LOCAL_STATUS_STYLE = {
    "success": "green",
    "submitted": "cyan",
    "failed": "red",
    "cancelled": "yellow",
}

_LOCAL_STATUS_ICON = {
    "success": "✓",
    "submitted": "↑",
    "failed": "✗",
    "cancelled": "○",
}


def show_template_table(
    templates: list[dict[str, Any]],
    *,
    default_template: str | None = None,
) -> None:
    """Display templates as a TableView (renders Rich or JSON)."""
    rows = [
        {
            "name": t["name"],
            "is_default": bool(default_template and t["name"] == default_template),
            "base": t.get("base", ""),
            "nodes": t.get("nodes", ""),
            "processes": t.get("processes", ""),
            "sku": t.get("sku", ""),
        }
        for t in templates
    ]

    def _name_fmt(v: Any, row: dict[str, Any]) -> str:
        return f"{v} [dim](default)[/dim]" if row.get("is_default") else str(v)

    view = TableView(
        title="Templates",
        rows=rows,
        empty_message="No templates found",
        columns=[
            Column(key="name", header="Name", style="highlight", format=_name_fmt),
            Column(key="base", style="dim"),
            Column(key="nodes", header="Nodes", justify="right"),
            Column(key="processes", header="Procs", justify="right"),
            Column(key="sku", header="SKU", style="dim"),
        ],
        metadata={"default_template": default_template} if default_template else {},
    )
    render_table(view)


def show_jobs_table(records: list[dict[str, Any]]) -> None:
    """Display local job records as a TableView."""
    rows: list[dict[str, Any]] = []
    for r in records:
        cmd_str = r.get("command", "")
        args = r.get("args", [])
        if args:
            cmd_str += " " + " ".join(args[:3])
            if len(args) > 3:
                cmd_str += " …"
        note = r.get("note", "")
        if note:
            first_line = note.split("\n")[0].strip()
            if first_line.startswith("(") and ") " in first_line:
                first_line = first_line.split(") ", 1)[1]
            note = first_line
        rows.append(
            {
                "id": r.get("id", ""),
                "status": r.get("status", "unknown"),
                "template": r.get("template", ""),
                "nodes": r.get("nodes", ""),
                "processes": r.get("processes", ""),
                "created_at": r.get("created_at", ""),
                "when": time_ago(r.get("created_at", "")),
                "command": cmd_str,
                "note": note,
            }
        )

    view = TableView(
        title="Jobs",
        rows=rows,
        empty_message="No jobs found",
        columns=[
            Column(key="id", header="ID", style="highlight", no_wrap=True),
            Column(
                key="status",
                header="Status",
                type="status",
                no_wrap=True,
                icon_map=_LOCAL_STATUS_ICON,
                style_map=_LOCAL_STATUS_STYLE,
            ),
            Column(key="template", header="Template"),
            Column(key="nodes", header="N", justify="right"),
            Column(key="processes", header="P", justify="right"),
            Column(key="when", header="When", style="dim", no_wrap=True),
            Column(key="command", header="Command"),
            Column(
                key="note",
                header="Note",
                style="dim",
                max_width=40,
                no_wrap=True,
                overflow="ellipsis",
            ),
        ],
    )
    render_table(view)


def show_cloud_jobs_table(
    jobs: list[dict[str, Any]],
    *,
    title: str = "Jobs",
) -> None:
    """Display cloud jobs (from REST) as a TableView."""
    rows: list[dict[str, Any]] = []
    for j in jobs:
        rows.append(
            {
                "name": j.get("name", ""),
                "display_name": j.get("display_name") or j.get("name", ""),
                "experiment": j.get("experiment", ""),
                "compute": j.get("compute", ""),
                "duration": j.get("duration", ""),
                "created": j.get("created", ""),
                "status": j.get("status", ""),
                "portal_url": j.get("portal_url", ""),
            }
        )

    view = TableView(
        title=title,
        rows=rows,
        empty_message="No jobs found",
        columns=[
            Column(key="status", header="Status", type="status", no_wrap=True),
            Column(
                key="display_name",
                header="Display Name",
                style="cyan",
                max_width=40,
                overflow="ellipsis",
                link_key="portal_url",
            ),
            Column(
                key="experiment",
                header="Experiment",
                style="dim",
                max_width=25,
                overflow="ellipsis",
            ),
            Column(key="compute", header="Compute", style="dim"),
            Column(key="duration", header="Duration", style="dim", no_wrap=True),
            Column(key="created", header="Created", style="dim", no_wrap=True),
        ],
    )
    render_table(view)


# ────────────────────────────────────────────────────────────────────────
# Environments
# ────────────────────────────────────────────────────────────────────────


def show_environments_table(envs: list[dict[str, Any]]) -> None:
    """Display Azure ML environments as a TableView."""
    rows = []
    for env in envs:
        props = env.get("properties", {})
        name = env.get("name", "")
        is_curated = (
            "curated"
            if props.get("isArchived") is False and name.startswith("AzureML")
            else "custom"
        )
        rows.append(
            {
                "name": name,
                "latest_version": props.get("latestVersion", ""),
                "type": is_curated,
            }
        )
    view = TableView(
        title="Environments",
        rows=rows,
        empty_message="No environments found",
        columns=[
            Column(key="name", style="cyan bold"),
            Column(key="latest_version", header="Latest Version", justify="right"),
            Column(key="type", header="Type", style="dim"),
        ],
    )
    render_table(view)


def show_environment_versions_table(
    name: str,
    versions: list[dict[str, Any]],
    *,
    last: int = 10,
) -> None:
    """Display environment versions as a TableView."""
    rows = []
    for v in versions[:last]:
        props = v.get("properties", {})
        sys_data = v.get("systemData", {}) or {}
        created_raw = sys_data.get("createdAt", "")
        rows.append(
            {
                "version": v.get("name", ""),
                "image": props.get("image", ""),
                "os": props.get("osType", ""),
                "created_at": created_raw,
                "created": format_time(created_raw[:19]) if created_raw else "",
            }
        )
    view = TableView(
        title=f"Environment: {name}",
        rows=rows,
        empty_message=f"No versions found for environment '{name}'",
        columns=[
            Column(key="version", style="cyan"),
            Column(key="image", header="Image"),
            Column(key="os", header="OS"),
            Column(key="created", header="Created", style="dim"),
        ],
        metadata={"name": name},
    )
    render_table(view)


# ────────────────────────────────────────────────────────────────────────
# Datastores
# ────────────────────────────────────────────────────────────────────────


def show_datastores_table(stores: list[dict[str, Any]]) -> None:
    """Display Azure ML datastores as a TableView."""
    rows = []
    for ds in stores:
        props = ds.get("properties", {})
        rows.append(
            {
                "name": ds.get("name", ""),
                "type": props.get("datastoreType", ""),
                "account": props.get("accountName", ""),
                "container": props.get("containerName", "")
                or props.get("fileSystemName", ""),
                "is_default": bool(props.get("isDefault")),
            }
        )
    view = TableView(
        title="Datastores",
        rows=rows,
        empty_message="No datastores found",
        columns=[
            Column(key="name", style="cyan bold"),
            Column(key="type", header="Type"),
            Column(key="account", header="Account", style="dim"),
            Column(key="container", header="Container / FS"),
            Column(
                key="is_default",
                header="Default",
                justify="center",
                format=lambda v, _r: "✓" if v else "",
            ),
        ],
    )
    render_table(view)


def show_datastore_detail(ds: dict[str, Any]) -> None:
    """Display a single datastore's details as a DetailView."""
    props = ds.get("properties", {}) or {}
    sys_data = ds.get("systemData", {}) or {}
    name = ds.get("name", "")
    data = {
        "name": name,
        "type": props.get("datastoreType", ""),
        "account": props.get("accountName", ""),
        "container": props.get("containerName", ""),
        "file_system": props.get("fileSystemName", ""),
        "endpoint": props.get("endpoint", ""),
        "protocol": props.get("protocol", ""),
        "is_default": bool(props.get("isDefault")),
        "description": props.get("description", ""),
        "created_at": sys_data.get("createdAt", ""),
        "modified_at": sys_data.get("lastModifiedAt", ""),
    }
    view = DetailView(
        title=f"Datastore: {name}",
        data=data,
        fields=[
            DetailField(key="name", label="Name"),
            DetailField(key="type", label="Type"),
            DetailField(key="account", label="Account"),
            DetailField(key="container", label="Container"),
            DetailField(key="file_system", label="File System"),
            DetailField(key="endpoint", label="Endpoint"),
            DetailField(key="protocol", label="Protocol"),
            DetailField(
                key="is_default",
                label="Default",
                format=lambda v, _d: "Yes" if v else "No",
            ),
            DetailField(key="description", label="Description"),
            DetailField(key="created_at", label="Created"),
            DetailField(key="modified_at", label="Modified"),
        ],
    )
    render_detail(view)


# ────────────────────────────────────────────────────────────────────────
# Singularity images
# ────────────────────────────────────────────────────────────────────────


def show_sing_images_table(images: list[dict[str, Any]]) -> None:
    """Display Singularity base images as a TableView."""
    rows = []
    for img in images:
        aliases = [a for a in img.get("aliases", []) if a != img.get("name")]
        rows.append(
            {
                "id": img.get("id", ""),
                "name": img.get("name", ""),
                "image": f"amlt-sing/{img.get('name', '')}",
                "aliases": aliases,
                "aliases_display": ", ".join(aliases[:3])
                + ("…" if len(aliases) > 3 else ""),
            }
        )
    view = TableView(
        title="Singularity Base Images",
        rows=rows,
        empty_message="No images found",
        columns=[
            Column(key="id", style="dim", no_wrap=True),
            Column(key="image", header="Image", style="highlight", no_wrap=True),
            Column(key="aliases_display", header="Aliases", style="dim"),
        ],
        metadata={"count": len(rows)},
    )
    render_table(view)


# ────────────────────────────────────────────────────────────────────────
# Azure auth status
# ────────────────────────────────────────────────────────────────────────


def show_auth_status(
    *,
    account: dict[str, Any],
    workspace_name: str = "",
    resource_group: str = "",
    credential_ok: bool = False,
    credential_error: str = "",
    credential_missing_pkg: bool = False,
) -> None:
    """Display Azure auth/login status as a DetailView."""
    if credential_missing_pkg:
        credential = "azure-identity not installed"
    elif credential_ok:
        credential = "valid"
    else:
        credential = credential_error or "invalid"

    data = {
        "logged_in": True,
        "user": account.get("user", {}).get("name", ""),
        "subscription": account.get("name", ""),
        "subscription_id": account.get("id", ""),
        "tenant": account.get("tenantId", ""),
        "credential": credential,
        "credential_ok": credential_ok,
        "workspace": workspace_name,
        "resource_group": resource_group,
    }
    view = DetailView(
        title="Azure Auth",
        data=data,
        fields=[
            DetailField(
                key="logged_in",
                label="Status",
                format=lambda v, _d: (
                    "[bold green]✓ Logged in[/bold green]" if v else "[red]✗ Not logged in[/red]"
                ),
            ),
            DetailField(key="user", label="User"),
            DetailField(key="subscription", label="Subscription"),
            DetailField(key="subscription_id", label="Subscription ID"),
            DetailField(key="tenant", label="Tenant"),
            DetailField(
                key="credential",
                label="Credential",
                format=lambda v, d: (
                    f"[bold green]✓ {v.title()}[/bold green]"
                    if d.get("credential_ok")
                    else (
                        f"[yellow]⚠ {v}[/yellow]"
                        if d.get("credential") == "azure-identity not installed"
                        else f"[bold red]✗ {v}[/bold red]"
                    )
                ),
            ),
            DetailField(
                key="workspace",
                label="Workspace",
                format=lambda v, _d: v or "[dim]Not configured[/dim]",
            ),
            DetailField(key="resource_group", label="Resource Group"),
        ],
    )
    render_detail(view)


# ────────────────────────────────────────────────────────────────────────
# Code upload preview (aj code stats)
# ────────────────────────────────────────────────────────────────────────


def show_code_stats(
    *,
    code_dir: str,
    template: str | None,
    ignore_count: int,
    files: list[Any],
    code_hash: str,
    total_bytes: int,
    top: int = 10,
    list_all: bool = False,
) -> None:
    """Display the next submission's upload payload — summary + file table.

    *files* items must expose ``size`` (int) and ``rel`` (str). Pure
    detail+table emission so the same data shape powers Rich and JSON.
    """
    summary = DetailView(
        title="Code Upload Preview",
        data={
            "code_dir": code_dir,
            "template": template or "",
            "ignore_patterns": ignore_count,
            "file_count": len(files),
            "total_bytes": total_bytes,
            "total_size": f"{format_size(total_bytes)} ({total_bytes:,} bytes)",
            "content_hash": code_hash,
        },
        fields=[
            DetailField(key="code_dir", label="code_dir"),
            DetailField(key="template", label="template"),
            DetailField(key="ignore_patterns", label="ignore patterns"),
            DetailField(key="file_count", label="files"),
            DetailField(key="total_size", label="total size"),
            DetailField(key="content_hash", label="content hash"),
        ],
    )
    render_detail(summary)

    sorted_files = sorted(files, key=lambda cf: cf.size, reverse=True)
    if not list_all:
        sorted_files = sorted_files[: max(top, 0)]
    if not sorted_files:
        return

    title = "All files" if list_all else f"Top {len(sorted_files)} largest"
    rows = [
        {"size": cf.size, "size_display": format_size(cf.size), "path": cf.rel}
        for cf in sorted_files
    ]
    view = TableView(
        title=title,
        rows=rows,
        empty_message="No files",
        columns=[
            Column(
                key="size_display",
                header="Size",
                justify="right",
                style="cyan",
                no_wrap=True,
            ),
            Column(key="path", header="Path"),
        ],
    )
    render_table(view)
