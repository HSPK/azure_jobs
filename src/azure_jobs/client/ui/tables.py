"""Table renderers — local records, cloud jobs, templates, resources."""

from __future__ import annotations

from typing import Any

from azure_jobs.shared.utils.format import format_size
from azure_jobs.shared.utils.time import format_time, time_ago

from .render import (
    Column,
    DetailField,
    DetailView,
    TableView,
    render_detail,
    render_table,
)

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

def show_environments_table(envs: list[Any]) -> None:
    """Display Azure ML environments as a TableView."""
    rows = [
        {
            "name": env.name,
            "latest_version": env.latest_version,
            "type": "curated" if env.is_curated else "custom",
        }
        for env in envs
    ]
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
    versions: list[Any],
    *,
    last: int = 10,
) -> None:
    """Display environment versions as a TableView."""
    rows = []
    for v in versions[:last]:
        rows.append(
            {
                "version": v.version,
                "image": v.image,
                "os": v.os_type,
                "created_at": v.created_at,
                "created": format_time(v.created_at[:19]) if v.created_at else "",
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

def show_datastores_table(stores: list[Any]) -> None:
    """Display Azure ML datastores as a TableView."""
    rows = [
        {
            "name": ds.name,
            "type": ds.datastore_type,
            "account": ds.account_name,
            "container": ds.container_name or ds.file_system_name,
            "is_default": ds.is_default,
        }
        for ds in stores
    ]
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

def show_datastore_detail(ds: Any) -> None:
    """Display a single datastore's details as a DetailView."""
    data = {
        "name": ds.name,
        "type": ds.datastore_type,
        "account": ds.account_name,
        "container": ds.container_name,
        "file_system": ds.file_system_name,
        "endpoint": ds.endpoint,
        "protocol": ds.protocol,
        "is_default": ds.is_default,
        "description": ds.description,
        "created_at": ds.created_at,
        "modified_at": ds.modified_at,
    }
    view = DetailView(
        title=f"Datastore: {ds.name}",
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

def show_sing_images_table(images: list[dict[str, Any]]) -> None:
    """Display Singularity base images as a TableView."""
    rows = []
    for img in images:
        aliases = [a for a in img.get("aliases", []) if a != img.get("name")]
        rows.append(
            {
                "image": f"amlt-sing/{img.get('name', '')}",
                "aliases": aliases,
            }
        )

    def _aliases_fmt(value: Any, _row: dict[str, Any]) -> str:
        aliases = list(value or [])
        return ", ".join(aliases[:3]) + ("…" if len(aliases) > 3 else "")

    view = TableView(
        title="Singularity Base Images",
        rows=rows,
        empty_message="No images found",
        columns=[
            Column(key="image", header="Image", style="highlight", no_wrap=True),
            Column(key="aliases", header="Aliases", style="dim", format=_aliases_fmt),
        ],
        metadata={"count": len(rows)},
    )
    render_table(view)

def show_uai_table(identities: list[Any]) -> None:
    """Display user-assigned managed identities as a TableView."""
    rows = [
        {
            "name": uai.name,
            "resource_group": uai.resource_group,
            "subscription_id": uai.subscription_id,
            "location": uai.location,
            "client_id": uai.client_id,
            "id": uai.id,
        }
        for uai in identities
    ]
    view = TableView(
        title="User-Assigned Managed Identities",
        rows=rows,
        empty_message="No user-assigned managed identities found",
        columns=[
            Column(key="name", style="cyan bold"),
            Column(key="resource_group", header="Resource Group"),
            Column(key="location", header="Location", style="dim"),
            Column(key="client_id", header="Client ID", style="dim", no_wrap=True),
            Column(key="id", header="ARM ID", style="dim"),
        ],
        metadata={"count": len(rows)},
    )
    render_table(view)

def show_storage_accounts_table(accounts: list[Any]) -> None:
    """Display Azure storage accounts as a TableView."""
    rows = [
        {
            "name": sa.name,
            "resource_group": sa.resource_group,
            "location": sa.location,
            "kind": sa.kind,
            "sku": sa.sku,
            "subscription_id": sa.subscription_id,
        }
        for sa in accounts
    ]
    view = TableView(
        title="Storage Accounts",
        rows=rows,
        empty_message="No storage accounts found",
        columns=[
            Column(key="name", style="cyan bold"),
            Column(key="resource_group", header="Resource Group"),
            Column(key="location", header="Location", style="dim"),
            Column(key="kind", header="Kind", style="dim"),
            Column(key="sku", header="SKU", style="dim"),
        ],
        metadata={"count": len(rows)},
    )
    render_table(view)

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
                    "[bold green]✓ Logged in[/bold green]"
                    if v
                    else "[red]✗ Not logged in[/red]"
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
    """Display the next submission's upload payload — summary + file table."""
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
