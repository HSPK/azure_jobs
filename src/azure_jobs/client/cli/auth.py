"""aj auth — report Azure authentication status.

Read-only on purpose. Signing in is ``az login``: wrapping it would add a
second way to do the same thing, and the client cannot do it on the daemon's
behalf anyway — the daemon is a different process with its own credential,
which is the one every command actually uses. So this reports *its* view, and
the daemon refuses to start at all when there is no usable sign-in.
"""

from __future__ import annotations

import logging

from . import main

log = logging.getLogger(__name__)


@main.group(name="auth")
def auth_group() -> None:
    """Report Azure authentication status."""

@auth_group.command(name="status")
def auth_status() -> None:
    """Show current Azure login status, subscription, and credential health."""
    from azure_jobs import connect
    from azure_jobs.shared.config import read_config
    from azure_jobs.client.ui import console, show_auth_status

    with connect() as d:
        status = d.auth.status()
    account = status.get("account")
    if account is None:
        console.print("[error]✗[/error] Not logged in (or Azure CLI not installed)")
        console.print("  Run [bold]az login[/bold] to authenticate")
        raise SystemExit(1)

    # Asked of the daemon: it is the process that will call Azure, so its
    # credential is the one whose health the user needs to know about.
    health = status.get("credential") or {}

    ws = read_config().workspace
    show_auth_status(
        account=account,
        workspace_name=ws.workspace_name,
        resource_group=ws.resource_group,
        credential_ok=bool(health.get("ok")),
        credential_error=str(health.get("error") or ""),
        credential_missing_pkg=bool(health.get("missing_package")),
    )
