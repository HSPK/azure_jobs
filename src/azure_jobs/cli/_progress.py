"""Rich-progress wrappers around the az_client job-query layer."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

log = logging.getLogger(__name__)

def fetch_jobs_with_progress(
    n: int,
    ws_name: str | None,
    *,
    cutoff_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    """Single-workspace fetch wrapped with a Rich progress spinner."""
    from azure_jobs.az_client import create_rest_client
    from azure_jobs.utils.ui import console

    client = create_rest_client(ws_name=ws_name)
    with console.status(
        "[bold cyan]Fetching jobs…[/bold cyan]",
        spinner="dots",
    ) as st:
        return client.jobs.fetch(
            n,
            cutoff_utc=cutoff_utc,
            on_progress=lambda matched, scanned: st.update(
                f"[bold cyan]Fetching jobs… {matched} loaded[/bold cyan]"
            ),
        )

def fetch_jobs_all_ws_with_progress(
    n_per_ws: int,
    *,
    cutoff_utc: datetime | None = None,
) -> list[dict[str, Any]]:
    """All-workspace parallel fetch with Rich progress + failure warnings."""
    from azure_jobs.az_client import (
        AzureARMClient,
        fetch_jobs_all_workspaces,
    )
    from azure_jobs.utils.ui import console, warning

    failures: list[tuple[Any, BaseException]] = []
    done = 0

    with console.status(
        "[bold cyan]Discovering workspaces…[/bold cyan]", spinner="dots"
    ) as st:
        arm = AzureARMClient()
        workspaces = arm.workspace.list()
        arm.ensure_token()
        if not workspaces:
            warning("No workspaces found")
            return []

        total = len(workspaces)
        st.update(f"[bold cyan]Fetching jobs (0/{total})…[/bold cyan]")

        def _on_done(ws_name: str, count: int) -> None:
            nonlocal done
            done += 1
            st.update(
                f"[bold cyan]Fetching jobs ({done}/{total}) {ws_name}…[/bold cyan]"
            )

        def _on_fail(ws: Any, exc: BaseException) -> None:
            nonlocal done
            done += 1
            failures.append((ws, exc))
            log.debug(
                "Skipping workspace %s",
                ws.name,
                exc_info=(type(exc), exc, exc.__traceback__),
            )

        jobs = fetch_jobs_all_workspaces(
            n_per_ws,
            cutoff_utc=cutoff_utc,
            workspaces=workspaces,
            on_workspace_done=_on_done,
            on_workspace_failure=_on_fail,
        )

    if failures:
        names = [ws.name for ws, _ in failures[:3]]
        warning(
            f"Skipped {len(failures)} workspace(s): {', '.join(names)}"
            + (" …" if len(failures) > 3 else "")
            + "  (run with AJ_DEBUG=1 for details)"
        )
    return jobs
