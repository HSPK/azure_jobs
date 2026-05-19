"""Backend-agnostic submission runner — pure CLI presentation.

Wraps any ``submit_fn(on_event) -> SubmitResult`` closure with a unified
Live-spinner UX, persistent ``log`` lines, error handling, and record
logging. Native, AMLT, and Volcano backends all flow through it.

Lives under :mod:`azure_jobs.cli` (not :mod:`core.submit`) because the
spinner, ``click.ClickException``, and ``SystemExit`` are presentation
concerns; the pure submission logic is in :mod:`azure_jobs.core.submit`.
"""

from __future__ import annotations

from typing import Callable

import click

from azure_jobs.core.record import SubmissionRecord, log_record
from azure_jobs.core.submit.models import SubmitEvent, SubmitResult
from azure_jobs.utils.ui import (
    console,
    dim,
    error,
    short_portal_url,
    success,
    truncate_middle,
)


def submit_and_record(
    submit_fn: Callable[[Callable[[SubmitEvent], None]], SubmitResult],
    rec: SubmissionRecord,
    display_name: str,
    *,
    backend_label: str = "",
) -> None:
    """Run a backend submission with a unified Live-spinner UX.

    ``submit_fn`` takes a single ``on_event`` callback (receiving
    :class:`SubmitEvent` records) and returns a :class:`SubmitResult`.
    """
    from rich.live import Live
    from rich.spinner import Spinner

    try:
        with Live(console=console, transient=True) as live:

            def _show_spinner(text: str) -> None:
                live.update(Spinner("dots", text=f" [bold cyan]{text}[/bold cyan]"))

            def _on_event(ev: SubmitEvent) -> None:
                if ev.kind == "log":
                    # Persistent informational line above the spinner.
                    live.console.print(f"  [dim]\u00b7 {ev.detail}[/dim]")
                elif ev.kind == "upload":
                    shown = (
                        truncate_middle(ev.current, 60)
                        if ev.current
                        else "preparing\u2026"
                    )
                    uploaded = ev.completed - ev.skipped
                    counts = (
                        f"[dim]({ev.completed}/{ev.total} \u00b7 {uploaded} new, "
                        f"{ev.skipped} cached)[/dim]"
                    )
                    live.update(
                        Spinner(
                            "dots",
                            text=(
                                f" [bold cyan]Uploading[/bold cyan] {shown}  {counts}"
                            ),
                        )
                    )
                else:
                    _show_spinner(ev.detail or ev.kind)

            _show_spinner("Authenticating\u2026")
            result = submit_fn(_on_event)

        if result.status == "failed":
            rec.status = "failed"
            rec.note = result.error or result.note
            error(f"Submission failed: {rec.note}")
            raise SystemExit(1)

        rec.status = "submitted"
        if result.azure_name:
            rec.azure_name = result.azure_name
        if result.portal_url:
            rec.portal = result.portal_url
        suffix = f" via {backend_label}" if backend_label else ""
        success(f"Job [bold]{display_name}[/bold] submitted{suffix}")
        if result.azure_name and result.azure_name != display_name:
            dim(f"Azure ID: {result.azure_name}")
        if result.portal_url:
            dim(f"Portal: {short_portal_url(result.portal_url)}")
        if result.note and not result.portal_url:
            # e.g. ``kubectl create`` output for Volcano
            dim(result.note)
    except SystemExit:
        raise
    except Exception as exc:
        rec.status = "failed"
        rec.note = str(exc)
        raise click.ClickException(f"Submission failed: {exc}")
    finally:
        log_record(rec)
