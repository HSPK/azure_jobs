"""Backend-agnostic submission runner — pure CLI presentation.

Wraps any ``submit_fn(on_event) -> SubmitResult`` closure with a unified
Live-spinner UX, persistent ``log`` lines, error handling, and record
logging. Native, AMLT, and Volcano backends all flow through it.

In JSON output mode (``aj --json`` / ``AJ_OUTPUT=json``) the spinner is
suppressed and the final result is emitted via
:func:`show_submission_result` — agents get a single JSON envelope on
stdout with no interleaved Rich output.

Lives under :mod:`azure_jobs.cli` (not :mod:`core.submit`) because the
spinner, ``click.ClickException``, and ``SystemExit`` are presentation
concerns; the pure submission logic is in :mod:`azure_jobs.core.submit`.
"""

from __future__ import annotations

from typing import Callable

import click

from azure_jobs.core.errors import parse_exception_message
from azure_jobs.core.submit import SubmissionRecord, log_record
from azure_jobs.core.submit.models import SubmitEvent, SubmitResult
from azure_jobs.utils.ui import (
    console,
    get_output_mode,
    show_submission_result,
    truncate_middle,
)


def submit_and_record(
    submit_fn: Callable[[Callable[[SubmitEvent], None]], SubmitResult],
    rec: SubmissionRecord,
    display_name: str,
    *,
    backend_label: str = "",
) -> None:
    """Run a backend submission and emit the final result.

    ``submit_fn`` takes a single ``on_event`` callback (receiving
    :class:`SubmitEvent` records) and returns a :class:`SubmitResult`.
    Rich mode wraps the call with a Live spinner; JSON mode passes a
    no-op event handler and produces only the final structured result.
    """
    json_mode = get_output_mode() == "json"

    try:
        if json_mode:
            result = submit_fn(lambda _ev: None)
        else:
            result = _run_with_spinner(submit_fn)

        if result.status == "failed":
            rec.status = "failed"
            rec.note = result.error or result.note
            show_submission_result(
                rec, result, display_name=display_name, backend_label=backend_label
            )
            raise SystemExit(1)

        rec.status = "submitted"
        if result.azure_name:
            rec.azure_name = result.azure_name
        if result.portal_url:
            rec.portal = result.portal_url
        show_submission_result(
            rec, result, display_name=display_name, backend_label=backend_label
        )
    except SystemExit:
        raise
    except Exception as exc:
        rec.status = "failed"
        msg = parse_exception_message(exc)
        rec.note = msg
        if json_mode:
            # Build a synthetic failure result so JSON output still happens.
            synth = SubmitResult(job_name=display_name, status="failed", error=msg)
            show_submission_result(
                rec, synth, display_name=display_name, backend_label=backend_label
            )
            raise SystemExit(1) from exc
        raise click.ClickException(f"Submission failed: {msg}")
    finally:
        log_record(rec)


def _run_with_spinner(
    submit_fn: Callable[[Callable[[SubmitEvent], None]], SubmitResult],
) -> SubmitResult:
    """Rich-mode submission with Live spinner + per-event progress."""
    from rich.live import Live
    from rich.spinner import Spinner

    with Live(console=console, transient=True) as live:

        def _show_spinner(text: str) -> None:
            live.update(Spinner("dots", text=f" [bold cyan]{text}[/bold cyan]"))

        def _on_event(ev: SubmitEvent) -> None:
            if ev.kind == "log":
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
                        text=f" [bold cyan]Uploading[/bold cyan] {shown}  {counts}",
                    )
                )
            else:
                _show_spinner(ev.detail or ev.kind)

        _show_spinner("Authenticating\u2026")
        return submit_fn(_on_event)
