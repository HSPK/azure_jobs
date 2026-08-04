"""Backend-agnostic submission runner — pure CLI presentation."""

from __future__ import annotations

import logging
import os
import traceback
from typing import Callable

import click

from azure_jobs.shared.errors import parse_exception_message
from azure_jobs.shared.journal import JobRecord, log_record
from azure_jobs.shared.job.spec import JobEvent, JobResult
from azure_jobs.client.ui import (
    console,
    get_output_mode,
    show_submission_result,
    truncate_middle,
)

log = logging.getLogger(__name__)

_DEBUG_ENV_FALSY = frozenset({"", "0", "false", "no", "off"})

def _debug_enabled() -> bool:
    return os.getenv("AJ_DEBUG", "").strip().lower() not in _DEBUG_ENV_FALSY

def submit_and_record(
    submit_fn: Callable[[Callable[[JobEvent], None]], JobResult],
    rec: JobRecord,
    display_name: str,
    *,
    backend_label: str = "",
) -> None:
    """Run a backend submission and emit the final result."""
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
        log.exception(
            "Submission failed for %s (backend=%s)", display_name, backend_label
        )
        rec.status = "failed"
        msg = parse_exception_message(exc)
        type_name = type(exc).__name__
        rec.note = f"{type_name}: {msg}"
        full_msg = f"{type_name}: {msg}"
        if json_mode:
            tb_str = traceback.format_exc()
            synth = JobResult(
                job_name=display_name,
                status="failed",
                error=f"{full_msg}\n{tb_str}",
            )
            show_submission_result(
                rec, synth, display_name=display_name, backend_label=backend_label
            )
            raise SystemExit(1) from exc
        if _debug_enabled():
            console.print_exception(show_locals=False)
        else:
            console.print(
                "[dim](run with AJ_DEBUG=1 for a full Python traceback)[/dim]"
            )
        raise click.ClickException(f"Submission failed: {full_msg}")
    finally:
        log_record(rec)

def _run_with_spinner(
    submit_fn: Callable[[Callable[[JobEvent], None]], JobResult],
) -> JobResult:
    from rich.live import Live
    from rich.spinner import Spinner

    with Live(console=console, transient=True) as live:

        def _show_spinner(text: str) -> None:
            live.update(Spinner("dots", text=f" [bold cyan]{text}[/bold cyan]"))

        def _on_event(ev: JobEvent) -> None:
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
