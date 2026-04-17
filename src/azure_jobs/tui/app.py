"""Interactive TUI dashboard for Azure Jobs.

Data: ``ml_client.jobs.list(list_view_type=ALL)`` → all workspace jobs.
Local ``record.jsonl`` merged to show template/command info.

Layout
------
Left pane   bordered OptionList with search (/) and status filter (f).
Right pane  bordered panel; border-title = job name + status.
            Content toggles between Info (i) and Logs (l) views.
"""

from __future__ import annotations

import io
import sys
from typing import Any

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Footer,
    Input,
    OptionList,
    RichLog,
    Static,
)
from textual.widgets.option_list import Option
from textual.worker import get_current_worker

# ---- status maps ------------------------------------------------------------

_AZ_ICON = {
    "Completed": "✓", "Running": "▶", "Starting": "◉", "Preparing": "◉",
    "Queued": "◷", "Failed": "✗", "Canceled": "⊘", "CancelRequested": "⊘",
    "NotStarted": "○", "Provisioning": "◉", "Finalizing": "◉",
}
_AZ_STYLE = {
    "Completed": "green", "Running": "cyan", "Starting": "cyan",
    "Preparing": "yellow", "Queued": "yellow", "Failed": "red",
    "Canceled": "dim", "CancelRequested": "dim yellow",
    "NotStarted": "dim", "Provisioning": "yellow", "Finalizing": "cyan",
}

_LOCAL_STATUS_MAP = {
    "success": "Completed", "failed": "Failed",
    "cancelled": "Canceled", "submitted": "Queued",
}

_KW = 10
_STATUS_CYCLE = ["", "Running", "Completed", "Failed", "Canceled", "Queued"]


# ---- helpers ----------------------------------------------------------------


def _icon_style(status: str) -> tuple[str, str]:
    return _AZ_ICON.get(status, "?"), _AZ_STYLE.get(status, "white")


def _make_option(job: dict[str, Any]) -> Option:
    """Compact list item: icon + display name."""
    name = job.get("display_name") or job.get("name", "?")
    icon, sty = _icon_style(job.get("status", ""))
    t = Text()
    t.append(f" {icon} ", style=sty)
    t.append(name)
    return Option(t, id=job.get("name", ""))


def _kv(pairs: list[tuple[str, str]], *, hint: str = "") -> str:
    """Aligned key-value lines. Empty key = blank separator."""
    out: list[str] = []
    for k, v in pairs:
        out.append("" if k == "" else f"  [bold]{k:>{_KW}}[/bold]  {v}")
    if hint:
        out += ["", f"  [dim]{hint}[/dim]"]
    return "\n".join(out)


def _fmt_dur(secs: int) -> str:
    if secs >= 3600:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    if secs >= 60:
        return f"{secs // 60}m {secs % 60}s"
    return f"{secs}s"


def _extract_job(job_obj: Any) -> dict[str, Any]:
    """Convert an Azure ML Job SDK object → plain dict."""
    props = getattr(job_obj, "properties", {}) or {}
    start = props.get("StartTimeUtc", "")
    end = props.get("EndTimeUtc", "")
    duration = ""
    if start and end:
        from datetime import datetime
        try:
            duration = _fmt_dur(int(
                (datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
                 - datetime.strptime(start, "%Y-%m-%d %H:%M:%S")).total_seconds()
            ))
        except ValueError:
            pass
    elif start:
        from datetime import datetime, timezone
        try:
            t0 = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            duration = _fmt_dur(int((datetime.now(timezone.utc) - t0).total_seconds())) + " ↻"
        except ValueError:
            pass

    compute = getattr(job_obj, "compute", "") or ""
    if "/" in compute:
        compute = compute.rstrip("/").rsplit("/", 1)[-1]

    return {
        "name": getattr(job_obj, "name", ""),
        "display_name": getattr(job_obj, "display_name", "") or "",
        "status": getattr(job_obj, "status", ""),
        "compute": compute,
        "portal_url": getattr(job_obj, "studio_url", "") or "",
        "start_time": start,
        "end_time": end,
        "duration": duration,
        "experiment": getattr(job_obj, "experiment_name", "") or "",
    }


# ---- app --------------------------------------------------------------------


class AjDashboard(App):
    """Azure Jobs interactive dashboard."""

    TITLE = "aj dashboard"

    CSS = """
    Screen {
        layout: horizontal;
    }

    .hidden { display: none; }

    /* ── left ── */
    #left-pane {
        width: 36;
        min-width: 28;
        height: 100%;
        border: round $accent;
        border-title-color: $accent;
        border-title-style: bold;
    }
    #left-pane:focus-within {
        border: round $accent;
    }
    #search-input {
        height: 1;
        border: none;
        padding: 0 1;
        background: $boost;
    }
    #job-list {
        height: 1fr;
        border: none;
        padding: 0;
        scrollbar-size: 1 1;
    }

    /* ── right ── */
    #right-pane {
        width: 1fr;
        height: 100%;
        margin-left: 1;
        border: round $accent 40%;
        border-title-style: bold;
        border-title-color: $text;
        border-subtitle-color: $text-muted;
        border-subtitle-style: italic;
    }
    #info-scroll {
        height: 1fr;
        padding: 1 2;
    }
    #info-content {
        width: 100%;
    }
    #log-content {
        height: 1fr;
        padding: 1 2;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("slash", "search", "Search"),
        Binding("f", "cycle_filter", "Filter"),
        Binding("c", "cancel_job", "Cancel"),
        Binding("l", "show_logs", "Logs"),
        Binding("i", "show_info", "Info"),
        Binding("escape", "dismiss", "Back", show=False),
    ]

    def __init__(self, last: int = 100, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._last = last
        self._all_jobs: list[dict[str, Any]] = []
        self._filtered: list[dict[str, Any]] = []
        self._local_map: dict[str, dict[str, Any]] = {}
        self._workspace: dict[str, str] | None = None
        self._selected_idx: int = -1
        self._logs_job: str = ""
        self._ml_client: Any = None
        self._status_filter: str = ""
        self._name_filter: str = ""
        self._view_mode: str = "info"  # "info" or "logs"

    # ---- compose ------------------------------------------------------------

    def compose(self) -> ComposeResult:
        with Horizontal():
            with Vertical(id="left-pane"):
                yield Input(
                    placeholder="Search...", id="search-input", classes="hidden",
                )
                yield OptionList(id="job-list")
            with Vertical(id="right-pane"):
                with VerticalScroll(id="info-scroll"):
                    yield Static(id="info-content")
                yield RichLog(
                    id="log-content", highlight=True, markup=True,
                    classes="hidden",
                )
        yield Footer()

    def on_mount(self) -> None:
        self._update_titles()
        self.query_one("#right-pane").border_subtitle = "Info"
        self._load_local_records()
        self._fetch_azure_jobs()

    # ---- data ---------------------------------------------------------------

    def _load_local_records(self) -> None:
        from azure_jobs.core.record import read_records

        records = read_records(last=self._last * 2)
        self._local_map = {
            r["azure_name"]: r for r in records if r.get("azure_name")
        }
        # Immediate fallback while Azure loads
        if not self._all_jobs:
            for r in read_records(last=self._last):
                st = _LOCAL_STATUS_MAP.get(r.get("status", ""), "Queued")
                self._all_jobs.append({
                    "name": r.get("azure_name", r.get("id", "")),
                    "display_name": r.get("azure_name", r.get("id", "")),
                    "status": st, "compute": "", "portal_url": r.get("portal", ""),
                    "start_time": "", "end_time": "", "duration": "",
                    "experiment": "", "_local": True,
                })
            self._apply_filter()

    @work(thread=True, exclusive=True, group="fetch")
    def _fetch_azure_jobs(self) -> None:
        ws = self._ensure_workspace()
        if ws is None:
            return

        from azure_jobs.core.submit import _quiet_azure_sdk, _suppress_sdk_output
        _quiet_azure_sdk()

        if self._ml_client is None:
            from azure.ai.ml import MLClient
            from azure.identity import AzureCliCredential
            with _suppress_sdk_output():
                self._ml_client = MLClient(
                    credential=AzureCliCredential(),
                    subscription_id=ws.get("subscription_id", ""),
                    resource_group_name=ws.get("resource_group", ""),
                    workspace_name=ws.get("workspace_name", ""),
                )

        from azure.ai.ml.constants import ListViewType

        jobs: list[dict[str, Any]] = []
        try:
            for job_obj in self._ml_client.jobs.list(
                list_view_type=ListViewType.ALL,
                max_results=self._last,
            ):
                jobs.append(_extract_job(job_obj))
        except Exception:
            pass

        if jobs and not get_current_worker().is_cancelled:
            self.call_from_thread(self._on_jobs_loaded, jobs)

    def _on_jobs_loaded(self, jobs: list[dict[str, Any]]) -> None:
        self._all_jobs = jobs
        self._apply_filter()
        self.notify(f"Loaded {len(jobs)} jobs")

    def _apply_filter(self) -> None:
        out = self._all_jobs
        if self._status_filter:
            out = [j for j in out if j.get("status") == self._status_filter]
        if self._name_filter:
            q = self._name_filter.lower()
            out = [
                j for j in out
                if q in (j.get("display_name") or j.get("name", "")).lower()
                or q in j.get("name", "").lower()
            ]
        self._filtered = out

        ol = self.query_one("#job-list", OptionList)
        ol.clear_options()
        for j in self._filtered:
            ol.add_option(_make_option(j))

        self._update_titles()

        if self._filtered:
            ol.highlighted = 0
            self._selected_idx = 0
            self._show_job_info(self._filtered[0])
        else:
            self._selected_idx = -1
            self.query_one("#info-content", Static).update(
                _kv([], hint="No matching jobs.")
            )
            rp = self.query_one("#right-pane")
            rp.border_title = ""
            rp.border_subtitle = "Info"

    def _update_titles(self) -> None:
        total, shown = len(self._all_jobs), len(self._filtered)
        parts = [f"Jobs ({shown})" if shown == total else f"Jobs ({shown}/{total})"]
        if self._status_filter:
            parts.append(f"▸ {self._status_filter}")
        if self._name_filter:
            parts.append(f'"{self._name_filter}"')
        self.query_one("#left-pane").border_title = "  ".join(parts)

    # ---- workspace / client -------------------------------------------------

    def _ensure_workspace(self) -> dict[str, str] | None:
        if self._workspace is not None:
            return self._workspace
        from azure_jobs.core.config import read_config
        ws = read_config().get("workspace", {})
        if all(ws.get(k) for k in ("subscription_id", "resource_group", "workspace_name")):
            self._workspace = ws
            return ws
        return None

    def _get_or_create_ml_client(self) -> Any:
        if self._ml_client is not None:
            return self._ml_client
        ws = self._ensure_workspace()
        if ws is None:
            return None
        from azure_jobs.core.submit import _quiet_azure_sdk, _suppress_sdk_output
        _quiet_azure_sdk()
        from azure.ai.ml import MLClient
        from azure.identity import AzureCliCredential
        with _suppress_sdk_output():
            self._ml_client = MLClient(
                credential=AzureCliCredential(),
                subscription_id=ws.get("subscription_id", ""),
                resource_group_name=ws.get("resource_group", ""),
                workspace_name=ws.get("workspace_name", ""),
            )
        return self._ml_client

    # ---- navigation ---------------------------------------------------------

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted,
    ) -> None:
        idx = event.option_index
        if 0 <= idx < len(self._filtered):
            self._selected_idx = idx
            self._show_job_info(self._filtered[idx])
            self._logs_job = ""

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected,
    ) -> None:
        idx = event.option_index
        if 0 <= idx < len(self._filtered):
            self._selected_idx = idx
            self.query_one("#info-content", Static).update(
                _kv([("", "")], hint="Refreshing...")
            )
            self._fetch_single(self._filtered[idx])

    # ---- right pane: info ---------------------------------------------------

    def _show_job_info(self, job: dict[str, Any]) -> None:
        icon, sty = _icon_style(job.get("status", ""))
        status = job.get("status", "?")
        name = job.get("name", "")
        display = job.get("display_name") or name

        # Update right-pane border
        rp = self.query_one("#right-pane")
        rp.border_title = f"{display}  [{sty}]{icon} {status}[/{sty}]"
        mode_label = "Logs" if self._view_mode == "logs" else "Info"
        rp.border_subtitle = mode_label

        pairs: list[tuple[str, str]] = []

        if job.get("compute"):
            pairs.append(("Compute", job["compute"]))
        if job.get("experiment"):
            pairs.append(("Experiment", job["experiment"]))
        pairs.append(("Azure ID", name))

        # Local record enrichment
        local = self._local_map.get(name)
        if local:
            pairs.append(("", ""))
            if local.get("template"):
                pairs.append(("Template", local["template"]))
            nodes = local.get("nodes", "")
            procs = local.get("processes", "")
            if nodes:
                pairs.append(("Nodes", str(nodes)))
            if procs:
                pairs.append(("Procs", str(procs)))
            cmd = local.get("command", "")
            args = local.get("args", [])
            if args:
                cmd += " " + " ".join(args)
            if cmd:
                pairs.append(("Command", cmd))

        if job.get("duration") or job.get("start_time"):
            pairs.append(("", ""))
            if job.get("duration"):
                pairs.append(("Duration", job["duration"]))
            if job.get("start_time"):
                pairs.append(("Started", job["start_time"]))
            if job.get("end_time"):
                pairs.append(("Ended", job["end_time"]))

        if job.get("portal_url"):
            pairs.append(("", ""))
            url = job["portal_url"]
            if "/runs/" in url:
                rp_str = url.split("/runs/", 1)[1].split("?")[0]
                url = f"ml.azure.com/runs/{rp_str}"
            pairs.append(("Portal", url))

        self.query_one("#info-content", Static).update(
            _kv(pairs, hint="Enter: refresh  ·  l: logs  ·  c: cancel")
        )

    @work(thread=True, exclusive=True, group="status")
    def _fetch_single(self, job: dict[str, Any]) -> None:
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return
        name = job.get("name", "")
        try:
            updated = _extract_job(ml.jobs.get(name))
        except Exception as exc:
            self.call_from_thread(self.notify, str(exc)[:80], severity="error")
            return
        if not get_current_worker().is_cancelled:
            self.call_from_thread(self._on_single_fetched, name, updated)

    def _on_single_fetched(self, name: str, updated: dict[str, Any]) -> None:
        for lst in (self._all_jobs, self._filtered):
            for i, j in enumerate(lst):
                if j.get("name") == name:
                    lst[i] = updated
                    break
        # Refresh list item
        for i, j in enumerate(self._filtered):
            if j.get("name") == name:
                self.query_one("#job-list", OptionList).replace_option_prompt_at_index(
                    i, _make_option(updated).prompt,
                )
                break
        if 0 <= self._selected_idx < len(self._filtered):
            if self._filtered[self._selected_idx].get("name") == name:
                self._show_job_info(updated)

    # ---- right pane: logs ---------------------------------------------------

    def action_show_logs(self) -> None:
        self._view_mode = "logs"
        self.query_one("#info-scroll").add_class("hidden")
        self.query_one("#log-content").remove_class("hidden")
        rp = self.query_one("#right-pane")
        rp.border_subtitle = "Logs"

        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            name = job.get("name", "")
            if name != self._logs_job:
                self._logs_job = name
                log_w = self.query_one("#log-content", RichLog)
                log_w.clear()
                log_w.write("[dim]Fetching logs...[/dim]")
                self._fetch_logs(name)

    def action_show_info(self) -> None:
        self._view_mode = "info"
        self.query_one("#log-content").add_class("hidden")
        self.query_one("#info-scroll").remove_class("hidden")
        rp = self.query_one("#right-pane")
        rp.border_subtitle = "Info"

    @work(thread=True, exclusive=True, group="logs")
    def _fetch_logs(self, azure_name: str) -> None:
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return

        old_out = sys.stdout
        buf = io.StringIO()
        sys.stdout = buf
        error_msg = ""
        try:
            ml.jobs.stream(azure_name)
        except Exception as exc:
            msg = str(exc)
            if "{" in msg:
                import json
                try:
                    s, e = msg.index("{"), msg.rindex("}") + 1
                    err = json.loads(msg[s:e])
                    error_msg = err.get("error", {}).get("message", msg).strip()
                except (ValueError, json.JSONDecodeError):
                    error_msg = msg
            else:
                error_msg = msg
        finally:
            sys.stdout = old_out

        raw = buf.getvalue()
        _SKIP = ("RunId:", "Web View:", "Execution Summary", "=====")
        lines = [ln for ln in raw.split("\n") if not any(ln.startswith(p) for p in _SKIP)]
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()

        if not get_current_worker().is_cancelled:
            self.call_from_thread(self._show_logs, "\n".join(lines), error_msg)

    def _show_logs(self, content: str, error: str) -> None:
        lw = self.query_one("#log-content", RichLog)
        lw.clear()
        if content.strip():
            lw.write(content)
        if error:
            lw.write(f"\n[bold red]Error:[/bold red] [red]{error}[/red]")
        if not content.strip() and not error:
            lw.write("[dim]No logs available.[/dim]")

    # ---- search -------------------------------------------------------------

    def action_search(self) -> None:
        inp = self.query_one("#search-input", Input)
        if inp.has_class("hidden"):
            inp.remove_class("hidden")
            inp.value = self._name_filter
            inp.focus()
        else:
            self._close_search()

    def _close_search(self) -> None:
        inp = self.query_one("#search-input", Input)
        inp.add_class("hidden")
        self.query_one("#job-list", OptionList).focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "search-input":
            self._name_filter = event.value
            self._apply_filter()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search-input":
            self._close_search()

    # ---- actions ------------------------------------------------------------

    def action_dismiss(self) -> None:
        """Escape: close search → switch to info → quit."""
        inp = self.query_one("#search-input", Input)
        if not inp.has_class("hidden"):
            inp.add_class("hidden")
            self._name_filter = ""
            self._apply_filter()
            self.query_one("#job-list", OptionList).focus()
            return
        if self._view_mode == "logs":
            self.action_show_info()
            return
        self.exit()

    def action_refresh(self) -> None:
        self._all_jobs.clear()
        self._filtered.clear()
        self._load_local_records()
        self._fetch_azure_jobs()
        self.notify("Refreshing...")

    def action_cycle_filter(self) -> None:
        try:
            idx = _STATUS_CYCLE.index(self._status_filter)
        except ValueError:
            idx = 0
        self._status_filter = _STATUS_CYCLE[(idx + 1) % len(_STATUS_CYCLE)]
        self.notify(f"Filter: {self._status_filter or 'All'}")
        self._apply_filter()

    def action_cancel_job(self) -> None:
        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            self.query_one("#info-content", Static).update(
                _kv([("", "")], hint="Cancelling...")
            )
            self._do_cancel(job)

    @work(thread=True, exclusive=True, group="cancel")
    def _do_cancel(self, job: dict[str, Any]) -> None:
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return
        name = job.get("name", "")
        display = job.get("display_name") or name
        try:
            cur = ml.jobs.get(name)
            st = getattr(cur, "status", "")
            if st in ("Completed", "Failed", "Canceled"):
                self.call_from_thread(self.notify, f"{display}: already {st}")
                return
            lro = ml.jobs.begin_cancel(name)
            try:
                lro.wait()
            except Exception:
                pass
            final = getattr(ml.jobs.get(name), "status", "?")
        except Exception as exc:
            self.call_from_thread(self.notify, str(exc)[:80], severity="error")
            return
        if not get_current_worker().is_cancelled:
            self.call_from_thread(self.notify, f"{display}: {final}")
            self.call_from_thread(lambda: self._fetch_single(job))
