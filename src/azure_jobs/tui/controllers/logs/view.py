"""View switching, header, file picker."""

from __future__ import annotations

from azure_jobs.tui.components import PickerModal
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.logs._shared import NO_LOG_STATUSES
from azure_jobs.tui.helpers import icon_style
from azure_jobs.tui.state import LogsState


class LogsView(Controller[LogsState]):
    """Owns the right-pane Info/Logs toggle, header, and log file picker."""

    # ---- view switching -----------------------------------------------------

    def switch_to_view(self) -> None:
        """Show the log pane (shared by multiple actions)."""
        app = self.app
        self.state.view_mode = "logs"
        app.query_one("#info-scroll").add_class("hidden")
        if app.widgets.log:
            app.widgets.log.remove_class("hidden")
            app.widgets.log.focus()
        self.update_tab_title()
        self.update_header()

    def update_header(self) -> None:
        """Render log meta into the right-pane border subtitle."""
        st = self.state
        try:
            rp = self.app.query_one("#right-pane")
        except Exception:
            return
        if st.view_mode != "logs":
            rp.border_subtitle = ""
            return
        if not st.job:
            rp.border_subtitle = " no job "
            return
        scroll_icon = "▶" if st.auto_scroll else "⏸"
        if st.backfilling:
            status = "[bold cyan]● backfill…[/bold cyan]"
        elif st.loading:
            status = "[bold yellow]● loading…[/bold yellow]"
        elif st.streaming:
            status = "[bold green]● LIVE[/bold green]"
        else:
            status = "[dim]○ idle[/dim]"
        file_part = st.current_file or "[dim](resolving file…)[/dim]"
        more = ""
        if st.head_offset > 0:
            kib = st.head_offset // 1024
            more = f"  [dim]↑ {kib} KiB more[/dim]"
        rp.border_subtitle = f" {status}  {file_part}{more}  {scroll_icon} "

    def update_tab_title(self) -> None:
        try:
            rp = self.app.query_one("#right-pane")
        except Exception:
            return
        st = self.state
        if st.view_mode != "logs":
            rp.border_title = "  [bold reverse] Info [/bold reverse]  Logs  "
            return
        rp.border_title = "  Info  [bold reverse] Logs [/bold reverse]  "

    def show(self) -> None:
        """Switch to logs view; restart streaming only on job change / cold start."""
        app = self.app
        st = self.state
        jobs_st = app.jobs.state
        if not (0 <= jobs_st.selected_idx < len(jobs_st.filtered)):
            self.switch_to_view()
            return
        job = jobs_st.filtered[jobs_st.selected_idx]
        name = job.get("name", "")

        # Same job: just flip view, preserve buffer + streaming.
        if name == st.job:
            self.switch_to_view()
            if not st.streaming and st.line_count == 0:
                self._begin_stream(job, name)
            return

        # Different job: capture outgoing, load incoming snapshot.
        self.app.logs.switch_to_job(name)
        self.switch_to_view()
        self._begin_stream(job, name)

    def _begin_stream(self, job: dict, name: str) -> None:
        """Reset buffer + kick off the stream for *name*."""
        app = self.app
        st = self.state
        snap = app.logs.buffer.snapshot_for(name)
        snap.buffer.clear()
        snap.line_count = 0
        st.line_count = 0
        if app.widgets.log:
            app.widgets.log.clear()
        status = job.get("status", "")
        if status in NO_LOG_STATUSES:
            icon, sty = icon_style(status)
            if app.widgets.log:
                app.widgets.log.write(
                    f"[{sty}]{icon} {status}[/{sty}]  \u2014 logs not available yet."
                )
            st.loading = False
            self.update_header()
            return
        # Surface an immediate loading hint — the worker may take 0.5–2s to
        # resolve the file list + signed URL + initial tail bytes.
        st.loading = True
        if app.widgets.log:
            app.widgets.log.write("[dim]Loading log…[/dim]")
        self.update_header()
        app.logs.stream.start_streaming(name, st.current_file)

    def show_info(self) -> None:
        app = self.app
        self.state.view_mode = "info"
        if app.widgets.log:
            app.widgets.log.add_class("hidden")
        app.query_one("#info-scroll").remove_class("hidden")
        self.update_tab_title()
        self.update_header()
        if app.widgets.jobs:
            app.widgets.jobs.focus()

    def toggle_scroll(self) -> None:
        st = self.state
        st.auto_scroll = not st.auto_scroll
        self.update_tab_title()
        self.update_header()
        self.app.notify(f"Auto-scroll {'ON' if st.auto_scroll else 'OFF'}", timeout=2)

    def on_job_changed(self, new_name: str) -> None:
        """Called when the user navigates to a different row in the jobs list.

        We *don't* eagerly tear down log state — we just save a snapshot
        of the outgoing job and (if logs are visible) re-render for the
        new job. If the user is on the info view, we leave streaming
        alone until they press ``l``.
        """
        st = self.state
        if not new_name or new_name == st.job:
            return
        if st.view_mode == "logs":
            self.show()
            return
        if st.streaming and st.job and st.job != new_name:
            self.app.logs.buffer.capture()
            self.app.logs.stream.stop_streaming()

    # ---- log file picker ----------------------------------------------------

    def pick_file(self) -> None:
        st = self.state
        if not st.files or not st.job:
            self.app.notify("No log files available", severity="warning", timeout=2)
            return
        items = [(p, p) for p in st.files]
        picker = PickerModal("Log Files", items, current=st.current_file)
        self.app.push_screen(picker, self._on_file_picked)

    def _on_file_picked(self, chosen: str | None) -> None:
        st = self.state
        if chosen is None or chosen == st.current_file:
            return
        app = self.app
        if st.streaming:
            app.logs.stream.stop_streaming()
        st.current_file = chosen
        st.line_count = 0
        snap = app.logs.buffer.snapshot_for(st.job)
        snap.current_file = chosen
        snap.buffer.clear()
        snap.line_count = 0
        if app.widgets.log:
            app.widgets.log.clear()
            app.widgets.log.write("[dim]Loading log…[/dim]")
        st.loading = True
        self.update_header()
        app.logs.stream.start_streaming(st.job, chosen)
