"""REST I/O for the jobs pane: pagination, refresh, single-job re-fetch."""

from __future__ import annotations

import logging
from typing import Any

from rich.markup import escape
from textual.worker import get_current_worker

from azure_jobs.az_client import create_rest_client
from azure_jobs.config import AJWorkspace
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.jobs._shared import short_error
from azure_jobs.tui.helpers import safe_notify
from azure_jobs.tui.state import JobsState, LoadStatus

log = logging.getLogger(__name__)

class JobsFetcher(Controller[JobsState]):
    """Owns all REST client interaction for the job list."""

    def _set_status(self, status: LoadStatus, *, error: str = "") -> None:
        st = self.state
        st.load_status = status
        st.last_error = error

    def _bump_session(self) -> int:
        self.state.session_seq += 1
        return self.state.session_seq

    def _show_loading(self, msg: str) -> None:
        self.hide_info_loading()
        self.render_info(msg)

    def show_info_loading(self, label: str) -> None:
        """Show the centered spinner + *label* overlay over the info pane."""
        from textual.widgets import Static

        try:
            ind = self.app.query_one("#info-loading")
            lbl = self.app.query_one("#info-loading-label", Static)
        except Exception:
            return
        lbl.update(label)
        ind.remove_class("hidden")

    def hide_info_loading(self) -> None:
        try:
            ind = self.app.query_one("#info-loading")
        except Exception:
            return
        ind.add_class("hidden")

    def _create_rest_client(self, ws: AJWorkspace) -> bool:
        try:
            self.app.workspace.state.rest_client = create_rest_client(ws)
            return True
        except Exception as exc:
            err = short_error(exc, limit=80)
            self.notify(f"Auth failed: {err}", severity="error")
            self._show_loading(f"[red]Auth failed:[/red] {err}")
            self._set_status(LoadStatus.ERROR, error=err)
            return False

    @staticmethod
    def _is_active(worker: Any, st: JobsState, seq: int) -> bool:
        return (not worker.is_cancelled) and st.session_seq == seq

    def init_fetch(self) -> None:
        """First load after app start (or after workspace switch)."""
        seq = self._bump_session()
        self._set_status(LoadStatus.LOADING_INITIAL)
        self.spawn(lambda: self._do_init_fetch(seq), group="fetch_init")

    def _do_init_fetch(self, seq: int) -> None:
        app = self.app
        st = self.state
        worker = get_current_worker()
        ws = app.workspace.ensure_workspace()
        app.call_from_thread(app.workspace.update_label)
        if not self._is_active(worker, st, seq):
            return
        if not ws:
            self._set_status(LoadStatus.IDLE)
            app.call_from_thread(
                self._show_loading,
                "No workspace configured. Press [bold]w[/bold] to select.",
            )
            return
        if not self._create_rest_client(ws):
            return
        if not self._is_active(worker, st, seq):
            return
        app.call_from_thread(
            self.show_info_loading, f"Reading {escape(ws.workspace_name)}…"
        )
        self._fetch_one_page(seq, status=LoadStatus.LOADING_INITIAL)

    def fetch_next_page(self) -> None:
        """Schedule a single server-page fetch in the background."""
        st = self.state
        if st.fetching or not st.has_more or len(st.all_jobs) >= st.fetch_limit:
            return
        seq = st.session_seq
        self._set_status(LoadStatus.LOADING_PAGE)
        self.spawn(
            lambda: self._fetch_one_page(seq, status=LoadStatus.LOADING_PAGE),
            group="fetch_page",
        )

    def _fetch_one_page(self, seq: int, *, status: LoadStatus) -> None:
        app = self.app
        st = self.state
        worker = get_current_worker()
        rest = app.workspace.state.rest_client
        if rest is None:
            app.call_from_thread(self._set_status, LoadStatus.IDLE)
            return
        try:
            batch, next_link = rest.jobs.list_page(
                next_link=st.next_link, top=st.page_size
            )
        except Exception as exc:
            log.exception("jobs.list_page failed")
            if self._is_active(worker, st, seq):
                err = short_error(exc)
                app.call_from_thread(self._show_loading, err)
                app.call_from_thread(self._set_status, LoadStatus.ERROR, error=err)
            return
        if not self._is_active(worker, st, seq):
            return
        app.call_from_thread(self._on_page_fetched, seq, batch, next_link)

    def _on_page_fetched(
        self,
        seq: int,
        batch: list[dict[str, Any]],
        next_link: str | None,
    ) -> None:
        st = self.state
        if seq != st.session_seq:
            return
        st.next_link = next_link
        st.has_more = next_link is not None
        self.merge_batch(batch)

    def merge_batch(self, batch: list[dict[str, Any]]) -> None:
        """Append *batch* to all_jobs (dedup by name) and refresh view."""
        st = self.state
        for j in batch:
            name = j.get("name")
            if not name or name in st.job_idx:
                continue
            st.all_jobs.append(j)
            st.job_idx[name] = len(st.all_jobs) - 1
        self._set_status(LoadStatus.IDLE)
        view = self.app.jobs.view
        view.refresh(restore_selection=not st.pending_advance)
        if st.pending_advance:
            st.pending_advance = False
            if st.current_page + 1 < len(st.pages):
                st.current_page += 1
                view.refresh()

    def load(self, jobs: list[dict[str, Any]]) -> None:
        """Direct injection used by tests + restart paths."""
        self._bump_session()
        st = self.state
        st.all_jobs = list(jobs)
        st.job_idx = {j.get("name", ""): i for i, j in enumerate(st.all_jobs)}
        st.current_page = 0
        st.has_more = False
        st.next_link = None
        self._set_status(LoadStatus.IDLE)
        self.app.jobs.view.refresh()

    def fetch_single(self, job: dict[str, Any]) -> None:
        seq = self.state.session_seq
        self.spawn(
            lambda: self._do_fetch_single(job, seq),
            group="single",
            exclusive=False,
        )

    def _do_fetch_single(self, job: dict[str, Any], seq: int) -> None:
        rest = self.app.workspace.state.rest_client
        if rest is None:
            return
        worker = get_current_worker()
        name = job.get("name", "")
        if not name:
            return
        try:
            updated = rest.jobs.get(name)
        except Exception as exc:
            log.warning("get %s failed: %s", name, exc)
            return
        if self._is_active(worker, self.state, seq):
            self.app.call_from_thread(self._on_single_fetched, name, updated)

    def _on_single_fetched(self, name: str, updated: dict[str, Any]) -> None:
        st = self.state
        idx = st.job_idx.get(name)
        if idx is None:
            return
        st.all_jobs[idx] = updated
        for i, j in enumerate(st.filtered):
            if j.get("name") == name:
                st.filtered[i] = updated
                if i == st.selected_idx:
                    self.app.jobs.view.show_info(updated)
                break

    def action_refresh(self) -> None:
        if self.app.workspace.state.rest_client is None:
            safe_notify(self.app, "No workspace configured", severity="warning")
            return
        seq = self.state.session_seq
        self._set_status(LoadStatus.REFRESHING)
        safe_notify(self.app, "Refreshing…", timeout=2)
        self.spawn(lambda: self._do_incremental_refresh(seq), group="refresh")

    def _do_incremental_refresh(self, seq: int) -> None:
        app = self.app
        st = self.state
        worker = get_current_worker()
        rest = app.workspace.state.rest_client
        if rest is None:
            app.call_from_thread(self._set_status, LoadStatus.IDLE)
            return
        try:
            batch, _next = rest.jobs.list_page(next_link=None, top=st.page_size)
        except Exception as exc:
            log.exception("refresh failed")
            if self._is_active(worker, st, seq):
                err = short_error(exc)
                app.call_from_thread(safe_notify, app, err, severity="error")
                app.call_from_thread(self._set_status, LoadStatus.ERROR, error=err)
            return
        new_jobs: list[dict[str, Any]] = []
        updated: list[tuple[str, dict[str, Any]]] = []
        for j in batch:
            name = j.get("name", "")
            if not name:
                continue
            if name not in st.job_idx:
                new_jobs.append(j)
            else:
                updated.append((name, j))
        if self._is_active(worker, st, seq):
            app.call_from_thread(self._on_refresh_done, seq, new_jobs, updated)

    def _on_refresh_done(
        self,
        seq: int,
        new_jobs: list[dict[str, Any]],
        updated: list[tuple[str, dict[str, Any]]],
    ) -> None:
        st = self.state
        if seq != st.session_seq:
            return
        for name, j in updated:
            idx = st.job_idx.get(name)
            if idx is not None:
                st.all_jobs[idx] = j
        if new_jobs:
            st.all_jobs = new_jobs + st.all_jobs
            st.job_idx = {j.get("name", ""): i for i, j in enumerate(st.all_jobs)}
        cur_name = ""
        if 0 <= st.selected_idx < len(st.filtered):
            cur_name = st.filtered[st.selected_idx].get("name", "")
        self._set_status(LoadStatus.IDLE)
        self.app.jobs.view.refresh(restore_name=cur_name)
        msg = f"Refreshed: {len(updated)} updated"
        if new_jobs:
            msg += f", {len(new_jobs)} new"
        safe_notify(self.app, msg, timeout=2)
