"""Cancel-job confirm modal + worker."""

from __future__ import annotations

from typing import Any

from rich.markup import escape
from textual.worker import get_current_worker

from azure_jobs.tui.components import ConfirmCancel
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.jobs._shared import short_error
from azure_jobs.tui.helpers import TERMINAL_STATUSES, kv, safe_notify
from azure_jobs.tui.state import JobsState

class JobsCancel(Controller[JobsState]):
    """Cancel-job action: confirm modal + REST cancel worker."""

    def action_cancel(self) -> None:
        st = self.state
        if 0 <= st.selected_idx < len(st.filtered):
            job = st.filtered[st.selected_idx]
            display = job.get("display_name") or job.get("name", "?")
            self.app.push_screen(ConfirmCancel(display), self._on_confirmed)

    def _on_confirmed(self, confirmed: bool) -> None:
        if not confirmed:
            return
        st = self.state
        if 0 <= st.selected_idx < len(st.filtered):
            job = st.filtered[st.selected_idx]
            if self.app.widgets.info:
                self.app.widgets.info.update(kv([("", "")], hint="Cancelling…"))
            seq = self.state.session_seq
            self.spawn(
                lambda: self._do_cancel(job, seq),
                group="cancel",
            )

    def _do_cancel(self, job: dict[str, Any], seq: int) -> None:
        app = self.app
        rest = app.workspace.state.rest_client
        safe_display = escape(job.get("display_name") or job.get("name", "?"))
        if self.state.session_seq != seq:
            return
        if rest is None:
            app.call_from_thread(
                safe_notify, app, "Workspace not configured", severity="warning"
            )
            return
        worker = get_current_worker()
        name = job.get("name", "")
        try:
            cur = rest.jobs.get(name)
            status = cur.get("status", "")
            if status in TERMINAL_STATUSES:
                if not worker.is_cancelled and self.state.session_seq == seq:
                    app.call_from_thread(
                        safe_notify, app, f"{safe_display}: already {escape(status)}"
                    )
                return
            rest.jobs.cancel(name)
            final_job = rest.jobs.get(name)
            final = escape(final_job.get("status", "?"))
        except Exception as exc:
            if not worker.is_cancelled and self.state.session_seq == seq:
                app.call_from_thread(
                    safe_notify, app, short_error(exc, limit=80), severity="error"
                )
            return
        if not worker.is_cancelled and self.state.session_seq == seq:
            app.call_from_thread(safe_notify, app, f"{safe_display}: {final}")
            app.call_from_thread(app.jobs.fetcher.fetch_single, job)
