"""Cancel-job confirm modal + worker."""

from __future__ import annotations

from typing import Any

from rich.markup import escape
from textual.worker import get_current_worker

from azure_jobs.tui.components import ConfirmCancel
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.helpers import TERMINAL_STATUSES, kv
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
            self.app.run_worker(
                lambda: self._do_cancel(job),
                thread=True,
                exclusive=True,
                group="cancel",
            )

    def _do_cancel(self, job: dict[str, Any]) -> None:
        app = self.app
        rest = app.workspace.state.rest_client
        if rest is None:
            app.call_from_thread(
                app.notify, "Workspace not configured", severity="warning"
            )
            return
        name = job.get("name", "")
        display = job.get("display_name") or name
        try:
            cur = rest.jobs.get(name)
            st = cur.get("status", "")
            if st in TERMINAL_STATUSES:
                app.call_from_thread(app.notify, f"{escape(display)}: already {st}")
                return
            rest.jobs.cancel(name)
            final_job = rest.jobs.get(name)
            final = final_job.get("status", "?")
        except Exception as exc:
            app.call_from_thread(app.notify, escape(str(exc)[:80]), severity="error")
            return
        if not get_current_worker().is_cancelled:
            app.call_from_thread(app.notify, f"{escape(display)}: {final}")
            app.call_from_thread(app.jobs.fetcher.fetch_single, job)
