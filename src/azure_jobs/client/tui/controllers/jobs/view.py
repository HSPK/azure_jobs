"""Pure jobs projection and navigation over JobsStore."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from rich.markup import escape

from azure_jobs.client.tui.controllers.base import Controller
from azure_jobs.client.tui.events import JobSelectionChanged
from azure_jobs.client.tui.helpers import icon_style, info_block, kv
from azure_jobs.client.tui.models import Job, as_job
from azure_jobs.client.tui.runtime import TaskRunner
from azure_jobs.client.tui.state import JobsState
from azure_jobs.client.tui.stores import JobsStore
from azure_jobs.client.tui.view_ports import JobsViewPort


class JobsView(Controller[JobsState]):
    """Render a read-only JobsState and dispatch navigation transitions."""

    def __init__(
        self,
        ui: JobsViewPort,
        tasks: TaskRunner,
        store: JobsStore,
        *,
        fetch_next: Callable[[], None],
        fetch_single: Callable[[Job], None],
        can_actions: Callable[[], bool],
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._fetch_next = fetch_next
        self._fetch_single = fetch_single
        self._can_actions = can_actions

    def render(self) -> None:
        state = self.state
        self.ui.set_jobs(state.page_jobs, highlighted=state.selected_index)
        self.update_titles()
        self._render_selection(state.selected_job)

    def refresh(self, preferred_id: str = "") -> None:
        """Compatibility alias; selection is now owned by JobsStore."""
        self.render()

    def on_selection_changed(self, event: JobSelectionChanged) -> None:
        self._render_selection(event.job)

    def _render_selection(self, selected: Job | None) -> None:
        if selected is None:
            self.ui.set_info(kv([], hint="No matching jobs."))
            self.ui.hide_info_loading()
            self._update_subtitle(None)
            return
        self.show_info(selected)

    def update_titles(self) -> None:
        state = self.state
        total = len(state.matching_jobs)
        cumulative = min(
            (state.current_page + 1) * state.page_size,
            total,
        )
        more = "+" if state.source_has_more else ""
        self.ui.set_jobs_title(f"({cumulative}/{total}{more})")

    def _update_subtitle(self, job: Job | None) -> None:
        if job is None:
            self.ui.set_info_subtitle("")
            return
        icon, style = icon_style(job.status)
        display = job.label
        max_name = max(20, self.ui.right_width - 20)
        if len(display) > max_name:
            display = display[: max_name - 1] + "…"
        self.ui.set_info_subtitle(
            f"{escape(display)}  [{style}]{icon} "
            f"{escape(job.status or '?')}[/{style}]"
        )

    def show_info(self, job: Job | Mapping[str, Any]) -> None:
        job = as_job(job)
        self._update_subtitle(job)
        self.ui.set_info(info_block(job))

    def on_option_selected(self, index: int) -> None:
        job = self.store.select_index(index)
        if job is not None and self._can_actions():
            self.ui.set_info(kv([("", "")], hint="Refreshing…"))
            self._fetch_single(job)

    def on_option_highlighted(self, index: int) -> None:
        previous = self.state.selected_id
        job = self.store.select_index(index)
        if job is None or job.id == previous:
            return
        if (
            self._can_actions()
            and job.status == "Failed"
            and not job.raw.get("error")
        ):
            self._fetch_single(job)

    def action_next_page(self) -> None:
        result = self.store.next_page()
        if result == "fetch":
            self.notify("Loading next page…", timeout=2)
            if not self.state.fetching:
                self._fetch_next()

    def action_prev_page(self) -> None:
        self.store.previous_page()

    def action_next_job(self) -> None:
        result = self.store.move_selection(1)
        if result == "fetch" and not self.state.fetching:
            self._fetch_next()

    def action_previous_job(self) -> None:
        self.store.move_selection(-1)
