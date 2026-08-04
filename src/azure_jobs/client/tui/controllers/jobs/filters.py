"""Search and filter commands dispatching JobsStore transitions."""

from __future__ import annotations

from rich.text import Text
from textual.timer import Timer

from azure_jobs.client.tui.components import PickerItem
from azure_jobs.client.tui.controllers.base import Controller
from azure_jobs.client.tui.helpers import STATUS_CYCLE, icon_style, safe_close
from azure_jobs.client.tui.runtime import TaskRunner
from azure_jobs.client.tui.state import JobsState
from azure_jobs.client.tui.stores import JobsStore
from azure_jobs.client.tui.view_ports import JobsViewPort


class JobsFilters(Controller[JobsState]):
    _SEARCH_DEBOUNCE = 0.15

    def __init__(
        self,
        ui: JobsViewPort,
        tasks: TaskRunner,
        store: JobsStore,
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._search_timer: Timer | None = None
        self._pending_query = ""

    def close_search_bar(self, *, clear: bool = False) -> bool:
        was_open = self.ui.close_search(clear=clear)
        if clear:
            safe_close(self._search_timer, "stop")
            self._search_timer = None
            self._pending_query = ""
            if self.state.search_query:
                self.store.set_search("")
        return was_open

    def action_search(self) -> None:
        if self.ui.search_open:
            self.ui.close_search(clear=False)
        else:
            self._pending_query = self.state.search_query
            self.ui.open_search(self.state.search_query)

    def on_input_changed(self, value: str) -> None:
        self._pending_query = value
        safe_close(self._search_timer, "stop")
        self._search_timer = self.ui.set_timer(
            self._SEARCH_DEBOUNCE,
            self._apply_query,
        )

    def _apply_query(self) -> None:
        self._search_timer = None
        self.store.set_search(self._pending_query)

    def on_input_submitted(self) -> None:
        safe_close(self._search_timer, "stop")
        self._search_timer = None
        self._apply_query()
        self.close_search_bar(clear=False)

    def action_pick_status(self) -> None:
        items = [PickerItem("", Text("All"))]
        for status in STATUS_CYCLE[1:]:
            icon, style = icon_style(status)
            items.append(PickerItem(status, Text(f"{icon} {status}", style=style)))
        self.ui.pick(
            "Status",
            items,
            self.state.status_filter,
            self.apply_status,
        )

    def apply_status(self, value: str | None) -> None:
        if value is None or value == self.state.status_filter:
            return
        self.store.set_status(value)
        self.notify(f"Status: {value or 'All'}")

    def action_pick_experiment(self) -> None:
        experiments = sorted(
            {job.experiment for job in self.state.loaded_jobs if job.experiment}
        )
        if not experiments:
            self.notify("No experiments to filter")
            return
        items = [PickerItem("", Text("All"))]
        items.extend(PickerItem(value, Text(value)) for value in experiments)
        self.ui.pick(
            "Experiment",
            items,
            self.state.experiment_filter,
            self.apply_experiment,
        )

    def apply_experiment(self, value: str | None) -> None:
        if value is None or value == self.state.experiment_filter:
            return
        self.store.set_experiment(value)
        self.notify(f"Experiment: {value or 'All'}")

    def action_clear(self) -> None:
        self.close_search_bar(clear=True)
        self.store.clear_filters()
        self.notify("Filters cleared")
        self.ui.focus_info()
