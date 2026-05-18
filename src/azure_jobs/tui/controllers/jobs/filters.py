"""Search bar + status / experiment / clear filter actions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.widgets import Input

from azure_jobs.tui.components import PickerModal
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.helpers import STATUS_CYCLE, icon_style, safe_close
from azure_jobs.tui.state import JobsState

if TYPE_CHECKING:
    from textual.timer import Timer

    from azure_jobs.tui.app import AjDashboard


class JobsFilters(Controller[JobsState]):
    """Search input + status/experiment pickers + clear-all."""

    # Debounce window for the search input (seconds). Coalesces bursts
    # of keystrokes into a single ``refresh()`` so the O(N) filter pass
    # over ``all_jobs`` doesn't run per character.
    _SEARCH_DEBOUNCE = 0.15

    def __init__(self, app: "AjDashboard", state: JobsState) -> None:
        super().__init__(app, state)
        self._search_timer: "Timer | None" = None

    def close_search_bar(self, *, clear: bool = False) -> bool:
        """Hide the search bar if open; return True if it was open.

        With ``clear=True``, also cancels the debounce timer and zeros
        the query. The caller is responsible for triggering a refresh
        when the query changes.
        """
        app = self.app
        try:
            search_bar = app.query_one("#search-bar")
        except Exception:
            return False
        if search_bar.has_class("hidden"):
            return False
        search_bar.add_class("hidden")
        if clear:
            safe_close(self._search_timer, "stop")
            self._search_timer = None
            try:
                inp = app.query_one("#search-input", Input)
                if inp.value:
                    inp.value = ""
            except Exception:
                pass
            self.state.search_query = ""
        if app.widgets.jobs:
            app.widgets.jobs.focus()
        return True

    # ---- search bar ---------------------------------------------------------

    def action_search(self) -> None:
        """Toggle the search bar (/ key). The query persists when the bar is hidden."""
        app = self.app
        search_bar = app.query_one("#search-bar")
        if search_bar.has_class("hidden"):
            search_bar.remove_class("hidden")
            inp = app.query_one("#search-input", Input)
            inp.value = self.state.search_query
            inp.focus()
        else:
            search_bar.add_class("hidden")
            if app.widgets.jobs:
                app.widgets.jobs.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "search-input":
            return
        self.state.search_query = event.value
        # Debounce: cancel any pending refresh and schedule a fresh one.
        safe_close(self._search_timer, "stop")
        self._search_timer = self.app.set_timer(
            self._SEARCH_DEBOUNCE, self._do_search_refresh
        )

    def _do_search_refresh(self) -> None:
        self._search_timer = None
        self.app.jobs.view.refresh()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search-input":
            # Submit cancels the pending debounce and refreshes immediately.
            safe_close(self._search_timer, "stop")
            self._search_timer = None
            self.app.jobs.view.refresh()
            self.close_search_bar(clear=False)

    # ---- status picker ------------------------------------------------------

    def action_pick_status(self) -> None:
        items: list[tuple[str, str]] = [("", "All")]
        for s in STATUS_CYCLE[1:]:
            icon, sty = icon_style(s)
            items.append((s, f"[{sty}]{icon} {s}[/{sty}]"))
        self.app.push_screen(
            PickerModal("Status", items, current=self.state.status_filter),
            self.apply_status,
        )

    def apply_status(self, value: str | None) -> None:
        if value is None:
            return
        if value != self.state.status_filter:
            self.state.status_filter = value
            self.app.jobs.view.refresh()
            self.app.notify(f"Status: {value or 'All'}")

    # ---- experiment picker --------------------------------------------------

    def action_pick_experiment(self) -> None:
        st = self.state
        experiments = sorted(
            {j.get("experiment", "") for j in st.all_jobs if j.get("experiment")}
        )
        if not experiments:
            self.app.notify("No experiments to filter")
            return
        items: list[tuple[str, str]] = [("", "All")]
        items.extend((exp, exp) for exp in experiments)
        self.app.push_screen(
            PickerModal("Experiment", items, current=st.experiment_filter),
            self.apply_experiment,
        )

    def apply_experiment(self, value: str | None) -> None:
        if value is None:
            return
        if value != self.state.experiment_filter:
            self.state.experiment_filter = value
            self.app.jobs.view.refresh()
            self.app.notify(f"Experiment: {value or 'All'}")

    # ---- clear --------------------------------------------------------------

    def action_clear(self) -> None:
        st = self.state
        changed = bool(st.status_filter or st.experiment_filter or st.search_query)
        st.status_filter = ""
        st.experiment_filter = ""
        # close_search_bar(clear=True) only clears when the bar is open;
        # zero the query directly so an already-hidden bar still releases
        # any active search filter.
        st.search_query = ""
        self.close_search_bar(clear=True)
        if changed:
            self.app.jobs.view.refresh()
        self.app.notify("Filters cleared")
        if self.app.widgets.jobs:
            self.app.widgets.jobs.focus()
