"""Render the job list, info pane and titles. Handles list events + paging.

``state.pages`` is rebuilt on every :meth:`refresh` from the filtered view
of ``state.all_jobs``. Filters thus span every fetched job, not just one
server page. If the visible page is short and there's more on the server,
``refresh`` schedules a background prefetch (capped by ``state.fetch_limit``).
"""

from __future__ import annotations

from typing import Any

from rich.markup import escape
from textual.widgets import OptionList

from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.helpers import icon_style, info_block, kv, make_option, safe_set
from azure_jobs.tui.state import JobsState


class JobsView(Controller[JobsState]):
    """Job list rendering, info panel, navigation."""

    # ---- core repaint -------------------------------------------------------

    def _apply_filters(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        st = self.state
        sf, ef = st.status_filter, st.experiment_filter
        sq = st.search_query.lower() if st.search_query else ""
        if not (sf or ef or sq):
            return list(jobs)
        out: list[dict[str, Any]] = []
        for j in jobs:
            if sf and j.get("status") != sf:
                continue
            if ef and j.get("experiment") != ef:
                continue
            if sq:
                haystack = (
                    f"{j.get('display_name', '')} {j.get('name', '')} "
                    f"{j.get('experiment', '')} {j.get('tags', '')}"
                ).lower()
                if sq not in haystack:
                    continue
            out.append(j)
        return out

    def refresh(
        self,
        restore_name: str = "",
        *,
        restore_selection: bool = False,
    ) -> None:
        """Repaint the job list from filtered ``all_jobs`` and re-derive pages.

        ``restore_selection``: when True, try to keep the currently selected
        job highlighted across the repaint (used after a background batch
        merges in).
        """
        st = self.state

        # Remember selection before we rebuild.
        if restore_selection and not restore_name:
            if 0 <= st.selected_idx < len(st.filtered):
                restore_name = st.filtered[st.selected_idx].get("name", "")

        # 1. Filter across everything fetched so far.
        all_filtered = self._apply_filters(st.all_jobs)

        # 2. Re-chunk into pages of page_size.
        if all_filtered:
            st.pages = [
                all_filtered[i : i + st.page_size]
                for i in range(0, len(all_filtered), st.page_size)
            ]
        else:
            st.pages = [[]]

        # 3. Clamp current_page after re-chunking.
        if st.current_page >= len(st.pages):
            st.current_page = max(0, len(st.pages) - 1)

        # 4. Display current page.
        st.filtered = list(st.pages[st.current_page])

        ol = self.app.widgets.jobs
        if ol is not None:
            ol.clear_options()
            for j in st.filtered:
                ol.add_option(make_option(j))

        self.update_titles()

        # 5. Restore selection by name where possible.
        target_idx = 0
        if restore_name:
            for i, j in enumerate(st.filtered):
                if j.get("name") == restore_name:
                    target_idx = i
                    break

        if st.filtered:
            if ol is not None:
                ol.highlighted = target_idx
            st.selected_idx = target_idx
            self.show_info(st.filtered[target_idx])
            self.app.jobs.fetcher.hide_info_loading()
        else:
            st.selected_idx = -1
            if self.app.widgets.info:
                self.app.widgets.info.update(kv([], hint="No matching jobs."))
            self.app.jobs.fetcher.hide_info_loading()
            self.app.logs.update_tab_title()
            self._update_subtitle(None)

        # 6. Auto-prefetch: if the visible page is short and we know the
        # server has more, schedule another fetch (bounded by fetch_limit).
        self._maybe_prefetch()

    def _maybe_prefetch(self) -> None:
        st = self.state
        if not st.has_more or st.fetching:
            return
        if len(st.all_jobs) >= st.fetch_limit:
            return
        # Only fetch more when the current page isn't full *and* we're on
        # the last page (no point pre-loading earlier pages).
        on_last_page = st.current_page == len(st.pages) - 1
        if not on_last_page:
            return
        if len(st.filtered) >= st.page_size:
            return
        self.app.jobs.fetcher.fetch_next_page()

    def update_titles(self) -> None:
        st = self.state
        total_pages = len(st.pages)
        current = st.current_page + 1
        page_label = f"Page {current}/{total_pages}"
        if st.has_more:
            page_label += "+"
        shown = len(st.filtered)
        parts = [page_label, f"({shown} jobs)"]
        if st.status_filter:
            parts.append(f"▸ {st.status_filter}")
        if st.experiment_filter:
            parts.append(f"▸ {st.experiment_filter}")
        if st.search_query:
            parts.append(f'"{st.search_query}"')
        if st.fetching:
            parts.append("[dim]…[/dim]")
        elif st.load_status.name == "ERROR":
            # Visually flag error state in the pane title (in addition to
            # the inline notification).
            err = st.last_error or "error"
            parts.append(f"[red]⚠ {err[:60]}[/red]")
        try:
            self.app.query_one("#jobs-pane").border_title = "  ".join(parts)
        except Exception:
            pass

    def _update_subtitle(self, job: dict[str, Any] | None = None) -> None:
        try:
            rp = self.app.query_one("#right-pane")
        except Exception:
            return
        if job is None:
            rp.border_subtitle = ""
            return
        icon, sty = icon_style(job.get("status", ""))
        status = job.get("status", "?")
        display = job.get("display_name") or job.get("name", "")
        max_name = max(20, (rp.size.width or 60) - 20)
        if len(display) > max_name:
            display = display[: max_name - 1] + "…"
        rp.border_subtitle = f"{escape(display)}  [{sty}]{icon} {status}[/{sty}]"

    def show_info(self, job: dict[str, Any]) -> None:
        self._update_subtitle(job)
        self.app.logs.update_tab_title()
        # info_block escapes user data; the safe_set boundary additionally
        # protects against any future markup leak by falling back to plain
        # text on parse failure.
        safe_set(self.app.widgets.info, info_block(job))

    # ---- list events --------------------------------------------------------

    def on_option_selected(self, event: OptionList.OptionSelected) -> None:
        st = self.state
        idx = event.option_index
        if 0 <= idx < len(st.filtered):
            st.selected_idx = idx
            if self.app.widgets.info:
                self.app.widgets.info.update(kv([("", "")], hint="Refreshing..."))
            self.app.jobs.fetcher.fetch_single(st.filtered[idx])

    def on_option_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        st = self.state
        if event.option_list.id != "job-list":
            return
        idx = event.option_index
        if 0 <= idx < len(st.filtered):
            st.selected_idx = idx
            job = st.filtered[idx]
            self.show_info(job)
            self.app.logs.on_job_changed(job.get("name", ""))
            if job.get("status") == "Failed" and not job.get("error"):
                self.app.jobs.fetcher.fetch_single(job)

    # ---- pagination ---------------------------------------------------------

    def action_next_page(self) -> None:
        st = self.state
        if st.current_page + 1 < len(st.pages):
            st.current_page += 1
            self.refresh()
        elif st.has_more and not st.fetching:
            # The new page isn't here yet — mark the intent so ``merge_batch``
            # advances as soon as the fetch completes (otherwise the user
            # would need to press right twice).
            st.pending_advance = True
            self.app.notify("Loading next page…", timeout=2)
            self.app.jobs.fetcher.fetch_next_page()

    def action_prev_page(self) -> None:
        if self.state.current_page > 0:
            self.state.current_page -= 1
            self.refresh()
