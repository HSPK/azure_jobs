"""Domain state dataclasses owned by each controller.

The TUI uses a *State + Controller* pattern: each behavioural domain
groups its mutable state into a single dataclass, owned by the matching
controller. The App holds the controllers (no flat state).

Cross-domain reads are explicit: ``app.workspace.state.rest_client``,
``app.logs.state.streaming``, etc.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from azure_jobs.core.config import AJWorkspace

if TYPE_CHECKING:
    from textual.widgets import OptionList, Static

    from azure_jobs.core.log_stream import LogStreamer
    from azure_jobs.core.rest_client import AzureMLClient
    from azure_jobs.tui.components import LogViewer


class LoadStatus(str, Enum):
    """Lifecycle of the jobs pane I/O.

    Replaces a bare ``fetching`` boolean: lets the renderer pick a
    distinct hint for first load vs. background page-fetch vs. manual
    refresh, and lets us short-circuit incompatible requests cleanly.
    """

    IDLE = "idle"
    LOADING_INITIAL = "loading_initial"
    LOADING_PAGE = "loading_page"
    REFRESHING = "refreshing"
    ERROR = "error"


@dataclass
class JobsState:
    """List/page/filter/selection state for the jobs pane.

    Field ownership (single-writer policy — enforce by review):

    Slice         Owner sub-controller          Fields
    ----------    --------------------------    ----------------------------
    list/page     ``JobsFetcher``               ``all_jobs``, ``job_idx``,
                                                ``pages``, ``current_page``,
                                                ``next_link``, ``has_more``,
                                                ``pending_advance``,
                                                ``page_size``, ``fetch_limit``
    selection     ``JobsView``                  ``selected_idx``, ``filtered``
    filters       ``JobsFilters``               ``status_filter``,
                                                ``experiment_filter``,
                                                ``search_query``
    load          ``JobsFetcher`` (writes)      ``load_status``, ``last_error``,
                  ``JobsController.reset``     ``session_seq``
                  (bumps ``session_seq``)

    Cross-slice readers are unrestricted; only the listed owner mutates
    each field. ``fetching`` is a read-only view derived from
    ``load_status`` (kept for backward-compat with old controller code).
    """

    all_jobs: list[dict[str, Any]] = field(default_factory=list)
    job_idx: dict[str, int] = field(default_factory=dict)
    filtered: list[dict[str, Any]] = field(default_factory=list)
    selected_idx: int = -1
    # Filters
    status_filter: str = ""
    experiment_filter: str = ""
    search_query: str = ""
    # Pagination
    pages: list[list[dict[str, Any]]] = field(default_factory=list)
    current_page: int = 0
    next_link: str | None = None
    has_more: bool = True
    # Load lifecycle (single source of truth — see ``fetching`` property below).
    load_status: LoadStatus = LoadStatus.IDLE
    last_error: str = ""
    # Bumped on every "reset" (initial load, workspace switch, ``load()``
    # injection). Workers carry a snapshot of this when they start; if the
    # value changes mid-flight, their results are dropped instead of
    # corrupting the new session.
    session_seq: int = 0
    page_size: int = 50
    # When the user presses ``right`` while the next page hasn't been
    # fetched yet, this flag tells the merge step to auto-advance after
    # the new batch arrives, so the keypress "sticks".
    pending_advance: bool = False
    # Hard cap on total jobs fetched (across all server pages) to bound
    # auto-prefetch loops triggered by narrow filters. Default lives in
    # :data:`azure_jobs.tui.helpers.FETCH_LIMIT`.
    fetch_limit: int = 500

    # ---- Derived view -------------------------------------------------------

    @property
    def fetching(self) -> bool:
        """True iff a network fetch is currently in flight.

        Derived from :attr:`load_status` so there's exactly one source of
        truth (previously a separate boolean had to be kept in sync).
        """
        return self.load_status in (
            LoadStatus.LOADING_INITIAL,
            LoadStatus.LOADING_PAGE,
            LoadStatus.REFRESHING,
        )


@dataclass
class JobLogSnapshot:
    """Per-job remembered log state (file selection + line buffer).

    The ``buffer`` field accumulates plain-text lines as they are written
    to the viewer; it is what ``Ctrl+S`` writes out to disk. Capped to
    keep memory bounded (see ``LogsController.write_line``).
    """

    current_file: str = ""
    buffer: list[str] = field(default_factory=list)
    line_count: int = 0


@dataclass
class LogsState:
    """Live-tail / view-mode state for the right pane.

    Field ownership (single-writer policy — enforce by review):

    Slice       Owner sub-controller       Fields
    --------    -----------------------    ----------------------------
    selection   ``LogsView``               ``job``, ``files``,
                                           ``current_file``, ``view_mode``
    buffer      ``LogsBuffer``             ``line_count``, ``snapshots``,
                                           ``last_update_ts``
    stream      ``LogsStream``             ``streaming``, ``streamer``,
                                           ``auto_scroll`` (toggled by view)

    Cross-domain teardown is owned by
    :meth:`LogsController.on_workspace_switching`.
    """

    job: str = ""
    files: list[str] = field(default_factory=list)
    current_file: str = ""
    line_count: int = 0
    streaming: bool = False
    # True between ``begin_stream`` and the first byte arriving from the
    # streamer. Drives a "● loading…" header badge so the user knows the
    # press of ``l`` was registered while the network resolves.
    loading: bool = False
    streamer: "LogStreamer | None" = None
    last_update_ts: float = 0.0
    view_mode: str = "info"  # "info" | "logs"
    auto_scroll: bool = True
    # ``head_offset``/``tail_offset`` describe the byte window currently
    # represented by ``snapshots[job].buffer``. ``head_offset > 0`` means
    # there's older content available for backfill via
    # :meth:`LogsController.backfill`. ``total_size`` is the last-known
    # blob length (refreshed by ``LogStreamer.get_size``).
    head_offset: int = 0
    total_size: int = 0
    backfilling: bool = False
    # Per-job cache. ``current_file`` survives job switches so the user
    # returns to the same file selection; ``buffer`` is live during a
    # session (powering ``Ctrl+S`` save and backfill prepend) but is
    # cleared by :meth:`LogsView.begin_stream` on every (re)start of the
    # tail. Dict insertion order doubles as an LRU (CPython 3.7+).
    snapshots: dict[str, JobLogSnapshot] = field(default_factory=dict)


@dataclass
class WorkspaceState:
    """Identity + auth + REST client for the current workspace."""

    current: AJWorkspace | None = None
    available: list[dict[str, str]] = field(default_factory=list)
    subscription_id: str = ""
    rest_client: "AzureMLClient | None" = None


@dataclass
class Widgets:
    """Cached widget references (populated on_mount)."""

    log: "LogViewer | None" = None
    info: "Static | None" = None
    jobs: "OptionList | None" = None
