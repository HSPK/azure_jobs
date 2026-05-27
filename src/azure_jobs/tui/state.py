"""Domain state dataclasses owned by each controller."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from azure_jobs.config import AJWorkspace

if TYPE_CHECKING:
    from textual.widgets import OptionList, Static

    from azure_jobs.az_client import AzureMLClient, LogStreamer
    from azure_jobs.tui.components import LogViewer

class LoadStatus(str, Enum):
    """Lifecycle of the jobs pane I/O."""

    IDLE = "idle"
    LOADING_INITIAL = "loading_initial"
    LOADING_PAGE = "loading_page"
    REFRESHING = "refreshing"
    ERROR = "error"

@dataclass
class JobsState:
    """List/page/filter/selection state for the jobs pane."""

    all_jobs: list[dict[str, Any]] = field(default_factory=list)
    job_idx: dict[str, int] = field(default_factory=dict)
    filtered: list[dict[str, Any]] = field(default_factory=list)
    selected_idx: int = -1
    status_filter: str = ""
    experiment_filter: str = ""
    search_query: str = ""
    pages: list[list[dict[str, Any]]] = field(default_factory=list)
    current_page: int = 0
    next_link: str | None = None
    has_more: bool = True
    load_status: LoadStatus = LoadStatus.IDLE
    last_error: str = ""
    session_seq: int = 0
    page_size: int = 50
    pending_advance: bool = False
    fetch_limit: int = 500

    @property
    def fetching(self) -> bool:
        """True iff a network fetch is currently in flight."""
        return self.load_status in (
            LoadStatus.LOADING_INITIAL,
            LoadStatus.LOADING_PAGE,
            LoadStatus.REFRESHING,
        )

@dataclass
class JobLogSnapshot:
    """Per-job remembered log state (file selection + line buffer)."""

    current_file: str = ""
    buffer: list[str] = field(default_factory=list)
    line_count: int = 0

@dataclass
class LogsState:
    """Live-tail / view-mode state for the right pane."""

    job: str = ""
    files: list[str] = field(default_factory=list)
    current_file: str = ""
    line_count: int = 0
    streaming: bool = False
    loading: bool = False
    streamer: "LogStreamer | None" = None
    last_update_ts: float = 0.0
    view_mode: str = "info"
    auto_scroll: bool = True
    head_offset: int = 0
    total_size: int = 0
    backfilling: bool = False
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
