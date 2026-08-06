"""Base class for presentation controllers."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from azure_jobs.client.tui.runtime import TaskRunner
    from azure_jobs.client.tui.view_ports import NoticeView

S = TypeVar("S")

class Controller(Generic[S]):
    """Holds narrow UI/runtime ports and one state slice."""

    def __init__(
        self,
        ui: "NoticeView",
        tasks: "TaskRunner",
        state: S | Callable[[], S],
    ) -> None:
        self.ui = ui
        self.tasks = tasks
        self._state_provider = state if callable(state) else lambda: state

    @property
    def state(self) -> S:
        return self._state_provider()

    def render_info(self, hint: str) -> None:
        from azure_jobs.client.tui.helpers import kv

        self.ui.set_info(kv([("", "")], hint=hint))

    def notify(
        self, msg: str, *, severity: str = "information", timeout: float = 5
    ) -> None:
        self.ui.notify(msg, severity=severity, timeout=timeout)
