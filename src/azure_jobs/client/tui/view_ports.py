"""Narrow presentation ports implemented by the Textual UI adapters."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol, runtime_checkable

from rich.text import Text
from textual.timer import Timer

from azure_jobs.client.tui.components import PickerItem
from azure_jobs.client.tui.models import Job


@runtime_checkable
class NoticeView(Protocol):
    def notify(
        self,
        markup: str,
        *,
        severity: str = "information",
        timeout: float = 5,
    ) -> None: ...


@runtime_checkable
class JobsViewPort(NoticeView, Protocol):
    def set_timer(self, delay: float, callback: Callable[[], None]) -> Timer: ...
    def pick(
        self,
        title: str,
        items: list[PickerItem],
        current: str,
        callback: Callable[[str | None], None],
    ) -> None: ...
    def confirm_cancel(
        self,
        display_name: str,
        callback: Callable[[bool], None],
    ) -> None: ...
    def confirm_delete(
        self,
        display_name: str,
        callback: Callable[[bool], None],
    ) -> None: ...
    def set_jobs(self, jobs: Sequence[Job], *, highlighted: int = -1) -> None: ...
    def set_jobs_title(self, markup: str) -> None: ...
    def set_info(self, markup: str) -> None: ...
    def set_info_subtitle(self, markup: str) -> None: ...
    def show_info_loading(self, label: str) -> None: ...
    def hide_info_loading(self) -> None: ...
    def open_search(self, value: str) -> None: ...
    def close_search(self, *, clear: bool) -> bool: ...
    @property
    def search_open(self) -> bool: ...
    def focus_info(self) -> None: ...
    @property
    def right_width(self) -> int: ...


@runtime_checkable
class LogsViewPort(NoticeView, Protocol):
    def set_timer(self, delay: float, callback: Callable[[], None]) -> Timer: ...
    def pick(
        self,
        title: str,
        items: list[PickerItem],
        current: str,
        callback: Callable[[str | None], None],
    ) -> None: ...
    def set_right_title(self, markup: str) -> None: ...
    def set_right_subtitle(self, markup: str) -> None: ...
    def show_logs(self) -> None: ...
    def show_info(self) -> None: ...
    def set_log_loading(self, visible: bool) -> None: ...
    def clear_log(self) -> None: ...
    def write_log_status(self, value: Text | str) -> None: ...
    def append_log_line(
        self,
        number: int,
        value: str,
        *,
        error: bool,
        scroll_end: bool,
    ) -> bool: ...
    def append_log_lines(
        self,
        first_number: int,
        values: Sequence[str],
        *,
        scroll_end: bool,
    ) -> bool: ...
    def replace_log_lines(
        self,
        lines: Sequence[str],
        *,
        previous_y: float = 0,
        prepended: int = 0,
        scroll_top: bool = False,
        scroll_end: bool = False,
    ) -> None: ...
    @property
    def log_scroll_y(self) -> float: ...
    @property
    def focused_id(self) -> str: ...
    def scroll_info(self, direction: str) -> None: ...


@runtime_checkable
class TargetViewPort(NoticeView, Protocol):
    def pick(
        self,
        title: str,
        items: list[PickerItem],
        current: str,
        callback: Callable[[str | None], None],
    ) -> None: ...
    def set_workspace(self, markup: str) -> None: ...
    def set_info(self, markup: str) -> None: ...
    def show_info_loading(self, label: str) -> None: ...
    def hide_info_loading(self) -> None: ...


@runtime_checkable
class ShellViewPort(NoticeView, Protocol):
    def show_help(self) -> None: ...
    def pick(
        self,
        title: str,
        items: list[PickerItem],
        current: str,
        callback: Callable[[str | None], None],
    ) -> None: ...
