"""Thin log-buffer compatibility and asynchronous persistence facade."""

from __future__ import annotations

import re
import secrets
import os
from datetime import datetime

from azure_jobs.const import AJ_LOGS_HOME
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.errors import format_error
from azure_jobs.tui.log_store import LogsStore
from azure_jobs.tui.runtime import CancellationToken, TaskRunner
from azure_jobs.tui.state import LogsState
from azure_jobs.tui.view_ports import LogsViewPort

_UNSAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")


class LogsBuffer(Controller[LogsState]):
    """Expose byte-store operations used by commands and legacy tests."""

    def __init__(
        self,
        ui: LogsViewPort,
        tasks: TaskRunner,
        store: LogsStore,
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store

    def current_lines(self) -> list[str]:
        return list(self.store.lines())

    def replace(self, path: str, text: str) -> None:
        self.store.replace_for_test(path, text.encode("utf-8"))

    def write_line(self, text: str, *, error: bool = False) -> None:
        self.store.append_for_test(text.encode("utf-8") + b"\n")

    def append_lines(self, text: str) -> None:
        self.store.append_for_test(text.encode("utf-8"))

    def append_error(self, error: str) -> None:
        self.store.append_for_test(error.encode("utf-8") + b"\n")

    def prepend_lines(
        self,
        text: str,
        *,
        new_head: int,
        scroll_top: bool = False,
    ) -> bool:
        return self.store.prepend_for_test(text.encode("utf-8"))

    def log_status(self, message: str, *, error: bool = False) -> None:
        self.ui.write_log_status(message)
        self.ui.set_log_loading(False)

    def save_to_file(self) -> None:
        state = self.state
        payload = self.store.raw_bytes()
        if state.job is None:
            self.notify("No active job", severity="warning", timeout=2)
            return
        if not payload:
            self.notify("No log content to save", severity="warning", timeout=2)
            return
        safe_job = _UNSAFE_FILENAME.sub("_", state.job.backend_ref)[:100]
        safe_file = _UNSAFE_FILENAME.sub(
            "_",
            state.current_file or "log",
        )[:100]

        def write(token: CancellationToken) -> str:
            AJ_LOGS_HOME.mkdir(parents=True, exist_ok=True)
            token.check()
            for _attempt in range(3):
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
                suffix = secrets.token_hex(4)
                path = AJ_LOGS_HOME / (
                    f"{safe_job}-{safe_file}-{timestamp}-{suffix}.log"
                )
                temporary = AJ_LOGS_HOME / f".{path.name}.tmp"
                try:
                    with temporary.open("xb") as output:
                        output.write(payload)
                        output.flush()
                        os.fsync(output.fileno())
                    os.link(temporary, path)
                    temporary.unlink()
                    directory_fd = os.open(
                        AJ_LOGS_HOME,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                    )
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                    return str(path)
                except FileExistsError:
                    temporary.unlink(missing_ok=True)
                    continue
                except Exception:
                    temporary.unlink(missing_ok=True)
                    raise
            raise FileExistsError("Could not allocate a unique log filename")

        self.tasks.run(
            write,
            group="logs.save",
            on_success=lambda saved: self.notify(f"Saved → {saved}", timeout=4),
            on_error=lambda exc: self.notify(
                format_error("Save logs", exc),
                severity="error",
            ),
            exclusive=False,
            priority=5,
        )
