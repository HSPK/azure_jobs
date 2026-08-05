"""Shared policy for selecting useful Azure ML log files."""

from __future__ import annotations

_PRIORITY = (
    "user_logs/std_log",
    "logs/amlt_code_runner",
    "azureml-logs/70_driver_log",
    "azureml-logs/75_job_post",
    "logs/",
)
_SUFFIXES = (".txt", ".log", ".out", ".err")


def is_log_file(path: str) -> bool:
    return (
        path.endswith(_SUFFIXES)
        or "/std_log" in path
        or any(path.startswith(prefix) for prefix in _PRIORITY)
    )


def _sort_key(path: str) -> tuple[int, str]:
    for index, prefix in enumerate(_PRIORITY):
        if path.startswith(prefix):
            return index, path
    return len(_PRIORITY), path


def order_log_files(files: list[str] | tuple[str, ...]) -> list[str]:
    """Filter non-log artifacts and order useful logs consistently."""
    return sorted((path for path in files if is_log_file(path)), key=_sort_key)


def pick_default_log(files: list[str] | tuple[str, ...]) -> str:
    """Pick the first log both the SDK and server would present."""
    ordered = order_log_files(files)
    return ordered[0] if ordered else ""


__all__ = ["is_log_file", "order_log_files", "pick_default_log"]
