from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, TypeVar

from rich.console import Console

T = TypeVar("T")
R = TypeVar("R")


def parallel_map(
    items: list[T],
    fn: Callable[[T], R],
    *,
    label: str,
    name: Callable[[T], str] = str,
    console: Console,
    max_workers: int = 8,
) -> tuple[list[tuple[T, R]], list[tuple[T, BaseException]]]:
    """Run fn(item) in parallel with a Rich progress spinner.

    Returns (successes, failures) as (item, result) and (item, exc) lists.
    """
    total = len(items)
    successes: list[tuple[T, R]] = []
    failures: list[tuple[T, BaseException]] = []
    done = 0
    with console.status(
        f"[bold cyan]{label} (0/{total})…[/bold cyan]", spinner="dots"
    ) as st:
        with ThreadPoolExecutor(max_workers=min(max_workers, total or 1)) as pool:
            fut_map = {pool.submit(fn, item): item for item in items}
            for fut in as_completed(fut_map):
                item = fut_map[fut]
                done += 1
                try:
                    successes.append((item, fut.result()))
                except BaseException as exc:
                    failures.append((item, exc))
                st.update(
                    f"[bold cyan]{label} ({done}/{total}) {name(item)}…[/bold cyan]"
                )
    return successes, failures
