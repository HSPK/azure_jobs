"""Threaded fan-out helpers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Callable, TypeVar

if TYPE_CHECKING:
    from rich.console import Console

T = TypeVar("T")
R = TypeVar("R")

ProgressCB = Callable[[T, R, int, int], None]
"""``on_done(item, result, completed, total)`` — called per success."""

FailureCB = Callable[[T, BaseException, int, int], None]
"""``on_failure(item, exc, completed, total)`` — called per failure."""

def parallel_each(
    items: list[T],
    fn: Callable[[T], R],
    *,
    on_done: ProgressCB | None = None,
    on_failure: FailureCB | None = None,
    max_workers: int = 8,
) -> tuple[list[tuple[T, R]], list[tuple[T, BaseException]]]:
    """Run fn(item) over *items* in parallel; return (successes, failures)."""
    total = len(items)
    successes: list[tuple[T, R]] = []
    failures: list[tuple[T, BaseException]] = []
    if total == 0:
        return successes, failures

    done = 0
    workers = min(max_workers, total)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        fut_map = {pool.submit(fn, item): item for item in items}
        for fut in as_completed(fut_map):
            item = fut_map[fut]
            done += 1
            try:
                result = fut.result()
            except BaseException as exc:
                failures.append((item, exc))
                if on_failure is not None:
                    on_failure(item, exc, done, total)
            else:
                successes.append((item, result))
                if on_done is not None:
                    on_done(item, result, done, total)
    return successes, failures

def parallel_map(
    items: list[T],
    fn: Callable[[T], R],
    *,
    label: str,
    name: Callable[[T], str] = str,
    console: Console,
    max_workers: int = 8,
) -> tuple[list[tuple[T, R]], list[tuple[T, BaseException]]]:
    """:func:parallel_each with a Rich spinner that ticks per item."""
    total = len(items)
    with console.status(
        f"[bold cyan]{label} (0/{total})…[/bold cyan]", spinner="dots"
    ) as st:

        def _tick(item: T, _result: Any, done: int, _total: int) -> None:
            st.update(
                f"[bold cyan]{label} ({done}/{total}) {name(item)}…[/bold cyan]"
            )

        def _tick_fail(item: T, _exc: BaseException, done: int, _total: int) -> None:
            st.update(
                f"[bold cyan]{label} ({done}/{total}) {name(item)}…[/bold cyan]"
            )

        return parallel_each(
            items,
            fn,
            on_done=_tick,
            on_failure=_tick_fail,
            max_workers=max_workers,
        )

