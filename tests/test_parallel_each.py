"""Tests for :func:`azure_jobs.utils.concurrent.parallel_each`."""

from __future__ import annotations

import threading
import time

from azure_jobs.utils.concurrent import parallel_each


def test_empty_input_returns_empty_lists():
    successes, failures = parallel_each([], lambda x: x)
    assert successes == []
    assert failures == []


def test_all_succeed():
    successes, failures = parallel_each([1, 2, 3], lambda x: x * 10)
    assert sorted(successes) == [(1, 10), (2, 20), (3, 30)]
    assert failures == []


def test_failures_isolated_from_successes():
    def fn(x):
        if x == 2:
            raise ValueError("boom")
        return x * 10

    successes, failures = parallel_each([1, 2, 3], fn)
    assert sorted(successes) == [(1, 10), (3, 30)]
    assert len(failures) == 1
    assert failures[0][0] == 2
    assert isinstance(failures[0][1], ValueError)


def test_on_done_callback_called_per_success():
    seen: list[tuple[int, int, int, int]] = []

    def on_done(item, result, done, total):
        seen.append((item, result, done, total))

    parallel_each([1, 2, 3], lambda x: x + 100, on_done=on_done)
    assert len(seen) == 3
    # done counter increments monotonically; total is constant
    dones = sorted(s[2] for s in seen)
    assert dones == [1, 2, 3]
    assert all(s[3] == 3 for s in seen)


def test_on_failure_callback_called_per_failure():
    failures_seen: list[tuple[int, BaseException, int, int]] = []

    def fn(x):
        raise RuntimeError(f"fail-{x}")

    def on_failure(item, exc, done, total):
        failures_seen.append((item, exc, done, total))

    successes, failures = parallel_each(
        [1, 2], fn, on_failure=on_failure
    )
    assert successes == []
    assert len(failures) == 2
    assert len(failures_seen) == 2


def test_runs_in_parallel():
    """Total wall-time of N×0.05s sleeps with max_workers=N should be ≈ 0.05s,
    not N*0.05s."""

    def slow(_):
        time.sleep(0.05)
        return None

    start = time.monotonic()
    parallel_each([1, 2, 3, 4], slow, max_workers=4)
    elapsed = time.monotonic() - start
    # Generous threshold to avoid flaky CI: serial would be 0.20s+.
    assert elapsed < 0.15, f"expected parallel exec, got {elapsed:.3f}s"


def test_max_workers_capped_at_item_count():
    """Even with max_workers > len(items), the pool size shouldn't crash on
    an empty list (regression for the original ``or 1`` workaround)."""
    # No items → no pool spawned.
    assert parallel_each([], lambda x: x, max_workers=8) == ([], [])

    # Single item → workers cap to 1.
    successes, failures = parallel_each([42], lambda x: x + 1, max_workers=8)
    assert successes == [(42, 43)]
    assert failures == []


def test_callbacks_invoked_from_orchestrator_thread():
    """``on_done`` fires inside ``as_completed`` (the caller's thread) so
    Rich ``console.status.update`` calls don't need extra synchronisation."""
    main_thread = threading.current_thread()
    cb_threads: list[threading.Thread] = []

    def on_done(item, _r, _d, _t):
        cb_threads.append(threading.current_thread())

    parallel_each([1, 2, 3], lambda x: x, on_done=on_done, max_workers=3)
    assert cb_threads and all(t is main_thread for t in cb_threads)
