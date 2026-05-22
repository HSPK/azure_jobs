"""Tests for utils.cache."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from azure_jobs.utils import cache


@pytest.fixture
def cache_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr("azure_jobs.core.const.AJ_CACHE_HOME", tmp_path / "cache")
    return tmp_path / "cache"


def test_cache_roundtrip(cache_home: Path) -> None:
    cache.cache_set("ns", "key1", {"a": 1})
    assert cache.cache_get("ns", "key1", 60) == {"a": 1}


def test_cache_expires(cache_home: Path) -> None:
    cache.cache_set("ns", "k", "v")
    fp = cache_home / "ns" / "k.json"
    old = fp.stat().st_mtime - 1000
    os.utime(fp, (old, old))
    assert cache.cache_get("ns", "k", 10) is None


def test_cache_missing(cache_home: Path) -> None:
    assert cache.cache_get("ns", "nope", 60) is None


def test_cache_clear(cache_home: Path) -> None:
    cache.cache_set("ns", "a", 1)
    cache.cache_set("ns", "b", 2)
    n = cache.cache_clear("ns")
    assert n == 2
