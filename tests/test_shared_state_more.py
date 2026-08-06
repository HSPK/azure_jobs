"""Extra hermetic coverage for console/package/config/cache helpers."""

from __future__ import annotations

import builtins
import importlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import azure_jobs
from azure_jobs.shared.config import store as store_mod
from azure_jobs.shared.utils import cache as cache_mod
from azure_jobs.client.ui.console import console as rich_console
from azure_jobs.client.ui.console import short_portal_url, truncate_middle


@pytest.fixture(autouse=True)
def _reset_store_caches() -> None:
    store_mod._config_cache = None
    store_mod._scoped_cache.clear()
    yield
    store_mod._config_cache = None
    store_mod._scoped_cache.clear()


@pytest.fixture
def cache_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr("azure_jobs.shared.const.AJ_CACHE_HOME", tmp_path / "cache")
    return tmp_path / "cache"


def test_short_portal_url_and_truncate_middle_cover_edge_cases() -> None:
    assert (
        short_portal_url("https://ml.azure.com/runs/job-1")
        == "[link=https://ml.azure.com/runs/job-1]ml.azure.com/runs/job-1[/link]"
    )
    assert short_portal_url("") == ""
    assert short_portal_url(
        "https://ml.azure.com/runs/job-1?wsid=/subscriptions/x",
        rich_link=False,
    ) == "ml.azure.com/runs/job-1"
    assert short_portal_url("https://example.com/job", rich_link=False) == "https://example.com/job"
    assert truncate_middle("short", maxlen=10) == "short"
    assert truncate_middle("abcdefghij", maxlen=5) == "ab…ij"


def test_console_helpers_render_expected_markup(monkeypatch: pytest.MonkeyPatch) -> None:
    printed: list[str] = []
    monkeypatch.setattr(rich_console, "print", lambda msg="": printed.append(msg))

    from azure_jobs.client.ui.console import dim, error, icon_style, status_badge, warning

    warning("careful")
    error("boom")
    dim("quiet")

    assert printed == [
        "[warning]⚠[/warning] careful",
        "[error]✗[/error] boom",
        "[dim]quiet[/dim]",
    ]
    assert icon_style("Unknown") == ("?", "white")
    assert status_badge("Unknown") == "[white] ? Unknown [/white]"


def test_package_lazy_getattr_caches_and_unknown_attr_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    monkeypatch.delattr(azure_jobs, "AjClient", raising=False)
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: SimpleNamespace(AjClient=sentinel),
    )

    assert azure_jobs.__getattr__("AjClient") is sentinel
    assert azure_jobs.AjClient is sentinel
    assert "AjClient" in azure_jobs.__dir__()
    with pytest.raises(AttributeError, match="does_not_exist"):
        azure_jobs.__getattr__("does_not_exist")


def test_package_version_falls_back_when_version_module_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Path(azure_jobs.__file__)
    spec = importlib.util.spec_from_file_location("azure_jobs_fallback_test", source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "azure_jobs._version":
            raise ImportError("missing")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    spec.loader.exec_module(module)

    assert module.__version__ == "0.0.0.dev0"


def test_read_config_at_uses_scoped_cache_on_repeat_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "proj"
    home.mkdir()
    config = home / "aj_config.json"
    config.write_text(json.dumps({"repo_id": "demo"}))
    reads: list[Path] = []
    real_read_text = Path.read_text

    def track_read_text(self: Path, *args, **kwargs):
        reads.append(self)
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", track_read_text)

    first = store_mod.read_config_at(home)
    second = store_mod.read_config_at(home)

    assert first.repo_id == "demo"
    assert second.repo_id == "demo"
    assert reads == [config]


def test_read_config_at_returns_empty_when_stat_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "proj"
    home.mkdir()
    config = home / "aj_config.json"
    config.write_text("{}")
    real_exists = Path.exists
    real_stat = Path.stat

    def forced_exists(self: Path):
        if self == config:
            return True
        return real_exists(self)

    def broken_stat(self: Path, *args, **kwargs):
        if self == config:
            raise OSError("gone")
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "exists", forced_exists)
    monkeypatch.setattr(Path, "stat", broken_stat)

    cfg = store_mod.read_config_at(home)

    assert cfg.repo_id == ""
    assert cfg.workspace.workspace_name == ""


def test_read_config_at_clears_scoped_cache_when_limit_is_reached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "proj"
    home.mkdir()
    (home / "aj_config.json").write_text(json.dumps({"repo_id": "demo"}))
    store_mod._scoped_cache["stale"] = (1.0, {"repo_id": "old"})
    monkeypatch.setattr(store_mod, "_SCOPED_CACHE_MAX", 1)

    cfg = store_mod.read_config_at(home)

    assert cfg.repo_id == "demo"
    assert list(store_mod._scoped_cache) == [str(home / "aj_config.json")]


def test_cache_helpers_cover_long_keys_bad_json_and_failures(
    cache_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    long_key = "x" * 140
    safe = cache_mod._safe_key(long_key)
    assert len(safe) <= 97
    assert safe.startswith("x" * 80 + "_")
    assert cache_mod._safe_key("!!!") == "empty"

    cache_mod.cache_set("ns", "keep", {"a": 1})
    fp = cache_home / "ns" / "keep.json"
    old = fp.stat().st_mtime - 1000
    fp.write_text("{bad json")
    Path(fp).touch()
    assert cache_mod.cache_get("ns", "keep", 0) is None

    broken = cache_home / "ns" / "broken.json"
    broken.parent.mkdir(parents=True, exist_ok=True)
    broken.write_text(json.dumps({"data": "still-here"}))
    old = broken.stat().st_mtime - 1000
    import os

    os.utime(broken, (old, old))
    assert cache_mod.cache_get("ns", "broken", 0) == "still-here"

    real_open = Path.open
    blocked = cache_home / "ns" / "blocked.json"

    def broken_open(self: Path, *args, **kwargs):
        if self == blocked:
            raise OSError("denied")
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", broken_open)
    cache_mod.cache_set("ns", "blocked", {"a": 1})
    assert not blocked.exists()


def test_cache_clear_counts_removed_files_and_ignores_unlink_failures(
    cache_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = cache_home / "ns" / "a.json"
    second = cache_home / "ns" / "b.json"
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_text("{}")
    second.write_text("{}")
    real_unlink = Path.unlink

    def broken_unlink(self: Path, *args, **kwargs):
        if self == second:
            raise OSError("busy")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", broken_unlink)

    removed = cache_mod.cache_clear("ns")

    assert removed == 1
    assert not first.exists()
    assert second.exists()
