"""Cross-process token caching and concurrent journal appends."""

from __future__ import annotations

import json
import multiprocessing
import os
import time
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from azure_jobs.az_client import auth
from azure_jobs.job.spec import JobSpec
from azure_jobs.journal import JobRecord, log_record, read_records


class _Token:
    def __init__(self, token: str, expires_on: float) -> None:
        self.token = token
        self.expires_on = expires_on


class _Credential:
    calls = 0

    def get_token(self, scope: str) -> _Token:
        type(self).calls += 1
        return _Token(f"tok-{type(self).calls}", time.time() + 3600)


@pytest.fixture
def cache_home(tmp_path, monkeypatch):
    monkeypatch.setattr("azure_jobs.const.AJ_CACHE_HOME", tmp_path / "cache")
    return tmp_path / "cache"


class TestTokenCache:
    def test_second_process_reuses_the_cached_token(self, cache_home):
        _Credential.calls = 0
        with patch("azure.identity.AzureCliCredential", _Credential):
            first = auth.fetch_token("scope://a")
            second = auth.fetch_token("scope://a")
        assert first == second
        assert _Credential.calls == 1, "az was shelled out to twice"

    def test_different_scopes_are_cached_separately(self, cache_home):
        _Credential.calls = 0
        with patch("azure.identity.AzureCliCredential", _Credential):
            auth.fetch_token("scope://a")
            auth.fetch_token("scope://b")
        assert _Credential.calls == 2

    def test_an_expired_token_is_refetched(self, cache_home):
        _Credential.calls = 0
        with patch("azure.identity.AzureCliCredential", _Credential):
            auth.fetch_token("scope://a")
            path = auth._token_cache_path("scope://a")
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["expires_on"] = time.time() - 10
            path.write_text(json.dumps(payload), encoding="utf-8")
            auth.fetch_token("scope://a")
        assert _Credential.calls == 2

    def test_a_token_about_to_expire_is_refetched(self, cache_home):
        """The leeway must prevent handing out a token that dies mid-request."""
        _Credential.calls = 0
        with patch("azure.identity.AzureCliCredential", _Credential):
            auth.fetch_token("scope://a")
            path = auth._token_cache_path("scope://a")
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["expires_on"] = time.time() + (auth._REFRESH_LEEWAY / 2)
            path.write_text(json.dumps(payload), encoding="utf-8")
            auth.fetch_token("scope://a")
        assert _Credential.calls == 2

    def test_the_cache_file_is_not_readable_by_others(self, cache_home):
        with patch("azure.identity.AzureCliCredential", _Credential):
            auth.fetch_token("scope://a")
        path = auth._token_cache_path("scope://a")
        assert path.stat().st_mode & 0o777 == 0o600

    def test_a_loosely_permissioned_cache_is_ignored(self, cache_home):
        """A token another user could have written must not be trusted."""
        _Credential.calls = 0
        with patch("azure.identity.AzureCliCredential", _Credential):
            auth.fetch_token("scope://a")
            os.chmod(auth._token_cache_path("scope://a"), 0o644)
            auth.fetch_token("scope://a")
        assert _Credential.calls == 2

    def test_a_corrupt_cache_falls_back_to_fetching(self, cache_home):
        _Credential.calls = 0
        with patch("azure.identity.AzureCliCredential", _Credential):
            auth.fetch_token("scope://a")
            path = auth._token_cache_path("scope://a")
            path.write_text("{not json", encoding="utf-8")
            os.chmod(path, 0o600)
            token, _ = auth.fetch_token("scope://a")
        assert _Credential.calls == 2
        assert token == "tok-2"

    def test_an_unwritable_cache_dir_does_not_break_auth(self, cache_home, monkeypatch):
        monkeypatch.setattr(
            "azure_jobs.const.AJ_CACHE_HOME", "/proc/definitely-not-writable"
        )
        _Credential.calls = 0
        with patch("azure.identity.AzureCliCredential", _Credential):
            token, _ = auth.fetch_token("scope://a")
        assert token == "tok-1"


def _append_worker(record_path: str, index: int) -> None:
    import azure_jobs.const as const_mod

    const_mod.AJ_RECORD = __import__("pathlib").Path(record_path)
    for i in range(20):
        log_record(
            JobRecord(
                request=JobSpec(name=f"w{index}-{i}"),
                created_at=datetime.now(timezone.utc).isoformat(),
                status="submitted",
            )
        )


class TestJournalConcurrency:
    def test_concurrent_appends_do_not_corrupt_the_journal(self, tmp_path, monkeypatch):
        """The daemon queue and a CLI can append at the same time."""
        record = tmp_path / "record.jsonl"
        monkeypatch.setattr("azure_jobs.const.AJ_RECORD", record)

        ctx = multiprocessing.get_context("spawn")
        procs = [
            ctx.Process(target=_append_worker, args=(str(record), i)) for i in range(4)
        ]
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=60)

        lines = record.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 80
        for line in lines:
            json.loads(line)  # every line must be a complete JSON object

    def test_records_round_trip(self, tmp_path, monkeypatch):
        record = tmp_path / "record.jsonl"
        monkeypatch.setattr("azure_jobs.const.AJ_RECORD", record)
        log_record(
            JobRecord(
                request=JobSpec(name="only"),
                created_at=datetime.now(timezone.utc).isoformat(),
                status="submitted",
            )
        )
        records = read_records()
        assert len(records) == 1
        assert records[0]["request"]["name"] == "only"
