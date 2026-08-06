"""Focused contract-model serialization and validation coverage."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from azure_jobs.shared.contract.models import (
    CANCELLED,
    DONE,
    CatalogItem,
    Cursor,
    Job,
    JobPage,
    JobQuerySpec,
    JobRef,
    LogChunk,
    Notification,
    QueuedJob,
    SubmitEvent,
    SubmitOutcome,
    Target,
)


def test_cursor_query_spec_and_job_ref_roundtrip_defaults() -> None:
    assert Cursor.from_json(None) is None
    assert Cursor.from_json({"token": 5}) == Cursor("5")
    assert JobQuerySpec.from_json(None) == JobQuerySpec()
    assert JobQuerySpec.from_json({"include_archived": 1}) == JobQuerySpec(True)
    assert JobRef.from_json({"id": 1, "backend_ref": None, "incarnation": 3}) == JobRef(
        "1", "", "3"
    )


@pytest.mark.parametrize(
    ("tags", "needle"),
    [
        ({"team": "ml"}, "team=ml"),
        (["alpha", 2], "alpha 2"),
        ("free-form", "free-form"),
    ],
)
def test_job_properties_and_roundtrip_cover_search_text_variants(tags, needle) -> None:
    job = Job.from_mapping(
        {
            "name": "demo",
            "display_name": "Demo",
            "status": "Running",
            "experiment": "exp",
            "created": "2026-01-01 00:00:00",
            "tags": tags,
        }
    )

    assert job.ref == JobRef("demo", "demo", "2026-01-01 00:00:00")
    assert job.label == "Demo"
    assert needle in job.search_text
    assert Job.from_json(job.to_json()).to_dict()["tags"] == tags


def test_job_requires_a_non_empty_name() -> None:
    with pytest.raises(ValueError, match="missing a non-empty 'name'"):
        Job.from_mapping({"status": "Running"})


def test_job_page_and_log_chunk_roundtrip_and_validation() -> None:
    job = Job.from_mapping(
        {
            "name": "demo",
            "display_name": "Demo",
            "status": "Queued",
            "experiment": "exp",
        }
    )
    page = JobPage((job,), Cursor("next"))
    assert JobPage.from_json(page.to_json()) == page

    chunk = LogChunk(b"abc", 2, 5, 9, reset=True)
    assert LogChunk.from_json(chunk.to_json()) == chunk

    with pytest.raises(ValueError, match="Invalid log chunk range"):
        LogChunk(b"", -1, 0, 0)
    with pytest.raises(ValueError, match="does not match"):
        LogChunk(b"ab", 0, 3, 3)
    with pytest.raises(ValueError, match="exceeds total size"):
        LogChunk(b"abc", 1, 4, 3)


def test_target_roundtrip_and_catalog_item_access_cover_mapping_and_object_fallbacks() -> None:
    target = Target.create(
        backend="azureml",
        native_id="sub/rg/ws",
        label="ws",
        detail="rg",
        metadata={"subscription_id": "sub"},
    )

    restored = Target.from_json(target.to_json())
    assert restored.id == target.id
    assert restored.key == target.id
    assert restored.name == "ws"
    assert restored.metadata["subscription_id"] == "sub"

    mapping_item = CatalogItem("compute", "gpu", {"vm_size": "ND96"})
    assert mapping_item.vm_size == "ND96"
    assert mapping_item.get("vm_size") == "ND96"
    with pytest.raises(AttributeError, match="payload has"):
        _ = mapping_item.unknown
    with pytest.raises(AttributeError, match="_secret"):
        getattr(mapping_item, "_secret")

    @dataclass
    class Raw:
        vm_size: str

    object_item = CatalogItem("compute", "gpu", Raw("ND96"))
    assert object_item.vm_size == "ND96"
    assert object_item.get("vm_size") == "ND96"
    assert CatalogItem.from_json(mapping_item.to_json()).raw["vm_size"] == "ND96"


def test_submit_queue_and_notification_models_roundtrip() -> None:
    event = SubmitEvent.from_json({"kind": "submit", "detail": "queued", "completed": 1, "total": 2})
    assert event == SubmitEvent(kind="submit", detail="queued", completed=1, total=2)

    submitted = SubmitOutcome.from_json(
        {
            "job_name": "job-1",
            "backend_ref": "backend-1",
            "status": "submitted",
            "portal_url": "https://portal",
            "note": "ok",
        }
    )
    assert submitted.succeeded is True
    failed = SubmitOutcome.from_json({"job_name": "job-2", "status": "failed", "error": "boom"})
    assert failed.succeeded is False

    queued = QueuedJob.from_json({"ticket": "q-1", "name": "demo"})
    assert queued.state == "queued"
    assert queued.terminal is False

    done = QueuedJob.from_json(
        {
            "ticket": "q-2",
            "name": "demo",
            "state": DONE,
            "detail": "ok",
            "outcome": submitted.to_json(),
        }
    )
    cancelled = QueuedJob(ticket="q-3", name="demo", state=CANCELLED)
    assert done.terminal is True
    assert cancelled.terminal is True
    assert done.outcome == submitted

    note = Notification.from_json(
        {
            "topic": "status",
            "title": "Updated",
            "body": "Queued → Running",
            "job": Job.from_mapping(
                {
                    "name": "job-1",
                    "display_name": "Job 1",
                    "status": "Running",
                    "experiment": "exp",
                }
            ).to_json(),
            "payload": {"state": "running"},
            "created_at": 12.5,
        }
    )
    assert note.job is not None
    assert note.job.name == "job-1"
    assert Notification.from_json({"topic": "plain"}).job is None
    assert note.to_json()["payload"] == {"state": "running"}
