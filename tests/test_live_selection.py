"""Fastest live candidate selection is deterministic and cheap to unit test."""

from azure_jobs.shared.types.azure import SeriesQuota, SlaTierQuota, VCInfo
from azure_jobs.shared.contract.models import Job

from .live_e2e import choose_fastest
from .test_live_submit import _cleanup_job


def workspace_pair(*, idle: int = 0, busy: int = 0, maximum: int = 1):
    return {
        "workspace": {
            "name": "ws",
            "resource_group": "rg",
            "subscription_id": "sub",
        },
        "computes": [
            {
                "name": "gpu",
                "compute_type": "AmlCompute",
                "provisioning_state": "Succeeded",
                "nodes_idle": idle,
                "nodes_busy": busy,
                "nodes_max": maximum,
            }
        ],
    }


def sing_vc():
    quota = SeriesQuota(
        series="NDH100v5",
        accelerator="H100",
        gpu_memory=80,
        user_limit=SlaTierQuota(limit=1, used=0),
    )
    return VCInfo("vc", "vc-rg", "vc-sub", quotas=[quota])


def test_idle_aml_wins() -> None:
    candidate = choose_fastest(
        [workspace_pair(idle=1)],
        [sing_vc()],
        [{"names": ["torch:latest"]}],
    )
    assert candidate.service == "aml"
    assert candidate.score == 0


def test_sing_wins_over_scaling_aml() -> None:
    candidate = choose_fastest(
        [workspace_pair()],
        [sing_vc()],
        [{"names": ["torch:latest"]}],
    )
    assert candidate.service == "sing"
    assert candidate.sku == "1x80G1-H100"


def test_sing_wins_over_busy_aml() -> None:
    candidate = choose_fastest(
        [workspace_pair(busy=1)],
        [sing_vc()],
        [{"names": ["torch:latest"]}],
    )
    assert candidate.service == "sing"
    assert candidate.score == 10


def test_service_can_be_forced() -> None:
    candidate = choose_fastest(
        [workspace_pair(idle=1)],
        [sing_vc()],
        [{"names": ["torch:latest"]}],
        service="sing",
    )
    assert candidate.service == "sing"


def test_missing_resources_returns_none() -> None:
    assert choose_fastest([], [], []) is None


def test_sing_can_be_disabled_when_identity_is_unavailable() -> None:
    candidate = choose_fastest(
        [workspace_pair()],
        [sing_vc()],
        [{"names": ["torch:latest"]}],
        allow_sing=False,
    )
    assert candidate.service == "aml"


def test_zero_capacity_aml_is_not_a_candidate() -> None:
    pair = workspace_pair(maximum=0)
    assert choose_fastest([pair], [], [], allow_sing=False) is None


def test_cleanup_cancels_waits_and_deletes(monkeypatch) -> None:
    monkeypatch.setattr("tests.test_live_submit.time.sleep", lambda _: None)

    class Jobs:
        statuses = iter(["Running", "Completed"])
        cancelled = []
        deleted = []

        def status(self, ref):
            return Job.from_mapping(
                {"name": ref, "status": next(self.statuses)}
            )

        def cancel(self, ref):
            self.cancelled.append(ref.backend_ref)

        def delete(self, ref):
            self.deleted.append(ref.backend_ref)

    class Client:
        job = Jobs()

    _cleanup_job(Client(), "job")
    assert Client.job.cancelled == ["job"]
    assert Client.job.deleted == ["job"]


def test_cleanup_treats_missing_ambiguous_submit_as_clean() -> None:
    class Jobs:
        def status(self, ref):
            raise RuntimeError("404 not found")

    class Client:
        job = Jobs()

    _cleanup_job(Client(), "missing")
