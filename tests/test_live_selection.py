"""Fastest live candidate selection is deterministic and cheap to unit test."""

from types import SimpleNamespace
from unittest.mock import patch

from azure_jobs.shared.types.azure import SeriesQuota, SlaTierQuota, VCInfo
from azure_jobs.shared.contract.models import Job

from .live_e2e import choose_auto_sing, choose_fastest
from .test_live_submit import _cleanup_job, _find_sing_workspace_and_identity


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


def test_auto_sing_candidate_keeps_compute_empty_and_exact_sku() -> None:
    candidate = choose_auto_sing(
        {
            "name": "ws",
            "resource_group": "rg",
            "subscription_id": "sub",
        },
        [{"names": ["torch:latest"]}],
        sku="1x40G1-A100",
    )

    assert candidate is not None
    assert candidate.compute == ""
    assert candidate.sku == "1x40G1-A100"
    assert candidate.image == "amlt-sing/torch:latest"


def test_auto_sing_candidate_requires_workspace_image_and_sku() -> None:
    assert choose_auto_sing(None, [], sku="1x40G1-A100") is None
    assert choose_auto_sing(
        {"name": "ws", "resource_group": "rg", "subscription_id": "sub"},
        [],
        sku="1x40G1-A100",
    ) is None
    assert choose_auto_sing(
        {"name": "ws", "resource_group": "rg", "subscription_id": "sub"},
        [{"names": ["torch:latest"]}],
        sku="",
    ) is None


def test_find_sing_workspace_honors_case_insensitive_workspace_and_uai() -> None:
    workspaces = [
        SimpleNamespace(
            name="Other",
            resource_group="rg-a",
            subscription_id="sub-a",
        ),
        SimpleNamespace(
            name="Embodied-AML",
            resource_group="rg-b",
            subscription_id="sub-b",
        ),
    ]
    identities = {
        "Other": {},
        "Embodied-AML": {
            "/subscriptions/sub-b/resourceGroups/rg/providers/"
            "Microsoft.ManagedIdentity/userAssignedIdentities/Runner": {}
        },
    }

    class WorkspaceClient:
        def __init__(self, _sub, _rg, name):
            self.name = name

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

        def info(self):
            return {
                "identity": {
                    "userAssignedIdentities": identities[self.name]
                }
            }

    with patch(
        "tests.test_live_submit.AzureWorkspaceClient",
        WorkspaceClient,
    ):
        workspace, uai = _find_sing_workspace_and_identity(
            workspaces,
            requested_workspace="embodied-aml",
            requested_uai=(
                "/SUBSCRIPTIONS/SUB-B/RESOURCEGROUPS/RG/PROVIDERS/"
                "MICROSOFT.MANAGEDIDENTITY/USERASSIGNEDIDENTITIES/RUNNER"
            ),
        )

    assert workspace == {
        "name": "Embodied-AML",
        "resource_group": "rg-b",
        "subscription_id": "sub-b",
    }
    assert uai.endswith("/Runner")


def test_find_sing_workspace_skips_inaccessible_or_identity_free_rows() -> None:
    workspaces = [
        SimpleNamespace(name="broken", resource_group="rg", subscription_id="s"),
        SimpleNamespace(name="empty", resource_group="rg", subscription_id="s"),
    ]

    class WorkspaceClient:
        def __init__(self, _sub, _rg, name):
            self.name = name

        def __enter__(self):
            if self.name == "broken":
                raise RuntimeError("forbidden")
            return self

        def __exit__(self, *_exc):
            return None

        def info(self):
            return {"identity": {"userAssignedIdentities": {}}}

    with patch(
        "tests.test_live_submit.AzureWorkspaceClient",
        WorkspaceClient,
    ):
        assert _find_sing_workspace_and_identity(
            workspaces,
            requested_workspace="",
            requested_uai="",
        ) == (None, "")


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
