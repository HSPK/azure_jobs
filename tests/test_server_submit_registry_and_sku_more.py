from __future__ import annotations

from unittest.mock import patch

import pytest

from azure_jobs.server import submit as submit_mod
from azure_jobs.server.submit.azureml import sku_match
from azure_jobs.shared.errors import BackendError, SkuResolveError
from azure_jobs.shared.job.spec import JobResult, JobSpec
from azure_jobs.shared.types.azure import SeriesQuota, SlaTierQuota, VCInfo
from azure_jobs.shared.types.instance import InstanceTypeInfo


def _fake_submit(request: JobSpec, *, on_event=None) -> JobResult:  # noqa: ARG001
    return JobResult(job_name=request.name, status="submitted")


class _SkuClient:
    def __init__(self, rows: list[InstanceTypeInfo]) -> None:
        self.sku = type(
            "SkuApi",
            (),
            {
                "list": lambda self, region, subscription_id="", strict=False: list(
                    rows
                )
            },
        )()


def _quota(
    series: str,
    *,
    accelerator: str,
    gpu_memory: int,
    user_limit: int = 8,
    tiers: dict[str, int] | None = None,
) -> SeriesQuota:
    quota = SeriesQuota(
        series=series,
        accelerator=accelerator,
        gpu_memory=gpu_memory,
        user_limit=SlaTierQuota(limit=user_limit, used=0),
    )
    for tier, limit in (tiers or {"Premium": 8}).items():
        quota.tiers[tier] = SlaTierQuota(limit=limit, used=0)
    return quota


def _vc(*quotas: SeriesQuota, region: str = "westus2") -> VCInfo:
    return VCInfo(
        name="vc-a",
        resource_group="rg-a",
        subscription_id="sub-a",
        quotas=list(quotas),
        locations=[region],
    )


def _gpu(series: str, *, accelerator: str, gpus: int, memory: int, nvlink: bool = False) -> InstanceTypeInfo:
    return InstanceTypeInfo(
        name=f"Singularity.{series}-{gpus}",
        series_id=series,
        num_gpus=gpus,
        num_cores=96,
        memory_gib=900,
        accelerator=accelerator,
        gpu_memory_gb=memory,
        nvlink=nvlink,
    )


def _cpu(series: str, *, cores: int) -> InstanceTypeInfo:
    return InstanceTypeInfo(
        name=f"Singularity.{series}-{cores}",
        series_id=series,
        num_gpus=0,
        num_cores=cores,
        memory_gib=cores * 8,
        accelerator="CPU",
    )


def test_submit_registry_registers_defaults_sorts_and_reports_known_services(monkeypatch) -> None:
    monkeypatch.setattr(submit_mod, "_REGISTRY", {})

    submit_mod.register_backend("zeta", _fake_submit)
    submit_mod.register_backend("alpha", _fake_submit, label="Alpha Service")

    assert submit_mod.get_backend("zeta").label == "zeta"
    assert submit_mod.get_backend("alpha").label == "Alpha Service"
    assert submit_mod.known_backends() == ("alpha", "zeta")


@pytest.mark.parametrize(
    ("registry", "expected"),
    [({}, r"Known: \(none\)"), ({"aml": submit_mod.BackendEntry("aml", _fake_submit, "Azure ML")}, "Known: aml")],
)
def test_get_backend_missing_raises_actionable_error(monkeypatch, registry, expected: str) -> None:
    monkeypatch.setattr(submit_mod, "_REGISTRY", registry)

    with pytest.raises(BackendError, match=expected):
        submit_mod.get_backend("missing")


def test_vendor_and_pick_instance_cover_cpu_and_empty_rows() -> None:
    cpu_row = _cpu("Eadsv5", cores=4)
    assert sku_match._vendor(cpu_row) == "cpu"
    assert sku_match._vendor(_gpu("ND_MI300X", accelerator="MI300X", gpus=8, memory=192)) == "amd"
    assert sku_match._pick_instance(sku_match.SkuSpec.parse("1xC1"), []) is None

    selected = sku_match._pick_instance(
        sku_match.SkuSpec.parse("1xC2"),
        [_cpu("Eadsv5", cores=4), _cpu("Eadsv5", cores=16)],
    )
    assert selected.num_cores == 16


def test_match_instance_type_requires_catalog_for_vc_series() -> None:
    vc = _vc(_quota("NDv4", accelerator="A100", gpu_memory=40))

    with pytest.raises(SkuResolveError, match="has no series with instance types"):
        sku_match.match_instance_type(
            "1x40G8-A100",
            vc=vc,
            client=_SkuClient([_gpu("OTHER", accelerator="A100", gpus=8, memory=40)]),
            tier="Premium",
            nodes=1,
            gpus_per_node=8,
        )


def test_match_instance_type_reports_available_accelerators() -> None:
    vc = _vc(
        _quota("NDv4", accelerator="A100", gpu_memory=40),
        _quota("NDH100v5", accelerator="H100", gpu_memory=80),
    )
    rows = [
        _gpu("NDv4", accelerator="A100", gpus=8, memory=40),
        _gpu("NDH100v5", accelerator="H100", gpus=8, memory=80),
    ]

    with pytest.raises(SkuResolveError, match="have: A100, H100"):
        sku_match.match_instance_type(
            "1x80G8-MI300X",
            vc=vc,
            client=_SkuClient(rows),
            tier="Premium",
        )


def test_match_instance_type_enforces_user_quota() -> None:
    vc = _vc(_quota("NDv4", accelerator="A100", gpu_memory=40, user_limit=3))

    with pytest.raises(SkuResolveError, match=r"user quota < 8"):
        sku_match.match_instance_type(
            "1x40G8-A100",
            vc=vc,
            client=_SkuClient([_gpu("NDv4", accelerator="A100", gpus=8, memory=40)]),
            tier="Premium",
            nodes=1,
            gpus_per_node=8,
        )


def test_match_instance_type_reports_missing_instance_size() -> None:
    vc = _vc(_quota("NDv4", accelerator="A100", gpu_memory=40))

    with pytest.raises(SkuResolveError, match=r"no instance type with 8 GPUs in series NDv4"):
        sku_match.match_instance_type(
            "1x40G8-A100",
            vc=vc,
            client=_SkuClient([_gpu("NDv4", accelerator="A100", gpus=4, memory=40)]),
            tier="Premium",
        )


def test_match_instance_type_falls_back_to_lower_tier() -> None:
    vc = _vc(
        _quota(
            "NDv4",
            accelerator="A100",
            gpu_memory=40,
            tiers={"Premium": 0, "Standard": 8},
        )
    )
    result = sku_match.match_instance_type(
        "1x40G8-A100",
        vc=vc,
        client=_SkuClient([_gpu("NDv4", accelerator="A100", gpus=8, memory=40)]),
        tier="Premium",
    )

    assert result.effective_tier == "Standard"


def test_match_instance_type_raises_when_no_capacity_in_any_tier() -> None:
    vc = _vc(
        _quota(
            "NDv4",
            accelerator="A100",
            gpu_memory=40,
            tiers={"Premium": 0, "Standard": 0, "Basic": 0},
        )
    )

    with pytest.raises(SkuResolveError, match="has no capacity at tier 'Premium' or below"):
        sku_match.match_instance_type(
            "1x40G8-A100",
            vc=vc,
            client=_SkuClient([_gpu("NDv4", accelerator="A100", gpus=8, memory=40)]),
            tier="Premium",
            nodes=1,
            gpus_per_node=8,
        )
