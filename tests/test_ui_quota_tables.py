"""Pure presentation tests for quota and SKU tables."""

from __future__ import annotations

import io
import json
import sys

import pytest

from azure_jobs.client.ui.quota_tables import (
    _active_tiers_and_user_limit,
    _fmt_nodes,
    _series_to_sku_rows,
    show_aml_quota_table,
    show_sing_quota_table,
    show_sku_table,
)
from azure_jobs.client.ui.render import set_output_mode
from azure_jobs.shared.types.azure import ComputeInfo, SeriesQuota, SlaTierQuota, VCInfo, WorkspaceInfo
from azure_jobs.shared.types.instance import InstanceTypeInfo


def _capture_stdout(fn) -> str:
    buf = io.StringIO()
    saved = sys.stdout
    sys.stdout = buf
    try:
        fn()
    finally:
        sys.stdout = saved
    return buf.getvalue()


def _quota(
    series: str,
    *,
    accelerator: str = "",
    memory: int = 0,
    premium: tuple[int, int | None] | None = None,
    standard: tuple[int, int | None] | None = None,
    user_limit: tuple[int, int | None] | None = None,
) -> SeriesQuota:
    quota = SeriesQuota(series=series, accelerator=accelerator, gpu_memory=memory)
    if premium is not None:
        quota.set_tier("Premium", premium[0], premium[1])
    if standard is not None:
        quota.set_tier("Standard", standard[0], standard[1])
    if user_limit is not None:
        quota.user_limit = SlaTierQuota(limit=user_limit[0], used=user_limit[1])
    return quota


def _gpu_row(
    series: str,
    name: str,
    *,
    accelerator: str,
    gpus: int,
    mem: int,
    nvlink: bool = False,
) -> InstanceTypeInfo:
    return InstanceTypeInfo(
        name=f"Singularity.{name}",
        series_id=series,
        num_gpus=gpus,
        num_cores=96,
        memory_gib=900,
        accelerator=accelerator,
        gpu_memory_gb=mem,
        nvlink=nvlink,
    )


def _cpu_row(series: str, name: str, *, cores: int) -> InstanceTypeInfo:
    return InstanceTypeInfo(
        name=f"Singularity.{name}",
        series_id=series,
        num_gpus=0,
        num_cores=cores,
        memory_gib=cores * 8,
        accelerator="CPU",
    )


@pytest.fixture(autouse=True)
def _reset_output_mode(monkeypatch):
    monkeypatch.delenv("AJ_OUTPUT", raising=False)
    set_output_mode("rich")
    yield
    set_output_mode("rich")


def test_active_tiers_and_user_limit_scan_all_vcs() -> None:
    vcs = [
        VCInfo(
            "vc-a",
            "rg",
            "sub",
            quotas=[_quota("NDH100v5", premium=(8, 2), user_limit=(16, 4))],
        ),
        VCInfo(
            "vc-b",
            "rg",
            "sub",
            quotas=[_quota("CPU", standard=(4, 0))],
        ),
    ]

    active, has_user_limit = _active_tiers_and_user_limit(
        vcs, ("Premium", "Standard", "Basic")
    )

    assert active == ["Premium", "Standard"]
    assert has_user_limit is True


def test_series_to_sku_rows_cover_cpu_gpu_and_unknown_fallbacks() -> None:
    cpu_rows = _series_to_sku_rows(
        _quota("Eadsv5", accelerator="CPU"),
        [_cpu_row("Eadsv5", "E4ads_v5", cores=4)],
    )
    gpu_rows = _series_to_sku_rows(
        _quota("NDH100v5", accelerator="H100", memory=80),
        [_gpu_row("NDH100v5", "ND96r_H100_v5", accelerator="H100", gpus=8, mem=80, nvlink=True)],
    )
    unknown_rows = _series_to_sku_rows(
        _quota("MysterySeries", accelerator="H200", memory=141),
        [],
    )

    assert cpu_rows[0]["kind"] == "cpu"
    assert cpu_rows[0]["sku_shorthand"] == "C1"
    assert gpu_rows[0]["kind"] == "gpu"
    assert gpu_rows[0]["sku_shorthand"] == "80G8-H100-NvLink"
    assert unknown_rows[0]["kind"] == "unknown"
    assert unknown_rows[0]["instance_type"] == "MysterySeries"
    assert unknown_rows[0]["gpu_memory_gb"] == 141


def test_fmt_nodes_handles_priority_and_zero_capacity() -> None:
    assert _fmt_nodes(0, 0, 0, low_priority=False) == "[dim]0/0[/dim]"
    assert "[red]" in _fmt_nodes(2, 1, 8, low_priority=True)
    assert "[green]" in _fmt_nodes(2, 1, 8, low_priority=False)


def test_show_sing_quota_table_json_includes_empty_and_full_metadata() -> None:
    set_output_mode("json")
    payload = json.loads(
        _capture_stdout(
            lambda: show_sing_quota_table(
                [
                    VCInfo(
                        "vc-a",
                        "rg-a",
                        "sub-a",
                        quotas=[
                            _quota(
                                "NDH100v5",
                                accelerator="H100",
                                memory=80,
                                premium=(8, 2),
                                standard=(4, None),
                                user_limit=(16, 4),
                            )
                        ],
                    ),
                    VCInfo("vc-empty", "rg-b", "sub-b"),
                ],
                full=True,
            )
        )
    )

    assert payload["columns"] == [
        "vc",
        "resource_group",
        "subscription_id",
        "series",
        "accelerator",
        "tier_Premium_limit",
        "tier_Standard_limit",
        "user_limit",
    ]
    assert payload["metadata"]["active_tiers"] == ["Premium", "Standard"]
    assert payload["metadata"]["has_user_limit"] is True
    assert payload["metadata"]["section_by"] == "vc"
    assert payload["rows"][0]["user_limit"] == 16
    assert payload["rows"][1]["vc"] == "vc-empty"
    assert payload["rows"][1]["no_quotas"] is True


def test_show_sku_table_rich_renders_cpu_gpu_unknown_and_no_quota_rows(
    capsys,
) -> None:
    show_sku_table(
        [
            VCInfo(
                "vc-a",
                "rg",
                "sub",
                quotas=[
                    _quota("Eadsv5", accelerator="CPU", premium=(4, 0)),
                    _quota("NDH100v5", accelerator="H100", memory=80, premium=(8, 2)),
                    _quota("MysterySeries", accelerator="H200", memory=141, premium=(1, 0)),
                ],
            ),
            VCInfo("vc-empty", "rg", "sub"),
        ],
        catalog=[
            _cpu_row("Eadsv5", "E4ads_v5", cores=4),
            _gpu_row("NDH100v5", "ND96r_H100_v5", accelerator="H100", gpus=8, mem=80, nvlink=True),
        ],
    )

    out = capsys.readouterr().out
    assert "Singularity SKUs" in out
    assert "4 vCPU" in out
    assert "8×H100" in out
    assert "MysterySeries" in out
    assert "no quotas" in out


def test_show_aml_quota_table_json_groups_rows_without_azure_calls() -> None:
    set_output_mode("json")
    payload = json.loads(
        _capture_stdout(
            lambda: show_aml_quota_table(
                [
                    (
                        WorkspaceInfo("ws-a", "rg-a", "sub-a"),
                        [
                            ComputeInfo(
                                name="cpu-cluster",
                                resource_group="rg-a",
                                subscription_id="sub-a",
                                workspace_name="ws-a",
                                location="eastus",
                                compute_type="AmlCompute",
                                vm_size="Standard_DS3_v2",
                                vm_priority="Dedicated",
                                nodes_idle=0,
                                nodes_busy=1,
                                nodes_max=2,
                            ),
                            ComputeInfo(
                                name="mystery-cluster",
                                resource_group="rg-a",
                                subscription_id="sub-a",
                                workspace_name="ws-a",
                                location="eastus2",
                                compute_type="AmlCompute",
                                vm_size="Unknown_VM",
                                vm_priority="Burst",
                                nodes_idle=2,
                                nodes_busy=0,
                                nodes_max=4,
                            ),
                        ],
                    ),
                    (WorkspaceInfo("ws-empty", "rg-b", "sub-b"), []),
                ]
            )
        )
    )

    assert payload["metadata"]["section_by"] == "workspace"
    assert payload["rows"][0]["cluster"] == "cpu-cluster"
    assert payload["rows"][1]["cluster"] == "mystery-cluster"
    assert payload["rows"][1]["sku"] == ""
    assert payload["rows"][2]["workspace"] == "ws-empty"
    assert payload["rows"][2]["no_clusters"] is True
    assert "ml.azure.com/compute/cpu-cluster/details" in payload["rows"][0]["portal_url"]


def test_show_aml_quota_table_rich_renders_priority_and_no_cluster_rows(capsys) -> None:
    show_aml_quota_table(
        [
            (
                WorkspaceInfo("ws-a", "rg-a", "sub-a"),
                [
                    ComputeInfo(
                        name="spot-cluster",
                        resource_group="rg-a",
                        subscription_id="sub-a",
                        workspace_name="ws-a",
                        location="eastus",
                        compute_type="AmlCompute",
                        vm_size="Unknown_VM",
                        vm_priority="LowPriority",
                        nodes_idle=1,
                        nodes_busy=0,
                        nodes_max=2,
                    ),
                    ComputeInfo(
                        name="burst-cluster",
                        resource_group="rg-a",
                        subscription_id="sub-a",
                        workspace_name="ws-a",
                        location="eastus2",
                        compute_type="AmlCompute",
                        vm_size="Unknown_VM",
                        vm_priority="Burst",
                        nodes_idle=0,
                        nodes_busy=0,
                        nodes_max=0,
                    ),
                ],
            ),
            (WorkspaceInfo("ws-empty", "rg-b", "sub-b"), []),
        ]
    )

    out = capsys.readouterr().out
    assert "AML Compute Clusters" in out
    assert "Low" in out
    assert "Burst" in out
    assert "ws-emp" in out
    assert "no cluste" in out
    assert "porta" in out
