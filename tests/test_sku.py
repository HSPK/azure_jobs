"""Tests for SKU parsing and resolution (``submit.native.sku``)."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from azure_jobs.cli import main
from azure_jobs.az_client.arm import InstanceTypeInfo, VCInfo
from azure_jobs.errors import SkuResolveError
from azure_jobs.backend.azureml.sku import (
    SkuSpec,
    match_instance_type,
)

# ---------------------------------------------------------------------------
# SkuSpec.parse
# ---------------------------------------------------------------------------


class TestSkuSpecParse:
    def test_cpu_simple(self):
        spec = SkuSpec.parse("1xC1")
        assert spec.num_nodes == 1
        assert spec.gpus_per_node == 1
        assert spec.is_cpu is True
        assert spec.accelerator == "CPU"

    def test_gpu_a100(self):
        spec = SkuSpec.parse("1x80G8-A100-NvLink")
        assert spec.num_nodes == 1
        assert spec.unit_memory == 80
        assert spec.gpus_per_node == 8
        assert spec.is_cpu is False
        assert spec.accelerator == "A100"
        assert spec.nvlink is True

    def test_gpu_40g(self):
        spec = SkuSpec.parse("2x40G4-A100")
        assert spec.num_nodes == 2
        assert spec.unit_memory == 40
        assert spec.gpus_per_node == 4
        assert spec.accelerator == "A100"
        assert spec.nvlink is False

    def test_generic_gpu(self):
        spec = SkuSpec.parse("G1")
        assert spec.num_nodes == 1
        assert spec.gpus_per_node == 1
        assert spec.is_cpu is False
        assert spec.unit_memory is None

    def test_multi_node(self):
        spec = SkuSpec.parse("4xC1")
        assert spec.num_nodes == 4
        assert spec.is_cpu is True

    def test_invalid_returns_defaults(self):
        spec = SkuSpec.parse("????")
        assert spec.num_nodes == 1
        assert spec.gpus_per_node == 1

    def test_placeholders_treated_as_wildcards(self):
        spec = SkuSpec.parse("{nodes}x80G{processes}-A100")
        # Placeholders → counts default; constraints still parsed.
        assert spec.unit_memory == 80
        assert spec.accelerator == "A100"
        assert spec.is_cpu is False

    def test_with_counts_overrides(self):
        spec = SkuSpec.parse("1x80G8-A100").with_counts(nodes=4, gpus_per_node=2)
        assert spec.num_nodes == 4
        assert spec.gpus_per_node == 2
        assert spec.accelerator == "A100"

    def test_with_counts_zero_preserves(self):
        spec = SkuSpec.parse("2x80G8-A100").with_counts(nodes=0, gpus_per_node=0)
        assert spec.num_nodes == 2
        assert spec.gpus_per_node == 8


# ---------------------------------------------------------------------------
# instance_types._row_to_info — catalog row description parsing
# ---------------------------------------------------------------------------


class TestRowToInfo:
    def test_parses_scratch_storage(self):
        from azure_jobs.az_client.arm.instance_types import _row_to_info

        row = {
            "name": "Singularity.ND96isr_H100_v5",
            "instanceTypeSeriesId": "NDH100v5",
            "numberOfGPUs": 8,
            "numberOfCores": 96,
            "memoryGiB": 1900,
            "description": (
                "Accelerator: NVIDIA H100 80GB GPU x 8, NVLink, IB, "
                "vCPU: 96, Memory GiB: 1900, Scratch Storage (SSD) GiB: 28000"
            ),
        }
        info = _row_to_info(row)
        assert info.scratch_gib == 28000
        assert info.accelerator == "H100"
        assert info.gpu_memory_gb == 80
        assert info.nvlink is True

    def test_scratch_missing(self):
        from azure_jobs.az_client.arm.instance_types import _row_to_info

        row = {
            "name": "Singularity.E16ads_v5",
            "instanceTypeSeriesId": "Eadsv5",
            "numberOfGPUs": 0,
            "numberOfCores": 16,
            "memoryGiB": 128,
            "description": "vCPU: 16, Memory GiB: 128",
        }
        info = _row_to_info(row)
        assert info.scratch_gib == 0
        assert info.is_cpu is True


# ---------------------------------------------------------------------------
# _spec_matches — catalog row vs parsed shorthand
# ---------------------------------------------------------------------------


def _gpu_row(
    series: str,
    name: str,
    *,
    accel: str,
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
        accelerator=accel,
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


# ---------------------------------------------------------------------------
# match_instance_type — end-to-end against mocked client
# ---------------------------------------------------------------------------


class _FakeVcQuota:
    def __init__(self, vcs):
        self._vcs = vcs

    def list(self, subscription_ids=None, *, include_zero=False):
        return self._vcs


class _FakeVc:
    def __init__(self, vcs):
        self.quota = _FakeVcQuota(vcs)


class _FakeInstanceTypes:
    def __init__(self, catalog):
        self._catalog = catalog

    def list(self, location, *, subscription_id=""):
        return list(self._catalog)


class FakeArm:
    def __init__(self, vcs=(), catalog=()):
        self.vc = _FakeVc(list(vcs))
        self.instance_types = _FakeInstanceTypes(list(catalog))


_SERIES_HW = {
    "Eadsv5": ("CPU", 0),
    "NDAMv4": ("A100", 80),
    "NDv4": ("A100", 40),
    "NC_A100_v4": ("A100", 80),
    "NDH100v5": ("H100", 80),
    "ND_MI300X": ("MI300X", 192),
}


def _vc(name="vc", series=("Eadsv5", "NDAMv4", "NDv4")):
    from azure_jobs.az_client.arm import SeriesQuota, SlaTierQuota

    vc = VCInfo(
        name=name,
        resource_group="rg",
        subscription_id="sub",
        locations=["westus2"],
        raw={"properties": {"managed": {"locations": ["westus2"]}}},
    )
    for s in series:
        accel, mem = _SERIES_HW.get(s, ("", 0))
        sq = SeriesQuota(series=s, accelerator=accel, gpu_memory=mem)
        sq.user_limit = SlaTierQuota(limit=8, used=0)
        sq.tiers["Premium"] = SlaTierQuota(limit=8, used=0)
        vc.quotas.append(sq)
    return vc


def _full_catalog():
    return [
        _cpu_row("Eadsv5", "E4ads_v5", cores=4),
        _cpu_row("Eadsv5", "E16ads_v5", cores=16),
        _gpu_row(
            "NDAMv4", "ND96amrs_A100_v4", accel="A100", gpus=8, mem=80, nvlink=True
        ),
        _gpu_row("NDv4", "ND96rs_v4", accel="A100", gpus=8, mem=40),
        _gpu_row(
            "NC_A100_v4", "NC96ad_A100_v4", accel="A100", gpus=4, mem=80, nvlink=False
        ),
        _gpu_row(
            "NDH100v5", "ND96r_H100_v5", accel="H100", gpus=8, mem=80, nvlink=True
        ),
    ]


class TestResolveInstanceType:
    def test_cpu_resolution_with_vc(self):
        vc_obj = _vc(series=("Eadsv5", "NC_A100_v4"))
        arm = FakeArm(vcs=[vc_obj], catalog=_full_catalog())
        result = match_instance_type(
            "1xC1",
            vc=vc_obj,
            client=arm,
            tier="Premium",
        )
        assert result.instances and result.instances[0].short_name == "E4ads_v5"

    def test_gpu_a100_80g_nvlink(self):
        vc_obj = _vc(series=("NDAMv4", "NDv4"))
        arm = FakeArm(vcs=[vc_obj], catalog=_full_catalog())
        result = match_instance_type(
            "1x80G8-A100-NvLink",
            vc=vc_obj,
            client=arm,
            tier="Premium",
        )
        assert result.instances and result.instances[0].short_name == "ND96amrs_A100_v4"

    def test_no_match_raises(self):
        vc_obj = _vc(series=("Eadsv5",))
        arm = FakeArm(vcs=[vc_obj], catalog=_full_catalog())
        with pytest.raises(SkuResolveError, match="GPU series"):
            match_instance_type(
                "1x80G8-H100",
                vc=vc_obj,
                client=arm,
                tier="Premium",
            )

    def test_quota_gate_filters_unavailable_series(self):
        """VC has only NDv4 (A100/40G) — 80G request must raise on memory."""
        vc_obj = _vc(series=("NDv4",))
        arm = FakeArm(vcs=[vc_obj], catalog=_full_catalog())
        with pytest.raises(SkuResolveError, match="80GB per-GPU memory"):
            match_instance_type(
                "1x80G8-A100-NvLink",
                vc=vc_obj,
                client=arm,
                tier="Premium",
            )

    def test_nvlink_soft_preference_falls_back(self):
        """If VC only has non-NVLink rows that still satisfy mem+accel, accept them."""
        # NC_A100_v4 has 80G A100 but no NVLink — should still match a NVLink request.
        vc_obj = _vc(series=("NC_A100_v4",))
        arm = FakeArm(vcs=[vc_obj], catalog=_full_catalog())
        result = match_instance_type(
            "1x80G4-A100-NvLink",
            vc=vc_obj,
            client=arm,
            tier="Premium",
        )
        assert result.instances and result.instances[0].short_name == "NC96ad_A100_v4"
        assert result.nvlink_satisfied is False

    def test_nvlink_satisfied_when_vc_has_nvlink(self):
        vc_obj = _vc(series=("NDAMv4",))
        arm = FakeArm(vcs=[vc_obj], catalog=_full_catalog())
        result = match_instance_type(
            "1x80G8-A100-NvLink",
            vc=vc_obj,
            client=arm,
            tier="Premium",
        )
        assert result.instances and result.nvlink_satisfied is True

    def test_nvlink_satisfied_true_when_not_requested(self):
        vc_obj = _vc(series=("NC_A100_v4",))
        arm = FakeArm(vcs=[vc_obj], catalog=_full_catalog())
        result = match_instance_type(
            "1x80G4-A100",
            vc=vc_obj,
            client=arm,
            tier="Premium",
        )
        assert result.instances and result.nvlink_satisfied is True

    def test_pre_fetched_vc_skips_lookup(self):
        """Passing vc= pins the region and gates by quota without any inner fetch."""
        arm = FakeArm(vcs=[], catalog=_full_catalog())
        pre_vc = _vc(series=("NDAMv4",))
        result = match_instance_type(
            "1x80G8-A100-NvLink",
            vc=pre_vc,
            client=arm,
            tier="Premium",
        )
        assert result.instances and result.instances[0].short_name == "ND96amrs_A100_v4"

    def test_vendor_preference_nvidia_over_amd(self):
        """Ambiguous shorthand without accelerator should prefer Nvidia."""

        amd = _gpu_row("ND_MI300X", "ND_MI300X_v5", accel="MI300X", gpus=8, mem=192)
        nvidia = _gpu_row(
            "NDAMv4", "ND96amrs_A100_v4", accel="A100", gpus=8, mem=80, nvlink=True
        )
        vc_obj = _vc(series=("ND_MI300X", "NDAMv4"))
        arm = FakeArm(vcs=[vc_obj], catalog=[amd, nvidia])
        result = match_instance_type(
            "1xG8",
            vc=vc_obj,
            client=arm,
            tier="Premium",
        )
        assert result.instances and all(
            r.accelerator == "A100" for r in result.instances
        )


# ---------------------------------------------------------------------------
# CLI smoke
# ---------------------------------------------------------------------------


class TestSkuCli:
    def test_sku_list_template_option_is_not_supported(self):
        result = CliRunner().invoke(main, ["sku", "list", "-t", "gpu"])
        assert result.exit_code != 0
        assert "No such option" in result.output
