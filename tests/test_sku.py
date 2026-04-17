"""Tests for core/sku.py — SKU parsing and resolution."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from azure_jobs.core.sku import SkuSpec, _match_family, _FAMILY_MAP, resolve_instance_type


class TestSkuSpecParse:
    def test_cpu_simple(self):
        spec = SkuSpec.parse("1xC1")
        assert spec.num_nodes == 1
        assert spec.num_units == 1
        assert spec.is_cpu is True
        assert spec.accelerators == ["CPU"]

    def test_gpu_a100(self):
        spec = SkuSpec.parse("1x80G8-A100-NvLink")
        assert spec.num_nodes == 1
        assert spec.unit_memory == 80
        assert spec.num_units == 8
        assert spec.is_cpu is False
        assert "A100" in spec.accelerators
        assert spec.nvlink is True

    def test_gpu_40g(self):
        spec = SkuSpec.parse("2x40G4-A100")
        assert spec.num_nodes == 2
        assert spec.unit_memory == 40
        assert spec.num_units == 4
        assert "A100" in spec.accelerators
        assert spec.nvlink is False

    def test_generic_gpu(self):
        spec = SkuSpec.parse("G1")
        assert spec.num_nodes == 1
        assert spec.num_units == 1
        assert spec.is_cpu is False
        assert spec.unit_memory is None

    def test_multi_node(self):
        spec = SkuSpec.parse("4xC1")
        assert spec.num_nodes == 4
        assert spec.is_cpu is True

    def test_invalid_returns_defaults(self):
        spec = SkuSpec.parse("????")
        assert spec.num_nodes == 1
        assert spec.num_units == 1


class TestMatchFamily:
    def test_cpu_matches_eadsv5(self):
        spec = SkuSpec.parse("1xC1")
        result = _match_family(spec, "Eadsv5", _FAMILY_MAP["Eadsv5"])
        assert result is not None
        assert "ads_v5" in result

    def test_cpu_rejects_gpu_family(self):
        spec = SkuSpec.parse("1xC1")
        result = _match_family(spec, "NC_A100_v4", _FAMILY_MAP["NC_A100_v4"])
        assert result is None

    def test_gpu_a100_80g_nvlink_matches_ndamv4(self):
        spec = SkuSpec.parse("1x80G8-A100-NvLink")
        result = _match_family(spec, "NDAMv4", _FAMILY_MAP["NDAMv4"])
        assert result == "ND96amsr_A100_v4"

    def test_gpu_a100_40g_matches_ndv4(self):
        spec = SkuSpec.parse("1x40G8-A100")
        result = _match_family(spec, "NDv4", _FAMILY_MAP["NDv4"])
        assert result == "ND96asr_v4"

    def test_gpu_rejects_cpu_family(self):
        spec = SkuSpec.parse("1x80G8-A100")
        result = _match_family(spec, "Eadsv5", _FAMILY_MAP["Eadsv5"])
        assert result is None

    def test_nvlink_rejects_non_nvlink(self):
        spec = SkuSpec.parse("1x80G8-A100-NvLink")
        result = _match_family(spec, "NC_A100_v4", _FAMILY_MAP["NC_A100_v4"])
        assert result is None


class TestResolveInstanceType:
    def test_direct_instance_name(self):
        """Instance type names with underscores pass through directly."""
        result = resolve_instance_type("E16ads_v5")
        assert result == ["E16ads_v5"]

    def test_direct_name_with_node_prefix(self):
        result = resolve_instance_type("2xStandard_ND40rs_v2")
        assert result == ["Standard_ND40rs_v2"]

    @patch("azure_jobs.core.sku._fetch_vc_families")
    def test_cpu_resolution_with_vc(self, mock_fetch):
        mock_fetch.return_value = ["Eadsv5", "NC_A100_v4"]
        result = resolve_instance_type(
            "1xC1",
            vc_subscription_id="sub", vc_resource_group="rg", vc_name="vc",
        )
        assert len(result) == 1
        assert "ads_v5" in result[0]  # E16ads_v5

    @patch("azure_jobs.core.sku._fetch_vc_families")
    def test_gpu_a100_80g_nvlink(self, mock_fetch):
        mock_fetch.return_value = ["NDAMv4", "NDv4"]
        result = resolve_instance_type(
            "1x80G8-A100-NvLink",
            vc_subscription_id="sub", vc_resource_group="rg", vc_name="vc",
        )
        assert result[0] == "ND96amsr_A100_v4"

    @patch("azure_jobs.core.sku._fetch_vc_families")
    def test_no_match_returns_empty(self, mock_fetch):
        mock_fetch.return_value = ["Eadsv5"]  # Only CPU available
        result = resolve_instance_type(
            "1x80G8-H100",
            vc_subscription_id="sub", vc_resource_group="rg", vc_name="vc",
        )
        assert result == []

    def test_no_vc_info_uses_all_families(self):
        """Without VC info, all known families are tried."""
        result = resolve_instance_type("1xC1")
        assert len(result) > 0  # Should find CPU instances from all families
