"""Tests for core/sku.py — SKU parsing and resolution."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from azure_jobs.core.sku import InstanceType, SkuSpec, resolve_instance_type


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


class TestInstanceTypeFromApi:
    def test_gpu_instance(self):
        entry = {
            "name": "Singularity.Standard_ND40rs_v2",
            "description": "NVIDIA V100 32GB GPU x 8 NVLink",
        }
        it = InstanceType.from_api(entry)
        assert it is not None
        assert it.name == "Standard_ND40rs_v2"
        assert it.num_gpus == 8
        assert it.gpu_memory_gb == 32
        assert it.nvlink is True

    def test_cpu_instance(self):
        entry = {
            "name": "Singularity.Standard_D2_v3",
            "description": "vCPU: 2, Memory GiB: 8",
        }
        it = InstanceType.from_api(entry)
        assert it is not None
        assert it.name == "Standard_D2_v3"
        assert it.is_cpu is True

    def test_n1_suffix_filtered(self):
        entry = {"name": "Singularity.ND40rs_v2-n1", "description": "foo"}
        it = InstanceType.from_api(entry)
        assert it is None


class TestResolveInstanceType:
    def test_direct_instance_name(self):
        """Instance type names with underscores pass through directly."""
        result = resolve_instance_type("Standard_ND40rs_v2")
        assert result == ["Standard_ND40rs_v2"]

    def test_direct_name_with_node_prefix(self):
        result = resolve_instance_type("2xStandard_ND40rs_v2")
        assert result == ["Standard_ND40rs_v2"]

    @patch("azure_jobs.core.sku._fetch_instance_types")
    def test_cpu_resolution(self, mock_fetch):
        mock_fetch.return_value = [
            InstanceType(name="Standard_D2_v3", is_cpu=True),
            InstanceType(name="Standard_ND40rs_v2", num_gpus=8, gpu_memory_gb=32),
        ]
        result = resolve_instance_type("1xC1")
        assert "Standard_D2_v3" in result
        assert "Standard_ND40rs_v2" not in result

    @patch("azure_jobs.core.sku._fetch_instance_types")
    def test_gpu_a100_resolution(self, mock_fetch):
        mock_fetch.return_value = [
            InstanceType(name="Standard_D2_v3", is_cpu=True),
            InstanceType(
                name="Standard_NC24ads_A100_v4", num_gpus=4,
                gpu_memory_gb=80, gpu_model="A100",
            ),
            InstanceType(
                name="Standard_ND96amsr_A100_v4", num_gpus=8,
                gpu_memory_gb=80, gpu_model="A100", nvlink=True,
            ),
        ]
        result = resolve_instance_type("1x80G8-A100-NvLink")
        assert result[0] == "Standard_ND96amsr_A100_v4"

    @patch("azure_jobs.core.sku._fetch_instance_types")
    def test_empty_when_no_match(self, mock_fetch):
        mock_fetch.return_value = [
            InstanceType(name="Standard_D2_v3", is_cpu=True),
        ]
        result = resolve_instance_type("1x80G8-H100")
        assert result == []
