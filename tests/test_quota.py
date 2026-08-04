"""Tests for ``aj quota list`` command and quota data model."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from azure_jobs.client.cli import main
from azure_jobs.shared.types.vm_gpu import vm_sku_label as _vm_sku_label
from azure_jobs.shared.errors import ConfigError
from azure_jobs.server.az_client.arm import (
    SeriesQuota,
    SlaTierQuota,
    VCInfo,
    parse_managed_quotas,
)
from azure_jobs.client.ui.quota_tables import (
    _fmt_nodes,
    _portal_compute_url,
)


def _quota_payload(series: str, name: str) -> dict:
    """Build a minimal raw VC payload exposing one limit row."""
    return {
        "properties": {
            "managed": {
                "quotas": {
                    "r1": {
                        "limits": [
                            {
                                "id": series,
                                "slaTier": "Premium",
                                "limit": 1,
                                "used": 0,
                                "name": name,
                            }
                        ]
                    }
                }
            }
        }
    }


# ---------------------------------------------------------------------------
# SlaTierQuota unit tests
# ---------------------------------------------------------------------------


class TestSlaTierQuota:
    def test_available_with_used(self):
        q = SlaTierQuota(limit=10, used=3)
        assert q.available == 7

    def test_available_when_full(self):
        q = SlaTierQuota(limit=5, used=5)
        assert q.available == 0

    def test_available_clamps_to_zero(self):
        q = SlaTierQuota(limit=5, used=8)
        assert q.available == 0

    def test_available_unknown_used(self):
        q = SlaTierQuota(limit=10, used=None)
        assert q.available == 10

    def test_bool_true(self):
        assert bool(SlaTierQuota(limit=1))

    def test_bool_false(self):
        assert not bool(SlaTierQuota(limit=0))


# ---------------------------------------------------------------------------
# SeriesQuota unit tests
# ---------------------------------------------------------------------------


class TestSeriesQuota:
    def test_set_tier_premium(self):
        sq = SeriesQuota(series="ND_A100_v4")
        sq.set_tier("Premium", 64, 32)
        assert sq.tiers["Premium"].limit == 64
        assert sq.tiers["Premium"].used == 32

    def test_set_tier_none_is_overall(self):
        sq = SeriesQuota(series="ND_A100_v4")
        sq.set_tier(None, 100, 50)
        assert sq.user_limit is not None
        assert sq.user_limit.limit == 100

    def test_set_tier_unknown_falls_back_to_basic(self):
        sq = SeriesQuota(series="X")
        sq.set_tier("WeirdTier", 10, 5)
        assert "Basic" in sq.tiers
        assert sq.tiers["Basic"].limit == 10

    def test_has_any_quota_true(self):
        sq = SeriesQuota(series="X")
        sq.set_tier("Premium", 10, 0)
        assert sq.has_any_quota()

    def test_has_any_quota_false(self):
        sq = SeriesQuota(series="X")
        assert not sq.has_any_quota()

    def test_has_any_quota_overall_only(self):
        sq = SeriesQuota(series="X")
        sq.set_tier(None, 10, 5)
        assert sq.has_any_quota()

    def test_accelerator_from_display_name(self):
        sq = parse_managed_quotas(
            _quota_payload(
                "NDH100v5", "Singularity NDH100v5 Series NVIDIA H100 80GB GPUs"
            )
        )[0]
        assert sq.accelerator == "H100"
        assert sq.gpu_memory == 80

    def test_accelerator_only_in_name_uses_default_memory(self):
        # "Family vCPUs" carries no GB but A100 → 80GB by default.
        sq = parse_managed_quotas(
            _quota_payload(
                "NC_A100_v4",
                "Singularity NC_A100_v4 Family vCPUs containing NVIDIA A100",
            )
        )[0]
        assert sq.accelerator == "A100"
        assert sq.gpu_memory == 80

    def test_accelerator_inferred_from_series_name(self):
        # No friendly name at all → series-name heuristic + default memory.
        sq = parse_managed_quotas(_quota_payload("ND_H100_custom_v6", ""))[0]
        assert sq.accelerator == "H100"
        assert sq.gpu_memory == 80

    def test_accelerator_cpu_series(self):
        sq = parse_managed_quotas(
            _quota_payload("Eadsv5", "Singularity Eadsv5 Family vCPUs")
        )[0]
        assert sq.accelerator == "CPU"
        assert sq.gpu_memory == 0

    def test_accelerator_unknown_series(self):
        sq = parse_managed_quotas(_quota_payload("UNKNOWN_SERIES_XYZ", ""))[0]
        assert sq.accelerator == ""
        assert sq.gpu_memory == 0

    def test_mi300x_default_memory(self):
        sq = parse_managed_quotas(
            _quota_payload("ND_MI300X_v5", "Singularity ND_MI300X_v5 Family vCPUs")
        )[0]
        assert sq.accelerator == "MI300X"
        assert sq.gpu_memory == 192

    def test_mi200_default_memory(self):
        sq = parse_managed_quotas(
            _quota_payload("ND_MI200_v4", "Singularity ND_MI200_v4 Family vCPUs")
        )[0]
        assert sq.accelerator == "MI200"
        assert sq.gpu_memory == 64


# ---------------------------------------------------------------------------
# AzureARMClient.list_virtual_clusters tests
# ---------------------------------------------------------------------------


_MOCK_VC_RESPONSE = {
    "properties": {
        "managed": {
            "defaultGroupPolicyOverallQuotas": {
                "limits": [
                    {"id": "ND_A100_v4", "slaTier": None, "limit": 128, "used": 64},
                ]
            },
            "quotas": {
                "eastus": {
                    "limits": [
                        {
                            "id": "ND_A100_v4",
                            "slaTier": "Premium",
                            "limit": 64,
                            "used": 32,
                        },
                        {
                            "id": "ND_H100_v5",
                            "slaTier": "Premium",
                            "limit": 16,
                            "used": 0,
                        },
                    ]
                },
            },
        }
    }
}


def _make_arm_client():
    """Construct a real ``AzureARMClient`` with namespaces stubbed."""
    from azure_jobs.server.az_client import AzureARMClient

    client = AzureARMClient()
    client.subscriptions.list = MagicMock()
    client.graph.query = MagicMock()
    return client


class TestListVirtualClusters:
    def test_lists_vcs_from_resource_graph(self):
        client = _make_arm_client()
        client.subscriptions.list.return_value = ["sub-1", "sub-2"]
        client.graph.query.return_value = [
            {"name": "vc1", "resourceGroup": "rg1", "subscriptionId": "sub-1"},
            {"name": "vc2", "resourceGroup": "rg2", "subscriptionId": "sub-2"},
        ]
        vcs = client.vc.list()
        assert [v.name for v in vcs] == ["vc1", "vc2"]

    def test_uses_provided_subscriptions(self):
        client = _make_arm_client()
        client.graph.query.return_value = [
            {"name": "vc1", "resourceGroup": "rg1", "subscriptionId": "sub-a"},
        ]
        vcs = client.vc.list(subscription_ids=["sub-a"])
        assert len(vcs) == 1
        client.subscriptions.list.assert_not_called()

    def test_skips_when_no_subscriptions(self):
        client = _make_arm_client()
        client.subscriptions.list.return_value = []
        assert client.vc.list() == []

    def test_handles_exception_gracefully(self):
        import requests

        client = _make_arm_client()
        client.subscriptions.list.side_effect = requests.ConnectionError("auth fail")
        assert client.vc.list() == []

    def test_empty_on_no_data(self):
        client = _make_arm_client()
        client.subscriptions.list.return_value = ["sub-1"]
        client.graph.query.return_value = []
        assert client.vc.list() == []

    def test_quota_list_parses_payload(self):
        client = _make_arm_client()
        row = dict(_MOCK_VC_RESPONSE)
        row.update(name="vc1", resourceGroup="rg1", subscriptionId="sub-1")
        client.subscriptions.list.return_value = ["sub-1"]
        client.graph.query.return_value = [row]
        vcs = client.vc.quota.list()
        series = sorted(sq.series for sq in vcs[0].quotas)
        assert "ND_A100_v4" in series
        assert "ND_H100_v5" in series

    def test_resolves_vc_by_name(self):
        client = _make_arm_client()
        client.subscriptions.list.return_value = ["sub-1"]
        client.graph.query.return_value = [
            {"name": "vc1", "resourceGroup": "rg1", "subscriptionId": "sub-1"},
        ]
        vc = client.vc.get("vc1")
        assert vc.name == "vc1"
        assert vc.resource_group == "rg1"
        assert vc.subscription_id == "sub-1"

    def test_resolve_vc_ambiguous_requires_filter(self):
        client = _make_arm_client()
        client.subscriptions.list.return_value = ["sub-1", "sub-2"]
        client.graph.query.return_value = [
            {"name": "vc1", "resourceGroup": "rg1", "subscriptionId": "sub-1"},
            {"name": "vc1", "resourceGroup": "rg2", "subscriptionId": "sub-2"},
        ]

        with pytest.raises(ConfigError, match="ambiguous"):
            client.vc.get("vc1")

        vc = client.vc.get("vc1", subscription_id="sub-2")
        assert vc.resource_group == "rg2"


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


class TestQuotaListCli:
    @pytest.fixture(autouse=True)
    def _daemon(self, cli_daemon):
        """Commands reach Azure only through the daemon now."""
        yield cli_daemon

    def setup_method(self):
        self.runner = CliRunner()
        self._arm_patcher = patch("azure_jobs.server.az_client.AzureARMClient")
        self.arm_cls = self._arm_patcher.start()
        self.arm = self.arm_cls.return_value
        # ``load_vcs_with_quotas`` now calls ``arm.vc.quota.list(...)`` which
        # internally re-parses ``vc.raw`` — short-circuit it back to
        # ``arm.vc.list`` so tests can supply pre-built ``VCInfo`` rows
        # (with ``quotas`` already populated) directly.
        self.arm.vc.quota.list.side_effect = lambda **kw: self.arm.vc.list()

    def teardown_method(self):
        self._arm_patcher.stop()

    def test_sing_no_vcs_found(self):
        self.arm.vc.list.return_value = []
        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code != 0
        assert "No Singularity" in result.output

    def test_sing_vc_with_no_quotas(self):
        self.arm.vc.list.return_value = [
            VCInfo(name="myvc", resource_group="rg", subscription_id="s"),
        ]
        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code == 0
        assert "myvc" in result.output

    def test_sing_template_option_is_not_supported(self):
        result = self.runner.invoke(main, ["quota", "list", "-t", "gpu"])
        assert result.exit_code != 0
        assert "No such option" in result.output

    def test_sing_shows_grouped_table(self):
        sq1 = SeriesQuota(series="NDH100v5", accelerator="H100", gpu_memory=80)
        sq1.set_tier("Premium", 64, 32)
        sq2 = SeriesQuota(series="NDAMv4", accelerator="A100", gpu_memory=80)
        sq2.set_tier("Premium", 16, 16)
        self.arm.vc.list.return_value = [
            VCInfo(name="vc1", resource_group="rg1", subscription_id="s", quotas=[sq1]),
            VCInfo(name="vc2", resource_group="rg2", subscription_id="s", quotas=[sq2]),
        ]

        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code == 0
        assert "vc1" in result.output
        assert "vc2" in result.output
        assert "rg1" not in result.output
        assert "Resource Group" not in result.output
        assert "NDH100v5" in result.output
        assert "NDAMv4" in result.output
        assert "H100" in result.output
        assert "A100" in result.output

        full_result = self.runner.invoke(main, ["quota", "list", "--full"])
        assert full_result.exit_code == 0
        assert "rg1" in full_result.output
        assert "rg2" in full_result.output
        assert "Resource Group" in full_result.output

    def test_ql_alias_works(self):
        self.arm.vc.list.return_value = []
        result = self.runner.invoke(main, ["ql"])
        assert "No Singularity" in result.output

    @patch("azure_jobs.client.cli.quota._show_aml_quotas")
    def test_aml_flag_routes_to_aml(self, mock_aml):
        self.runner.invoke(main, ["quota", "list", "--aml"])
        mock_aml.assert_called_once_with(False)

    @patch("azure_jobs.client.cli.quota._show_aml_quotas")
    def test_aml_all_flag(self, mock_aml):
        self.runner.invoke(main, ["quota", "list", "--aml", "--all"])
        mock_aml.assert_called_once_with(True)


# ---------------------------------------------------------------------------
# AML helper tests
# ---------------------------------------------------------------------------


class TestAmlHelpers:
    def test_vm_sku_label_gpu(self):
        assert _vm_sku_label("Standard_ND96amsr_A100_v4") == "80G8-A100"

    def test_vm_sku_label_h100(self):
        assert _vm_sku_label("Standard_ND96isr_H100_v5") == "80G8-H100"

    def test_vm_sku_label_cpu(self):
        assert _vm_sku_label("Standard_DS3_v2") == "CPU"

    def test_vm_sku_label_unknown(self):
        assert _vm_sku_label("UnknownVm_XYZ") == ""

    def test_portal_compute_url(self):
        url = _portal_compute_url("sub1", "rg1", "ws1", "gpu-cluster")
        assert "ml.azure.com/compute/gpu-cluster/details" in url
        assert "sub1" in url
        assert "rg1" in url
        assert "ws1" in url

    def test_fmt_nodes_all_zero_activity(self):
        s = _fmt_nodes(0, 0, 16, low_priority=True)
        assert "dim" in s  # fully dimmed

    def test_fmt_nodes_busy_highlighted(self):
        s = _fmt_nodes(0, 4, 8, low_priority=False)
        assert "cyan" in s  # busy highlighted
        assert "dim" in s  # idle dimmed

    def test_fmt_nodes_max_zero(self):
        assert _fmt_nodes(0, 0, 0, low_priority=False) == "[dim]0/0[/dim]"
