"""Tests for ``aj quota list`` command and quota data model."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from azure_jobs.cli import main
from azure_jobs.core.sku import SLA_TIERS, SeriesQuota, SlaTierQuota, fetch_vc_quotas


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
        assert q.available == 10  # assume all free

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
        assert sq.overall is not None
        assert sq.overall.limit == 100

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

    def test_accelerator_from_family_map(self):
        # _FAMILY_MAP uses keys like "NDAMv4", "NDH100v5" etc.
        sq = SeriesQuota(series="NDH100v5")
        acc = sq.accelerator
        assert acc == "H100"

    def test_accelerator_unknown_series(self):
        sq = SeriesQuota(series="UNKNOWN_SERIES_XYZ")
        assert sq.accelerator == ""  # no match in _FAMILY_MAP

    def test_gpu_memory_from_family_map(self):
        sq = SeriesQuota(series="NDH100v5")
        assert sq.gpu_memory == 80


# ---------------------------------------------------------------------------
# fetch_vc_quotas tests
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
                        {"id": "ND_A100_v4", "slaTier": "Premium", "limit": 64, "used": 32},
                        {"id": "ND_A100_v4", "slaTier": "Standard", "limit": 32, "used": 10},
                        {"id": "ND_H100_v5", "slaTier": "Premium", "limit": 16, "used": 0},
                    ]
                },
                "westus": {
                    "limits": [
                        {"id": "NoProd", "slaTier": "Premium", "limit": 0, "used": 0},
                    ]
                },
            },
        }
    }
}


class TestFetchVcQuotas:
    @patch("azure_jobs.core.config._az_json", return_value=_MOCK_VC_RESPONSE)
    def test_merges_both_sources(self, mock_az):
        results = fetch_vc_quotas("sub", "rg", "myvc")
        series_names = [s.series for s in results]
        assert "ND_A100_v4" in series_names
        assert "ND_H100_v5" in series_names

    @patch("azure_jobs.core.config._az_json", return_value=_MOCK_VC_RESPONSE)
    def test_sla_tiers_populated(self, mock_az):
        results = fetch_vc_quotas("sub", "rg", "myvc")
        a100 = next(s for s in results if s.series == "ND_A100_v4")
        assert "Premium" in a100.tiers
        assert a100.tiers["Premium"].limit == 64
        assert a100.tiers["Premium"].used == 32
        assert "Standard" in a100.tiers
        assert a100.overall is not None
        assert a100.overall.limit == 128

    @patch("azure_jobs.core.config._az_json", return_value=_MOCK_VC_RESPONSE)
    def test_excludes_zero_by_default(self, mock_az):
        results = fetch_vc_quotas("sub", "rg", "myvc")
        series_names = [s.series for s in results]
        assert "NoProd" not in series_names

    @patch("azure_jobs.core.config._az_json", return_value=_MOCK_VC_RESPONSE)
    def test_include_zero(self, mock_az):
        results = fetch_vc_quotas("sub", "rg", "myvc", include_zero=True)
        series_names = [s.series for s in results]
        assert "NoProd" in series_names

    @patch("azure_jobs.core.config._az_json", return_value=None)
    def test_empty_on_none_response(self, mock_az):
        assert fetch_vc_quotas("sub", "rg", "myvc") == []

    @patch("azure_jobs.core.config._az_json", return_value={})
    def test_empty_on_missing_keys(self, mock_az):
        assert fetch_vc_quotas("sub", "rg", "myvc") == []


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


class TestQuotaListCli:
    def setup_method(self):
        self.runner = CliRunner()

    @patch("azure_jobs.cli.quota._resolve_vc_config", return_value={"name": ""})
    def test_sing_no_vc_configured(self, mock_resolve):
        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code != 0
        assert "No virtual cluster" in result.output

    @patch("azure_jobs.core.sku.fetch_vc_quotas", return_value=[])
    @patch(
        "azure_jobs.cli.quota._resolve_vc_config",
        return_value={"name": "myvc", "subscription_id": "s", "resource_group": "rg"},
    )
    def test_sing_no_quotas(self, mock_resolve, mock_fetch):
        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code == 0
        assert "No quotas" in result.output

    @patch("azure_jobs.core.sku.fetch_vc_quotas")
    @patch(
        "azure_jobs.cli.quota._resolve_vc_config",
        return_value={"name": "myvc", "subscription_id": "s", "resource_group": "rg"},
    )
    def test_sing_shows_table_with_tiers(self, mock_resolve, mock_fetch):
        sq1 = SeriesQuota(series="ND_A100_v4")
        sq1.set_tier("Premium", 64, 32)
        sq1.set_tier("Standard", 32, 10)
        sq2 = SeriesQuota(series="ND_H100_v5")
        sq2.set_tier("Premium", 16, 16)
        mock_fetch.return_value = [sq1, sq2]

        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code == 0
        assert "ND_A100_v4" in result.output
        assert "ND_H100_v5" in result.output
        assert "myvc" in result.output
        # SLA tier columns should appear
        assert "Premium" in result.output
        assert "Standard" in result.output

    @patch("azure_jobs.core.sku.fetch_vc_quotas")
    @patch(
        "azure_jobs.cli.quota._resolve_vc_config",
        return_value={"name": "vc1", "subscription_id": "s", "resource_group": "r"},
    )
    def test_full_quota_series(self, mock_resolve, mock_fetch):
        sq = SeriesQuota(series="Full")
        sq.set_tier("Premium", 10, 10)
        mock_fetch.return_value = [sq]
        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code == 0
        assert "Full" in result.output

    def test_ql_alias_works(self):
        with patch("azure_jobs.cli.quota._resolve_vc_config", return_value={"name": ""}):
            result = self.runner.invoke(main, ["ql"])
            assert "No virtual cluster" in result.output

    @patch("azure_jobs.cli.quota._show_aml_quotas")
    def test_aml_flag_routes_to_aml(self, mock_aml):
        self.runner.invoke(main, ["quota", "list", "--aml"])
        mock_aml.assert_called_once_with(False)

    @patch("azure_jobs.cli.quota._show_aml_quotas")
    def test_aml_all_flag(self, mock_aml):
        self.runner.invoke(main, ["quota", "list", "--aml", "--all"])
        mock_aml.assert_called_once_with(True)


class TestResolveVcConfig:
    @patch("azure_jobs.core.config.get_workspace_config", return_value={
        "subscription_id": "ws-sub", "resource_group": "ws-rg",
    })
    @patch("azure_jobs.core.config.get_defaults", return_value={})
    def test_fallback_to_workspace(self, mock_def, mock_ws):
        from azure_jobs.cli.quota import _resolve_vc_config
        result = _resolve_vc_config(vc_override="myvc")
        assert result["name"] == "myvc"
        assert result["subscription_id"] == "ws-sub"
        assert result["resource_group"] == "ws-rg"

    @patch("azure_jobs.core.config.get_workspace_config", return_value={})
    @patch("azure_jobs.core.config.get_defaults", return_value={})
    def test_vc_override_sets_name(self, mock_def, mock_ws):
        from azure_jobs.cli.quota import _resolve_vc_config
        result = _resolve_vc_config(vc_override="override-vc")
        assert result["name"] == "override-vc"
