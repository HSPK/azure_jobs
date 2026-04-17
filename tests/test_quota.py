"""Tests for ``aj quota list`` command and QuotaInfo dataclass."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from azure_jobs.cli import main
from azure_jobs.core.sku import QuotaInfo, fetch_vc_quotas


# ---------------------------------------------------------------------------
# QuotaInfo unit tests
# ---------------------------------------------------------------------------


class TestQuotaInfo:
    def test_available_positive(self):
        q = QuotaInfo(family="Ax", limit=10, used=3)
        assert q.available == 7

    def test_available_zero_when_full(self):
        q = QuotaInfo(family="Ax", limit=5, used=5)
        assert q.available == 0

    def test_available_clamps_to_zero(self):
        q = QuotaInfo(family="Ax", limit=5, used=8)
        assert q.available == 0

    def test_description_gpu_family(self):
        q = QuotaInfo(family="ND_A100_v4", gpu_model="A100", gpu_memory=80, gpu_count=8)
        desc = q.description
        assert "A100" in desc
        assert "80GB" in desc

    def test_description_cpu_family(self):
        q = QuotaInfo(family="Standard_D", is_cpu=True)
        assert q.description == "CPU"

    def test_description_unknown_family(self):
        q = QuotaInfo(family="Unknown_Family_XYZ")
        # Falls back to GPU with default
        assert "GPU" in q.description


# ---------------------------------------------------------------------------
# fetch_vc_quotas tests
# ---------------------------------------------------------------------------


_MOCK_VC_RESPONSE = {
    "properties": {
        "managed": {
            "defaultGroupPolicyOverallQuotas": {
                "limits": [
                    {"id": "ND_A100_v4", "limit": 64, "currentValue": 32},
                    {"id": "ND_H100_v5", "limit": 16, "currentValue": 0},
                    {"id": "NoProd", "limit": 0, "currentValue": 0},
                ]
            }
        }
    }
}


class TestFetchVcQuotas:
    @patch("azure_jobs.core.config._az_json", return_value=_MOCK_VC_RESPONSE)
    def test_returns_nonzero_by_default(self, mock_az):
        results = fetch_vc_quotas("sub", "rg", "myvc")
        assert len(results) == 2
        assert results[0].family == "ND_A100_v4"
        assert results[0].limit == 64
        assert results[0].used == 32
        assert results[0].available == 32
        assert results[1].family == "ND_H100_v5"

    @patch("azure_jobs.core.config._az_json", return_value=_MOCK_VC_RESPONSE)
    def test_include_zero(self, mock_az):
        results = fetch_vc_quotas("sub", "rg", "myvc", include_zero=True)
        assert len(results) == 3
        assert results[2].family == "NoProd"
        assert results[2].limit == 0

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

    @patch(
        "azure_jobs.core.sku.fetch_vc_quotas",
        return_value=[
            QuotaInfo(family="ND_A100_v4", limit=64, used=32, gpu_model="A100", gpu_memory=80),
            QuotaInfo(family="ND_H100_v5", limit=16, used=16, gpu_model="H100", gpu_memory=80),
        ],
    )
    @patch(
        "azure_jobs.cli.quota._resolve_vc_config",
        return_value={"name": "myvc", "subscription_id": "s", "resource_group": "rg"},
    )
    def test_sing_shows_table(self, mock_resolve, mock_fetch):
        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code == 0
        assert "ND_A100_v4" in result.output
        assert "ND_H100_v5" in result.output
        assert "myvc" in result.output

    @patch(
        "azure_jobs.core.sku.fetch_vc_quotas",
        return_value=[
            QuotaInfo(family="Full", limit=10, used=10),
        ],
    )
    @patch(
        "azure_jobs.cli.quota._resolve_vc_config",
        return_value={"name": "vc1", "subscription_id": "s", "resource_group": "r"},
    )
    def test_full_quota_shows_red(self, mock_resolve, mock_fetch):
        result = self.runner.invoke(main, ["quota", "list"])
        assert result.exit_code == 0
        assert "Full" in result.output

    def test_ql_alias_works(self):
        """``aj ql`` should invoke the same logic as ``aj quota list``."""
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
    """Test VC config resolution from template and workspace config."""

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
