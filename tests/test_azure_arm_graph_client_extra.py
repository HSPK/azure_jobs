from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import azure_jobs.server.az_client.arm as arm_mod
from azure_jobs.server.az_client.arm.graph import ResourceGraphAPI
from azure_jobs.server.az_client.arm.vc import (
    VCQuotaAPI,
    VirtualClustersAPI,
    _infer_accelerator_from_series,
    _parse_quota_name,
    parse_managed_quotas,
)
from azure_jobs.shared.errors import ConfigError
from azure_jobs.shared.types.azure import VCInfo

pytestmark = pytest.mark.azure_unit


class _Response:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def test_resource_graph_query_posts_expected_body() -> None:
    client = MagicMock()
    client.post.return_value = {"data": [{"name": "row"}]}

    rows = ResourceGraphAPI(client).query("resources | take 1", ["sub-1"])

    assert rows == [{"name": "row"}]
    client.post.assert_called_once_with(
        "https://management.azure.com/providers/Microsoft.ResourceGraph"
        "/resources?api-version=2021-03-01",
        {
            "query": "resources | take 1",
            "subscriptions": ["sub-1"],
        },
    )


def test_azure_client_get_and_post_authenticate_and_return_json() -> None:
    client = arm_mod.AzureClient()
    client.ensure_token = MagicMock()
    client.session = MagicMock()
    get_response = _Response({"name": "get"})
    post_response = _Response({"name": "post"})
    client.session.get.return_value = get_response
    client.session.post.return_value = post_response

    with patch.object(arm_mod, "raise_for_rest_error") as raise_error:
        assert client.get("https://example.test/get", timeout=7) == {"name": "get"}
        assert client.post(
            "https://example.test/post",
            {"value": 1},
            timeout=9,
        ) == {"name": "post"}

    assert client.ensure_token.call_count == 2
    client.session.get.assert_called_once_with("https://example.test/get", timeout=7)
    client.session.post.assert_called_once_with(
        "https://example.test/post",
        json={"value": 1},
        timeout=9,
    )
    assert raise_error.call_args_list[0].args == (get_response,)
    assert raise_error.call_args_list[1].args == (post_response,)


def test_private_quota_helpers_cover_empty_cpu_and_series_fallbacks() -> None:
    assert _parse_quota_name("") == ("", 0)
    assert _parse_quota_name("Singularity Dv5 Family vCPUs") == ("CPU", 0)
    assert _infer_accelerator_from_series("Eadsv5") == "CPU"


def test_parse_managed_quotas_skips_blank_ids_and_include_zero_controls_filtering() -> None:
    raw = {
        "properties": {
            "managed": {
                "defaultGroupPolicyOverallQuotas": {
                    "limits": [
                        {"id": "", "limit": 10},
                        {
                            "id": "NC_ZERO",
                            "slaTier": "Premium",
                            "limit": 0,
                            "used": 0,
                            "name": "Singularity NC_ZERO Family vCPUs containing NVIDIA A100",
                        },
                    ]
                }
            }
        }
    }

    assert parse_managed_quotas(raw) == []
    [quota] = parse_managed_quotas(raw, include_zero=True)
    assert quota.series == "NC_ZERO"
    assert quota.accelerator == "A100"


def test_vc_quota_get_by_name_covers_required_missing_ambiguous_and_success() -> None:
    api = VCQuotaAPI(SimpleNamespace(vc=SimpleNamespace(list=lambda **_: [])))

    try:
        api.get_by_name("")
    except ConfigError as exc:
        assert "required" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected ConfigError for an empty VC name")

    with patch.object(api, "list", return_value=[]):
        with pytest.raises(ConfigError, match="was not found"):
            api.get_by_name("missing")

    ambiguous = [
        VCInfo(name="vc", resource_group="rg-a", subscription_id="sub-a"),
        VCInfo(name="vc", resource_group="rg-b", subscription_id="sub-b"),
    ]
    with patch.object(api, "list", return_value=ambiguous):
        with pytest.raises(ConfigError, match="ambiguous"):
            api.get_by_name("vc")

    expected = VCInfo(name="vc", resource_group="rg", subscription_id="sub")
    with patch.object(api, "list", return_value=[expected]):
        assert api.get_by_name("vc") == expected


def test_virtual_clusters_list_with_raw_extracts_locations_and_skips_blank_names() -> None:
    client = MagicMock()
    client.subscription.list.return_value = ["sub-1"]
    client._graph.query.return_value = [
        {"name": "", "resourceGroup": "skip", "subscriptionId": "sub-1"},
        {
            "name": "vc-1",
            "resourceGroup": "rg-1",
            "subscriptionId": "sub-1",
            "properties": {"managed": {"locations": ["eastus", "", 7]}},
        },
        {
            "name": "vc-2",
            "resourceGroup": "rg-2",
            "subscriptionId": "sub-1",
            "properties": {"managed": {"locations": "not-a-list"}},
        },
    ]

    values = VirtualClustersAPI(client).list(with_raw=True)

    assert [(value.name, value.locations) for value in values] == [
        ("vc-1", ["eastus", "7"]),
        ("vc-2", []),
    ]
    query = client._graph.query.call_args.args[0]
    assert "project name" not in query


def test_virtual_clusters_get_formats_not_found_suffix_and_ambiguity_more() -> None:
    api = VirtualClustersAPI(MagicMock())

    with pytest.raises(ConfigError, match="target.name"):
        api.get("")

    with patch.object(api, "list", return_value=[]) as listing:
        with pytest.raises(ConfigError, match=r"'missing'.*subscription_id=sub-1, resource_group=rg-1"):
            api.get("missing", subscription_id="sub-1", resource_group="rg-1")
    assert listing.call_args.kwargs["subscription_ids"] == ["sub-1"]

    matches = [
        VCInfo(name="dup", resource_group=f"rg-{i}", subscription_id=f"sub-{i}")
        for i in range(6)
    ]
    with patch.object(api, "list", return_value=matches):
        with pytest.raises(ConfigError, match=r"ambiguous: .* …\.") as raised:
            api.get("dup")
    assert "Set target.subscription_id or target.resource_group" in str(raised.value)
