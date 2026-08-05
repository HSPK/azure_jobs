"""Account-scoped Azure namespace success and failure branches."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from azure_jobs.server.az_client.arm.compute import ComputesAPI
from azure_jobs.server.az_client.arm.identity import IdentitiesAPI
from azure_jobs.server.az_client.arm.image import ImagesAPI
from azure_jobs.server.az_client.arm.storage import StoragesAPI
from azure_jobs.server.az_client.arm.workspace import WorkspacesAPI
from azure_jobs.shared.errors import ConfigError
from azure_jobs.shared.types.azure import WorkspaceInfo

pytestmark = pytest.mark.azure_unit


def client() -> MagicMock:
    value = MagicMock()
    value.subscription.list.return_value = ["sub"]
    value._graph.query.return_value = []
    return value


def test_workspace_list_and_get() -> None:
    value = client()
    value._graph.query.return_value = [
        {
            "name": "ws",
            "resourceGroup": "rg",
            "subscriptionId": "sub",
            "location": "eastus",
        }
    ]
    api = WorkspacesAPI(value)
    assert api.list()[0].name == "ws"
    assert api.get("ws").resource_group == "rg"


def test_workspace_discovery_failures_and_ambiguity() -> None:
    value = client()
    value.subscription.list.side_effect = requests.ConnectionError("gone")
    assert WorkspacesAPI(value).list() == []

    value = client()
    value._graph.query.side_effect = requests.ConnectionError("gone")
    assert WorkspacesAPI(value).list() == []

    value = client()
    value._graph.query.return_value = [
        {"name": "same", "resourceGroup": "a", "subscriptionId": "one"},
        {"name": "same", "resourceGroup": "b", "subscriptionId": "two"},
    ]
    api = WorkspacesAPI(value)
    with pytest.raises(ConfigError, match="ambiguous"):
        api.get("same")
    with pytest.raises(ConfigError, match="not found"):
        api.get("missing")
    with pytest.raises(ConfigError, match="required"):
        api.get("")


def test_storage_and_identity_rows() -> None:
    value = client()
    value._graph.query.side_effect = [
        [
            {
                "name": "storage",
                "resourceGroup": "rg",
                "subscriptionId": "sub",
                "location": "eastus",
                "kind": "StorageV2",
                "sku": "Standard_LRS",
            }
        ],
        [
            {
                "name": "identity",
                "resourceGroup": "rg",
                "subscriptionId": "sub",
                "location": "eastus",
                "id": "/identity",
                "clientId": "client",
                "principalId": "principal",
            }
        ],
    ]
    assert StoragesAPI(value).list()[0].name == "storage"
    assert IdentitiesAPI(value).list()[0].client_id == "client"


def test_storage_and_identity_return_empty_on_graph_failure() -> None:
    value = client()
    value._graph.query.side_effect = requests.ConnectionError("gone")
    assert StoragesAPI(value).list(["sub"]) == []

    value = client()
    value._graph.query.side_effect = requests.ConnectionError("gone")
    assert IdentitiesAPI(value).list(["sub"]) == []


def test_images_walk_subscriptions_and_tolerate_failure() -> None:
    value = client()
    value.get.side_effect = [
        requests.ConnectionError("first failed"),
        {"value": [{"id": "image", "names": ["torch:latest"]}]},
    ]
    assert ImagesAPI(value).list(["one", "two"])[0]["id"] == "image"

    value.subscription.list.side_effect = RuntimeError("cannot list")
    assert ImagesAPI(value).list() == []


def compute_row(name: str, compute_type: str = "AmlCompute") -> dict:
    return {
        "name": name,
        "location": "eastus",
        "properties": {
            "computeType": compute_type,
            "provisioningState": "Succeeded",
            "properties": {
                "vmSize": "Standard_D2",
                "scaleSettings": {"maxNodeCount": 2},
                "nodeStateCounts": {
                    "idleNodeCount": 1,
                    "runningNodeCount": 1,
                    "preparingNodeCount": 1,
                    "leavingNodeCount": 1,
                },
            },
        },
    }


def test_compute_list_and_get_parse_rows() -> None:
    value = client()
    value.get.side_effect = [
        {"value": [compute_row("cluster")]},
        compute_row("cluster"),
    ]
    api = ComputesAPI(value)
    listed = api.list("sub", "rg", "ws")[0]
    assert (listed.nodes_idle, listed.nodes_busy, listed.nodes_max) == (1, 3, 2)
    assert api.get("sub", "rg", "ws", "cluster").name == "cluster"


def test_compute_list_all_filters_and_reports_callbacks(monkeypatch) -> None:
    value = client()
    workspaces = [
        WorkspaceInfo("good", "rg", "sub"),
        WorkspaceInfo("bad", "rg", "sub"),
    ]
    api = ComputesAPI(value)
    from azure_jobs.shared.types.azure import ComputeInfo

    compute = ComputeInfo(
        "cluster", "rg", "sub", "good", compute_type="AmlCompute"
    )

    def listing(sub, rg, ws):
        if ws == "bad":
            raise RuntimeError("denied")
        return [compute]
    monkeypatch.setattr(api, "list", listing)
    done = []
    failed = []
    result = api.list_all(
        workspaces=workspaces,
        on_workspace_done=lambda name, count: done.append((name, count)),
        on_workspace_failure=lambda ws, exc: failed.append((ws.name, str(exc))),
    )
    assert result[0][0].name == "good"
    assert done == [("good", 1)]
    assert failed == [("bad", "denied")]


def test_compute_owner_validation(monkeypatch) -> None:
    api = ComputesAPI(client())
    one = WorkspaceInfo("one", "rg", "sub")
    two = WorkspaceInfo("two", "rg", "sub")
    from azure_jobs.shared.types.azure import ComputeInfo

    cluster = ComputeInfo("gpu", "rg", "sub", "one", compute_type="AmlCompute")
    with pytest.raises(ConfigError, match="required"):
        api.get_workspace("")

    monkeypatch.setattr(api, "list_all", lambda: [])
    with pytest.raises(ConfigError, match="not found"):
        api.get_workspace("gpu")

    monkeypatch.setattr(
        api,
        "list_all",
        lambda: [(one, [cluster]), (two, [cluster])],
    )
    with pytest.raises(ConfigError, match="multiple"):
        api.get_workspace("gpu")

    monkeypatch.setattr(api, "list_all", lambda: [(one, [cluster])])
    assert api.get_workspace("gpu") == one
