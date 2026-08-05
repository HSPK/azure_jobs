"""Workspace REST context, environment and datastore branches."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from azure_jobs.server.az_client.ml.context import (
    RestContext,
    _data_scope_from_url,
)
from azure_jobs.server.az_client.ml.datastores import DatastoresAPI
from azure_jobs.server.az_client.ml.environments import EnvironmentsAPI


class Response:
    def __init__(self, payload=None, status: int = 200) -> None:
        self.payload = payload or {}
        self.status_code = status

    def json(self):
        return self.payload


def context() -> RestContext:
    ctx = RestContext("sub", "rg", "ws")
    ctx.ensure_token = MagicMock()
    ctx.session = MagicMock()
    return ctx


def test_data_scope_defaults_and_china_cloud() -> None:
    assert _data_scope_from_url("") == "https://ml.azure.com/.default"
    assert (
        _data_scope_from_url("https://eastus.api.ml.azure.cn")
        == "https://ml.azure.cn/.default"
    )
    assert (
        _data_scope_from_url("https://custom.example")
        == "https://ml.azure.com/.default"
    )


def test_data_token_uses_cache_then_refreshes(monkeypatch) -> None:
    ctx = context()
    ctx._data_token.update("cached", time.time() + 3600)
    assert ctx.ensure_data_token() == "cached"

    ctx._data_token.expires_on = 0
    ctx.data_plane_base = "https://eastus.api.ml.azure.cn"
    monkeypatch.setattr(ctx, "get_location", lambda: "eastus")
    fetch = MagicMock(return_value=("fresh", time.time() + 3600))
    monkeypatch.setattr("azure_jobs.server.az_client.ml.context.fetch_token", fetch)
    assert ctx.ensure_data_token() == "fresh"
    fetch.assert_called_once_with("https://ml.azure.cn/.default")


def test_workspace_and_location_are_cached() -> None:
    ctx = context()
    ctx.session.get.return_value = Response(
        {
            "location": "eastus",
            "properties": {
                "discoveryUrl": "https://eastus.api.ml.azure.com/discovery/"
            },
        }
    )
    first = ctx.get_workspace()
    assert ctx.get_workspace() is first
    assert ctx.session.get.call_count == 1
    assert ctx.get_location() == "eastus"
    assert ctx.get_location() == "eastus"
    assert ctx.data_plane_base == "https://eastus.api.ml.azure.com"


def test_list_datastore_secrets_quotes_name() -> None:
    ctx = context()
    ctx.session.post.return_value = Response({"sasToken": "token"})
    assert ctx.list_datastore_secrets("a/b") == {"sasToken": "token"}
    assert "a%2Fb/listSecrets" in ctx.session.post.call_args.args[0]


def test_environment_list_versions_get_and_create() -> None:
    ctx = context()
    api = EnvironmentsAPI(ctx)
    ctx.session.get.side_effect = [
        Response(
            {
                "value": [{"name": "env", "properties": {"latestVersion": "2"}}],
                "nextLink": "next",
            }
        ),
        Response({"value": []}),
        Response({"value": [{"name": "3", "properties": {}}]}),
        Response(status=404),
        Response({"name": "version", "properties": {"image": "image"}}),
    ]
    assert api.list()[0].latest_version == "2"
    assert api.list_versions("env")[0].version == "3"
    assert api.get("env", "missing") is None
    assert api.get("env", "version").name == "env"

    ctx.session.put.return_value = Response(
        {"name": "v1", "id": "env-id", "properties": {}}
    )
    created = api.create_or_update("env/name", "v1", "image:tag")
    assert created.name == "env/name"
    assert created.version == "v1"
    body = ctx.session.put.call_args.kwargs["json"]
    assert body["properties"]["image"] == "image:tag"
    assert "env%2Fname" in ctx.session.put.call_args.args[0]


def test_environment_http_error_is_forwarded() -> None:
    ctx = context()
    api = EnvironmentsAPI(ctx)
    ctx.session.get.return_value = Response(status=500)
    with patch(
        "azure_jobs.server.az_client.ml.environments.raise_for_rest_error",
        side_effect=requests.HTTPError("bad"),
    ):
        with pytest.raises(requests.HTTPError, match="bad"):
            api.list()


def test_datastore_list_get_create_and_secret_delegate() -> None:
    ctx = context()
    api = DatastoresAPI(ctx)
    ctx.session.get.side_effect = [
        Response(
            {
                "value": [
                    {
                        "name": "store",
                        "properties": {
                            "datastoreType": "AzureBlob",
                            "accountName": "account",
                        },
                    }
                ]
            }
        ),
        Response(status=404),
        Response({"name": "store", "properties": {"containerName": "container"}}),
    ]
    assert api.list()[0].account_name == "account"
    assert api.get("missing") is None
    assert api.get("store").container_name == "container"

    ctx.session.put.return_value = Response(
        {"name": "new", "properties": {"accountName": "a"}}
    )
    created = api.create_or_update("new", "a", "c", "desc")
    assert created.account_name == "a"
    body = ctx.session.put.call_args.kwargs["json"]["properties"]
    assert body["credentials"]["credentialsType"] == "None"

    ctx.list_datastore_secrets = MagicMock(return_value={"key": "secret"})
    assert api.list_secrets("new") == {"key": "secret"}


def test_get_or_create_reuses_or_wraps_create_failure(monkeypatch) -> None:
    api = DatastoresAPI(context())
    existing = MagicMock()
    monkeypatch.setattr(api, "get", lambda name: existing)
    assert api.get_or_create("store", "a", "c") is existing

    monkeypatch.setattr(api, "get", lambda name: None)

    def fail(**kwargs):
        raise requests.ConnectionError("denied")

    monkeypatch.setattr(api, "create_or_update", fail)
    with pytest.raises(RuntimeError, match="account=a, container=c"):
        api.get_or_create("store", "a", "c")
