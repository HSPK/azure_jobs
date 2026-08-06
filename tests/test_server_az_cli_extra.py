from __future__ import annotations

import subprocess
from unittest.mock import patch

import azure_jobs.server.discovery.az_cli as az_cli


def test_az_json_returns_none_for_missing_binary_spawn_timeout_and_nonzero() -> None:
    with patch.object(az_cli, "find_az", side_effect=FileNotFoundError("no az")):
        assert az_cli.az_json(["account", "show"]) is None

    with (
        patch.object(az_cli, "find_az", return_value="/usr/bin/az"),
        patch.object(
            az_cli.subprocess,
            "run",
            side_effect=OSError("spawn failed"),
        ),
    ):
        assert az_cli.az_json(["account", "show"]) is None

    with (
        patch.object(az_cli, "find_az", return_value="/usr/bin/az"),
        patch.object(
            az_cli.subprocess,
            "run",
            side_effect=subprocess.TimeoutExpired(["az"], timeout=1),
        ),
    ):
        assert az_cli.az_json(["account", "show"]) is None

    with (
        patch.object(az_cli, "find_az", return_value="/usr/bin/az"),
        patch.object(
            az_cli.subprocess,
            "run",
            return_value=subprocess.CompletedProcess(
                ["az"], 2, stdout="", stderr="denied"
            ),
        ),
    ):
        assert az_cli.az_json(["account", "show"]) is None


def test_az_json_account_show_and_workspace_detection_cover_invalid_shapes() -> None:
    with (
        patch.object(az_cli, "find_az", return_value="/usr/bin/az"),
        patch.object(
            az_cli.subprocess,
            "run",
            return_value=subprocess.CompletedProcess(
                ["az"], 0, stdout="{not json", stderr=""
            ),
        ),
    ):
        assert az_cli.az_json(["account", "show"]) is None

    with patch.object(az_cli, "az_json", return_value=["not", "a", "dict"]):
        assert az_cli.account_show() is None
        assert az_cli.detect_subscription() is None

    with patch.object(
        az_cli,
        "az_json",
        return_value={"id": "sub-1", "name": "Sub 1"},
    ):
        assert az_cli.detect_subscription() == {
            "subscription_id": "sub-1",
            "subscription_name": "Sub 1",
        }

    with patch.object(az_cli, "az_json", return_value={"name": "not-a-list"}):
        assert az_cli.detect_workspaces("sub-1") == []


def test_detect_workspaces_maps_rows_to_compact_shape() -> None:
    rows = [
        {"name": "ws-1", "resourceGroup": "rg-1", "location": "eastus"},
        {"name": "ws-2", "resourceGroup": "rg-2"},
    ]

    with patch.object(az_cli, "az_json", return_value=rows) as az_json:
        workspaces = az_cli.detect_workspaces("sub-1")

    assert az_json.call_args.args[0][:3] == [
        "resource",
        "list",
        "--resource-type",
    ]
    assert workspaces == [
        {"name": "ws-1", "resource_group": "rg-1", "location": "eastus"},
        {"name": "ws-2", "resource_group": "rg-2", "location": ""},
    ]
