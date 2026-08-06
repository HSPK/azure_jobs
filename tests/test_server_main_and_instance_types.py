"""Hermetic daemon entry-point and instance-type catalog tests."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import azure_jobs.server.main as main_mod
from azure_jobs.server.az_client.arm.instance_types import (
    InstanceTypesAPI,
    _parse_description,
    _row_to_info,
)
from azure_jobs.shared.errors import AuthError


class TestInstanceTypeParsing:
    def test_parse_description_extracts_defaults_and_transport_capabilities(self):
        assert _parse_description("plain CPU node", 0) == ("CPU", 0, False, False)
        assert _parse_description(
            "Accelerator: NVIDIA H100 GPU x 8, NVLink, IB",
            8,
        ) == ("H100", 80, True, True)
        assert _parse_description(
            "Accelerator: AMD MI300X 192 GB GPU x 8",
            8,
        ) == ("MI300X", 192, False, False)

    def test_row_to_info_tolerates_sparse_or_malformed_rows(self):
        info = _row_to_info({})

        assert info.name == ""
        assert info.series_id == ""
        assert info.is_cpu is True
        assert info.description == ""
        assert info.scratch_gib == 0

    def test_list_uses_first_subscription_filters_variants_and_parses_rows(self):
        client = SimpleNamespace(
            subscription=SimpleNamespace(list=MagicMock(return_value=["sub-a", "sub-b"])),
            get=MagicMock(
                return_value={
                    "value": [
                        {
                            "name": "Singularity.ND96_H100_v5",
                            "instanceTypeSeriesId": "NDH100v5",
                            "numberOfGPUs": 8,
                            "description": "Accelerator: NVIDIA H100 80GB GPU x 8, NVLink, IB",
                        },
                        {
                            "name": "Singularity.ND96_H100_v5-n2",
                            "instanceTypeSeriesId": "NDH100v5",
                            "numberOfGPUs": 8,
                            "description": "variant",
                        },
                    ]
                }
            ),
        )

        result = InstanceTypesAPI(client).list("eastus")

        client.subscription.list.assert_called_once_with()
        assert client.get.call_args.args[0].endswith(
            "/locations/eastus/instanceTypeSeries?api-version=2021-03-01-preview"
        )
        assert [row.name for row in result] == ["Singularity.ND96_H100_v5"]
        assert result[0].accelerator == "H100"
        assert result[0].nvlink is True
        assert result[0].infiniband is True

    def test_strict_list_propagates_discovery_errors_and_rejects_no_subscriptions(self):
        client = SimpleNamespace(
            subscription=SimpleNamespace(list=MagicMock(return_value=[])),
            get=MagicMock(),
        )
        api = InstanceTypesAPI(client)

        with pytest.raises(AuthError, match="No enabled Azure subscriptions"):
            api.list("eastus", strict=True)

        client.subscription.list.return_value = ["sub-a"]
        client.get.side_effect = OSError("catalog down")
        with pytest.raises(OSError, match="catalog down"):
            api.list("eastus", strict=True)

    def test_list_returns_empty_for_missing_location_malformed_response_or_network_errors(self):
        client = SimpleNamespace(
            subscription=SimpleNamespace(list=MagicMock(return_value=["sub-a"])),
            get=MagicMock(return_value={"unexpected": []}),
        )
        api = InstanceTypesAPI(client)

        assert api.list("") == []
        assert api.list("eastus") == []

        client.subscription.list.side_effect = OSError("offline")
        assert api.list("eastus") == []

        client.subscription.list.side_effect = None
        client.get.side_effect = OSError("down")
        assert api.list("eastus") == []


class TestDaemonMain:
    def test_main_skips_login_check_binds_and_serves(self):
        daemon = MagicMock()

        with (
            patch.object(main_mod, "Daemon", return_value=daemon) as daemon_cls,
            patch.object(main_mod.signal, "signal") as register_signal,
            patch("azure_jobs.server.discovery.require_login") as require_login,
        ):
            rc = main_mod.main(
                [
                    "--socket",
                    "daemon.sock",
                    "--idle-timeout",
                    "12.5",
                    "--watch-interval",
                    "3.0",
                    "--shutdown-when-idle",
                    "1.0",
                    "--skip-login-check",
                ]
            )

        assert rc == 0
        require_login.assert_not_called()
        daemon_cls.assert_called_once_with(
            Path("daemon.sock"),
            idle_timeout=12.5,
            watch_interval=3.0,
            shutdown_when_idle=1.0,
        )
        assert register_signal.call_count == 2
        daemon.bind.assert_called_once_with()
        daemon.serve_forever.assert_called_once_with()

    def test_main_returns_auth_exit_code_when_login_check_fails(self, capsys):
        with (
            patch("azure_jobs.server.discovery.require_login", side_effect=AuthError("sign in first")),
            patch.object(main_mod, "Daemon") as daemon_cls,
        ):
            rc = main_mod.main(["--socket", "daemon.sock"])

        assert rc == main_mod.AUTH_EXIT_CODE
        assert "ajd: sign in first" in capsys.readouterr().err
        daemon_cls.assert_not_called()

    def test_main_reports_bind_failures(self, capsys):
        daemon = MagicMock()
        daemon.bind.side_effect = OSError("address already in use")

        with (
            patch.object(main_mod, "Daemon", return_value=daemon),
            patch.object(main_mod.signal, "signal"),
        ):
            rc = main_mod.main(["--socket", "daemon.sock", "--skip-login-check"])

        assert rc == 1
        assert "failed to bind daemon.sock" in capsys.readouterr().err
        daemon.serve_forever.assert_not_called()
