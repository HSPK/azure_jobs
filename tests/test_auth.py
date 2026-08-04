"""Tests for ``aj auth`` CLI commands."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from azure_jobs.client.cli import main


@pytest.fixture(autouse=True)
def _mock_find_az():
    """Ensure tests don't depend on ``az`` being installed."""
    with patch("azure_jobs.shared.utils.fs.find_az", return_value="az"):
        yield


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


# ---------------------------------------------------------------------------
# aj auth status
# ---------------------------------------------------------------------------

_ACCOUNT = {
    "id": "00000000-0000-0000-0000-000000000000",
    "name": "My Subscription",
    "tenantId": "tenant-abc",
    "user": {"name": "user@example.com", "type": "user"},
}


def _account(payload: dict | None = _ACCOUNT):
    """Patch the daemon lookup ``aj auth status`` uses for account details."""
    return patch("azure_jobs.client.discovery.account", return_value=payload)


def _credential(ok: bool = True, error: str = "", missing_package: bool = False):
    """Credential health is the daemon's answer now, not a local token call."""
    return patch(
        "azure_jobs.client.discovery.credential",
        return_value={"ok": ok, "error": error, "missing_package": missing_package},
    )


class TestAuthStatus:
    """Tests for ``aj auth status``."""

    def test_logged_in(self, runner: CliRunner) -> None:
        """Shows user, subscription, and workspace when logged in."""
        from azure_jobs.shared.config import AJConfig, AJWorkspace

        ws_cfg = AJConfig(
            workspace=AJWorkspace(
                subscription_id="sub-1",
                resource_group="rg-1",
                workspace_name="ws-1",
            )
        )
        with (
            _account(),
            _credential(ok=True),
            patch(
                "azure_jobs.shared.config.read_config",
                return_value=ws_cfg,
            ),
        ):
            result = runner.invoke(main, ["auth", "status"])

        assert result.exit_code == 0
        assert "Logged in" in result.output
        assert "user@example.com" in result.output
        assert "My Subscription" in result.output
        assert "ws-1" in result.output

    def test_not_logged_in(self, runner: CliRunner) -> None:
        """Exits with error when not logged in."""
        with _account(None):
            result = runner.invoke(main, ["auth", "status"])
        assert result.exit_code != 0
        assert "Not logged in" in result.output

    def test_az_cli_missing(self, runner: CliRunner) -> None:
        """Exits with error when the daemon cannot reach the Azure CLI."""
        with _account(None):
            result = runner.invoke(main, ["auth", "status"])
        assert result.exit_code != 0
        assert "not installed" in result.output

    def test_no_workspace(self, runner: CliRunner) -> None:
        """Shows 'Not configured' when workspace not set."""
        from azure_jobs.shared.config import AJConfig

        with (
            _account(),
            _credential(ok=True),
            patch(
                "azure_jobs.shared.config.read_config",
                return_value=AJConfig(),
            ),
        ):
            result = runner.invoke(main, ["auth", "status"])

        assert result.exit_code == 0
        assert "Not configured" in result.output

    def test_sdk_credential_failure(self, runner: CliRunner) -> None:
        """Shows the credential error the daemon reported."""
        from azure_jobs.shared.config import AJConfig

        with (
            _account(),
            _credential(ok=False, error="ClientAuthenticationError: token expired"),
            patch(
                "azure_jobs.shared.config.read_config",
                return_value=AJConfig(),
            ),
        ):
            result = runner.invoke(main, ["auth", "status"])

        assert result.exit_code == 0
        assert "token expired" in result.output


# ---------------------------------------------------------------------------
# aj auth surface
# ---------------------------------------------------------------------------


class TestAuthIsReadOnly:
    """Signing in is `az login`; aj does not wrap it.

    The daemon holds its own credential and refuses to start without one, so a
    client-side `aj auth login` would both duplicate `az` and authenticate the
    wrong process.
    """

    @pytest.mark.parametrize("removed", ["login", "logout"])
    def test_the_mutating_commands_are_gone(
        self, runner: CliRunner, removed: str
    ) -> None:
        result = runner.invoke(main, ["auth", removed])
        assert result.exit_code != 0

    def test_help_offers_only_status(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["auth", "--help"])
        assert result.exit_code == 0
        listed = result.output.split("Commands:", 1)[1]
        assert [line.split()[0] for line in listed.splitlines() if line.strip()] == [
            "status"
        ]
