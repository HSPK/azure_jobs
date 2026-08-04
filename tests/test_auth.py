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
# aj auth login
# ---------------------------------------------------------------------------


class TestAuthLogin:
    def test_delegates_to_az_login(self, runner: CliRunner) -> None:
        with patch("subprocess.run") as mock_run:
            result = runner.invoke(main, ["auth", "login"])
        assert result.exit_code == 0
        mock_run.assert_called_once_with(["az", "login"], check=False)

    def test_az_missing(self, runner: CliRunner) -> None:
        with patch("subprocess.run", side_effect=FileNotFoundError):
            result = runner.invoke(main, ["auth", "login"])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# aj auth logout
# ---------------------------------------------------------------------------


class TestAuthLogout:
    def test_success(self, runner: CliRunner) -> None:
        mock = MagicMock()
        mock.returncode = 0
        with patch("subprocess.run", return_value=mock):
            result = runner.invoke(main, ["auth", "logout"])
        assert result.exit_code == 0
        assert "Logged out" in result.output

    def test_failure(self, runner: CliRunner) -> None:
        mock = MagicMock()
        mock.returncode = 1
        mock.stderr = "Already logged out"
        with patch("subprocess.run", return_value=mock):
            result = runner.invoke(main, ["auth", "logout"])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# aj auth (subcommand help)
# ---------------------------------------------------------------------------


def test_auth_help(runner: CliRunner) -> None:
    result = runner.invoke(main, ["auth", "--help"])
    assert result.exit_code == 0
    assert "status" in result.output
    assert "login" in result.output
    assert "logout" in result.output
