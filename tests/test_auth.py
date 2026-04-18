"""Tests for ``aj auth`` CLI commands."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from azure_jobs.cli import main


@pytest.fixture(autouse=True)
def _mock_find_az():
    """Ensure tests don't depend on ``az`` being installed."""
    with patch("azure_jobs.core.config.find_az", return_value="az"):
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


def _mock_az_account_show(returncode: int = 0, stdout: str = "") -> MagicMock:
    """Create a mock for ``az account show``."""
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout or json.dumps(_ACCOUNT)
    m.stderr = ""
    return m


class TestAuthStatus:
    """Tests for ``aj auth status``."""

    def test_logged_in(self, runner: CliRunner) -> None:
        """Shows user, subscription, and workspace when logged in."""
        ws_cfg = {
            "workspace": {
                "subscription_id": "sub-1",
                "resource_group": "rg-1",
                "workspace_name": "ws-1",
            }
        }
        mock_token = MagicMock()
        mock_token.token = "fake-token"

        with (
            patch("subprocess.run", return_value=_mock_az_account_show()),
            patch("azure.identity.AzureCliCredential") as mock_cred_cls,
            patch(
                "azure_jobs.core.config.read_config",
                return_value=ws_cfg,
            ),
        ):
            mock_cred_cls.return_value.get_token.return_value = mock_token
            result = runner.invoke(main, ["auth", "status"])

        assert result.exit_code == 0
        assert "Logged in" in result.output
        assert "user@example.com" in result.output
        assert "My Subscription" in result.output
        assert "ws-1" in result.output

    def test_not_logged_in(self, runner: CliRunner) -> None:
        """Exits with error when not logged in."""
        mock = _mock_az_account_show(returncode=1, stdout="")
        with patch("subprocess.run", return_value=mock):
            result = runner.invoke(main, ["auth", "status"])
        assert result.exit_code != 0
        assert "Not logged in" in result.output

    def test_az_cli_missing(self, runner: CliRunner) -> None:
        """Exits with error when Azure CLI not found."""
        with patch("subprocess.run", side_effect=FileNotFoundError):
            result = runner.invoke(main, ["auth", "status"])
        assert result.exit_code != 0
        assert "not installed" in result.output

    def test_no_workspace(self, runner: CliRunner) -> None:
        """Shows 'Not configured' when workspace not set."""
        mock_token = MagicMock()
        mock_token.token = "fake-token"

        with (
            patch("subprocess.run", return_value=_mock_az_account_show()),
            patch("azure.identity.AzureCliCredential") as mock_cred_cls,
            patch(
                "azure_jobs.core.config.read_config",
                return_value={},
            ),
        ):
            mock_cred_cls.return_value.get_token.return_value = mock_token
            result = runner.invoke(main, ["auth", "status"])

        assert result.exit_code == 0
        assert "Not configured" in result.output

    def test_sdk_credential_failure(self, runner: CliRunner) -> None:
        """Shows SDK credential error when token fetch fails."""
        with (
            patch("subprocess.run", return_value=_mock_az_account_show()),
            patch("azure.identity.AzureCliCredential") as mock_cred_cls,
            patch(
                "azure_jobs.core.config.read_config",
                return_value={},
            ),
        ):
            mock_cred_cls.return_value.get_token.side_effect = Exception("token expired")
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
