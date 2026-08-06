"""Azure bearer tokens are sent, not redacted placeholders."""

from unittest.mock import patch

from azure_jobs.server.az_client.auth import AuthSession


def test_auth_session_sets_complete_bearer_header() -> None:
    session = AuthSession()
    with patch(
        "azure_jobs.server.az_client.auth.fetch_token",
        return_value=("token-value", 10**12),
    ):
        assert session.ensure_token() == "token-value"
    assert session.session.headers["Authorization"] == "Bearer token-value"
    session.close()
