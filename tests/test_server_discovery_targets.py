"""Hermetic server-side workspace discovery and target resolution tests."""

from __future__ import annotations

import builtins
import sys
import types

import pytest

from azure_jobs.shared.contract import http as H
from azure_jobs.shared.config.models import AJWorkspace
from azure_jobs.shared.errors import AuthError, WorkspaceError


class TestWorkspaceDiscovery:
    def test_configured_workspace_requires_all_fields(self, monkeypatch):
        from azure_jobs.server.discovery import workspace as mod

        monkeypatch.setattr(
            mod,
            "_config_workspace",
            lambda root: AJWorkspace(subscription_id="sub", resource_group="rg"),
        )

        assert mod.configured_workspace() is None

    def test_resolve_workspace_without_name_uses_configured_workspace(self, monkeypatch):
        from azure_jobs.server.discovery import workspace as mod

        expected = AJWorkspace(
            subscription_id="sub",
            resource_group="rg",
            workspace_name="ws",
        )
        monkeypatch.setattr(mod, "_config_workspace", lambda root: expected)

        assert mod.resolve_workspace(root=None) == expected

    def test_resolve_workspace_without_name_errors_when_unconfigured(self, monkeypatch):
        from azure_jobs.server.discovery import workspace as mod

        monkeypatch.setattr(mod, "_config_workspace", lambda root: AJWorkspace())

        with pytest.raises(WorkspaceError, match="No workspace configured"):
            mod.resolve_workspace(root=None)

    def test_resolve_workspace_uses_detected_subscription_when_config_missing(self, monkeypatch):
        from azure_jobs.server.discovery import workspace as mod

        monkeypatch.setattr(mod, "_config_workspace", lambda root: AJWorkspace())
        monkeypatch.setattr(
            mod,
            "detect_subscription",
            lambda: {"subscription_id": "detected-sub"},
        )
        monkeypatch.setattr(
            mod,
            "detect_workspaces",
            lambda sub_id: [
                {
                    "name": "chosen-ws",
                    "resource_group": "rg-picked",
                    "location": "eastus",
                }
            ],
        )

        resolved = mod.resolve_workspace("chosen-ws")

        assert resolved == AJWorkspace(
            subscription_id="detected-sub",
            resource_group="rg-picked",
            workspace_name="chosen-ws",
        )

    def test_resolve_workspace_requires_a_subscription_when_none_can_be_detected(self, monkeypatch):
        from azure_jobs.server.discovery import workspace as mod

        monkeypatch.setattr(mod, "_config_workspace", lambda root: AJWorkspace())
        monkeypatch.setattr(mod, "detect_subscription", lambda: None)

        with pytest.raises(AuthError, match="Cannot detect subscription"):
            mod.resolve_workspace("chosen-ws")


class TestCredentialHealth:
    def test_missing_package_is_reported_without_crashing(self, monkeypatch):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "azure.identity":
                raise ImportError("missing")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        from azure_jobs.server.discovery.credential import credential_health

        assert credential_health() == {
            "ok": False,
            "error": "",
            "missing_package": True,
        }

    def test_token_failure_is_reported_with_exception_details(self, monkeypatch):
        module = types.ModuleType("azure.identity")

        class BrokenCredential:
            def get_token(self, scope):
                raise RuntimeError(f"boom for {scope}")

        module.AzureCliCredential = BrokenCredential
        monkeypatch.setitem(sys.modules, "azure.identity", module)

        from azure_jobs.server.discovery.credential import credential_health

        health = credential_health()

        assert health["ok"] is False
        assert health["missing_package"] is False
        assert "RuntimeError: boom for https://management.azure.com/.default" == health["error"]

    def test_empty_token_is_treated_as_unhealthy(self, monkeypatch):
        module = types.ModuleType("azure.identity")

        class EmptyTokenCredential:
            def get_token(self, scope):
                return types.SimpleNamespace(token="")

        module.AzureCliCredential = EmptyTokenCredential
        monkeypatch.setitem(sys.modules, "azure.identity", module)

        from azure_jobs.server.discovery.credential import credential_health

        assert credential_health() == {
            "ok": False,
            "error": "",
            "missing_package": False,
        }


class TestRequireLogin:
    def test_require_login_reports_missing_az_sign_in(self, monkeypatch):
        from azure_jobs.server.discovery import credential as mod

        monkeypatch.setattr("azure_jobs.server.discovery.az_cli.account_show", lambda: None)

        with pytest.raises(AuthError, match="az login"):
            mod.require_login()

    def test_require_login_reports_missing_identity_package(self, monkeypatch):
        from azure_jobs.server.discovery import credential as mod

        monkeypatch.setattr(
            "azure_jobs.server.discovery.az_cli.account_show",
            lambda: {"user": {"name": "me@example.com"}},
        )
        monkeypatch.setattr(
            mod,
            "credential_health",
            lambda: {"ok": False, "missing_package": True, "error": ""},
        )

        with pytest.raises(AuthError, match="pip install azure-identity"):
            mod.require_login()

    def test_require_login_reports_unhealthy_token_for_signed_in_user(self, monkeypatch):
        from azure_jobs.server.discovery import credential as mod

        monkeypatch.setattr(
            "azure_jobs.server.discovery.az_cli.account_show",
            lambda: {"user": {"name": "me@example.com"}},
        )
        monkeypatch.setattr(
            mod,
            "credential_health",
            lambda: {
                "ok": False,
                "missing_package": False,
                "error": "RuntimeError: token expired",
            },
        )

        with pytest.raises(AuthError, match="me@example.com"):
            mod.require_login()

    def test_require_login_succeeds_when_account_and_token_are_healthy(self, monkeypatch):
        from azure_jobs.server.discovery import credential as mod

        monkeypatch.setattr(
            "azure_jobs.server.discovery.az_cli.account_show",
            lambda: {"user": {"name": "me@example.com"}},
        )
        monkeypatch.setattr(
            mod,
            "credential_health",
            lambda: {"ok": True, "missing_package": False, "error": ""},
        )

        mod.require_login()


class TestTargets:
    def test_catalog_configured_maps_the_configured_workspace(self, monkeypatch):
        from azure_jobs.server.targets import ConfigTargetCatalog

        monkeypatch.setattr(
            "azure_jobs.server.discovery.configured_workspace",
            lambda root=None: AJWorkspace(
                subscription_id="sub",
                resource_group="rg",
                workspace_name="ws",
            ),
        )

        target = ConfigTargetCatalog().configured()

        assert target is not None
        assert target.label == "ws"
        assert target.detail == "rg"
        assert target.metadata["subscription_id"] == "sub"

    def test_catalog_discover_uses_the_active_subscription_when_unspecified(self, monkeypatch):
        from azure_jobs.server.targets import ConfigTargetCatalog

        seen: list[str] = []
        monkeypatch.setattr(
            "azure_jobs.server.discovery.detect_subscription",
            lambda: {"subscription_id": "active-sub"},
        )
        monkeypatch.setattr(
            "azure_jobs.server.discovery.detect_workspaces",
            lambda sub_id: seen.append(sub_id) or [
                {"name": "ws-a", "resource_group": "rg-a", "location": "eastus"}
            ],
        )

        [target] = ConfigTargetCatalog().discover()

        assert seen == ["active-sub"]
        assert target.label == "ws-a"
        assert target.metadata["location"] == "eastus"

    def test_catalog_discover_returns_empty_without_an_active_subscription(self, monkeypatch):
        from azure_jobs.server.targets import ConfigTargetCatalog

        monkeypatch.setattr(
            "azure_jobs.server.discovery.detect_subscription",
            lambda: None,
        )

        assert ConfigTargetCatalog().discover() == ()

    def test_catalog_configured_returns_none_when_workspace_is_unset(self, monkeypatch):
        from azure_jobs.server.targets import ConfigTargetCatalog

        monkeypatch.setattr(
            "azure_jobs.server.discovery.configured_workspace",
            lambda root=None: None,
        )

        assert ConfigTargetCatalog().configured() is None

    def test_catalog_discover_uses_an_explicit_subscription_without_detecting_one(
        self, monkeypatch
    ):
        from azure_jobs.server.targets import ConfigTargetCatalog

        monkeypatch.setattr(
            "azure_jobs.server.discovery.detect_subscription",
            lambda: (_ for _ in ()).throw(AssertionError("should not be called")),
        )
        monkeypatch.setattr(
            "azure_jobs.server.discovery.detect_workspaces",
            lambda sub_id: [{"name": "ws-a", "resource_group": "rg-a"}],
        )

        [target] = ConfigTargetCatalog().discover("sub-explicit")

        assert target.metadata["subscription_id"] == "sub-explicit"

    def test_resolve_named_default_workspace_requires_configuration(self, monkeypatch):
        from azure_jobs.server.targets import resolve_named

        monkeypatch.setattr(
            "azure_jobs.server.targets.ConfigTargetCatalog.configured",
            lambda self: None,
        )

        with pytest.raises(WorkspaceError, match="No workspace configured"):
            resolve_named()

    def test_resolve_named_named_workspace_uses_server_side_resolution(self, monkeypatch):
        from azure_jobs.server.targets import resolve_named

        monkeypatch.setattr(
            "azure_jobs.server.discovery.resolve_workspace",
            lambda name, *, root=None: AJWorkspace(
                subscription_id="sub-2",
                resource_group="rg-2",
                workspace_name=name,
            ),
        )

        target = resolve_named("other-ws")

        assert target.label == "other-ws"
        assert target.metadata == {
            "subscription_id": "sub-2",
            "resource_group": "rg-2",
            "workspace_name": "other-ws",
        }

    def test_resolve_named_default_workspace_returns_the_configured_target(
        self, monkeypatch
    ):
        from azure_jobs.server.targets import ConfigTargetCatalog, resolve_named

        configured = ConfigTargetCatalog._target("sub-1", "rg-1", "ws-1")
        monkeypatch.setattr(
            "azure_jobs.server.targets.ConfigTargetCatalog.configured",
            lambda self: configured,
        )

        assert resolve_named(H.DEFAULT_WORKSPACE) == configured
