"""Resolving which workspace a request acts on.

Server-side only: resolution shells out to ``az`` for subscription and
workspace discovery, and the client must never run Azure commands — it asks
for a workspace *by name* and the daemon works out the rest.
"""

from __future__ import annotations

from pathlib import Path

from azure_jobs.shared.contract.models import Target


class ConfigTargetCatalog:
    """Targets from local config plus Azure discovery.

    Scoped to a project *root* because one daemon serves many checkouts: the
    configured workspace is a property of the project that asked, not of the
    daemon process.
    """

    def __init__(self, root: Path | None = None) -> None:
        self._root = root

    def configured(self) -> Target | None:
        from azure_jobs.server.discovery import configured_workspace

        configured = configured_workspace(self._root)
        if configured is None:
            return None
        return self._target(
            configured.subscription_id,
            configured.resource_group,
            configured.workspace_name,
        )

    def discover(self, subscription_id: str = "") -> tuple[Target, ...]:
        """Workspaces in *subscription_id*, or in the active subscription."""
        from azure_jobs.server.discovery import detect_subscription, detect_workspaces

        if not subscription_id:
            subscription = detect_subscription()
            if not subscription:
                return ()
            subscription_id = subscription["subscription_id"]
        return tuple(
            self._target(
                subscription_id,
                value["resource_group"],
                value["name"],
                location=value.get("location", ""),
            )
            for value in detect_workspaces(subscription_id)
        )

    @staticmethod
    def _target(
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
        location: str = "",
    ) -> Target:
        return Target.create(
            backend="azureml",
            native_id=f"{subscription_id}/{resource_group}/{workspace_name}",
            label=workspace_name,
            detail=resource_group,
            metadata={
                "subscription_id": subscription_id,
                "resource_group": resource_group,
                "workspace_name": workspace_name,
                "location": location,
            },
        )


ConfigWorkspaceCatalog = ConfigTargetCatalog

__all__ = ["ConfigTargetCatalog", "ConfigWorkspaceCatalog", "resolve_named"]


def resolve_named(name: str = "", root: Path | None = None) -> Target:
    """Resolve a workspace *name* (empty means the one *root* is configured for).

    ``resolve_workspace`` is preferred over plain discovery because it honours
    the configured subscription and resource-group fallback, which matters when
    the same workspace name exists in more than one subscription.
    """
    from azure_jobs.server.discovery import resolve_workspace
    from azure_jobs.shared.contract import routes as R
    from azure_jobs.shared.errors import WorkspaceError

    if not name or name == R.DEFAULT_WORKSPACE:
        target = ConfigTargetCatalog(root).configured()
        if target is not None:
            return target
        raise WorkspaceError(
            "No workspace configured. Run 'aj init' or 'aj ws set' first."
        )
    workspace = resolve_workspace(name, root=root)
    return Target.create(
        backend="azureml",
        native_id=(
            f"{workspace.subscription_id}/{workspace.resource_group}/"
            f"{workspace.workspace_name}"
        ),
        label=workspace.workspace_name,
        detail=workspace.resource_group,
        metadata={
            "subscription_id": workspace.subscription_id,
            "resource_group": workspace.resource_group,
            "workspace_name": workspace.workspace_name,
        },
    )
