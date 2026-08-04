"""Which target a command acts on.

Resolving this reads local config and the local ``az`` login, so both the CLI
(deciding what to ask for) and the daemon (answering ``targets.*``) need it.
"""

from __future__ import annotations

from azure_jobs.shared.contract.models import Target


class ConfigTargetCatalog:
    """Targets from local config plus Azure discovery."""

    def configured(self) -> Target | None:
        from azure_jobs.shared.config import read_config

        configured = read_config().workspace
        if not (
            configured.subscription_id
            and configured.resource_group
            and configured.workspace_name
        ):
            return None
        return self._target(
            configured.subscription_id,
            configured.resource_group,
            configured.workspace_name,
        )

    def discover(self) -> tuple[Target, ...]:
        from azure_jobs.shared.config import detect_subscription, detect_workspaces

        subscription = detect_subscription()
        if not subscription:
            return ()
        subscription_id = subscription["subscription_id"]
        return tuple(
            self._target(
                subscription_id,
                value["resource_group"],
                value["name"],
            )
            for value in detect_workspaces(subscription_id)
        )

    @staticmethod
    def _target(
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
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
            },
        )


ConfigWorkspaceCatalog = ConfigTargetCatalog

__all__ = ["ConfigTargetCatalog", "ConfigWorkspaceCatalog"]
