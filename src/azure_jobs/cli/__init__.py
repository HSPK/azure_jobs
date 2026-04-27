from __future__ import annotations

from typing import Any

import click


class _LazyGroup(click.Group):
    """Click group that defers command module imports until needed."""

    # Map every registered command/group name → module to import.
    # Modules that register multiple commands appear multiple times.
    _CMD_TO_MODULE = {
        # run.py
        "run": ".run",
        # templates.py
        "template": ".templates", "tl": ".templates",
        "pull": ".templates", "push": ".templates",
        # jobs.py
        "job": ".jobs", "list": ".jobs",
        "js": ".jobs", "jl": ".jobs", "jc": ".jobs", "jlogs": ".jobs",
        # images.py
        "image": ".images",
        # dashboard.py
        "dash": ".dashboard", "d": ".dashboard",
        # config.py
        "config": ".config",
        # auth.py
        "auth": ".auth",
        # workspace.py
        "ws": ".workspace",
        # experiment.py
        "exp": ".experiment",
        # quota.py
        "quota": ".quota", "ql": ".quota",
        # env.py
        "env": ".env",
        # ds.py
        "ds": ".ds",
        # sku.py
        "sku": ".sku",
        # init.py
        "init": ".init",
    }

    def list_commands(self, ctx: click.Context) -> list[str]:
        # Eagerly import all modules to discover every command
        self._load_all()
        return super().list_commands(ctx)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.BaseCommand | None:
        # Already loaded?
        cmd = super().get_command(ctx, cmd_name)
        if cmd is not None:
            return cmd
        mod_path = self._CMD_TO_MODULE.get(cmd_name)
        if mod_path is None:
            return None
        import importlib
        importlib.import_module(mod_path, package=__name__)
        return super().get_command(ctx, cmd_name)

    def _load_all(self) -> None:
        """Import every command module (for help/list_commands)."""
        import importlib
        for mod_path in set(self._CMD_TO_MODULE.values()):
            importlib.import_module(mod_path, package=__name__)


@click.group(cls=_LazyGroup)
@click.version_option(package_name="azure_jobs")
def main(**kwargs: Any) -> None:
    pass
