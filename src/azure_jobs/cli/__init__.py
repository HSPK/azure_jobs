from __future__ import annotations

import logging
import os
from typing import Any

import click


_FALSY_ENV_VALUES = frozenset({"0", "false", "no", "off"})


def _configure_debug_logging() -> None:
    """Enable stderr DEBUG logging when ``AJ_DEBUG`` is set.

    All modules log via ``logging.getLogger(__name__)`` with ``log.debug(…)``
    on swallowed exceptions and other diagnostic events. Set ``AJ_DEBUG=1``
    (or any truthy value) before running ``aj`` to surface them on stderr.
    """
    val = os.getenv("AJ_DEBUG", "").strip().lower()
    if val and val not in _FALSY_ENV_VALUES:
        if not logging.root.hasHandlers():
            logging.basicConfig(
                level=logging.DEBUG,
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            )
        else:
            logging.root.setLevel(logging.DEBUG)


_configure_debug_logging()


class _LazyGroup(click.Group):
    """Click group that defers command module imports until needed.

    Adding a new ``aj`` command:

    1. Create ``cli/<module>.py`` with ``@main.command`` / ``@main.group``.
    2. Add one entry below in ``_MODULE_TO_COMMANDS``.

    Adding a hidden alias: edit ``cli/_aliases.py`` only — its module
    registration below already covers any name listed in ``_aliases.ALIASES``.
    """

    # One row per module; commands listed in load-order. Adding/removing a
    # command edits exactly one line here.
    _MODULE_TO_COMMANDS: dict[str, tuple[str, ...]] = {
        ".run": ("run",),
        ".templates": ("template",),
        ".jobs": ("job", "list"),
        ".images": ("image",),
        ".dashboard": ("dash",),
        ".config": ("config",),
        ".auth": ("auth",),
        ".workspace": ("ws",),
        ".experiment": ("exp",),
        ".quota": ("quota",),
        ".env": ("env",),
        ".ds": ("ds",),
        ".sku": ("sku",),
        ".init": ("init",),
        ".code": ("code",),
        # Hidden aliases — populated from cli/_aliases.py:ALIASES at first use.
        "._aliases": (),
    }

    @classmethod
    def _cmd_to_module(cls) -> dict[str, str]:
        # Flatten _MODULE_TO_COMMANDS + dynamically import aliases registry.
        flat: dict[str, str] = {
            cmd: mod
            for mod, cmds in cls._MODULE_TO_COMMANDS.items()
            for cmd in cmds
        }
        try:
            from . import _aliases as _aliases_mod

            for name in _aliases_mod.ALIASES:
                flat[name] = "._aliases"
        except ImportError:
            pass
        return flat

    def list_commands(self, ctx: click.Context) -> list[str]:
        # Eagerly import all modules to discover every command
        self._load_all()
        return super().list_commands(ctx)

    def get_command(
        self, ctx: click.Context, cmd_name: str
    ) -> click.BaseCommand | None:
        # Already loaded?
        cmd = super().get_command(ctx, cmd_name)
        if cmd is not None:
            return cmd
        mod_path = self._cmd_to_module().get(cmd_name)
        if mod_path is None:
            return None
        import importlib

        importlib.import_module(mod_path, package=__name__)
        return super().get_command(ctx, cmd_name)

    def _load_all(self) -> None:
        """Import every command module (for help/list_commands)."""
        import importlib

        for mod_path in set(self._MODULE_TO_COMMANDS) | {"._aliases"}:
            importlib.import_module(mod_path, package=__name__)


@click.group(cls=_LazyGroup)
@click.version_option(package_name="azure_jobs")
def main(**kwargs: Any) -> None:
    pass
