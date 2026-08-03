from __future__ import annotations

import logging
import os
from typing import Any

import click

_FALSY_ENV_VALUES = frozenset({"0", "false", "no", "off"})

def _configure_debug_logging() -> None:
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

log = logging.getLogger(__name__)

class _LazyGroup(click.Group):

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
        ".uai": ("uai",),
        ".sa": ("sa",),
        ".daemon": ("daemon",),
        ".queue": ("queue",),
        ".watch": ("watch",),
    }
    _ALIASES_MODULE = "._aliases"

    _cmd_map_cache: dict[str, str] | None = None

    @classmethod
    def _cmd_to_module(cls) -> dict[str, str]:
        if cls._cmd_map_cache is not None:
            return cls._cmd_map_cache

        flat: dict[str, str] = {
            cmd: mod
            for mod, cmds in cls._MODULE_TO_COMMANDS.items()
            for cmd in cmds
        }
        try:
            from . import _aliases as _aliases_mod

            for name in _aliases_mod.ALIASES:
                flat[name] = cls._ALIASES_MODULE
        except ImportError:
            log.debug("Failed to import cli._aliases", exc_info=True)
        cls._cmd_map_cache = flat
        return flat

    def list_commands(self, ctx: click.Context) -> list[str]:
        self._load_all()
        return super().list_commands(ctx)

    def get_command(
        self, ctx: click.Context, cmd_name: str
    ) -> click.BaseCommand | None:
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
        import importlib

        for mod_path in set(self._MODULE_TO_COMMANDS) | {self._ALIASES_MODULE}:
            importlib.import_module(mod_path, package=__name__)

@click.group(cls=_LazyGroup)
@click.version_option(package_name="azure_jobs")
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    help="Emit machine-readable JSON instead of Rich tables.",
)
def main(json_output: bool, **kwargs: Any) -> None:
    if json_output:
        from azure_jobs.utils.ui.render import set_output_mode

        set_output_mode("json")
