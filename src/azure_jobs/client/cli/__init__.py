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
        ".skill": ("skill",),
    }
    _ALIASES_MODULE = "._aliases"
    _COMMAND_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("Getting Started", ("init", "run", "dash")),
        ("Jobs", ("job", "exp", "queue", "watch", "list")),
        (
            "Azure Resources",
            ("ws", "ds", "env", "image", "sku", "quota", "sa", "uai"),
        ),
        ("Project", ("template", "config", "code", "skill")),
        ("System", ("auth", "daemon")),
    )

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
        available = set(super().list_commands(ctx))
        ordered = [
            name
            for _title, names in self._COMMAND_GROUPS
            for name in names
            if name in available
        ]
        return ordered + sorted(available - set(ordered))

    def format_commands(
        self,
        ctx: click.Context,
        formatter: click.HelpFormatter,
    ) -> None:
        """Show the flat Click command set in stable, task-oriented groups."""
        self._load_all()
        visible = {
            name: command
            for name in super().list_commands(ctx)
            if (command := self.get_command(ctx, name)) is not None
            and not command.hidden
        }
        rendered: set[str] = set()
        for title, names in self._COMMAND_GROUPS:
            rows = [
                (name, visible[name].get_short_help_str())
                for name in names
                if name in visible
            ]
            if not rows:
                continue
            with formatter.section(title):
                formatter.write_dl(rows)
            rendered.update(name for name, _help in rows)

        other = [
            (name, command.get_short_help_str())
            for name, command in visible.items()
            if name not in rendered
        ]
        if other:
            with formatter.section("Other"):
                formatter.write_dl(sorted(other))

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

    def invoke(self, ctx: click.Context) -> Any:
        """Render a domain failure as a message, never as a traceback.

        One place rather than a ``try`` per command: any command can reach the
        daemon, so any command can fail because it is not running or Azure is
        not signed in, and every one of those must read as instructions.
        ``AJ_DEBUG=1`` still gets the traceback.
        """
        from azure_jobs.shared.errors import AJError

        try:
            return super().invoke(ctx)
        except AJError as exc:
            log.debug("Command failed", exc_info=exc)
            if os.getenv("AJ_DEBUG", "").strip().lower() not in _FALSY_ENV_VALUES | {
                ""
            }:
                raise
            raise click.ClickException(str(exc)) from exc

@click.group(cls=_LazyGroup)
@click.version_option(package_name="azure_jobs")
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    help="Emit machine-readable JSON instead of Rich tables.",
)
def main(json_output: bool, **kwargs: Any) -> None:
    from azure_jobs.client.ui.render import set_output_mode

    set_output_mode("json" if json_output else "rich")
