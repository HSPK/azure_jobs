"""``aj config`` — view and set tool-wide configuration."""

from __future__ import annotations

import click

from . import main


@main.group(name="config")
def config_group() -> None:
    """View and set aj configuration."""


@config_group.command(name="timezone")
@click.argument("tz", required=False)
def config_timezone(tz: str | None) -> None:
    """Get or set the display timezone.

    Without arguments, prints the current timezone.
    With a timezone name (e.g. Asia/Shanghai, UTC, US/Eastern),
    saves it to aj_config.json.
    """
    from azure_jobs.core.config import read_config, write_config
    from azure_jobs.utils.ui import console

    if tz is None:
        from azure_jobs.utils.time import get_display_tz_name
        console.print(f"[bold]{get_display_tz_name()}[/bold]")
        return

    # Validate the timezone name
    from azure_jobs.utils.time import _resolve_tz
    try:
        _resolve_tz(tz)
    except Exception:
        console.print(f"[error]✗[/error] Unknown timezone: {tz}")
        raise SystemExit(1)

    cfg = read_config()
    cfg["timezone"] = tz
    write_config(cfg)
    console.print(f"[success]✓[/success] Timezone set to [bold]{tz}[/bold]")


@config_group.command(name="show")
def config_show() -> None:
    """Show all configuration."""
    from azure_jobs.core.config import read_config
    from azure_jobs.utils.ui import console

    import json
    cfg = read_config()
    if cfg:
        console.print_json(json.dumps(cfg, indent=2))
    else:
        console.print("[dim]No configuration set.[/dim]")
