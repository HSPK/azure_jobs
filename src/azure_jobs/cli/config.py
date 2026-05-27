"""aj config — view and set tool-wide configuration."""

from __future__ import annotations

import click

from . import main

@main.group(name="config")
def config_group() -> None:
    """View and set aj configuration."""

@config_group.command(name="timezone")
@click.argument("tz", required=False)
def config_timezone(tz: str | None) -> None:
    """Get or set the display timezone."""
    from azure_jobs.config import read_config, write_config
    from azure_jobs.utils.ui import (
        console,
        emit_json,
        get_output_mode,
        show_command_result,
    )

    if tz is None:
        from azure_jobs.utils.time import get_display_tz_name

        current = get_display_tz_name()
        if get_output_mode() == "json":
            emit_json({"kind": "config_value", "key": "timezone", "value": current})
            return
        console.print(f"[bold]{current}[/bold]")
        return

    from azure_jobs.utils.time import resolve_tz

    try:
        resolve_tz(tz)
    except Exception:
        if get_output_mode() == "json":
            show_command_result(
                "config.timezone",
                status="failed",
                message=f"Unknown timezone: {tz}",
            )
        else:
            console.print(f"[error]✗[/error] Unknown timezone: {tz}")
        raise SystemExit(1)

    cfg = read_config()
    cfg.timezone = tz
    write_config(cfg)
    if get_output_mode() != "json":
        console.print(f"[success]✓[/success] Timezone set to [bold]{tz}[/bold]")
    show_command_result(
        "config.timezone", status="ok", message=f"Timezone set to {tz}", value=tz
    )

@config_group.command(name="experiment")
@click.argument("name", required=False)
def config_experiment(name: str | None) -> None:
    """Get or set the experiment name."""
    from azure_jobs.config import get_experiment, read_config, write_config
    from azure_jobs.utils.ui import (
        console,
        emit_json,
        get_output_mode,
        show_command_result,
    )

    if name is None:
        exp = get_experiment()
        if get_output_mode() == "json":
            emit_json({"kind": "config_value", "key": "experiment", "value": exp or ""})
            return
        if exp:
            console.print(f"[bold]{exp}[/bold]")
        else:
            console.print(
                "[dim]No experiment set. Run [bold]aj config experiment <name>[/bold] or submit a job.[/dim]"
            )
        return

    cfg = read_config()
    cfg.experiment = name.strip()
    write_config(cfg)
    if get_output_mode() != "json":
        console.print(
            f"[success]✓[/success] Experiment set to [bold]{name.strip()}[/bold]"
        )
    show_command_result(
        "config.experiment",
        status="ok",
        message=f"Experiment set to {name.strip()}",
        value=name.strip(),
    )

@config_group.command(name="show")
def config_show() -> None:
    """Show all configuration."""
    import json

    from azure_jobs.config import read_config
    from azure_jobs.utils.ui import console, emit_json, get_output_mode

    cfg = read_config().to_dict()
    if get_output_mode() == "json":
        emit_json({"kind": "config", "config": cfg})
        return
    if cfg:
        console.print_json(json.dumps(cfg, indent=2))
    else:
        console.print("[dim]No configuration set.[/dim]")
