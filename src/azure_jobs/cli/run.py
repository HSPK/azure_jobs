from __future__ import annotations

import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import click
import yaml

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.core.conf import read_conf
from azure_jobs.core.config import get_defaults, get_workspace_config, save_defaults
from azure_jobs.core.record import SubmissionRecord, log_record
from azure_jobs.utils.ui import console, dim, error, info, show_submission_preview, success


def validate_config(conf: dict, template_fp: Path) -> None:
    """Validate that a merged config has the required structure."""
    if "jobs" not in conf:
        raise click.ClickException(
            f"Template {template_fp} is missing required 'jobs' key."
        )
    if not isinstance(conf["jobs"], list) or len(conf["jobs"]) == 0:
        raise click.ClickException(
            f"Template {template_fp}: 'jobs' must be a non-empty list."
        )
    if "sku" not in conf["jobs"][0]:
        raise click.ClickException(
            f"Template {template_fp}: first job is missing required 'sku' key."
        )


def resolve_name(command: str, sid: str) -> str:
    """Build a job name from environment, cwd, command, and session id."""
    name = os.getenv("AJ_NAME", None)
    if name is None:
        name = Path.cwd().name
        cmd_path = Path(command.split(" ")[-1])
        if cmd_path.exists():
            name += f"_{cmd_path.stem}"
    return f"{name}_{sid}"


def resolve_sku(
    sku_template: str | dict, nodes: int, processes: int
) -> str:
    """Resolve a SKU template (string or range-dict) into a concrete SKU string."""
    if isinstance(sku_template, str):
        return sku_template.format(nodes=nodes, processes=processes)

    if isinstance(sku_template, dict):
        for key, value in sku_template.items():
            key_str = str(key)
            if "-" in key_str:
                min_s, max_s = key_str.split("-", 1)
                min_val = int(min_s)
                max_val = int(max_s) if max_s != "+" else float("inf")
                if min_val <= nodes <= max_val:
                    return value.format(nodes=nodes, processes=processes)
            elif key_str.endswith("+"):
                if nodes >= int(key_str[:-1]):
                    return value.format(nodes=nodes, processes=processes)
            else:
                if int(key_str) == nodes:
                    return value.format(nodes=nodes, processes=processes)
        raise click.ClickException(
            f"No matching SKU template found for {nodes} nodes in {sku_template}"
        )

    raise click.ClickException(
        f"Unsupported SKU template type: {type(sku_template).__name__}. "
        "Only str and dict are supported."
    )


def build_command_list(
    conf_commands: list[str],
    user_command: str,
    user_args: tuple[str, ...],
    *,
    nodes: int,
    processes: int,
    name: str,
    sid: str,
    template: str,
) -> list[str]:
    """Assemble the full command list: env exports + template commands + user command."""
    cmd_list: list[str] = [
        f"export AJ_NODES={nodes}",
        f"export AJ_PROCESSES={processes * nodes}",
        f"export AJ_NAME={name}",
        f"export AJ_ID={sid}",
        f"export AJ_TEMPLATE={template}",
        f"export AJ_SUBMIT_TIMESTAMP_UTC={datetime.now(timezone.utc).isoformat()}",
        "export PATH=$$HOME/.local/bin:$$PATH",
    ]
    cmd_list.extend(conf_commands)

    if Path(user_command).is_file():
        if user_command.endswith(".sh"):
            cmd = f"bash {user_command} {' '.join(user_args)}".strip()
        elif user_command.endswith(".py"):
            cmd = f"uv run {user_command} {' '.join(user_args)}".strip()
        else:
            raise click.ClickException(
                f"Unsupported script type: {user_command}. Only .sh and .py are supported."
            )
    else:
        cmd = f"{user_command} {' '.join(user_args)}".strip()

    cmd_list.append(cmd)
    return cmd_list


@main.command(
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
        "allow_interspersed_args": False,
    }
)
@click.option(
    "-t",
    "--template",
    help="Template environment to execute the command",
    default=None,
)
@click.option("-n", "--nodes", default=None, help="Number of nodes")
@click.option("-p", "--processes", default=None, help="Number of processes")
@click.option(
    "-d", "--dry-run", is_flag=True, help="Dry run the command without executing"
)
@click.option("-y", "--yes", is_flag=True, hidden=True, help="(Deprecated, no-op)")
@click.option("-L", "--run-local", is_flag=True, help="Run the command locally")
@click.argument("command", nargs=1)
@click.argument("args", nargs=-1)
def run(
    command: str,
    args: tuple[str, ...],
    template: str | None,
    nodes: str | None,
    processes: str | None,
    dry_run: bool,
    run_local: bool,
    yes: bool,
) -> None:
    defaults = get_defaults()

    if template is None:
        template = defaults.get("template")
    if template is None:
        raise click.ClickException(
            "No template specified. Use -t <template> or set a default with aj config."
        )

    template_fp = const.AJ_TEMPLATE_HOME / f"{template}.yaml"
    if not template_fp.exists():
        raise click.ClickException(
            f"Template {template} does not exist at {template_fp}"
        )

    conf = read_conf(template_fp)
    if not conf:
        raise click.ClickException(f"Empty configuration file: {template_fp}")

    sid = uuid.uuid4().hex[:8]
    name = resolve_name(command, sid)

    extra = conf.get("_extra", {})
    nodes_int = int(
        nodes
        or extra.get("nodes")
        or defaults.get("nodes")
        or 1
    )
    processes_int = int(
        processes
        or extra.get("processes")
        or defaults.get("processes")
        or 1
    )
    conf.pop("_extra", None)

    # Remember this template as the new default
    save_defaults(template=template)

    validate_config(conf, template_fp)

    conf["description"] = name
    conf["jobs"][0]["name"] = name
    conf["jobs"][0]["sku"] = resolve_sku(
        conf["jobs"][0]["sku"], nodes_int, processes_int
    )

    conf["jobs"][0]["command"] = build_command_list(
        conf["jobs"][0].get("command", []),
        command,
        args,
        nodes=nodes_int,
        processes=processes_int,
        name=name,
        sid=sid,
        template=template,
    )
    final_cmd = conf["jobs"][0]["command"][-1]

    if run_local:
        info(f"Running locally: {final_cmd}")
        subprocess.run(final_cmd, shell=True)
        return

    submission_fp = const.AJ_SUBMISSION_HOME / f"{sid}.yaml"
    submission_fp.parent.mkdir(parents=True, exist_ok=True)
    with open(submission_fp, "w") as f:
        yaml.dump(conf, f, default_flow_style=False)

    show_submission_preview(
        job_id=sid,
        job_name=name,
        template=template,
        sku=conf["jobs"][0]["sku"],
        nodes=nodes_int,
        processes=processes_int,
        command=final_cmd,
        submission_file=str(submission_fp),
        dry_run=dry_run,
    )

    if dry_run:
        dim(f"Config written to {submission_fp}")
        return

    # ── Submit to Azure ML ──────────────────────────────────────────
    from azure_jobs.core.submit import SubmitResult, build_request_from_config, submit

    workspace = get_workspace_config()
    request = build_request_from_config(conf, name=name, workspace=workspace)
    # Override with resolved values
    request.nodes = nodes_int
    request.processes_per_node = processes_int
    request.command = conf["jobs"][0]["command"]

    rec = SubmissionRecord(
        id=sid,
        template=template,
        nodes=nodes_int,
        processes=processes_int,
        portal="azure",
        created_at=datetime.now(timezone.utc).isoformat(),
        status="submitted",
        command=command,
        args=list(args),
    )

    current_step = ""

    def on_status(step: str, detail: str) -> None:
        nonlocal current_step
        current_step = detail

    try:
        with console.status("", spinner="dots") as status_ctx:
            def _on_status(step: str, detail: str) -> None:
                on_status(step, detail)
                status_ctx.update(f"[bold cyan]{detail}[/bold cyan]")

            result: SubmitResult = submit(request, on_status=_on_status)

        if result.status == "failed":
            rec.status = "failed"
            rec.note = result.error
            error(f"Submission failed: {result.error}")
            raise SystemExit(1)

        rec.status = "submitted"
        rec.azure_name = result.azure_name
        if result.portal_url:
            rec.portal = result.portal_url
        success(f"Job [bold]{name}[/bold] submitted")
        if result.azure_name != name:
            dim(f"Azure ID: {result.azure_name}")
        if result.portal_url:
            from azure_jobs.utils.ui import short_portal_url

            dim(f"Portal: {short_portal_url(result.portal_url)}")
    except SystemExit:
        raise
    except Exception as exc:
        rec.status = "failed"
        rec.note = str(exc)
        raise click.ClickException(f"Submission failed: {exc}")
    finally:
        log_record(rec)
