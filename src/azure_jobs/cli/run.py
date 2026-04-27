from __future__ import annotations

import os
import subprocess
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import click
import yaml

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.core.conf import read_conf
from azure_jobs.core.config import get_defaults, get_workspace_config, save_defaults
from azure_jobs.core.record import SubmissionRecord, log_record
from azure_jobs.utils.ui import (
    console,
    dim,
    error,
    info,
    show_submission_preview,
    success,
)


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
    # SKU is required for AML/Singularity but not Volcano
    service = conf.get("target", {}).get("service", "aml")
    if service != "volcano" and "sku" not in conf["jobs"][0]:
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


def resolve_sku(sku_template: str | dict, nodes: int, processes: int) -> str:
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
    service: str = "aml",
) -> list[str]:
    """Assemble the full command list: env exports + template commands + user command."""
    cmd_list: list[str] = [
        # SSH setup (works for both aj and amlt)
        '[ -f ".azure_jobs/scripts/copy_ssh.sh" ] && bash .azure_jobs/scripts/copy_ssh.sh && [ -f /tmp/.aj_ssh_env ] && source /tmp/.aj_ssh_env',
        f"export AJ_NODES={nodes}",
        f"export AJ_PROCESSES={processes * nodes}",
        f"export AJ_NAME={name}",
        f"export AJ_ID={sid}",
        f"export AJ_TEMPLATE={template}",
        f"export AJ_SUBMIT_TIMESTAMP_UTC={datetime.now(timezone.utc).isoformat()}",
        "export PATH=$HOME/.local/bin:$PATH",
    ]

    # Volcano distributed env (fallback if not already set by amlt)
    if service == "volcano" and nodes > 1:
        cmd_list.extend([
            '# Distributed env (Volcano)',
            'JOB_NAME=$(echo "$HOSTNAME" | sed \'s/-\\(master\\|worker\\)-[0-9]*$//\')',
            'if echo "$HOSTNAME" | grep -q "master"; then export NODE_RANK=0; else export NODE_RANK=$((${VK_TASK_INDEX:-0} + 1)); fi',
            'export RANK=${RANK:-$NODE_RANK}',
            f'export WORLD_SIZE=${{WORLD_SIZE:-{nodes}}}',
            'export MASTER_ADDR="${MASTER_ADDR:-${JOB_NAME}-master-0.${JOB_NAME}}"',
            'export MASTER_PORT=${MASTER_PORT:-6105}',
        ])
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
@click.option(
    "--amlt",
    is_flag=True,
    help="Submit via amlt instead of aj REST API",
)
@click.option(
    "-i",
    "--interactive",
    is_flag=True,
    help="Interactive amlt submission (manual confirmation)",
)
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
    amlt: bool,
    interactive: bool,
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

    # Work on a copy so the original template config is never mutated.
    conf = deepcopy(conf)

    sid = uuid.uuid4().hex[:8]
    name = resolve_name(command, sid)

    extra = conf.get("_extra", {})
    nodes_int = int(nodes or extra.get("nodes") or defaults.get("nodes") or 1)
    processes_int = int(
        processes or extra.get("processes") or defaults.get("processes") or 1
    )
    conf.pop("_extra", None)

    # Remember this template as the new default (only after validation passes)
    validate_config(conf, template_fp)
    save_defaults(template=template)

    service = conf.get("target", {}).get("service", "aml")

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
        service=service,
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
        if service == "volcano":
            from azure_jobs.core.submit._volcano import (
                build_volcano_config_from_template,
                build_volcano_job,
            )

            vcfg = build_volcano_config_from_template(
                conf,
                name=name,
                nodes=nodes_int,
                processes_per_node=processes_int,
            )
            info("Generated Volcano Job YAML:")
            click.echo(yaml.dump(build_volcano_job(vcfg), default_flow_style=False))
        return

    # ── Choose submission backend ────────────────────────────────────
    from azure_jobs.core.config import ensure_experiment

    experiment = ensure_experiment()

    rec = SubmissionRecord(
        id=sid,
        template=template,
        nodes=nodes_int,
        processes=processes_int,
        portal="",
        created_at=datetime.now(timezone.utc).isoformat(),
        status="submitted",
        command=command,
        args=list(args),
    )

    if amlt and _amlt_available():
        # --amlt flag takes priority over all other backends
        _clean_config_for_amlt(submission_fp)
        _submit_via_amlt(submission_fp, experiment, rec, name, interactive=interactive)
    elif service == "volcano":
        _submit_via_volcano(conf, name, nodes_int, processes_int, rec, dry_run)
    else:
        from azure_jobs.core.submit import build_request_from_config

        workspace = get_workspace_config()
        request = build_request_from_config(
            conf,
            name=name,
            workspace=workspace,
            experiment=experiment,
            nodes=nodes_int,
            processes_per_node=processes_int,
        )
        _submit_and_record(request, rec, name)


def _submit_and_record(
    request,
    rec: SubmissionRecord,
    display_name: str,
) -> None:
    """Submit job to Azure ML and update the submission record."""
    from azure_jobs.core.submit import SubmitResult, submit

    try:
        from rich.live import Live
        from rich.progress import BarColumn, Progress, TextColumn
        from rich.spinner import Spinner

        with Live(console=console, transient=True) as live:
            _upload_progress: Progress | None = None
            _upload_task_id: int | None = None

            def _show_spinner(text: str) -> None:
                nonlocal _upload_progress, _upload_task_id
                _upload_progress = None
                _upload_task_id = None
                live.update(Spinner("dots", text=f" [bold cyan]{text}[/bold cyan]"))

            def _on_status(step: str, detail: str) -> None:
                _show_spinner(detail)

            def _on_upload(completed: int, total: int, skipped: int) -> None:
                nonlocal _upload_progress, _upload_task_id
                if _upload_progress is None:
                    _upload_progress = Progress(
                        TextColumn("[bold cyan]Uploading code"),
                        BarColumn(bar_width=30),
                        TextColumn("[bold]{task.completed}/{task.total}[/bold] files"),
                        TextColumn("[dim]{task.fields[extra]}[/dim]"),
                    )
                    _upload_task_id = _upload_progress.add_task(
                        "upload", total=total, extra=""
                    )
                    live.update(_upload_progress)
                uploaded = completed - skipped
                extra = f"({uploaded} new, {skipped} cached)" if completed > 0 else ""
                _upload_progress.update(
                    _upload_task_id,
                    completed=completed,
                    extra=extra,  # type: ignore[arg-type]
                )

            _show_spinner("Authenticating…")
            result: SubmitResult = submit(
                request,
                on_status=_on_status,
                on_upload_progress=_on_upload,
            )

        if result.status == "failed":
            rec.status = "failed"
            rec.note = result.error
            error(f"Submission failed: {result.error}")
            raise SystemExit(1)

        rec.status = "submitted"
        rec.azure_name = result.azure_name
        if result.portal_url:
            rec.portal = result.portal_url
        success(f"Job [bold]{display_name}[/bold] submitted")
        if result.azure_name != display_name:
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


def _submit_via_volcano(
    conf: dict,
    name: str,
    nodes: int,
    processes: int,
    rec: SubmissionRecord,
    dry_run: bool,
) -> None:
    """Submit job to Kubernetes via Volcano (kubectl apply)."""
    from azure_jobs.core.submit._volcano import (
        build_volcano_config_from_template,
        submit_volcano_job,
    )

    vcfg = build_volcano_config_from_template(
        conf,
        name=name,
        nodes=nodes,
        processes_per_node=processes,
    )
    ok, output = submit_volcano_job(vcfg, dry_run=dry_run)

    if dry_run:
        info("Generated Volcano Job YAML:")
        click.echo(output)
        return

    if ok:
        rec.status = "submitted"
        log_record(rec)
        success(f"Job [bold]{name}[/bold] submitted to Volcano")
        dim(output)
    else:
        rec.status = "failed"
        rec.note = output
        log_record(rec)
        error(f"kubectl apply failed: {output}")
        raise SystemExit(1)


def _amlt_available() -> bool:
    """Check if amlt CLI is installed and a project is configured."""
    import shutil

    if not shutil.which("amlt"):
        return False
    return Path(".amltconfig").exists()


def _clean_config_for_amlt(fp: Path) -> None:
    """Rewrite a submission YAML to be amlt-compatible.

    1. Remove aj-specific fields from ``target`` that amlt rejects:
       ``subscription_id`` and ``resource_group`` are only used by aj
       for VC resolution and are invalid in amlt's target schema.
       ``workspace_name`` is kept — amlt uses it to route cross-workspace jobs.
    2. Escape ``$`` → ``$$`` so amlt passes shell variables through
       literally.  Already-doubled ``$$`` is left untouched, and
       ``$CONFIG_DIR`` is preserved for amlt to resolve.
    """
    import re

    conf = yaml.safe_load(fp.read_text()) or {}

    # Strip aj-only target fields (VC subscription/rg used by direct REST path)
    target = conf.get("target")
    if isinstance(target, dict):
        for key in ("subscription_id", "resource_group"):
            target.pop(key, None)

    text = yaml.dump(conf, default_flow_style=False)

    # Escape $ → $$ for amlt, preserving existing $$ and $CONFIG_DIR
    text = re.sub(
        r"\$\$|\$(?!CONFIG_DIR\b)",
        lambda m: m.group() if len(m.group()) == 2 else "$$",
        text,
    )
    fp.write_text(text)


def _submit_via_amlt(
    config_fp: Path,
    exp_name: str,
    rec: SubmissionRecord,
    display_name: str,
    *,
    interactive: bool = False,
) -> None:
    """Submit job via ``amlt run`` with streaming output.

    Default: auto-answer prompts (Enter + ``-y``) and stream output as dim.
    Interactive (``-i``): full stdin/stdout pass-through for manual control.
    """
    cmd = ["amlt", "run", str(config_fp), exp_name]
    if not interactive:
        cmd.append("-y")

    try:
        if interactive:
            # Full pass-through — user controls amlt directly
            result = subprocess.run(cmd, timeout=600)
            if result.returncode != 0:
                rec.status = "failed"
                rec.note = "amlt run failed"
                error("amlt run failed")
                raise SystemExit(1)
            rec.status = "submitted"
            success(f"Job [bold]{display_name}[/bold] submitted via amlt")
        else:
            # Auto-answer: pipe Enter for "press enter" prompt, -y for yes/no.
            # Stream stdout line-by-line as dim text.
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            # Feed newlines for any Enter prompts, then close stdin
            try:
                proc.stdin.write("\n\n\n")  # type: ignore[union-attr]
                proc.stdin.close()  # type: ignore[union-attr]
            except BrokenPipeError:
                pass

            output_lines: list[str] = []
            for line in proc.stdout:  # type: ignore[union-attr]
                line = line.rstrip()
                if not line:
                    continue
                output_lines.append(line)
                dim(line)
            proc.wait()

            if proc.returncode != 0:
                rec.status = "failed"
                rec.note = "\n".join(output_lines[-10:])
                error("amlt run failed")
                raise SystemExit(1)

            portal_url = _extract_portal_url("\n".join(output_lines))
            rec.status = "submitted"
            if portal_url:
                rec.portal = portal_url
            success(f"Job [bold]{display_name}[/bold] submitted via amlt")
    except SystemExit:
        raise
    except subprocess.TimeoutExpired:
        rec.status = "failed"
        rec.note = "amlt run timed out"
        error("amlt run timed out")
        raise SystemExit(1)
    except Exception as exc:
        rec.status = "failed"
        rec.note = str(exc)
        raise click.ClickException(f"Submission failed: {exc}")
    finally:
        log_record(rec)


def _extract_portal_url(output: str) -> str:
    """Extract Azure portal URL from amlt run output, if present."""
    for line in output.splitlines():
        stripped = line.strip()
        if "portal.azure.com" in stripped or "ml.azure.com" in stripped:
            # Find URL in the line
            for token in stripped.split():
                if token.startswith("http"):
                    return token
    return ""
