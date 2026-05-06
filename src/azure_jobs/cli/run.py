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
from azure_jobs.core.config import (
    AJWorkspace,
    ensure_experiment,
    get_defaults,
    get_experiment,
    get_workspace_config,
    save_defaults,
)
from azure_jobs.core.record import SubmissionRecord, log_record
from azure_jobs.core.sku import resolve_sku
from azure_jobs.core.submit import (
    amlt_available,
    build_submit_request,
    render_amlt_config,
    submit_via_amlt,
    submit_via_native,
    submit_via_volcano,
)
from azure_jobs.core.template import Template
from azure_jobs.utils.ui import (
    console,
    dim,
    error,
    info,
    show_submission_preview,
    success,
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
@click.option("-L", "--run-local", is_flag=True, help="Run the command locally")
@click.option(
    "--amlt",
    is_flag=True,
    help="Submit via amlt instead of aj REST API",
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
) -> None:
    """Submit a job to Azure ML using a template.

    Loads the named template, resolves its ``base`` inheritance chain,
    merges configs, applies CLI overrides (``-n``/``-p``), uploads code,
    registers the environment, and submits via REST. The resolved template
    is remembered as the default for the next ``aj run``.

    Use ``-d`` to inspect the assembled config without submitting, and
    ``-L`` to execute the command locally instead of in the cloud.
    """
    defaults = get_defaults()

    if template is None:
        template = defaults.template
    if template is None:
        raise click.ClickException(
            "No template specified. Use -t <template> or set a default with aj config."
        )

    template_fp = const.AJ_TEMPLATE_HOME / f"{template}.yaml"
    if not template_fp.exists():
        raise click.ClickException(
            f"Template {template} does not exist at {template_fp}"
        )

    tmpl_dict = read_conf(template_fp)
    if not tmpl_dict:
        raise click.ClickException(f"Empty configuration file: {template_fp}")
    tmpl = Template.from_dict(tmpl_dict)
    if not tmpl.jobs:
        raise click.ClickException("Template missing 'jobs' section")

    sid = uuid.uuid4().hex[:8]
    name = resolve_name(command, sid)

    nodes_int = int(nodes or defaults.nodes or 1)
    processes_int = int(processes or defaults.processes or 1)
    sku_resolved = resolve_sku(tmpl.jobs[0].sku, nodes_int, processes_int)

    # Remember this template as the new default (only after template validation passes)
    save_defaults(template=template, nodes=nodes_int, processes=processes_int)
    workspace = AJWorkspace() if (dry_run or run_local) else get_workspace_config()
    experiment = (
        get_experiment() or "aj" if (dry_run or run_local) else ensure_experiment()
    )

    try:
        request = build_submit_request(
            tmpl,
            name=name,
            sid=sid,
            sku=sku_resolved,
            user_command=command,
            user_args=args,
            workspace=workspace,
            template_name=template,
            experiment=experiment,
            nodes=nodes_int,
            processes_per_node=processes_int,
        )
    except ValueError as e:
        raise click.ClickException(str(e))
    amlt_conf = render_amlt_config(request)
    final_cmd = request.command[-1] if request.command else ""

    if run_local:
        info(f"Running locally: {final_cmd}")
        subprocess.run(final_cmd, shell=True)
        return

    if dry_run:
        submission_fp = const.AJ_DRYRUN_HOME / f"{sid}.yaml"
    else:
        submission_fp = const.AJ_SUBMISSION_HOME / f"{sid}.yaml"
    submission_fp.parent.mkdir(parents=True, exist_ok=True)
    with open(submission_fp, "w") as f:
        yaml.dump(amlt_conf, f, default_flow_style=False)

    show_submission_preview(
        request, submission_file=str(submission_fp), dry_run=dry_run
    )

    if dry_run:
        dim(f"Config written to {submission_fp}")
        if tmpl.target.service == "volcano":
            from azure_jobs.core.submit.volcano import (
                build_volcano_config_from_template,
                build_volcano_job,
            )

            vcfg = build_volcano_config_from_template(
                amlt_conf,
                name=name,
                nodes=nodes_int,
                processes_per_node=processes_int,
            )
            info("Generated Volcano Job YAML:")
            click.echo(yaml.dump(build_volcano_job(vcfg), default_flow_style=False))
        return

    # ── Choose submission backend ────────────────────────────────────
    service = tmpl.target.service
    rec = SubmissionRecord(
        request=request,
        created_at=datetime.now(timezone.utc).isoformat(),
        status="submitted",
    )

    if amlt and amlt_available():
        # --amlt flag takes priority over all other backends
        ok, portal_url, note = submit_via_amlt(
            submission_fp, experiment, on_output_line=dim
        )
        if ok:
            rec.status = "submitted"
            if portal_url:
                rec.portal = portal_url
            success(f"Job [bold]{name}[/bold] submitted via amlt")
        else:
            rec.status = "failed"
            rec.note = note or "amlt run failed"
            error(rec.note)
            log_record(rec)
            raise SystemExit(1)
        log_record(rec)
    elif service == "volcano":

        def _on_status(phase: str, msg: str) -> None:
            dim(msg)

        ok, output = submit_via_volcano(
            amlt_conf,
            name,
            nodes_int,
            processes_int,
            dry_run=dry_run,
            on_status=_on_status,
        )

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
    else:
        _submit_and_record(request, rec, name)


def _submit_and_record(
    request,
    rec: SubmissionRecord,
    display_name: str,
) -> None:
    """Submit job to Azure ML and update the submission record."""
    from azure_jobs.core.submit import SubmitResult

    try:
        from rich.live import Live
        from rich.spinner import Spinner

        from azure_jobs.utils.ui import truncate_middle

        with Live(console=console, transient=True) as live:

            def _show_spinner(text: str) -> None:
                live.update(Spinner("dots", text=f" [bold cyan]{text}[/bold cyan]"))

            def _on_status(step: str, detail: str) -> None:
                _show_spinner(detail)

            def _on_upload(
                completed: int,
                total: int,
                skipped: int,
                current: str = "",
            ) -> None:
                # Show the file just processed; truncate long paths in the middle
                shown = truncate_middle(current, 60) if current else "preparing…"
                uploaded = completed - skipped
                counts = (
                    f"[dim]({completed}/{total} · {uploaded} new, "
                    f"{skipped} cached)[/dim]"
                )
                live.update(
                    Spinner(
                        "dots",
                        text=f" [bold cyan]Uploading[/bold cyan] {shown}  {counts}",
                    )
                )

            _show_spinner("Authenticating…")
            result: SubmitResult = submit_via_native(
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
