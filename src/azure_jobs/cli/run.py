from __future__ import annotations

import subprocess
import uuid
from datetime import datetime, timezone

import click
import yaml

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.core.config import (
    AJWorkspace,
    ensure_experiment,
    get_defaults,
    get_experiment,
    get_workspace_config,
    save_defaults,
)
from azure_jobs.core.record import SubmissionRecord
from azure_jobs.core.sku import resolve_sku
from azure_jobs.core.submit import (
    amlt_available,
    build_submit_request,
    render_amlt_config,
    submit_and_record,
    submit_via_amlt,
    submit_via_native,
    submit_via_volcano,
)
from azure_jobs.core.template import Template
from azure_jobs.utils.naming import resolve_name
from azure_jobs.utils.ui import (
    info,
    show_submission_preview,
)

__all__ = ["resolve_name"]


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
@click.option(
    "-p",
    "--processes",
    default=None,
    help="GPUs per node (drives SKU resolution and AJ_PROCESSES env)",
)
@click.option(
    "--ppn",
    "--processes-per-node",
    "ppn",
    default=None,
    help="Launcher processes per node (e.g. torchrun --nproc-per-node). Default: 1",
)
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
    ppn: str | None,
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

    tmpl = Template.from_conf_path(template_fp)
    if not tmpl.jobs:
        raise click.ClickException("Template missing 'jobs' section")

    sid = uuid.uuid4().hex[:8]
    name = resolve_name(command, sid)

    nodes_int = int(nodes or defaults.nodes or 1)
    processes_int = int(processes or defaults.processes or 1)
    ppn_int = int(ppn or 1)
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
            processes=processes_int,
            processes_per_node=ppn_int,
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
        submit_and_record(
            lambda on_event: submit_via_amlt(
                submission_fp, experiment, name=name, on_event=on_event
            ),
            rec,
            name,
            backend_label="amlt",
        )
    elif service == "volcano":
        submit_and_record(
            lambda on_event: submit_via_volcano(request, on_event=on_event),
            rec,
            name,
            backend_label="Volcano",
        )
    else:
        submit_and_record(
            lambda on_event: submit_via_native(request, on_event=on_event),
            rec,
            name,
            backend_label="Azure ML",
        )
