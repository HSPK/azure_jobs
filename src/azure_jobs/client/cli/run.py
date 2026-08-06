from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Callable

import click

from azure_jobs.shared.sku import resolve_sku
from azure_jobs.client.cli import main
from azure_jobs.client.cli.runner import submit_and_record
from azure_jobs.shared.config import (
    ensure_experiment,
    get_defaults,
    get_experiment,
    save_defaults,
)
from azure_jobs.shared.errors import AJError
from azure_jobs.shared.job import build_job_spec, write_amlt_yaml
from azure_jobs.shared.job.spec import JobSpec
from azure_jobs.shared.journal import JobRecord
from azure_jobs.shared.template import Template
from azure_jobs.shared.utils.naming import resolve_name
from azure_jobs.client.ui import (
    get_output_mode,
    show_dry_run_result,
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
    "--gpn",
    "--gpus-per-node",
    "gpus_per_node",
    default=None,
    help="GPUs per node (drives SKU resolution + AJ_GPUS_PER_NODE env)",
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
@click.option(
    "--amlt",
    is_flag=True,
    help="Submit via amlt instead of aj REST API",
)
@click.option(
    "--queue",
    is_flag=True,
    help="Hand the submission to the daemon and return a ticket immediately",
)
@click.argument("command", nargs=1)
@click.argument("args", nargs=-1)
def run(
    command: str,
    args: tuple[str, ...],
    template: str | None,
    nodes: str | None,
    gpus_per_node: str | None,
    ppn: str | None,
    dry_run: bool,
    amlt: bool,
    queue: bool,
) -> None:
    """Submit a job to Azure ML using a template."""
    tmpl, template_name = _load_template(template)

    sid = uuid.uuid4().hex[:8]
    name = resolve_name(command, sid)

    defaults = get_defaults()
    nodes_int = int(nodes or defaults.nodes or 1)
    gpn_int = int(gpus_per_node or defaults.processes or 1)
    ppn_int = int(ppn or 1)
    try:
        sku_resolved = resolve_sku(tmpl.jobs[0].sku, nodes_int, gpn_int)
    except AJError as exc:
        raise click.ClickException(str(exc)) from exc

    save_defaults(template=template_name, nodes=nodes_int, processes=gpn_int)
    experiment = get_experiment()
    if not experiment:
        if dry_run:
            experiment = "aj"
        elif get_output_mode() == "json":
            from azure_jobs.client.ui import show_command_result

            show_command_result(
                "job.submit",
                status="failed",
                message=(
                    "No experiment configured. Run "
                    "`aj config experiment <name>` first."
                ),
            )
            raise SystemExit(1)
        else:
            experiment = ensure_experiment()

    try:
        request = build_job_spec(
            tmpl,
            name=name,
            sid=sid,
            sku=sku_resolved,
            user_command=command,
            user_args=args,
            template_name=template_name,
            experiment=experiment,
            nodes=nodes_int,
            gpus_per_node=gpn_int,
            processes_per_node=ppn_int,
        )
    except (AJError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    show_submission_preview(request, dry_run=dry_run)
    if dry_run:
        submission_fp = write_amlt_yaml(request, dry_run=True)
        if get_output_mode() != "json":
            click.echo(f"[dry-run] wrote submission YAML → {submission_fp}")
        show_dry_run_result(request)
        return

    service = "amlt" if amlt else request.service
    if amlt:
        request.service = "amlt"

    if queue:
        _enqueue(request, name)
        return

    rec = JobRecord(
        request=request,
        created_at=datetime.now(timezone.utc).isoformat(),
        status="submitted",
    )
    _submit_via_daemon(request, rec, name, service)


def _load_template(template: str | None) -> tuple[Template, str]:
    from azure_jobs.shared import const

    if template is None:
        template = get_defaults().template
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
    return tmpl, template


def _submit_via_daemon(
    request: JobSpec, rec: JobRecord, name: str, service: str
) -> None:
    """Submit through the daemon, relaying its progress to the spinner.

    Submission uploads code and mutates remote state, so it belongs on the same
    side as everything else; running it in the CLI process would be a second
    execution path with different auth and different failure modes.
    """
    from azure_jobs.shared.contract.models import SubmitEvent
    from azure_jobs import connect
    from azure_jobs.shared.job.spec import JobEvent, JobResult

    payload = request.to_dict()

    def submit(on_event: Callable[[JobEvent], None]) -> JobResult:
        def relay(event: SubmitEvent) -> None:
            on_event(
                JobEvent(
                    kind=event.kind,
                    detail=event.detail,
                    completed=event.completed,
                    total=event.total,
                )
            )

        with connect() as d:
            outcome = d.job.submit(payload, on_event=relay)
        return JobResult(
            job_name=outcome.job_name,
            azure_name=outcome.backend_ref,
            status=outcome.status,
            portal_url=outcome.portal_url,
            error=outcome.error,
            note=outcome.note,
        )

    submit_and_record(submit, rec, name, backend_label=service)


def _enqueue(request: JobSpec, name: str) -> None:
    """Hand a built JobSpec to the daemon's queue and report the ticket."""
    from azure_jobs import connect
    from azure_jobs.client.ui import (
        get_output_mode,
        show_command_result,
    )

    with connect() as d:
        entry = d.job.queue(request.to_dict(), name=name)
    if get_output_mode() == "json":
        show_command_result(
            "job.queue",
            status="ok",
            ticket=entry.ticket,
            name=entry.name,
            queue_state=entry.state,
        )
        return
    click.echo(f"Queued {name} as {entry.ticket}")
    click.echo(f"  aj queue show {entry.ticket}")
    click.echo(f"  aj queue wait {entry.ticket}")
