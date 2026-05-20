from __future__ import annotations

import uuid
from datetime import datetime, timezone

import click

from azure_jobs.cli import main
from azure_jobs.cli.runner import submit_and_record
from azure_jobs.core.config import (
    AJWorkspace,
    ensure_experiment,
    get_defaults,
    get_experiment,
    get_workspace_config,
    save_defaults,
)
from azure_jobs.core.errors import AJError
from azure_jobs.core.sku import resolve_sku
from azure_jobs.core.submit import (
    SubmissionRecord,
    amlt_available,
    build_submit_request,
    get_backend,
    materialise_submission,
)
from azure_jobs.core.template import Template
from azure_jobs.utils.naming import resolve_name
from azure_jobs.utils.ui import show_dry_run_result, show_submission_preview, warning

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
) -> None:
    """Submit a job to Azure ML using a template.

    Loads the named template, resolves its ``base`` inheritance chain,
    merges configs, applies CLI overrides (``-n``/``-p``), uploads code,
    registers the environment, and submits via the registered backend
    for the template's ``target.service`` (or amlt with ``--amlt``).

    Use ``-d`` to inspect the assembled config without submitting.
    """
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
    workspace = AJWorkspace() if dry_run else get_workspace_config()
    experiment = get_experiment() or "aj" if dry_run else ensure_experiment()

    try:
        request = build_submit_request(
            tmpl,
            name=name,
            sid=sid,
            sku=sku_resolved,
            user_command=command,
            user_args=args,
            workspace=workspace,
            template_name=template_name,
            experiment=experiment,
            nodes=nodes_int,
            gpus_per_node=gpn_int,
            processes_per_node=ppn_int,
        )
    except (AJError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    if amlt:
        if not amlt_available():
            raise click.ClickException(
                "amlt CLI not available or no .amltconfig in current directory."
            )
        request.service = "amlt"
    else:
        try:
            _resolve_or_warn_sing_target_coords(
                request,
                template_name=template_name,
                dry_run=dry_run,
            )
        except AJError as exc:
            raise click.ClickException(str(exc)) from exc

    if dry_run or request.service == "amlt":
        materialise_submission(request, dry_run=dry_run)

    show_submission_preview(request, dry_run=dry_run)
    if dry_run:
        show_dry_run_result(request)
        return

    try:
        entry = get_backend(request.service)
    except AJError as exc:
        raise click.ClickException(str(exc)) from exc

    rec = SubmissionRecord(
        request=request,
        created_at=datetime.now(timezone.utc).isoformat(),
        status="submitted",
    )
    submit_and_record(
        lambda on_event: entry.fn(request, on_event=on_event),
        rec,
        name,
        backend_label=entry.label,
    )


def _load_template(template: str | None) -> tuple[Template, str]:
    """Resolve the template name (with default fallback) and load it."""
    from azure_jobs.core import const

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


def _resolve_or_warn_sing_target_coords(
    request,
    *,
    template_name: str,
    dry_run: bool,
) -> None:
    """Resolve missing Singularity VC coordinates, or warn during dry-run."""
    if request.service != "sing":
        return
    if request.sing.vc_subscription_id and request.sing.vc_resource_group:
        return

    if dry_run:
        warning(
            f"Template '{template_name}' omits target.subscription_id/resource_group; "
            "aj will discover the Singularity VC via Azure Resource Graph at submit time. "
            "Add both fields to skip that lookup."
        )
        return

    from azure_jobs.core.sku import resolve_virtual_cluster

    vc = resolve_virtual_cluster(
        request.compute,
        subscription_id=request.sing.vc_subscription_id,
        resource_group=request.sing.vc_resource_group,
    )
    request.sing.vc_subscription_id = vc.subscription_id
    request.sing.vc_resource_group = vc.resource_group
    warning(
        f"Template '{template_name}' omits target.subscription_id/resource_group; "
        f"discovered subscription_id={vc.subscription_id}, "
        f"resource_group={vc.resource_group}. Add both fields to skip this lookup."
    )
