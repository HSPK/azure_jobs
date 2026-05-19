"""Submission orchestration — the SDK one-call path.

Wraps the multi-step build flow (template → SKU resolution →
:class:`SubmitRequest` → submission YAML on disk) into a single
function. SDK consumers and the ``aj run`` CLI both flow through it.

Compared to calling :func:`build_submit_request` directly, this:

* resolves the SKU template (``str`` or range-dict) → concrete SKU,
* materialises the rendered ``amlt`` config to ``AJ_SUBMISSION_HOME``
  (or ``AJ_DRYRUN_HOME`` when ``dry_run=True``),
* returns both the :class:`SubmitRequest` and the YAML path so
  callers can hand the YAML to ``submit_via_amlt`` or inspect it.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from .. import const
from ..config import AJWorkspace
from ..sku import resolve_sku
from .config import build_submit_request, render_amlt_config
from .models import SubmitRequest

if TYPE_CHECKING:
    from ..template import Template


@dataclass(frozen=True)
class PreparedSubmission:
    """Output of :func:`orchestrate` — request + materialized YAML path."""

    request: SubmitRequest
    submission_path: Path


def orchestrate(
    template: Template,
    *,
    user_command: str,
    user_args: tuple[str, ...] = (),
    template_name: str = "unknown",
    workspace: AJWorkspace | None = None,
    experiment: str = "aj",
    nodes: int = 1,
    processes: int = 1,
    processes_per_node: int = 1,
    sid: str | None = None,
    name: str | None = None,
    code_dir: str | None = None,
    dry_run: bool = False,
) -> PreparedSubmission:
    """Build a ready-to-submit request from *template* + parameters.

    ``sid`` defaults to a fresh 8-char hex. ``name`` defaults to
    ``"{cwd_basename}_{sid}"`` (matching ``utils.naming.resolve_name``).
    ``workspace`` defaults to an empty :class:`AJWorkspace` — fine for
    ``dry_run=True`` previews; real submissions should pass a populated
    one (e.g. from :func:`azure_jobs.core.config.get_workspace_config`).

    Raises ``ValueError`` if the template is unusable (missing jobs
    section, unresolvable SKU template, etc.).
    """
    if not template.jobs:
        raise ValueError("Template missing 'jobs' section")

    if sid is None:
        sid = uuid.uuid4().hex[:8]
    if name is None:
        from azure_jobs.utils.naming import resolve_name

        name = resolve_name(user_command, sid)

    sku_resolved = resolve_sku(template.jobs[0].sku, nodes, processes)

    if workspace is None:
        workspace = AJWorkspace()

    request = build_submit_request(
        template,
        name=name,
        sid=sid,
        sku=sku_resolved,
        user_command=user_command,
        user_args=user_args,
        workspace=workspace,
        template_name=template_name,
        experiment=experiment,
        nodes=nodes,
        processes=processes,
        processes_per_node=processes_per_node,
        code_dir=code_dir,
    )

    amlt_conf = render_amlt_config(request)
    home = const.AJ_DRYRUN_HOME if dry_run else const.AJ_SUBMISSION_HOME
    submission_path = home / f"{sid}.yaml"
    submission_path.parent.mkdir(parents=True, exist_ok=True)
    submission_path.write_text(yaml.dump(amlt_conf, default_flow_style=False))

    return PreparedSubmission(request=request, submission_path=submission_path)


# Re-export so callers can sniff the constant without reaching into core.const.
__all__ = ["PreparedSubmission", "orchestrate"]
