"""Hidden short-form aliases for aj commands."""

from __future__ import annotations

import click

from . import main

@main.command(name="d", hidden=True)
@click.option("-n", "--last", default=100)
@click.option("--page-size", default=None, type=int)
@click.option("--mouse/--no-mouse", default=False)
def _alias_d(last: int, page_size: int | None, mouse: bool) -> None:
    from .dashboard import dashboard

    dashboard.callback(last, page_size, mouse)

@main.command(name="tl", hidden=True)
def _alias_tl() -> None:
    from .templates import template_list

    template_list.callback()

@main.command(name="pull", hidden=True)
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force re-clone (discard local changes)"
)
def _alias_pull(repo_id: str | None, force: bool) -> None:
    from .pull import _do_pull

    _do_pull(repo_id, force)

@main.command(name="push", hidden=True)
@click.option("-m", "--message", default=None, help="Commit message")
def _alias_push(message: str | None) -> None:
    from .pull import _do_push

    _do_push(message)

@main.command(name="js", hidden=True)
@click.argument("job_id")
def _alias_js(job_id: str) -> None:
    from .jobs import job_status

    job_status.callback(job_id)

@main.command(name="jl", hidden=True)
@click.option("-n", "--last", default=30)
@click.option("-s", "--status", default=None)
@click.option("-e", "--experiment", default=None)
@click.option("-T", "--type", "job_type", default=None)
@click.option("--tag", default=None)
@click.option("-a", "--archived", is_flag=True, default=False)
@click.option("--ws", "ws_name", default=None)
def _alias_jl(
    last: int,
    status: str | None,
    experiment: str | None,
    job_type: str | None,
    tag: str | None,
    archived: bool,
    ws_name: str | None,
) -> None:
    from .jobs import job_list

    job_list.callback(last, status, experiment, job_type, tag, archived, ws_name)

@main.command(name="jc", hidden=True)
@click.argument("job_id")
def _alias_jc(job_id: str) -> None:
    from .jobs import job_cancel

    job_cancel.callback(job_id)

@main.command(name="jlogs", hidden=True)
@click.argument("job_id")
def _alias_jlogs(job_id: str) -> None:
    from .jobs import job_logs

    job_logs.callback(job_id)

@main.command(name="ql", hidden=True)
@click.option("--aml", "backend", flag_value="aml")
@click.option("--sing", "backend", flag_value="sing", default=True)
@click.option("--all", "show_all", is_flag=True)
@click.option("--full", "full", is_flag=True)
def _alias_ql(backend: str, show_all: bool, full: bool) -> None:
    from .quota import quota_list

    quota_list.callback(backend, show_all, full)

ALIASES: list[str] = [
    "d",
    "tl",
    "pull",
    "push",
    "js",
    "jl",
    "jc",
    "jlogs",
    "ql",
]
