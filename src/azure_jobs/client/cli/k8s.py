"""aj k8s / aj k — set up access and manage Volcano resources."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, TypeVar

import click

from azure_jobs.client.k8s_manager import K8sManager, PROFILES
from azure_jobs.shared.errors import K8sError

from . import main

_Command = TypeVar("_Command", bound=Callable[..., object])
_Result = TypeVar("_Result")


def _connection_options(command: _Command) -> _Command:
    command = click.option(
        "-n",
        "--namespace",
        default="",
        help="Kubernetes namespace [default: current context namespace].",
    )(command)
    command = click.option(
        "--context",
        default="",
        help="kubectl context [default: current context].",
    )(command)
    command = click.option(
        "--kubeconfig",
        type=click.Path(path_type=Path, dir_okay=False),
        default=None,
        help="Kubeconfig path [default: ~/.kube/config].",
    )(command)
    return command


def _manager(
    kubeconfig: Path | None,
    context: str,
    namespace: str,
) -> K8sManager:
    return K8sManager(
        kubeconfig=kubeconfig,
        context=context,
        namespace=namespace,
    )


@main.group(name="k8s")
def k8s_group() -> None:
    """Set up Kubernetes access and manage Volcano tasks."""


def _profile_options(command: _Command) -> _Command:
    command = click.option(
        "--dry-run",
        is_flag=True,
        help="Print the login plan only.",
    )(command)
    command = click.option(
        "--reset-namespace",
        is_flag=True,
        help="Replace the existing context namespace with the profile default.",
    )(command)
    command = click.option(
        "--fresh/--cached",
        default=True,
        help=(
            "Clear cached OIDC tokens before authentication; requires "
            "confirmation because it may sign out the current user."
        ),
    )(command)
    command = click.option("--verify/--no-verify", default=True)(command)
    command = click.option(
        "--kubeconfig",
        type=click.Path(path_type=Path, dir_okay=False),
        default=None,
    )(command)
    command = click.option(
        "--extra-scopes",
        default="",
        help="Comma-separated OIDC scopes.",
    )(command)
    command = click.option("--client-id", default="", help="OIDC public client ID.")(
        command
    )
    command = click.option("--issuer-url", default="", help="OIDC issuer HTTPS URL.")(
        command
    )
    command = click.option("--user", "user_name", default="", help="Kubeconfig user name.")(
        command
    )
    command = click.option("--namespace", default="", help="Initial namespace.")(
        command
    )
    command = click.option(
        "--context",
        "context_name",
        default="",
        help="Override context name.",
    )(command)
    command = click.option(
        "--server",
        default="",
        help="Override Kubernetes HTTPS API URL.",
    )(command)
    command = click.option("--cluster", default="", help="Override cluster name.")(
        command
    )
    command = click.option(
        "--profile",
        type=click.Choice(tuple(sorted(PROFILES))),
        default="lambda-msr02",
        show_default=True,
    )(command)
    return command


@k8s_group.command(name="install")
@click.option(
    "--kubernetes-minor",
    default="v1.32",
    show_default=True,
    help="pkgs.k8s.io client repository minor.",
)
@click.option(
    "--force-repo",
    is_flag=True,
    help="Remove conflicting pkgs.k8s.io apt source files.",
)
@click.option(
    "--reinstall",
    is_flag=True,
    help="Run installation even when kubectl and oidc-login already exist.",
)
@click.option("--dry-run", is_flag=True, help="Print the install plan only.")
@click.option("-y", "--yes", is_flag=True, help="Skip the mutation confirmation.")
def k8s_install(
    kubernetes_minor: str,
    force_repo: bool,
    reinstall: bool,
    dry_run: bool,
    yes: bool,
) -> None:
    """Install kubectl, Krew, and oidc-login without changing kubeconfig."""
    from azure_jobs.client.ui import (
        DetailField,
        DetailView,
        get_output_mode,
        render_detail,
        show_command_result,
        success,
    )

    plan = {
        "kubernetes_minor": kubernetes_minor,
        "force_repo": force_repo,
        "reinstall": reinstall,
        "installs": ["kubectl", "krew", "oidc-login"],
    }
    if dry_run:
        render_detail(
            DetailView(
                data=plan,
                fields=[DetailField(key) for key in plan],
                title="Kubernetes Tool Install Plan",
                metadata={"kind": "k8s_install_plan"},
            )
        )
        return
    if get_output_mode() == "json":
        show_command_result(
            "k8s.install",
            status="failed",
            message="Kubernetes tool installation is interactive; rerun without --json.",
        )
        raise click.exceptions.Exit(1)
    if not yes:
        click.confirm(
            "Install system Kubernetes client tools with sudo and user-local Krew?",
            abort=True,
        )
    result = _call_manager(
        "k8s.install",
        lambda: K8sManager().install_tools(
            kubernetes_minor=kubernetes_minor,
            force_repo=force_repo,
            reinstall=reinstall,
        ),
    )
    if result["changed"]:
        success(f"Kubernetes tools installed; OIDC plugin: {result['plugin']}")
    else:
        success(
            "Kubernetes tools already installed; no apt/Krew changes were made."
        )


@k8s_group.command(name="login")
@_profile_options
def k8s_login(
    profile: str,
    cluster: str,
    server: str,
    context_name: str,
    namespace: str,
    user_name: str,
    issuer_url: str,
    client_id: str,
    extra_scopes: str,
    kubeconfig: Path | None,
    verify: bool,
    fresh: bool,
    reset_namespace: bool,
    dry_run: bool,
) -> None:
    """Merge an OIDC kubeconfig profile and authenticate."""
    _login_impl(
        action="k8s.login",
        profile=profile,
        cluster=cluster,
        server=server,
        context_name=context_name,
        namespace=namespace,
        user_name=user_name,
        issuer_url=issuer_url,
        client_id=client_id,
        extra_scopes=extra_scopes,
        kubeconfig=kubeconfig,
        verify=verify,
        fresh=fresh,
        reset_namespace=reset_namespace,
        dry_run=dry_run,
    )


@k8s_group.command(name="setup", hidden=True)
@_profile_options
def k8s_setup(
    profile: str,
    cluster: str,
    server: str,
    context_name: str,
    namespace: str,
    user_name: str,
    issuer_url: str,
    client_id: str,
    extra_scopes: str,
    kubeconfig: Path | None,
    verify: bool,
    fresh: bool,
    reset_namespace: bool,
    dry_run: bool,
) -> None:
    """Deprecated alias for `aj k8s login`."""
    from azure_jobs.client.ui import get_output_mode, warning

    if get_output_mode() != "json":
        warning("`aj k setup` is deprecated; use `aj k login`.")
    _login_impl(
        action="k8s.setup",
        profile=profile,
        cluster=cluster,
        server=server,
        context_name=context_name,
        namespace=namespace,
        user_name=user_name,
        issuer_url=issuer_url,
        client_id=client_id,
        extra_scopes=extra_scopes,
        kubeconfig=kubeconfig,
        verify=verify,
        fresh=fresh,
        reset_namespace=reset_namespace,
        dry_run=dry_run,
    )


def _login_impl(
    *,
    action: str,
    profile: str,
    cluster: str,
    server: str,
    context_name: str,
    namespace: str,
    user_name: str,
    issuer_url: str,
    client_id: str,
    extra_scopes: str,
    kubeconfig: Path | None,
    verify: bool,
    fresh: bool,
    reset_namespace: bool,
    dry_run: bool,
) -> None:
    from azure_jobs.client.ui import (
        DetailField,
        DetailView,
        get_output_mode,
        render_detail,
        show_command_result,
        success,
    )

    manager = K8sManager(kubeconfig=kubeconfig)
    selected = _call_manager(
        action,
        lambda: manager.resolve_profile(
            profile,
            cluster=cluster,
            server=server,
            context=context_name,
            namespace=namespace,
            user=user_name,
            issuer_url=issuer_url,
            client_id=client_id,
            extra_scopes=extra_scopes,
        ),
    )
    plan = {
        "profile": selected.name,
        "cluster": selected.cluster,
        "server": selected.server,
        "context": selected.context,
        "namespace": selected.namespace,
        "kubeconfig": str(manager.kubeconfig),
        "fresh": fresh,
        "verify": verify,
        "preserve_namespace": not reset_namespace,
    }
    if dry_run:
        render_detail(
            DetailView(
                data=plan,
                fields=[DetailField(key) for key in plan],
                title="Kubernetes Login Plan",
                metadata={"kind": "k8s_login_plan"},
            )
        )
        return
    if get_output_mode() == "json":
        show_command_result(
            action,
            status="failed",
            message="Kubernetes login is interactive; rerun without --json.",
        )
        raise click.exceptions.Exit(1)
    if fresh and verify:
        click.confirm(
            "Clear cached Kubernetes OIDC tokens before authentication? "
            "This may sign out the current user.",
            abort=True,
        )
    result = _call_manager(
        action,
        lambda: manager.login(
            selected,
            verify=verify,
            fresh=fresh,
            preserve_namespace=not reset_namespace,
        ),
    )
    success(
        f"Kubernetes context {result['context']} configured at "
        f"{result['kubeconfig']}"
    )
    if result.get("namespace"):
        success(f"Namespace: {result['namespace']}")


@k8s_group.command(name="status")
@click.option(
    "--check-cluster",
    is_flag=True,
    help="Contact /readyz; may start the OIDC login flow.",
)
@_connection_options
def k8s_status(
    kubeconfig: Path | None,
    context: str,
    namespace: str,
    check_cluster: bool,
) -> None:
    """Show kubectl, OIDC plugin, context, namespace, and reachability."""
    from azure_jobs.client.ui import DetailField, DetailView, render_detail

    status = _call_manager(
        "k8s.status",
        lambda: _manager(kubeconfig, context, namespace).status(
            check_cluster=check_cluster
        ),
    )
    render_detail(
        DetailView(
            data=status,
            fields=[
                DetailField("kubectl"),
                DetailField("oidc_login", "OIDC login"),
                DetailField("kubeconfig"),
                DetailField("context"),
                DetailField("namespace"),
                DetailField("server"),
                DetailField("reachable", type="status"),
                DetailField("error"),
            ],
            title="Kubernetes Status",
            metadata={"kind": "k8s_status"},
        )
    )


@k8s_group.command(name="jobs")
@click.argument("name", required=False, default="")
@_connection_options
def k8s_jobs(
    name: str,
    kubeconfig: Path | None,
    context: str,
    namespace: str,
) -> None:
    """List Volcano Jobs or show one narrow status row."""
    _render_table(
        _call_manager(
            "k8s.jobs",
            lambda: _manager(kubeconfig, context, namespace).list_jobs(name=name),
        ),
        title="Volcano Jobs",
        columns=("name", "queue", "phase", "reason"),
    )


@k8s_group.command(name="queues")
@_connection_options
def k8s_queues(
    kubeconfig: Path | None,
    context: str,
    namespace: str,
) -> None:
    """List Volcano queues."""
    _render_table(
        _call_manager(
            "k8s.queues",
            lambda: _manager(kubeconfig, context, namespace).list_queues(),
        ),
        title="Volcano Queues",
        columns=("name", "state", "weight"),
    )


@k8s_group.command(name="pods")
@click.option("--job", default="", help="Filter through the Job's app label.")
@_connection_options
def k8s_pods(
    job: str,
    kubeconfig: Path | None,
    context: str,
    namespace: str,
) -> None:
    """List pods, optionally for one generated Volcano Job name."""
    _render_table(
        _call_manager(
            "k8s.pods",
            lambda: _manager(kubeconfig, context, namespace).list_pods(job=job),
        ),
        title="Kubernetes Pods",
        columns=("name", "phase", "node", "restarts", "role"),
    )


@k8s_group.command(name="logs")
@click.argument("pod")
@click.option("-c", "--container", default="")
@click.option("--tail", type=click.IntRange(1, 5000), default=200, show_default=True)
@click.option("--previous", is_flag=True)
@click.option(
    "--raw",
    is_flag=True,
    help="Disable credential redaction; output may contain signed URLs.",
)
@_connection_options
def k8s_logs(
    pod: str,
    container: str,
    tail: int,
    previous: bool,
    raw: bool,
    kubeconfig: Path | None,
    context: str,
    namespace: str,
) -> None:
    """Print a bounded pod log tail; credentials are redacted by default."""
    from azure_jobs.client.ui import console, emit_json, get_output_mode

    content = _call_manager(
        "k8s.logs",
        lambda: _manager(kubeconfig, context, namespace).logs(
            pod,
            container=container,
            tail=tail,
            previous=previous,
            raw=raw,
        ),
    )
    if get_output_mode() == "json":
        emit_json(
            {
                "kind": "k8s_logs",
                "pod": pod,
                "container": container,
                "previous": previous,
                "redacted": not raw,
                "content": content,
            }
        )
        return
    console.print(content, markup=False, soft_wrap=True)


@k8s_group.command(name="events")
@click.argument("pod")
@_connection_options
def k8s_events(
    pod: str,
    kubeconfig: Path | None,
    context: str,
    namespace: str,
) -> None:
    """List bounded, redacted events for one pod."""
    _render_table(
        _call_manager(
            "k8s.events",
            lambda: _manager(kubeconfig, context, namespace).events(pod),
        ),
        title=f"Events: {pod}",
        columns=("time", "type", "reason", "message"),
    )


@k8s_group.command(name="delete")
@click.argument("job")
@click.option("-y", "--yes", is_flag=True, help="Skip deletion confirmation.")
@_connection_options
def k8s_delete(
    job: str,
    yes: bool,
    kubeconfig: Path | None,
    context: str,
    namespace: str,
) -> None:
    """Delete one exact generated Volcano Job."""
    from azure_jobs.client.ui import (
        get_output_mode,
        show_command_result,
        success,
    )

    if not yes:
        if get_output_mode() == "json":
            show_command_result(
                "k8s.delete",
                status="failed",
                message="Pass --yes after confirming the exact Job.",
                job=job,
            )
            raise click.exceptions.Exit(1)
        click.confirm(
            f"Delete Volcano Job {job!r} from namespace "
            f"{namespace or '(current)'}?",
            abort=True,
        )
    output = _call_manager(
        "k8s.delete",
        lambda: _manager(kubeconfig, context, namespace).delete_job(job),
    )
    if get_output_mode() == "json":
        show_command_result(
            "k8s.delete",
            status="ok",
            job=job,
            output=output,
        )
        return
    success(output or f"Deleted Volcano Job {job}")


def _render_table(
    rows: list[dict[str, str]],
    *,
    title: str,
    columns: tuple[str, ...],
) -> None:
    from azure_jobs.client.ui import Column, TableView, render_table

    render_table(
        TableView(
            rows=rows,
            columns=[Column(key) for key in columns],
            title=title,
            empty_message=f"No {title.lower()} found",
        )
    )


def _call_manager(action: str, operation: Callable[[], _Result]) -> _Result:
    try:
        return operation()
    except K8sError as exc:
        from azure_jobs.client.ui import get_output_mode, show_command_result

        if get_output_mode() != "json":
            raise
        show_command_result(
            action,
            status="failed",
            message=str(exc),
        )
        raise click.exceptions.Exit(1) from exc


@main.group(name="k", hidden=True)
def k_alias() -> None:
    """Short alias for `aj k8s`."""


for _name, _command in k8s_group.commands.items():
    k_alias.add_command(_command, name=_name)


__all__ = ["k8s_group", "k_alias"]
