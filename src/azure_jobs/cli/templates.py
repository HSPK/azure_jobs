from __future__ import annotations

import subprocess

import click
import yaml

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.core.config import get_defaults, read_config
from azure_jobs.utils.ui import console, info, show_template_table, success, warning


@main.group(name="template")
def template_group() -> None:
    """Manage job templates."""


@template_group.command(name="list")
def template_list() -> None:
    """List available templates."""
    _show_templates()


@template_group.command(name="pull")
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force re-clone (discard local changes)"
)
def template_pull(repo_id: str | None, force: bool) -> None:
    """Pull templates from a git repository."""
    from azure_jobs.cli.pull import _do_pull
    _do_pull(repo_id, force)


@template_group.command(name="push")
@click.option("-m", "--message", default=None, help="Commit message")
def template_push(message: str | None) -> None:
    """Push local template changes to the remote repository."""
    from azure_jobs.cli.pull import _do_push
    _do_push(message)


@template_group.command(name="show")
@click.argument("name", type=str)
def template_show(name: str) -> None:
    """Show the fully resolved config for a template."""
    from azure_jobs.core.conf import ConfigError, read_conf

    tp = const.AJ_TEMPLATE_HOME / f"{name}.yaml"
    if not tp.exists():
        raise click.ClickException(f"Template '{name}' not found")

    try:
        merged = read_conf(tp)
    except (ConfigError, FileNotFoundError) as exc:
        raise click.ClickException(str(exc))

    # Show inheritance chain
    raw = yaml.safe_load(tp.read_text()) or {}
    base = raw.get("base", None)
    if base:
        if isinstance(base, str):
            base = [base]
        chain = " → ".join(base) + f" → {name}"
        console.print(f"\n[dim]Inheritance:[/dim] {chain}")

    # Pretty-print the resolved YAML
    output = yaml.dump(merged, default_flow_style=False, sort_keys=False, allow_unicode=True)
    from rich.syntax import Syntax
    console.print()
    console.print(Syntax(output, "yaml", theme="monokai", line_numbers=False))


@template_group.command(name="validate")
@click.argument("name", type=str, required=False, default=None)
def template_validate(name: str | None) -> None:
    """Validate template config (all templates if no name given)."""
    from azure_jobs.core.conf import ConfigError, read_conf

    if not const.AJ_TEMPLATE_HOME.exists():
        raise click.ClickException(f"No templates found in {const.AJ_TEMPLATE_HOME}")

    if name:
        targets = [const.AJ_TEMPLATE_HOME / f"{name}.yaml"]
        if not targets[0].exists():
            raise click.ClickException(f"Template '{name}' not found")
    else:
        targets = sorted(const.AJ_TEMPLATE_HOME.glob("*.yaml"))
        if not targets:
            raise click.ClickException("No templates found")

    errors: list[tuple[str, str]] = []
    ok_count = 0

    for tp in targets:
        tname = tp.stem
        try:
            raw = yaml.safe_load(tp.read_text()) or {}
            conf = read_conf(tp)
        except (ConfigError, FileNotFoundError) as exc:
            errors.append((tname, f"inheritance error: {exc}"))
            continue

        # Templates without a base key are building blocks, not submittable
        if "base" not in raw:
            ok_count += 1
            continue

        # Check required structure for submittable templates
        issues: list[str] = []
        if "jobs" not in conf:
            issues.append("missing 'jobs' key")
        elif not isinstance(conf["jobs"], list) or len(conf["jobs"]) == 0:
            issues.append("'jobs' must be a non-empty list")
        elif "sku" not in conf["jobs"][0]:
            issues.append("first job missing 'sku' key")

        if "target" not in conf:
            issues.append("missing 'target' key")
        elif not isinstance(conf.get("target"), dict):
            issues.append("'target' must be a dict")
        else:
            if "service" not in conf["target"]:
                issues.append("target missing 'service'")
            if "name" not in conf["target"]:
                issues.append("target missing 'name'")

        if issues:
            errors.append((tname, "; ".join(issues)))
        else:
            ok_count += 1

    # Report results
    if ok_count > 0:
        success(f"{ok_count} template(s) valid")
    for tname, msg in errors:
        from azure_jobs.utils.ui import error as ui_error
        ui_error(f"{tname}: {msg}")

    if errors:
        raise SystemExit(1)


@template_group.command(name="diff")
def template_diff() -> None:
    """Show local changes compared to the remote repository."""
    import tempfile

    config = read_config()
    repo_id = config.get("repo_id")
    if not repo_id:
        raise click.ClickException(
            "No remote repo configured. Run `aj pull <repo>` first."
        )
    if not const.AJ_HOME.exists():
        raise click.ClickException("No AJ home found. Run `aj pull` first.")

    with tempfile.TemporaryDirectory() as tmp:
        try:
            with console.status("[bold cyan]Fetching remote…[/bold cyan]", spinner="dots"):
                subprocess.run(
                    ["git", "clone", "--depth=1", repo_id, tmp],
                    check=True, capture_output=True, text=True,
                )
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                f"Failed to clone remote: {exc.stderr.strip()}"
            )

        result = subprocess.run(
            ["diff", "-rq", "--exclude=.git", "--exclude=aj_config.json",
             "--exclude=submission", "--exclude=record.jsonl",
             tmp, str(const.AJ_HOME)],
            capture_output=True, text=True,
        )

        if not result.stdout.strip():
            info("No differences with remote")
            return

        # Show detailed diff
        detail = subprocess.run(
            ["diff", "-ru", "--exclude=.git", "--exclude=aj_config.json",
             "--exclude=submission", "--exclude=record.jsonl",
             "--color=never",
             tmp, str(const.AJ_HOME)],
            capture_output=True, text=True,
        )
        from rich.syntax import Syntax
        console.print()
        console.print(Syntax(detail.stdout, "diff", theme="monokai", line_numbers=False))


# ---------------------------------------------------------------------------
# Top-level aliases (backward compat / convenience)
# ---------------------------------------------------------------------------

@main.command(name="list", hidden=True)
def list_templates() -> None:
    _show_templates()


@main.command(name="pull", hidden=True)
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force re-clone (discard local changes)"
)
def pull_alias(repo_id: str | None, force: bool) -> None:
    from azure_jobs.cli.pull import _do_pull
    _do_pull(repo_id, force)


@main.command(name="push", hidden=True)
@click.option("-m", "--message", default=None, help="Commit message")
def push_alias(message: str | None) -> None:
    from azure_jobs.cli.pull import _do_push
    _do_push(message)


def _show_templates() -> None:
    if not const.AJ_TEMPLATE_HOME.exists():
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return
    template_files = sorted(const.AJ_TEMPLATE_HOME.glob("*.yaml"))
    if not template_files:
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return

    defaults = get_defaults()
    default_template = defaults.get("template")

    templates: list[dict] = []
    for tp in template_files:
        raw = yaml.safe_load(tp.read_text()) or {}
        conf = raw.get("config", {})
        extra = conf.get("_extra", {})
        base = raw.get("base", None)
        if isinstance(base, list):
            # Strip the common "base" entry and show short labels
            # e.g. ["base", "account.drl", "environment.ath200", "storage.x"]
            #   → "drl · ath200 · x"
            parts = [b.split(".")[-1] for b in base if b != "base"]
            base = " · ".join(parts) if parts else "base"
        sku = "—"
        jobs = conf.get("jobs", [])
        if jobs and isinstance(jobs[0], dict):
            sku_val = jobs[0].get("sku", "—")
            sku = str(sku_val) if not isinstance(sku_val, dict) else "range{…}"

        templates.append(
            {
                "name": tp.stem,
                "base": base or "—",
                "nodes": extra.get("nodes", "—"),
                "processes": extra.get("processes", "—"),
                "sku": sku,
            }
        )
    show_template_table(templates, default_template=default_template)
