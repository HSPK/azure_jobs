from __future__ import annotations

import subprocess
from pathlib import Path

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

@template_group.command(name="init")
@click.argument("name", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Overwrite an existing leaf template"
)
def template_init(name: str | None, force: bool) -> None:
    """Interactively author leaf templates."""
    from azure_jobs.cli._template_init import run_wizard
    from azure_jobs.utils.ui import get_output_mode, show_command_result

    if get_output_mode() == "json":
        show_command_result(
            "template_init",
            status="failed",
            message="aj template init is interactive — not supported in JSON mode.",
        )
        raise SystemExit(1)

    run_wizard(name, force=force)

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
    from azure_jobs.core.template import ConfigError, read_conf
    from azure_jobs.utils.ui import emit_json, get_output_mode

    tp = const.AJ_TEMPLATE_HOME / f"{name}.yaml"
    if not tp.exists():
        raise click.ClickException(f"Template '{name}' not found")

    try:
        merged = read_conf(tp)
    except (ConfigError, FileNotFoundError) as exc:
        raise click.ClickException(str(exc)) from exc

    raw = yaml.safe_load(tp.read_text()) or {}
    base = raw.get("base", None)

    if get_output_mode() == "json":
        chain: list[str] | None = None
        if base:
            chain = [base] if isinstance(base, str) else list(base)
        emit_json(
            {
                "kind": "template_detail",
                "name": name,
                "path": str(tp),
                "base": chain,
                "config": merged,
            }
        )
        return

    if base:
        if isinstance(base, str):
            base = [base]
        chain = " → ".join(base) + f" → {name}"
        console.print(f"\n[dim]Inheritance:[/dim] {chain}")

    output = yaml.dump(
        merged, default_flow_style=False, sort_keys=False, allow_unicode=True
    )
    from rich.syntax import Syntax

    console.print()
    console.print(Syntax(output, "yaml", theme="monokai", line_numbers=False))

@template_group.command(name="validate")
@click.argument("name", type=str, required=False, default=None)
def template_validate(name: str | None) -> None:
    """Validate template config (all templates if no name given)."""
    from azure_jobs.core.template import validate_template
    from azure_jobs.utils.ui import emit_json, get_output_mode
    from azure_jobs.utils.ui import error as ui_error

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

    results: list[dict[str, object]] = []
    for tp in targets:
        issues = validate_template(tp)
        results.append(
            {"name": tp.stem, "ok": not issues, "issues": issues}
        )

    valid = sum(1 for r in results if r["ok"])
    invalid = len(results) - valid

    if get_output_mode() == "json":
        emit_json(
            {
                "kind": "template_validate",
                "valid_count": valid,
                "invalid_count": invalid,
                "results": results,
            }
        )
        if invalid:
            raise SystemExit(1)
        return

    if valid:
        success(f"{valid} template(s) valid")
    for r in results:
        if not r["ok"]:
            ui_error(f"{r['name']}: {'; '.join(r['issues'])}")
    if invalid:
        raise SystemExit(1)

@template_group.command(name="diff")
def template_diff() -> None:
    """Show local changes compared to the remote repository."""
    from azure_jobs.utils.ui import emit_json, get_output_mode

    config = read_config()
    repo_id = config.repo_id
    if not repo_id:
        raise click.ClickException(
            "No remote repo configured. Run `aj pull <repo>` first."
        )
    if not const.AJ_HOME.exists():
        raise click.ClickException("No AJ home found. Run `aj pull` first.")

    diff_text = _compute_remote_diff(repo_id)

    if not diff_text:
        if get_output_mode() != "json":
            info("No differences with remote")
        emit_json({"kind": "template_diff", "has_changes": False, "diff": ""})
        return

    if get_output_mode() == "json":
        emit_json({"kind": "template_diff", "has_changes": True, "diff": diff_text})
        return

    from rich.syntax import Syntax

    console.print()
    console.print(Syntax(diff_text, "diff", theme="monokai", line_numbers=False))

_DIFF_EXCLUDE = {".git", "aj_config.json", "submission", "record.jsonl"}

def _clone_remote(repo_id: str, dst: str) -> None:
    try:
        with console.status(
            "[bold cyan]Fetching remote…[/bold cyan]", spinner="dots"
        ):
            subprocess.run(
                ["git", "clone", "--depth=1", repo_id, dst],
                check=True,
                capture_output=True,
                text=True,
            )
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"Failed to clone remote: {exc.stderr.strip()}"
        ) from exc

def _collect_diff_files(root: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if any(part in _DIFF_EXCLUDE for part in rel.parts):
            continue
        files[rel.as_posix()] = p
    return files

def _unified_diff_lines(
    rel: str, a_path: Path | None, b_path: Path | None
) -> list[str]:
    import difflib

    a_lines = (
        a_path.read_text(errors="replace").splitlines(keepends=True)
        if a_path
        else []
    )
    b_lines = (
        b_path.read_text(errors="replace").splitlines(keepends=True)
        if b_path
        else []
    )
    return list(
        difflib.unified_diff(
            a_lines,
            b_lines,
            fromfile=f"remote/{rel}",
            tofile=f"local/{rel}",
        )
    )

def _compute_remote_diff(repo_id: str) -> str:
    import filecmp
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        _clone_remote(repo_id, tmp)

        remote_files = _collect_diff_files(Path(tmp))
        local_files = _collect_diff_files(const.AJ_HOME)
        all_keys = sorted(set(remote_files) | set(local_files))

        diff_output: list[str] = []
        for key in all_keys:
            r = remote_files.get(key)
            ll = local_files.get(key)
            if r and ll:
                if not filecmp.cmp(str(r), str(ll), shallow=False):
                    diff_output.extend(_unified_diff_lines(key, r, ll))
            elif r and not ll:
                diff_output.extend(_unified_diff_lines(key, r, None))
            else:
                diff_output.extend(_unified_diff_lines(key, None, ll))
        return "".join(diff_output)

def _show_templates() -> None:
    if not const.AJ_TEMPLATE_HOME.exists():
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return
    template_files = sorted(const.AJ_TEMPLATE_HOME.glob("*.yaml"))
    if not template_files:
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return

    defaults = get_defaults()
    default_template = defaults.template

    templates: list[dict] = []
    for tp in template_files:
        raw = yaml.safe_load(tp.read_text()) or {}
        conf = raw.get("config", {})
        extra = conf.get("_extra", {})
        base = raw.get("base", None)
        if isinstance(base, list):
            parts = [b.split(".")[-1] for b in base if b != "base"]
            base = " · ".join(parts) if parts else "base"
        sku: str | dict = ""
        jobs = conf.get("jobs", [])
        if jobs and isinstance(jobs[0], dict):
            sku_val = jobs[0].get("sku", "")
            sku = str(sku_val) if not isinstance(sku_val, dict) else "range{…}"

        templates.append(
            {
                "name": tp.stem,
                "base": base or "",
                "nodes": extra.get("nodes", ""),
                "processes": extra.get("processes", ""),
                "sku": sku,
            }
        )
    show_template_table(templates, default_template=default_template)
