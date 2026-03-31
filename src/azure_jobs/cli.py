from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import click
import yaml

from .conf import read_conf
from .const import (
    AJ_CONFIG_FP,
    AJ_DEFAULT_TEMPLATE,
    AJ_HOME,
    AJ_RECORD,
    AJ_SUBMISSION_HOME,
    AJ_TEMPLATE_HOME,
)


@dataclass
class SubmissionRecord:
    id: str
    template: str
    nodes: int
    processes: int
    portal: str
    created_at: str
    status: str
    command: str
    args: list[str] = field(default_factory=list)


def log_record(record: SubmissionRecord) -> None:
    AJ_RECORD.parent.mkdir(parents=True, exist_ok=True)
    with open(AJ_RECORD, "a") as f:
        f.write(json.dumps(asdict(record)) + "\n")


@click.group()
@click.version_option(package_name="azure_jobs")
def main() -> None:
    pass


def check_dot_ssh() -> None:
    dot_ssh_dir = Path.cwd() / ".ssh"
    if not dot_ssh_dir.exists():
        raise click.ClickException(
            ".ssh directory not found in the current working directory."
        )
    if not any(dot_ssh_dir.iterdir()):
        raise click.ClickException(".ssh directory is empty.")


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
    if "sku" not in conf["jobs"][0]:
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


def resolve_sku(
    sku_template: str | dict, nodes: int, processes: int
) -> str:
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
) -> list[str]:
    """Assemble the full command list: env exports + template commands + user command."""
    cmd_list: list[str] = [
        f"export AJ_NODES={nodes}",
        f"export AJ_PROCESSES={processes * nodes}",
        f"export AJ_NAME={name}",
        f"export AJ_ID={sid}",
        f"export AJ_TEMPLATE={template}",
        f"export AJ_SUBMIT_TIMESTAMP_UTC={datetime.now(timezone.utc).isoformat()}",
        "export PATH=$$HOME/.local/bin:$$PATH",
    ]
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


# aj run -t template_name -n 2 -p 4 python train.py --arg1 val1
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
    default="default",
)
@click.option("-n", "--nodes", default=None, help="Number of nodes")
@click.option("-p", "--processes", default=None, help="Number of processes")
@click.option(
    "-d", "--dry-run", is_flag=True, help="Dry run the command without executing"
)
@click.option("-y", "--yes", is_flag=True, help="Skip confirmation prompts")
@click.option("-L", "--run-local", is_flag=True, help="Run the command locally")
@click.option(
    "-s", "--skip-ssh-check", is_flag=True, help="Skip checking for .ssh directory"
)
@click.argument("command", nargs=1)
@click.argument("args", nargs=-1)
def run(
    command: str,
    args: tuple[str, ...],
    template: str,
    nodes: str | None,
    processes: str | None,
    dry_run: bool,
    run_local: bool,
    yes: bool,
    skip_ssh_check: bool,
) -> None:
    if not skip_ssh_check:
        check_dot_ssh()

    template_fp = AJ_TEMPLATE_HOME / f"{template}.yaml"
    if not template_fp.exists():
        raise click.ClickException(
            f"Template {template} does not exist at {template_fp}"
        )

    conf = read_conf(template_fp)
    if not conf:
        raise click.ClickException(f"Empty configuration file: {template_fp}")
    if template_fp != AJ_DEFAULT_TEMPLATE:
        shutil.copy(template_fp, AJ_DEFAULT_TEMPLATE)

    sid = uuid.uuid4().hex[:8]
    name = resolve_name(command, sid)

    nodes_int = int(nodes or conf.get("_extra", {}).get("nodes", 1))
    processes_int = int(processes or conf.get("_extra", {}).get("processes", 1))
    conf.pop("_extra", None)

    validate_config(conf, template_fp)

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
    )
    click.echo(f"Final command to execute: {conf['jobs'][0]['command'][-1]}")

    if run_local:
        subprocess.run(conf["jobs"][0]["command"][-1], shell=True)
        return

    submission_fp = AJ_SUBMISSION_HOME / f"{sid}.yaml"
    submission_fp.parent.mkdir(parents=True, exist_ok=True)
    click.echo(f"Writing submission file to {submission_fp}")
    with open(submission_fp, "w") as f:
        yaml.dump(conf, f, default_flow_style=False)

    if dry_run:
        click.echo("Dry run mode: not executing command")
        return

    amlt_command: list[str | Path] = ["amlt", "run", submission_fp, sid]
    rec = SubmissionRecord(
        id=sid,
        template=template,
        nodes=nodes_int,
        processes=processes_int,
        portal="azure",
        created_at=datetime.now(timezone.utc).isoformat(),
        status="success",
        command=command,
        args=list(args),
    )
    try:
        if yes:
            # Pipe 'yes' into amlt via stdin to auto-confirm prompts
            with subprocess.Popen(
                ["yes"], stdout=subprocess.PIPE
            ) as yes_proc:
                result = subprocess.run(
                    amlt_command,
                    stdin=yes_proc.stdout,
                    check=True,
                )
        else:
            result = subprocess.run(amlt_command, check=True)
    except subprocess.CalledProcessError as exc:
        rec.status = "failed"
        raise click.ClickException(
            f"amlt submission failed (exit code {exc.returncode})"
        )
    except FileNotFoundError:
        rec.status = "failed"
        raise click.ClickException(
            "amlt is not installed or not on PATH. "
            "Install it with: pip install amlt"
        )
    finally:
        log_record(rec)


@main.command()
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force pull even if template home exists"
)
def pull(repo_id: str | None, force: bool) -> None:
    if AJ_CONFIG_FP.exists():
        config: dict = yaml.safe_load(AJ_CONFIG_FP.read_text()) or {}
    else:
        config = {}
    if repo_id is None and "repo_id" in config:
        repo_id = config["repo_id"]
    if repo_id is None:
        raise click.ClickException("Repository ID must be provided")
    config["repo_id"] = repo_id

    if AJ_HOME.exists() and not force:
        click.echo(f"AJ home {AJ_HOME} already exists. Remove it first.")
        return
    if AJ_HOME.exists() and force:
        click.echo(f"Removing existing AJ home {AJ_HOME}")
        shutil.rmtree(AJ_HOME)

    AJ_HOME.mkdir(parents=True, exist_ok=True)
    click.echo(f"Cloning repository {repo_id} to {AJ_HOME}")
    cmd = ["git", "clone", repo_id, str(AJ_HOME)]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"Failed to clone {repo_id}: {exc.stderr.strip()}"
        )

    click.echo(f"Successfully cloned {repo_id} to {AJ_HOME}")
    git_fp = AJ_HOME / ".git"
    if git_fp.exists() and git_fp.is_dir():
        shutil.rmtree(git_fp)
        click.echo(f"Removed .git folder from {AJ_HOME}")

    AJ_CONFIG_FP.parent.mkdir(parents=True, exist_ok=True)
    with open(AJ_CONFIG_FP, "w") as f:
        yaml.dump(config, f, default_flow_style=False)
    click.echo(f"Wrote configuration to {AJ_CONFIG_FP}")


@main.command(name="list")
def list_templates() -> None:
    if not AJ_TEMPLATE_HOME.exists():
        click.echo(f"No templates found in {AJ_TEMPLATE_HOME}")
        return
    templates = list(AJ_TEMPLATE_HOME.glob("*.yaml"))
    if not templates:
        click.echo(f"No templates found in {AJ_TEMPLATE_HOME}")
        return
    click.echo("Available templates:")
    for tp in templates:
        click.echo(f"- {tp.stem}")


if __name__ == "__main__":
    main()
