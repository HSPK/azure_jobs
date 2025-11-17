import shutil
import subprocess

import click
import yaml

from .conf import read_conf
from .const import (
    AJ_DEFAULT_TEMPLATE,
    AJ_TEMPLATE_HOME,
    AJ_RECORD,
    AJ_SUBMISSION_HOME,
    AJ_HOME,
    AJ_CONFIG_FP,
)
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import uuid
import os


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
    args: list[str]


def log_record(record: SubmissionRecord):
    with open(AJ_RECORD, "a") as f:
        f.write(json.dumps(asdict(record)) + "\n")


@click.group()
@click.version_option(package_name="azure_jobs")
def main():
    pass


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
@click.argument("command", nargs=1)
@click.argument("args", nargs=-1)
def run(command, args, template, nodes, processes, dry_run, run_local, yes):
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
    name = os.getenv("AJ_NAME", Path.cwd().name) + f"_{sid}"
    processes = int(processes or conf.get("_extra", {}).get("processes", 1))
    nodes = int(nodes or conf.get("_extra", {}).get("nodes", 1))
    conf.pop("_extra", None)
    conf["description"] = name
    conf["jobs"][0]["name"] = name
    conf["jobs"][0]["sku"] = conf["jobs"][0]["sku"].format(
        nodes=nodes, processes=processes
    )
    cmd_list = conf["jobs"][0].get("command", [])
    cmd_list.extend(
        [
            f"export AJ_NODES={nodes}",
            f"export AJ_PROCESSES={processes * nodes}",
            f"export AJ_NAME={name}",
            f"export AJ_ID={sid}",
        ]
    )
    if Path(command).is_file():
        if command.endswith(".sh"):
            cmd = f"bash {command} {' '.join(args)}".strip()
        elif command.endswith(".py"):
            cmd = f"uv run {command} {' '.join(args)}".strip()
    else:
        cmd = f"{command} {' '.join(args)}".strip()
    cmd_list.append(cmd)
    print(f"Final command to execute: {cmd}")
    conf["jobs"][0]["command"] = cmd_list

    if run_local:
        subprocess.run(cmd, shell=True)
        return
    submission_fp = AJ_SUBMISSION_HOME / f"{sid}.yaml"
    submission_fp.parent.mkdir(parents=True, exist_ok=True)
    with open(submission_fp, "w") as f:
        print(f"Writing submission file to {submission_fp}")
        yaml.dump(conf, f, default_flow_style=False)

    if dry_run:
        print("Dry run mode: not executing command")
        return

    else:
        amlt_command = ["amlt", "run", submission_fp, sid]
        if yes:
            amlt_command = ["yes", "|"] + amlt_command
        rec = SubmissionRecord(
            id=sid,
            template=template,
            nodes=nodes,
            processes=processes,
            portal="azure",
            created_at=datetime.now(timezone.utc).isoformat(),
            status="success",
            command=command,
            args=args,
        )
        try:
            subprocess.run(amlt_command, shell=False)
        except Exception:
            rec.status = "failed"
        log_record(rec)


@main.command()
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force pull even if template home exists"
)
def pull(repo_id: str, force: bool):
    if AJ_CONFIG_FP.exists():
        config = read_conf(AJ_CONFIG_FP)
    else:
        config = {}
    if repo_id is None and "repo_id" in config:
        repo_id = config["repo_id"]
    if repo_id is None:
        raise click.ClickException("Repository ID must be provided")
    config["repo_id"] = repo_id

    if AJ_HOME.exists() and not force:
        print(f"AJ home {AJ_HOME} already exists. Remove it first.")
        return
    if AJ_HOME.exists() and force:
        print(f"Removing existing AJ home {AJ_HOME}")
        shutil.rmtree(AJ_HOME)
    AJ_HOME.mkdir(parents=True, exist_ok=True)
    print(f"Cloning repository {repo_id} to {AJ_HOME}")
    cmd = ["git", "clone", repo_id, str(AJ_HOME)]
    outputs = subprocess.run(cmd, check=True)
    if outputs.returncode == 0:
        print(f"Successfully cloned {repo_id} to {AJ_HOME}")
        # delete .git folder
        git_fp = AJ_HOME / ".git"
        if git_fp.exists() and git_fp.is_dir():
            shutil.rmtree(git_fp)
            print(f"Removed .git folder from {AJ_HOME}")
    else:
        print(f"Failed to clone {repo_id}: {outputs.stderr}")

    with open(AJ_CONFIG_FP, "w") as f:
        yaml.dump(config, f, default_flow_style=False)
        print(f"Wrote configuration to {AJ_CONFIG_FP}")


@main.command(name="list")
def list_templates():
    if not AJ_TEMPLATE_HOME.exists():
        print(f"No templates found in {AJ_TEMPLATE_HOME}")
        return
    templates = list(AJ_TEMPLATE_HOME.glob("*.yaml"))
    if not templates:
        print(f"No templates found in {AJ_TEMPLATE_HOME}")
        return
    print("Available templates:")
    for tp in templates:
        print(f"- {tp.stem}")


if __name__ == "__main__":
    main()
