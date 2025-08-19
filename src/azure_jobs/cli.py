import shutil
import subprocess

import click
import yaml

from .conf import read_conf
from .const import AJ_DEFAULT_TEMPLATE, AJ_TEMPLATE_HOME, AJ_RECORD, AJ_SUBMISSION_HOME
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


@main.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
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
@click.argument("command", nargs=1)
@click.argument("args", nargs=-1)
def run(command, args, template, nodes, processes, dry_run):
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
    processes = processes or conf.get("_extra", {}).get("processes", 1)
    nodes = nodes or conf.get("_extra", {}).get("nodes", 1)
    conf.pop("_extra", None)
    conf["description"] = name
    conf["jobs"][0]["name"] = name
    conf["jobs"][0]["sku"] = conf["jobs"][0]["sku"].format(
        nodes=nodes, processes=processes
    )
    cmd = conf["jobs"][0].get("command", [])
    cmd.extend([f"export AJ_NODES={nodes}", f"export AJ_PROCESSES={processes * nodes}"])
    if Path(command).is_file():
        if command.endswith(".sh"):
            cmd.append(f"bash {command} {' '.join(args)}")
        elif command.endswith(".py"):
            cmd.append(f"uv run {command} {' '.join(args)}")
    else:
        cmd.append(f"{command} {' '.join(args)}")
    conf["jobs"][0]["command"] = cmd

    submission_fp = AJ_SUBMISSION_HOME / f"{sid}.yaml"
    submission_fp.parent.mkdir(parents=True, exist_ok=True)
    with open(submission_fp, "w") as f:
        print(f"Writing submission file to {submission_fp}")
        yaml.dump(conf, f, default_flow_style=False)

    if dry_run:
        print("Dry run mode: not executing command")
        return
    amlt_command = ["amlt", "run", submission_fp, sid]
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
@click.argument("repo_id", type=str)
def pull(repo_id: str):
    pass


if __name__ == "__main__":
    main()
