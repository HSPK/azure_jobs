"""aj template init — interactive wizard for authoring leaf templates."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import click
import yaml
from rich.panel import Panel
from rich.table import Table

from azure_jobs.shared import const
from azure_jobs.shared.errors import AJError

log = logging.getLogger(__name__)

_STEPS = 4


def _step(num: int, title: str, hint: str = "") -> None:
    from azure_jobs.client.ui import console

    console.print()
    bar = f"[bold cyan][{num}/{_STEPS}][/bold cyan]"
    console.print(f"{bar} [bold]{title}[/bold]")
    if hint:
        console.print(f"     [dim]{hint}[/dim]")


def _pick_row(prompt: str, rows: list[Any]) -> Any:
    from azure_jobs.client.ui import console

    console.print()
    for i, row in enumerate(rows, 1):
        console.print(f"  [yellow]{i:>2}.[/yellow] {row['_label']}")
    console.print()

    raw = click.prompt(f"  {prompt} (1-{len(rows)})", default="1")
    try:
        idx = int(raw) - 1
        if 0 <= idx < len(rows):
            return rows[idx]
    except (TypeError, ValueError):
        pass
    raise click.ClickException(f"Invalid selection: {raw}")


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True))


_SAFE_NAME = re.compile(r"[^a-z0-9._-]+")


def _sanitise(name: str) -> str:
    return _SAFE_NAME.sub("-", name.lower()).strip("-") or "default"


def _pick_account() -> str:
    from azure_jobs import connect
    from azure_jobs.client.ui import console, dim, warning

    with console.status(
        "[bold cyan]Discovering managed identities…[/bold cyan]", spinner="dots"
    ):
        try:
            with connect() as d:
                uais = d.uai.list()
        except AJError:
            # An unreachable daemon already explains how to recover;
            # wrapping it again would bury the instructions.
            raise
        except Exception as exc:
            log.exception("Could not list UAIs")
            warning(
                f"Could not list UAIs ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            uais = []

    if uais:
        rows = [
            {
                "_label": f"[bold]{u.name}[/bold]   [dim]{u.resource_group} · {u.location}[/dim]",
                "uai": u,
            }
            for u in uais
        ]
        picked = _pick_row("Managed identity", rows)
        uai = picked["uai"]
        name = _sanitise(uai.name)
        uai_id = uai.id
    else:
        uai_id = click.prompt("  Paste UAI ARM ID")
        name = _sanitise(uai_id.rsplit("/", 1)[-1])

    data = {
        "base": None,
        "config": {
            "jobs": [{"submit_args": {"env": {"_AZUREML_SINGULARITY_JOB_UAI": uai_id}}}]
        },
    }
    path = const.AJ_HOME / "account" / f"{name}.yaml"
    _write_yaml(path, data)
    dim(f"  → wrote {path}")
    return f"account.{name}"


def _pick_environment() -> tuple[str, str]:
    from azure_jobs.client.cli.images import _fetch_sing_images
    from azure_jobs.client.ui import console, dim, warning

    with console.status(
        "[bold cyan]Fetching Singularity images…[/bold cyan]", spinner="dots"
    ):
        try:
            images = _fetch_sing_images()
        except AJError:
            # An unreachable daemon already explains how to recover;
            # wrapping it again would bury the instructions.
            raise
        except Exception as exc:
            log.exception("Could not list Singularity images")
            warning(
                f"Could not list images ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            images = []

    if images:
        rows = [
            {
                "_label": f"[bold]amlt-sing/{img['name']}[/bold]",
                "image": f"amlt-sing/{img['name']}",
            }
            for img in images
        ]
        image = _pick_row("Image", rows)["image"]
    else:
        image = click.prompt("  Image")

    _ensure_environment_base()
    _ensure_copy_ssh_sh()
    _ensure_install_sh()

    data = {
        "base": "base",
        "config": {
            "target": {"service": "sing"},
            "environment": {
                "image": image,
                "setup": [
                    "bash .azure_jobs/scripts/copy_ssh.sh",
                    "bash .azure_jobs/scripts/install.sh",
                ],
            },
        },
    }
    path = const.AJ_HOME / "environment" / "sing.yaml"
    _write_yaml(path, data)
    dim(f"  → wrote {path}")
    return "environment.sing", image


def _pick_storage() -> str:
    from azure_jobs import connect
    from azure_jobs.client.ui import console, dim, info, warning

    with console.status(
        "[bold cyan]Discovering storage accounts…[/bold cyan]", spinner="dots"
    ):
        try:
            with connect() as d:
                sas = d.sa.list()
        except AJError:
            raise
        except Exception as exc:
            log.exception("Could not list storage accounts")
            warning(
                f"Could not list storage accounts ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            sas = []

    mounts: dict[str, dict[str, str]] = {}
    while True:
        if sas:
            rows = [
                {
                    "_label": f"[bold]{s.name}[/bold]   [dim]{s.resource_group} · {s.location}[/dim]",
                    "name": s.name,
                }
                for s in sas
            ]
            account = _pick_row(f"Storage account ({len(mounts) + 1})", rows)["name"]
        else:
            account = click.prompt("  Storage account name")
        container = click.prompt("  Container name")
        alias = click.prompt("  Mount alias", default=container)
        mount_dir = click.prompt("  Mount path", default=f"/mnt/{alias}")
        mounts[alias] = {
            "storage_account_name": account,
            "container_name": container,
            "mount_dir": mount_dir,
        }
        info(f"  ✓ {alias} → {account}/{container} at {mount_dir}")
        if not click.confirm("  Add another mount?", default=False):
            break

    data = {"base": None, "config": {"storage": mounts}}
    path = const.AJ_HOME / "storage" / "default.yaml"
    _write_yaml(path, data)
    dim(f"  → wrote {path}")
    return "storage.default"


def _sku_for(accelerator: str, gpu_memory: int) -> str:
    if gpu_memory > 0:
        return f"{{nodes}}x{gpu_memory}G{{processes}}-{accelerator}"
    return "{nodes}xC{processes}"


def _generate_leaves(
    *,
    base_refs: list[str],
    workspace: dict[str, str],
    force: bool,
) -> list[dict[str, str]]:
    from azure_jobs import connect
    from azure_jobs.client.ui import console, error, warning

    with console.status(
        "[bold cyan]Fetching quota across visible subscriptions…[/bold cyan]",
        spinner="dots",
    ):
        try:
            with connect() as d:
                vcs = d.quota.list()
        except AJError:
            raise
        except Exception as exc:
            log.exception("Could not fetch VC quota")
            error(
                f"Could not fetch VC quota ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            raise SystemExit(1) from exc

    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, int]] = set()

    for vc in vcs:
        for sq in vc.quotas:
            ul = sq.user_limit
            if not (ul and ul.limit > 0):
                continue
            if not sq.accelerator or sq.gpu_memory <= 0:
                continue
            key = (vc.name, sq.accelerator, sq.gpu_memory)
            if key in seen:
                continue
            seen.add(key)

            leaf_name = f"{vc.name}_{sq.accelerator}_{sq.gpu_memory}"
            leaf_path = const.AJ_TEMPLATE_HOME / f"{leaf_name}.yaml"
            status = "wrote"
            if leaf_path.exists() and not force:
                status = "skipped"
            else:
                leaf = {
                    "base": list(base_refs),
                    "config": {
                        "target": {
                            "name": vc.name,
                            "workspace_name": workspace["workspace_name"],
                            "gpus_per_node": 1,
                        },
                        "jobs": [
                            {
                                "sku": _sku_for(
                                    sq.accelerator,
                                    sq.gpu_memory,
                                ),
                                "instance_count": 1,
                            }
                        ],
                    },
                }
                _write_yaml(leaf_path, leaf)
            rows.append(
                {
                    "leaf": leaf_name,
                    "vc": vc.name,
                    "accelerator": sq.accelerator,
                    "memory": str(sq.gpu_memory),
                    "status": status,
                }
            )

    if not rows:
        warning(
            "No quota with user_limit > 0 found — no leaves generated. "
            "Run `aj quota list` to inspect."
        )
    return rows


def _pick_workspace() -> dict[str, str]:
    from azure_jobs import connect
    from azure_jobs.client.cli._workspace_setup import _target_workspace_row
    from azure_jobs.client.ui import console, error

    with console.status(
        "[bold cyan]Discovering AML workspaces…[/bold cyan]", spinner="dots"
    ):
        try:
            with connect() as d:
                targets = d.ws.list()
        except AJError:
            raise
        except Exception as exc:
            log.exception("Could not discover workspaces")
            error(
                f"Could not discover workspaces ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            raise SystemExit(1) from exc

    if not targets:
        error("No AML workspaces visible to this account.")
        console.print(
            "  Make sure you are logged in (`az login`) and have Reader "
            "on at least one workspace."
        )
        raise SystemExit(1)

    workspaces = [_target_workspace_row(target) for target in targets]
    rows = [
        {
            "_label": (
                f"[bold]{workspace['name']}[/bold]   "
                f"[dim]{workspace['resource_group']} · "
                f"{workspace['location']}[/dim]"
            ),
            "workspace": workspace,
        }
        for workspace in workspaces
    ]
    workspace = _pick_row("Workspace", rows)["workspace"]
    return {
        "workspace_name": workspace["name"],
        "resource_group": workspace["resource_group"],
        "subscription_id": workspace["subscription_id"],
    }


_INSTALL_SH = """\
echo "Installing dependencies and setting up environment..."

set -eo pipefail

exec 1>/dev/null

$SUDO apt-get update
if [ -d /opt/conda ]; then
    $SUDO mv /opt/conda /opt/conda.bak
fi
$SUDO apt-get install apt-utils git ffmpeg libsm6 libxext6 jq curl -y
$SUDO apt-get install software-properties-common -y
$SUDO add-apt-repository ppa:deadsnakes/ppa -y
$SUDO apt-get update
curl -LsSf https://astral.sh/uv/install.sh | sh
$SUDO cp $HOME/.local/bin/uv /usr/local/bin
$SUDO cp $HOME/.local/bin/uvx /usr/local/bin
uv python install 3.10
"""


def _ensure_install_sh() -> None:
    from azure_jobs.client.ui import dim

    p = const.AJ_HOME / "scripts" / "install.sh"
    if p.exists():
        return
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_INSTALL_SH, encoding="utf-8")
    p.chmod(0o755)
    dim(f"  → wrote {p}")


_COPY_SSH_SH = r"""#!/usr/bin/env bash
# Materialise SSH into /tmp/aj-ssh and export GIT_SSH_COMMAND so
# git/uv work regardless of $HOME. /tmp/.aj_ssh_env is sourced by
# the user-command preamble.

set -uo pipefail

DST="/tmp/aj-ssh"
ENV_FILE="/tmp/.aj_ssh_env"
SRC="${AJ_SSH_SRC:-./.ssh}"

if [[ -z "${HOME:-}" || "${HOME}" == "/" || ! -w "${HOME}" ]]; then
    export HOME="/tmp/aj-home-$(id -u)"
    mkdir -p "${HOME}"
fi
export USER="${USER:-$(id -un 2>/dev/null || echo "uid$(id -u)")}"

mkdir -p "${DST}" && chmod 700 "${DST}"
[[ -d "${SRC}" ]] && cp -rL "${SRC}/." "${DST}/" 2>/dev/null || true
chmod 600 "${DST}"/id_* 2>/dev/null || true
touch "${DST}/known_hosts"

OPTS="-o UserKnownHostsFile=${DST}/known_hosts -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes"
for k in "${DST}"/id_{ed25519,rsa,ecdsa}; do
    [[ -f "${k}" ]] && OPTS+=" -i ${k}"
done

cat >"${ENV_FILE}" <<EOF
export HOME="${HOME}"
export USER="${USER}"
export AJ_SSH_DIR="${DST}"
export GIT_SSH_COMMAND="ssh ${OPTS}"
EOF

echo "[copy_ssh] ${DST}:" && ls -la "${DST}"
"""


def _ensure_copy_ssh_sh() -> None:
    from azure_jobs.client.ui import dim

    p = const.AJ_HOME / "scripts" / "copy_ssh.sh"
    if p.exists():
        return
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_COPY_SSH_SH, encoding="utf-8")
    p.chmod(0o755)
    dim(f"  → wrote {p}")


def _ensure_environment_base() -> None:
    p = const.AJ_HOME / "environment" / "base.yaml"
    if p.exists():
        return
    data = {
        "base": None,
        "config": {
            "jobs": [
                {
                    "identity": "managed",
                    "sla_tier": "Premium",
                    "priority": "high",
                    "instance_count": 1,
                    "process_count_per_node": 1,
                }
            ]
        },
    }
    _write_yaml(p, data)


def _ensure_template_base() -> None:
    p = const.AJ_TEMPLATE_HOME / "base.yaml"
    if p.exists():
        return
    data = {
        "config": {
            "code": {
                "local_dir": "$CONFIG_DIR/../../",
                "ignore": [
                    ".git/",
                    ".venv/",
                    "__pycache__/",
                    "wandb/",
                    "runs/",
                    "log/",
                    "output/",
                    "outputs/",
                    "checkpoints/",
                ],
            }
        },
    }
    _write_yaml(p, data)


def _intro() -> None:
    from azure_jobs.client.ui import console

    console.print()
    console.print(
        Panel.fit(
            "[bold]aj template init[/bold]\n"
            "[dim]Interactive wizard for bootstrapping leaf templates.[/dim]",
            border_style="cyan",
            padding=(1, 4),
        )
    )


def _summary(
    *,
    account_ref: str,
    env_ref: str,
    image: str,
    storage_ref: str,
    workspace: dict[str, str],
    leaves: list[dict[str, str]],
) -> None:
    from azure_jobs.client.ui import console

    meta = Table(show_header=False, box=None, pad_edge=False)
    meta.add_column(style="dim")
    meta.add_column(style="bold")
    meta.add_row("Account", account_ref)
    meta.add_row("Environment", f"{env_ref}  [dim]({image})[/dim]")
    meta.add_row("Storage", storage_ref)
    meta.add_row(
        "Workspace",
        f"{workspace['workspace_name']}  [dim]({workspace['resource_group']})[/dim]",
    )
    console.print()
    console.print(
        Panel(meta, title="[bold]Shared components[/bold]", border_style="green")
    )

    if not leaves:
        return

    table = Table(title="Generated leaves", show_lines=False)
    table.add_column("Leaf", style="bold")
    table.add_column("VC", style="cyan")
    table.add_column("Accel")
    table.add_column("Mem (GB)", justify="right")
    table.add_column("Status", style="dim")
    for r in leaves:
        table.add_row(r["leaf"], r["vc"], r["accelerator"], r["memory"], r["status"])
    console.print(table)


def run_wizard(leaf_name: str | None, *, force: bool) -> None:
    from azure_jobs.client.ui import console, info, success, warning

    if leaf_name:
        warning(
            f"Positional name '{leaf_name}' ignored — leaves are auto-named "
            "{vc}_{accelerator}_{memory}.yaml."
        )

    _intro()

    _ensure_template_base()

    _step(
        1, "Account (managed identity)", "Picks an Azure managed identity for the job."
    )
    account_ref = _pick_account()

    _step(
        2,
        "Environment (Singularity image)",
        "Container image baked by the Singularity team.",
    )
    env_ref, image = _pick_environment()

    _step(
        3,
        "Storage (blob mounts)",
        "One or more storage account / container pairs to mount inside the job.",
    )
    storage_ref = _pick_storage()

    _step(4, "Workspace", "The Azure ML workspace that owns these leaves' runs.")
    workspace = _pick_workspace()

    info("Auto-generating one leaf per (VC, accelerator, memory) with positive quota…")
    leaves = _generate_leaves(
        base_refs=["base", account_ref, storage_ref, env_ref],
        workspace=workspace,
        force=force,
    )

    _summary(
        account_ref=account_ref,
        env_ref=env_ref,
        image=image,
        storage_ref=storage_ref,
        workspace=workspace,
        leaves=leaves,
    )
    if leaves:
        wrote = sum(1 for r in leaves if r["status"] == "wrote")
        skipped = len(leaves) - wrote
        msg = f"Generated {wrote} leaf template(s)"
        if skipped:
            msg += f"; {skipped} already existed (use -f to overwrite)"
        success(msg + ".")
        console.print()
        first = leaves[0]["leaf"]
        info("Try one:")
        console.print(f"  [bold cyan]aj template show {first}[/bold cyan]")
        console.print(f"  [bold cyan]aj template validate {first}[/bold cyan]")
        console.print(
            f"  [bold cyan]aj run -t {first} -d -n 1 -p 1 echo hello[/bold cyan]"
        )
