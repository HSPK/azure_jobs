"""``aj template init`` — interactive wizard for authoring a new leaf template.

The wizard always picks from live Azure data — no "create from
scratch" branches, no filename prompts. Components are auto-named
from the picked resource:

* account     → ``account/<sanitised-uai-name>.yaml``
* storage     → ``storage/default.yaml`` (append mounts)
* environment → ``environment/sing.yaml`` (Singularity only)
* template    → ``template/<leaf-name>.yaml`` (the file ``-t`` selects)

Plus two zero-config bases auto-created if missing:
``template/base.yaml`` (code upload rules) and
``environment/base.yaml`` (sla / priority / shm defaults).

Order: account → environment → storage → target → SKU → workspace → leaf name.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import click
import yaml
from rich.panel import Panel
from rich.table import Table

from azure_jobs.core import const

_STEPS = 7


# ────────────────────────────────────────────────────────────────────────
# UI primitives
# ────────────────────────────────────────────────────────────────────────


def _step(num: int, title: str, hint: str = "") -> None:
    from azure_jobs.utils.ui import console

    console.print()
    bar = f"[bold cyan][{num}/{_STEPS}][/bold cyan]"
    console.print(f"{bar} [bold]{title}[/bold]")
    if hint:
        console.print(f"     [dim]{hint}[/dim]")


def _pick_row(prompt: str, rows: list[Any]) -> Any:
    """Render a numbered list, return the selected row item."""
    from azure_jobs.utils.ui import console

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


# ────────────────────────────────────────────────────────────────────────
# component pickers — each returns the dotted base-ref for the leaf
# ────────────────────────────────────────────────────────────────────────


def _pick_account() -> str:
    """Pick a managed identity, write ``account/<name>.yaml``."""
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.utils.ui import console, dim, warning

    with console.status(
        "[bold cyan]Discovering managed identities…[/bold cyan]", spinner="dots"
    ):
        try:
            uais = AzureARMClient().identity.list()
        except Exception as exc:
            warning(f"Could not list UAIs: {exc}")
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
    """Pick a Singularity image, write ``environment/sing.yaml``.

    Returns ``(base_ref, image_label)`` so the summary panel can show
    which image was chosen.
    """
    from azure_jobs.cli.images import _fetch_sing_images
    from azure_jobs.utils.ui import console, dim, warning

    with console.status(
        "[bold cyan]Fetching Singularity images…[/bold cyan]", spinner="dots"
    ):
        try:
            images = _fetch_sing_images()
        except Exception as exc:
            warning(f"Could not list images: {exc}")
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
    _ensure_install_sh()

    data = {
        "base": "base",
        "config": {
            "target": {"service": "sing"},
            "environment": {
                "image": image,
                "setup": ["bash .azure_jobs/scripts/install.sh"],
            },
        },
    }
    path = const.AJ_HOME / "environment" / "sing.yaml"
    _write_yaml(path, data)
    dim(f"  → wrote {path}")
    return "environment.sing", image


def _pick_storage() -> str:
    """Pick one or more blob mounts, write ``storage/default.yaml``."""
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.utils.ui import console, dim, info, warning

    with console.status(
        "[bold cyan]Discovering storage accounts…[/bold cyan]", spinner="dots"
    ):
        try:
            sas = AzureARMClient().storage.list()
        except Exception as exc:
            warning(f"Could not list storage accounts: {exc}")
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


def _pick_target() -> dict[str, str]:
    """Discover Singularity VCs and let the user pick one."""
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.utils.ui import console, warning

    with console.status(
        "[bold cyan]Discovering virtual clusters…[/bold cyan]", spinner="dots"
    ):
        try:
            vcs = AzureARMClient().vc.list()
        except Exception as exc:
            warning(f"Could not discover VCs: {exc}")
            vcs = []

    if not vcs:
        return {"name": click.prompt("  Target name")}

    rows = [
        {
            "_label": f"[bold]{vc.name}[/bold]   [dim]{vc.resource_group}[/dim]",
            "vc": vc,
        }
        for vc in vcs
    ]
    vc = _pick_row("Target VC", rows)["vc"]
    return {"name": vc.name}


_SKU_SUGGESTIONS = [
    "{nodes}x40G{processes}-A100",
    "{nodes}x80G{processes}-A100",
    "{nodes}x80G{processes}-H100",
    "{nodes}x141G{processes}-H200",
]


def _pick_sku() -> str:
    rows = [{"_label": f"[bold]{s}[/bold]", "sku": s} for s in _SKU_SUGGESTIONS]
    rows.append({"_label": "[italic]custom (type your own)[/italic]", "sku": None})
    picked = _pick_row("SKU pattern", rows)
    if picked["sku"]:
        return picked["sku"]
    return click.prompt("  Custom SKU string")


# ────────────────────────────────────────────────────────────────────────
# base files
# ────────────────────────────────────────────────────────────────────────


def _pick_workspace() -> dict[str, str]:
    """Discover AML workspaces and force the user to pick one."""
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.utils.ui import console, error

    with console.status(
        "[bold cyan]Discovering AML workspaces…[/bold cyan]", spinner="dots"
    ):
        try:
            workspaces = AzureARMClient().workspace.list()
        except Exception as exc:
            error(f"Could not discover workspaces: {exc}")
            raise SystemExit(1) from exc

    if not workspaces:
        error("No AML workspaces visible to this account.")
        console.print(
            "  Make sure you are logged in (`az login`) and have Reader "
            "on at least one workspace."
        )
        raise SystemExit(1)

    rows = [
        {
            "_label": (
                f"[bold]{ws.name}[/bold]   "
                f"[dim]{ws.resource_group} · {ws.location}[/dim]"
            ),
            "ws": ws,
        }
        for ws in workspaces
    ]
    ws = _pick_row("Workspace", rows)["ws"]
    return {
        "workspace_name": ws.name,
        "resource_group": ws.resource_group,
        "subscription_id": ws.subscription_id,
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
    """Drop a default ``install.sh`` under ``.azure_jobs/scripts/`` if missing.

    The env yaml references it via
    ``setup: [bash .azure_jobs/scripts/install.sh]`` so it runs on the
    cluster before the user command. Lives inside ``.azure_jobs/`` so it
    ships with the rest of the template tree.
    """
    from azure_jobs.utils.ui import dim

    p = const.AJ_HOME / "scripts" / "install.sh"
    if p.exists():
        return
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_INSTALL_SH, encoding="utf-8")
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
                    "process_count_per_node": 1,
                    "submit_args": {"container_args": {"shm_size": "256g"}},
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


# ────────────────────────────────────────────────────────────────────────
# top-level wizard
# ────────────────────────────────────────────────────────────────────────


def _intro() -> None:
    from azure_jobs.utils.ui import console

    console.print()
    console.print(
        Panel.fit(
            "[bold]aj template init[/bold]\n"
            "[dim]Interactive wizard for creating a new leaf template.[/dim]\n\n"
            "Steps: [cyan]account · environment · storage · target · sku · workspace · name[/cyan]",
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
    target: dict[str, str],
    sku: str,
    workspace: dict[str, str],
) -> None:
    from azure_jobs.utils.ui import console

    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column(style="dim")
    table.add_column(style="bold")
    table.add_row("Account", account_ref)
    table.add_row("Environment", f"{env_ref}  [dim]({image})[/dim]")
    table.add_row("Storage", storage_ref)
    table.add_row("Target", target.get("name", "?"))
    table.add_row("SKU", sku)
    table.add_row(
        "Workspace",
        f"{workspace['workspace_name']}  [dim]({workspace['resource_group']})[/dim]",
    )
    console.print()
    console.print(Panel(table, title="[bold]Summary[/bold]", border_style="green"))


def run_wizard(leaf_name: str | None, *, force: bool) -> None:
    from azure_jobs.utils.ui import console, info, success, warning

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

    _step(4, "Target (virtual cluster)", "Which Singularity VC this leaf submits to.")
    target = _pick_target()

    _step(
        5,
        "SKU",
        "Per-node instance type — `{nodes}` / `{processes}` are filled in at submit.",
    )
    sku = _pick_sku()

    _step(6, "Workspace", "The Azure ML workspace that owns this leaf's runs.")
    workspace = _pick_workspace()

    _step(
        7,
        "Leaf name",
        "The name you'll pass to `aj run -t <name>` and the filename under .azure_jobs/template/.",
    )
    leaf_name = leaf_name or click.prompt("  Leaf name (e.g. vca100, h100)")
    leaf_path = const.AJ_TEMPLATE_HOME / f"{leaf_name}.yaml"
    if leaf_path.exists() and not force:
        if not click.confirm(f"  {leaf_path} exists — overwrite?", default=False):
            warning("Aborted.")
            return

    leaf = {
        "base": ["base", account_ref, storage_ref, env_ref],
        "config": {
            "target": {
                **target,
                "workspace_name": workspace["workspace_name"],
            },
            "_extra": {"nodes": 1, "processes": 1},
            "jobs": [{"sku": sku}],
        },
    }
    _write_yaml(leaf_path, leaf)

    _summary(
        account_ref=account_ref,
        env_ref=env_ref,
        image=image,
        storage_ref=storage_ref,
        target=target,
        sku=sku,
        workspace=workspace,
    )
    success(f"Wrote {leaf_path}")
    console.print()
    info("Try it:")
    console.print(f"  [bold cyan]aj template show {leaf_name}[/bold cyan]")
    console.print(f"  [bold cyan]aj template validate {leaf_name}[/bold cyan]")
    console.print(
        f"  [bold cyan]aj run -t {leaf_name} -d -n 1 -p 1 echo hello[/bold cyan]"
    )
