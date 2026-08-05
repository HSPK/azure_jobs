"""Layering guards for the client/server split.

These exist because the split is only worth anything if the contract stays
transport-neutral: the moment ``api/ports.py`` imports the Azure SDK, a second
frontend or a second backend stops being cheap to add.
"""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

import pytest

SRC = Path(__file__).parents[1] / "src" / "azure_jobs"
SHARED = SRC / "shared"
CONTRACT = SHARED / "contract"
CLIENT = SRC / "client"
SDK = SRC / "sdk"
SERVER = SRC / "server"
CLI = CLIENT / "cli"

#: Files that define the contract itself. Implementations may import more.
CONTRACT_FILES = ("models.py", "errors.py", "routes.py")

FORBIDDEN_IN_CONTRACT = (
    "azure_jobs.server.az_client",
    "azure_jobs.client.tui",
    "azure_jobs.server.submit",
    "azure_jobs.client.cli",
    "textual",
    "rich",
    "azure.identity",
)


def _imports(path: Path) -> list[str]:
    names: list[str] = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return names


@pytest.mark.parametrize("filename", CONTRACT_FILES)
def test_contract_files_are_transport_and_sdk_free(filename):
    """The contract must not know about Azure, Textual, or any transport."""
    violations = [
        name
        for name in _imports(CONTRACT / filename)
        for forbidden in FORBIDDEN_IN_CONTRACT
        if name == forbidden or name.startswith(f"{forbidden}.")
    ]
    assert violations == [], f"{filename} imports {violations}"


def test_models_do_not_import_a_socket_or_http_stack():
    """Values travel over any transport; they must not embed one."""
    for filename in ("models.py",):
        names = _imports(CONTRACT / filename)
        assert "socket" not in names
        assert "requests" not in names
        assert "http" not in names


def test_api_never_depends_on_a_frontend():
    """api/ is the lower layer: tui/ and cli/ depend on it, never the reverse.

    Regression guard — daemon.py and inprocess.py originally reached into
    ``tui.adapters.azureml`` for the Azure range reader and target catalog,
    which made the daemon depend on the dashboard.
    """
    offenders = []
    for path in SERVER.rglob("*.py"):
        for name in _imports(path):
            if name.startswith(("azure_jobs.client.tui", "azure_jobs.client.cli")):
                offenders.append(f"{path.name}: {name}")
    assert offenders == [], offenders


def test_there_is_one_canonical_job_model():
    """Two Job classes would silently diverge across the transport."""
    from azure_jobs.shared.contract.models import Job as ApiJob
    from azure_jobs.shared.contract.models import JobRef as ApiJobRef
    from azure_jobs.shared.contract.models import Target as ApiTarget
    from azure_jobs.client.tui.models import Job as TuiJob
    from azure_jobs.client.tui.models import JobRef as TuiJobRef
    from azure_jobs.client.tui.models import Target as TuiTarget

    assert TuiJob is ApiJob
    assert TuiJobRef is ApiJobRef
    assert TuiTarget is ApiTarget


#: The Azure adapter layer. Everything else in api/ reaches Azure through it.
AZURE_ADAPTER_FILES = ("backend.py", "azure.py")


def _layer_of(path: Path) -> str:
    rel = path.relative_to(SRC)
    return rel.parts[0] if len(rel.parts) > 1 else "root"


def _azure_jobs_imports(path: Path) -> list[str]:
    return [n for n in _imports(path) if n.startswith("azure_jobs")]


def test_azure_adapters_live_only_in_the_server_layer():
    """Only the server executes, so only it may reach the Azure adapters."""
    offenders = []
    for layer, root in (("shared", SHARED), ("client", CLIENT)):
        for path in root.rglob("*.py"):
            for name in _azure_jobs_imports(path):
                if "az_client" in name:
                    offenders.append(f"{layer}/{path.name}: {name}")
    assert offenders == [], offenders


def test_the_server_never_imports_the_client():
    """The daemon must run headless; importing UI code would couple them."""
    offenders = [
        f"{path.relative_to(SERVER)}: {name}"
        for path in SERVER.rglob("*.py")
        for name in _azure_jobs_imports(path)
        if name.startswith("azure_jobs.client")
    ]
    assert offenders == [], offenders


def test_the_client_never_imports_the_server():
    """Everything the client needs is on the wire, not in the server package."""
    offenders = [
        f"{path.relative_to(CLIENT)}: {name}"
        for path in CLIENT.rglob("*.py")
        for name in _azure_jobs_imports(path)
        if name.startswith("azure_jobs.server")
    ]
    assert offenders == [], offenders


def test_the_sdk_depends_on_neither_frontend_nor_server():
    """The public SDK is a peer of the frontends, not hidden inside one."""
    offenders = [
        f"{path.relative_to(SDK)}: {name}"
        for path in SDK.rglob("*.py")
        for name in _azure_jobs_imports(path)
        if name.startswith(("azure_jobs.client", "azure_jobs.server"))
    ]
    assert offenders == [], offenders


def test_shared_depends_on_neither_side():
    """Shared code is the vocabulary both sides speak; it cannot know either."""
    offenders = [
        f"{path.relative_to(SHARED)}: {name}"
        for path in SHARED.rglob("*.py")
        for name in _azure_jobs_imports(path)
        if name.startswith(("azure_jobs.client", "azure_jobs.server"))
    ]
    assert offenders == [], offenders


def test_rendering_is_client_only():
    """Rich/Textual are presentation; the daemon must not depend on them."""
    offenders = []
    for layer, root in (("shared", SHARED), ("server", SERVER)):
        for path in root.rglob("*.py"):
            for name in _imports(path):
                if name.split(".")[0] in ("rich", "textual"):
                    offenders.append(f"{layer}/{path.name}: {name}")
    assert offenders == [], offenders


def test_within_the_server_only_the_adapter_reaches_the_sdk_directly():
    """daemon/queue/watch go through backend.py, so one place owns the SDK."""
    allowed = {"backend.py", "azure.py"}
    offenders = []
    for path in SERVER.glob("*.py"):
        if path.name in allowed:
            continue
        for name in _azure_jobs_imports(path):
            if "az_client" in name or name.startswith("azure_jobs.server.submit"):
                offenders.append(f"{path.name}: {name}")
    assert offenders == [], offenders


def test_every_contract_port_has_a_route():
    """A capability that exists only in-process breaks transport parity."""
    from azure_jobs.server.app import DaemonState, create_app
    from azure_jobs.shared.contract import routes as R

    paths = {getattr(r, "path", "") for r in create_app(DaemonState()).routes}
    required = {
        R.jobs("{ws}"),
        R.jobs_fetch("{ws}"),
        R.job("{ws}", "{job_id}"),
        R.job_cancel("{ws}", "{job_id}"),
        R.job_logs("{ws}", "{job_id}"),
        R.job_log_content("{ws}", "{job_id}"),
        R.job_log_download("{ws}", "{job_id}"),
        R.workspace_info("{ws}"),
        R.datastores("{ws}"),
        R.datastore("{ws}", "{name}"),
        R.environments("{ws}"),
        R.environment_versions("{ws}", "{name}"),
        R.computes("{ws}"),
        R.quota("{ws}"),
        R.subscriptions(),
        R.storage_accounts(),
        R.identities(),
        R.instance_types(),
        R.images(),
        R.vc_quota(),
        R.account_computes(),
        R.workspace_computes(),
        R.all_jobs(),
        R.auth_status(),
        R.submissions("{ws}"),
        R.queue("{ws}"),
        R.queue_ticket("{ws}", "{ticket}"),
        R.watches("{ws}"),
        R.watch_job("{ws}", "{job_id}"),
        R.events(),
    }
    assert required <= paths, required - paths


def test_routes_are_version_prefixed():
    """Versioning by path is what lets an old client keep working."""
    from azure_jobs.shared.contract import routes as R

    for path in (R.ping(), R.info(), R.jobs("t"), R.subscriptions(), R.events()):
        assert path.startswith(f"{R.API_PREFIX}/")


def test_the_sdk_builds_urls_from_the_shared_route_table():
    """A path typo should be an import error, not a 404 at runtime."""
    offenders = []
    for path in SDK.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        # Every request goes through R.<helper>(...), never a literal path.
        if re.search(r"""["']/v\d+""", source):
            offenders.append(str(path.relative_to(SDK)))
    assert offenders == [], offenders


def test_only_the_transport_speaks_http():
    """The namespaces describe resources; httpx belongs to one module."""
    offenders = []
    for path in SDK.rglob("*.py"):
        if path.name == "_transport.py":
            continue
        for name in _imports(path):
            if name == "httpx" or name.startswith("httpx."):
                offenders.append(str(path.relative_to(SDK)))
    assert offenders == [], offenders


def test_the_server_backend_exposes_every_port():
    """The adapter keeps the port names; only the client got namespaces.

    Two vocabularies on purpose: the server's shape follows the Azure clients
    it adapts, the client's follows the CLI groups a person types.
    """
    from azure_jobs.server.backend import AzureBackend

    expected = {
        "jobs",
        "actions",
        "delete_jobs",
        "logs",
        "catalog",
        "submitter",
        "queue",
        "watcher",
        "close",
    }
    source = inspect.getsource(AzureBackend)
    for attribute in expected:
        assert (
            f"self.{attribute}" in source or f"def {attribute}" in source
        ), f"AzureBackend is missing {attribute}"


def test_server_azure_clients_follow_the_public_sdk_namespace_shape():
    """Both sides use account root + callable workspace scope + resources."""
    from azure_jobs.server.az_client import AzureClient, AzureWorkspaceClient

    with AzureClient() as azure:
        for name in ("subscription", "ws", "sku", "sa", "uai", "image", "quota", "compute"):
            assert hasattr(azure, name), name
        for legacy in (
            "subscriptions",
            "workspace",
            "instance_types",
            "storage",
            "identity",
        ):
            assert not hasattr(azure, legacy), legacy

        scoped = azure.ws("sub", "rg", "workspace")
        try:
            assert isinstance(scoped, AzureWorkspaceClient)
            for name in ("job", "log", "ds", "env", "info"):
                assert hasattr(scoped, name), name
            for legacy in ("jobs", "logs", "datastores", "environments"):
                assert not hasattr(scoped, legacy), legacy
        finally:
            scoped.close()


def test_the_sdk_covers_every_capability_the_server_offers():
    """Liskov: every port the daemon serves is reachable from a namespace."""
    from azure_jobs.sdk import AjClient
    from azure_jobs.sdk.workspace import WorkspaceClient

    workspace_namespaces = {
        "job",
        "log",
        "ds",
        "env",
        "compute",
        "quota",
        "queue",
        "watch",
    }
    source = inspect.getsource(WorkspaceClient)
    for name in workspace_namespaces:
        assert f"self.{name} = " in source, f"WorkspaceClient is missing {name}"

    account_namespaces = {
        "auth",
        "subscription",
        "ws",
        "sku",
        "sa",
        "uai",
        "image",
        "quota",
        "compute",
    }
    root = inspect.getsource(AjClient)
    for name in account_namespaces:
        assert f"self.{name} = " in root, f"AjClient is missing {name}"


def test_the_root_shorthands_reach_the_configured_workspace():
    """`d.job`, `d.workspace.job` and `d.ws().job` must be one namespace."""
    from azure_jobs.sdk import AjClient

    client = AjClient(object())
    for name in ("job", "log", "ds", "env", "queue", "watch"):
        assert getattr(client, name) is getattr(client.workspace, name)
        assert getattr(client, name) is getattr(client.ws(), name)


def test_the_server_does_not_hand_roll_a_transport():
    """uvicorn owns framing, concurrency and shutdown; we own the domain."""
    import inspect

    from azure_jobs.server import app, runner

    for module in (app, runner):
        source = inspect.getsource(module)
        assert "def serve_connection" not in source
        assert "recv(" not in source


def test_cli_never_imports_az_client():
    """Commands go through the contract so the daemon can serve them.

    A direct az_client import silently bypasses the daemon, so this is the
    invariant that keeps the split real rather than aspirational.
    """
    offenders = []
    for path in CLI.rglob("*.py"):
        for name in _imports(path):
            if name == "azure_jobs.server.az_client" or name.startswith(
                "azure_jobs.server.az_client."
            ):
                offenders.append(f"{path.name}: {name}")
    assert offenders == [], offenders


def test_cli_does_not_construct_azure_clients():
    """Catch `from azure_jobs import az_client` style access too."""
    offenders = []
    for path in CLI.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        for needle in (
            "AzureClient(",
            "AzureWorkspaceClient(",
        ):
            if needle in source:
                offenders.append(f"{path.name}: {needle}")
    assert offenders == [], offenders


#: No exemptions: signing in is `az login`, run by the user, so there is no
#: longer any client code that needs the Azure CLI at all.
AZ_EXEMPT: set[str] = set()


def test_the_client_never_acquires_an_azure_token():
    """`azure.identity` shells out to ``az`` too, so it is server-only.

    Not covered by the CLI exemption: only spawning ``az login`` needs a
    terminal, and acquiring a token does not.
    """
    offenders = []
    for path in CLIENT.rglob("*.py"):
        for name in _imports(path):
            if name == "azure.identity" or name.startswith("azure.identity."):
                offenders.append(f"{path.relative_to(CLIENT)}: {name}")
    assert offenders == [], offenders


def test_the_client_never_runs_the_azure_cli():
    """Discovery belongs to the daemon; the client asks it by workspace name.

    Without this, a command can quietly re-acquire its own ``az`` dependency
    and the daemon stops being the single execution path.
    """
    forbidden = (
        "azure_jobs.server.discovery",
        "azure_jobs.shared.config.az_cli",
    )
    offenders = []
    for path in CLIENT.rglob("*.py"):
        if path.name in AZ_EXEMPT:
            continue
        for name in _imports(path):
            if any(name == f or name.startswith(f + ".") for f in forbidden):
                offenders.append(f"{path.relative_to(CLIENT)}: {name}")
    assert offenders == [], offenders


def test_the_client_does_not_shell_out_to_az():
    """Catch a hand-rolled subprocess call that skips the import guard."""
    offenders = []
    for path in CLIENT.rglob("*.py"):
        if path.name in AZ_EXEMPT:
            continue
        source = path.read_text(encoding="utf-8")
        for needle in ('"az"', "'az'", "find_az(", "az_json("):
            if needle in source:
                offenders.append(f"{path.relative_to(CLIENT)}: {needle}")
    assert offenders == [], offenders


def test_workspace_resolution_is_server_side_only():
    """``resolve_workspace`` runs ``az``; only the daemon may call it."""
    offenders = []
    for path in CLIENT.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        if "resolve_workspace" in source or "detect_subscription(" in source:
            offenders.append(str(path.relative_to(CLIENT)))
    assert offenders == [], offenders


def test_catalog_item_does_not_shadow_payload_keys():
    """`kind` is a storage-account attribute, so the field is `category`."""
    from azure_jobs.shared.contract.models import CatalogItem

    item = CatalogItem("storage_account", "sa", {"kind": "StorageV2", "sku": "LRS"})
    assert item.category == "storage_account"
    assert item.kind == "StorageV2"
    assert item.sku == "LRS"
