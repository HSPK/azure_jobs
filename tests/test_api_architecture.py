"""Layering guards for the client/server split.

These exist because the split is only worth anything if the contract stays
transport-neutral: the moment ``api/ports.py`` imports the Azure SDK, a second
frontend or a second backend stops being cheap to add.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

API = Path(__file__).parents[1] / "src" / "azure_jobs" / "api"

#: Files that define the contract itself. Implementations may import more.
CONTRACT_FILES = ("ports.py", "models.py", "errors.py", "rpc.py")

FORBIDDEN_IN_CONTRACT = (
    "azure_jobs.az_client",
    "azure_jobs.tui",
    "azure_jobs.backend",
    "azure_jobs.cli",
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
        for name in _imports(API / filename)
        for forbidden in FORBIDDEN_IN_CONTRACT
        if name == forbidden or name.startswith(f"{forbidden}.")
    ]
    assert violations == [], f"{filename} imports {violations}"


def test_models_do_not_import_a_socket_or_http_stack():
    """Values travel over any transport; they must not embed one."""
    for filename in ("models.py", "ports.py"):
        names = _imports(API / filename)
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
    for path in API.rglob("*.py"):
        for name in _imports(path):
            if name.startswith(("azure_jobs.tui", "azure_jobs.cli")):
                offenders.append(f"{path.name}: {name}")
    assert offenders == [], offenders


def test_there_is_one_canonical_job_model():
    """Two Job classes would silently diverge across the transport."""
    from azure_jobs.api.models import Job as ApiJob
    from azure_jobs.api.models import JobRef as ApiJobRef
    from azure_jobs.api.models import Target as ApiTarget
    from azure_jobs.tui.models import Job as TuiJob
    from azure_jobs.tui.models import JobRef as TuiJobRef
    from azure_jobs.tui.models import Target as TuiTarget

    assert TuiJob is ApiJob
    assert TuiJobRef is ApiJobRef
    assert TuiTarget is ApiTarget


#: The Azure adapter layer. Everything else in api/ reaches Azure through it.
AZURE_ADAPTER_FILES = ("backend.py", "azure.py", "typed.py")


def test_only_the_azure_adapter_talks_to_az_client():
    """The SDK stays in one layer, so the daemon cannot drift from direct calls."""
    offenders = []
    for path in API.rglob("*.py"):
        if path.name in AZURE_ADAPTER_FILES:
            continue
        for name in _imports(path):
            if name.startswith("azure_jobs.az_client"):
                offenders.append(f"{path.name}: {name}")
    # daemon.py and client.py may not reach the SDK directly; they go through
    # the adapter or the wire respectively.
    assert offenders == [], offenders


def test_every_contract_port_is_reachable_over_the_wire():
    """A capability that exists only in-process breaks transport parity."""
    from azure_jobs.api import ports
    from azure_jobs.api.daemon import METHODS

    # Ports whose methods must each map to a daemon method.
    mapping = {
        ports.JobQuery: "jobs",
        ports.JobActions: "jobs",
        ports.JobDelete: "jobs",
        ports.RangeLogSource: "logs",
        ports.RangeLogReader: "logs",
        ports.Catalog: "catalog",
        ports.SubmitQueue: "queue",
    }
    missing: list[str] = []
    for protocol, namespace in mapping.items():
        for name, member in vars(protocol).items():
            if name.startswith("_") or not callable(member):
                continue
            if f"{namespace}.{name}" not in METHODS:
                missing.append(f"{namespace}.{name}")
    assert missing == [], missing


def test_remote_and_inprocess_expose_the_same_backend_attributes():
    """A frontend must not have to ask which transport it received."""
    from azure_jobs.api.client import DaemonBackend
    from azure_jobs.api.backend import AzureBackend

    expected = {
        "target",
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
    for cls in (DaemonBackend, AzureBackend):
        source = inspect.getsource(cls)
        for attribute in expected:
            assert (
                f"self.{attribute}" in source or f"def {attribute}" in source
            ), f"{cls.__name__} is missing {attribute}"


def test_remote_ports_satisfy_the_runtime_protocols():
    """Liskov: a remote port must be substitutable for a local one."""
    from azure_jobs.api import ports
    from azure_jobs.api.client import (
        RemoteCatalog,
        RemoteJobs,
        RemoteLogReader,
        RemoteLogs,
        RemoteQueue,
        RemoteWatcher,
    )

    stub = object.__new__(RemoteJobs)
    assert isinstance(stub, ports.JobQuery)
    assert isinstance(stub, ports.JobActions)
    assert isinstance(stub, ports.JobDelete)
    assert isinstance(object.__new__(RemoteLogs), ports.RangeLogSource)
    assert isinstance(object.__new__(RemoteLogReader), ports.RangeLogReader)
    assert isinstance(object.__new__(RemoteCatalog), ports.Catalog)
    assert isinstance(object.__new__(RemoteQueue), ports.SubmitQueue)
    assert isinstance(object.__new__(RemoteWatcher), ports.Watcher)


def test_inprocess_ports_satisfy_the_runtime_protocols():
    from azure_jobs.api import ports
    from azure_jobs.api.backend import AzureCatalog, AzureJobs, AzureLogs

    stub = object.__new__(AzureJobs)
    assert isinstance(stub, ports.JobQuery)
    assert isinstance(stub, ports.JobActions)
    assert isinstance(stub, ports.JobDelete)
    assert isinstance(object.__new__(AzureLogs), ports.RangeLogSource)
    assert isinstance(object.__new__(AzureCatalog), ports.Catalog)


def test_daemon_dispatch_has_no_if_elif_chain_on_method_names():
    """Dispatch is a table; adding a method must not mean editing a branch."""
    source = (API / "daemon.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    dispatch = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "dispatch"
    )
    branches = [n for n in ast.walk(dispatch) if isinstance(n, ast.If)]
    # A single guard for "unknown method" is fine; a chain is not.
    assert len(branches) <= 1


def test_every_daemon_method_is_registered_in_one_table():
    from azure_jobs.api.daemon import METHODS, _METHODS

    assert set(METHODS) == set(_METHODS)
    assert len(METHODS) == len(set(METHODS))


CLI = Path(__file__).parents[1] / "src" / "azure_jobs" / "cli"


def test_cli_never_imports_az_client():
    """Commands go through the contract so the daemon can serve them.

    A direct az_client import silently bypasses the daemon, so this is the
    invariant that keeps the split real rather than aspirational.
    """
    offenders = []
    for path in CLI.rglob("*.py"):
        for name in _imports(path):
            if name == "azure_jobs.az_client" or name.startswith(
                "azure_jobs.az_client."
            ):
                offenders.append(f"{path.name}: {name}")
    assert offenders == [], offenders


def test_cli_does_not_construct_azure_clients():
    """Catch `from azure_jobs import az_client` style access too."""
    offenders = []
    for path in CLI.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        for needle in ("AzureARMClient(", "create_rest_client(", "AzureMLClient("):
            if needle in source:
                offenders.append(f"{path.name}: {needle}")
    assert offenders == [], offenders


def test_every_account_port_method_is_reachable_over_the_wire():
    from azure_jobs.api import ports
    from azure_jobs.api.daemon import METHODS

    missing = [
        f"account.{name}"
        for name, member in vars(ports.Account).items()
        if not name.startswith("_")
        and callable(member)
        and f"account.{name}" not in METHODS
    ]
    assert missing == [], missing


def test_catalog_item_does_not_shadow_payload_keys():
    """`kind` is a storage-account attribute, so the field is `category`."""
    from azure_jobs.api.models import CatalogItem

    item = CatalogItem("storage_account", "sa", {"kind": "StorageV2", "sku": "LRS"})
    assert item.category == "storage_account"
    assert item.kind == "StorageV2"
    assert item.sku == "LRS"
