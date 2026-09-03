"""Prepare and validate Azure Workload Identity for Volcano Pods."""

from __future__ import annotations

import json
import logging
import re
import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING, Sequence

from azure_jobs.server.az_client.auth import MGMT
from azure_jobs.shared.errors import AJError
from azure_jobs.shared.types.azure import ManagedIdentityInfo

if TYPE_CHECKING:
    from azure_jobs.server.az_client import AzureClient

WI_CLIENT_ID_ANNOTATION = "azure.workload.identity/client-id"
WI_TENANT_ID_ANNOTATION = "azure.workload.identity/tenant-id"
WI_USE_LABEL = "azure.workload.identity/use"
WI_AUDIENCE = "api://AzureADTokenExchange"
_IDENTITY_API_VERSION = "2023-01-31"
_BLOB_DATA_ROLE_IDS = frozenset(
    {
        "ba92f5b4-2d11-453d-a403-e96b0029c9fe",
        "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1",
        "b7e6dc6d-f1e8-4753-8033-0f276bb0955b",
    }
)
_GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

log = logging.getLogger(__name__)


class WorkloadIdentityError(AJError):
    """The configured Kubernetes/Azure Workload Identity is incomplete."""


@dataclass(frozen=True)
class WorkloadIdentityInfo:
    service_account: str
    client_id: str
    tenant_id: str = ""
    action: str = "existing"
    warnings: tuple[str, ...] = ()


def _kubectl(
    args: list[str],
    *,
    context: str,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = ["kubectl", *args]
    if context:
        cmd.extend(["--context", context])
    try:
        return subprocess.run(
            cmd,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except FileNotFoundError as exc:
        raise WorkloadIdentityError(
            "kubectl is required to prepare the FIC ServiceAccount."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise WorkloadIdentityError(
            f"Kubernetes Workload Identity setup timed out after {exc.timeout}s."
        ) from exc


def _service_account_annotations(
    service_account: str,
    *,
    namespace: str,
    context: str,
) -> tuple[str, str] | None:
    jsonpath = (
        "{.metadata.annotations.azure\\.workload\\.identity/client-id}"
        '{"\\t"}'
        "{.metadata.annotations.azure\\.workload\\.identity/tenant-id}"
    )
    result = _kubectl(
        [
            "get",
            "serviceaccount",
            service_account,
            "--namespace",
            namespace,
            "-o",
            f"jsonpath={jsonpath}",
        ],
        context=context,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip() or "(no output)"
        if "not found" in detail.lower():
            return None
        raise WorkloadIdentityError(
            f"Cannot read ServiceAccount {namespace}/{service_account}: {detail}"
        )
    client_id, _, tenant_id = result.stdout.strip().partition("\t")
    return client_id.strip(), tenant_id.strip()


def _validate_annotations(
    service_account: str,
    namespace: str,
    annotations: tuple[str, str] | None,
) -> WorkloadIdentityInfo:
    if annotations is None:
        raise WorkloadIdentityError(
            f"ServiceAccount {namespace}/{service_account} does not exist."
        )
    client_id, tenant_id = annotations
    if not client_id:
        raise WorkloadIdentityError(
            f"ServiceAccount {namespace}/{service_account} is missing annotation "
            f"{WI_CLIENT_ID_ANNOTATION}."
        )
    if not _GUID_RE.fullmatch(client_id):
        raise WorkloadIdentityError(
            f"ServiceAccount {namespace}/{service_account} has an invalid "
            f"{WI_CLIENT_ID_ANNOTATION}; expected a GUID."
        )
    if tenant_id and not _GUID_RE.fullmatch(tenant_id):
        raise WorkloadIdentityError(
            f"ServiceAccount {namespace}/{service_account} has an invalid "
            f"{WI_TENANT_ID_ANNOTATION}; expected a GUID."
        )
    return WorkloadIdentityInfo(service_account, client_id, tenant_id)


def validate_workload_identity(
    service_account: str,
    *,
    namespace: str,
    context: str = "",
) -> WorkloadIdentityInfo:
    """Validate an existing ServiceAccount without changing it."""
    return _validate_annotations(
        service_account,
        namespace,
        _service_account_annotations(
            service_account,
            namespace=namespace,
            context=context,
        ),
    )


def _resolve_managed_identity(
    value: str,
    *,
    azure: "AzureClient",
) -> ManagedIdentityInfo:
    if value.startswith("/"):
        data = azure.get(
            f"{MGMT}{value}?api-version={_IDENTITY_API_VERSION}"
        )
        props = data.get("properties") or {}
        parts = value.strip("/").split("/")
        identity = ManagedIdentityInfo(
            name=str(data.get("name") or parts[-1]),
            resource_group=str(data.get("resourceGroup") or parts[3]),
            subscription_id=str(data.get("subscriptionId") or parts[1]),
            location=str(data.get("location") or ""),
            id=str(data.get("id") or value),
            client_id=str(props.get("clientId") or ""),
            principal_id=str(props.get("principalId") or ""),
        )
        if not identity.client_id:
            raise WorkloadIdentityError(
                f"Managed Identity {value!r} has no client ID."
            )
        return identity
    if not _GUID_RE.fullmatch(value):
        raise WorkloadIdentityError(
            "managed_identity must be a UAI ARM resource ID or client-ID GUID."
        )
    matches = [
        identity
        for identity in azure.uai.list()
        if identity.client_id.casefold() == value.casefold()
    ]
    if len(matches) > 1:
        raise WorkloadIdentityError(
            f"Managed Identity client ID {value} resolved ambiguously."
        )
    if matches:
        return matches[0]
    return ManagedIdentityInfo(
        name="",
        resource_group="",
        subscription_id="",
        client_id=value,
    )


def _federated_credentials(
    identity: ManagedIdentityInfo,
    *,
    azure: "AzureClient",
) -> list[dict]:
    data = azure.get(
        f"{MGMT}{identity.id}/federatedIdentityCredentials"
        f"?api-version={_IDENTITY_API_VERSION}"
    )
    return list(data.get("value") or [])


def _find_service_account_by_client_id(
    client_id: str,
    *,
    namespace: str,
    context: str,
) -> str:
    jsonpath = (
        "{range .items[*]}{.metadata.name}"
        '{"\\t"}'
        "{.metadata.annotations.azure\\.workload\\.identity/client-id}"
        '{"\\n"}{end}'
    )
    result = _kubectl(
        [
            "get",
            "serviceaccounts",
            "--namespace",
            namespace,
            "-o",
            f"jsonpath={jsonpath}",
        ],
        context=context,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip() or "(no output)"
        raise WorkloadIdentityError(
            f"Cannot discover Workload Identity ServiceAccounts in "
            f"namespace {namespace}: {detail}"
        )
    matches = sorted(
        name
        for line in result.stdout.splitlines()
        for name, _, found in [line.partition("\t")]
        if found.strip().casefold() == client_id.casefold()
    )
    return matches[0] if matches else ""


def _sanitize_service_account(name: str) -> str:
    value = re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")
    value = re.sub(r"-+", "-", value)
    return value[:253].rstrip("-")


def _matching_fic_service_account(
    fics: list[dict],
    *,
    namespace: str,
) -> str:
    prefix = f"system:serviceaccount:{namespace}:"
    matches = []
    for fic in fics:
        props = fic.get("properties") or {}
        subject = str(props.get("subject") or "")
        audiences = set(props.get("audiences") or ())
        if subject.startswith(prefix) and WI_AUDIENCE in audiences:
            matches.append(subject[len(prefix) :])
    return sorted(matches)[0] if matches else ""


def _ensure_service_account(
    service_account: str,
    client_id: str,
    *,
    namespace: str,
    context: str,
) -> str:
    annotations = _service_account_annotations(
        service_account,
        namespace=namespace,
        context=context,
    )
    if annotations is None:
        manifest = {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": service_account,
                "namespace": namespace,
                "annotations": {WI_CLIENT_ID_ANNOTATION: client_id},
            },
        }
        result = _kubectl(
            [
                "create",
                "-f",
                "-",
            ],
            context=context,
            input_text=json.dumps(manifest),
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip() or "(no output)"
            raise WorkloadIdentityError(
                f"Failed to create ServiceAccount {namespace}/{service_account}: "
                f"{detail}"
            )
        return "created"

    existing_client_id, _tenant_id = annotations
    if existing_client_id == client_id:
        return "existing"
    if existing_client_id:
        raise WorkloadIdentityError(
            f"ServiceAccount {namespace}/{service_account} is already bound to "
            f"a different Managed Identity ({existing_client_id}); refusing "
            "to replace a shared identity annotation."
        )
    patch = {
        "metadata": {
            "annotations": {WI_CLIENT_ID_ANNOTATION: client_id}
        }
    }
    result = _kubectl(
        [
            "patch",
            "serviceaccount",
            service_account,
            "--namespace",
            namespace,
            "--type",
            "merge",
            "--patch",
            json.dumps(patch),
        ],
        context=context,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip() or "(no output)"
        raise WorkloadIdentityError(
            f"Failed to annotate ServiceAccount {namespace}/{service_account}: "
            f"{detail}"
        )
    return "patched"


def _storage_role_warnings(
    identity: ManagedIdentityInfo,
    storage_accounts: Sequence[str],
    *,
    azure: "AzureClient",
) -> list[str]:
    if not identity.principal_id or not storage_accounts:
        return []
    discovered = {
        account.name.casefold(): account
        for account in azure.sa.list()
    }
    warnings: list[str] = []
    for name in sorted(set(storage_accounts)):
        account = discovered.get(name.casefold())
        if account is None:
            continue
        scope = (
            f"/subscriptions/{account.subscription_id}"
            f"/resourceGroups/{account.resource_group}"
            f"/providers/Microsoft.Storage/storageAccounts/{account.name}"
        )
        url = (
            f"{MGMT}{scope}/providers/Microsoft.Authorization/roleAssignments"
            "?api-version=2022-04-01"
            f"&$filter=assignedTo('{identity.principal_id}')"
        )
        try:
            assignments = azure.get(url).get("value") or []
        except Exception as exc:
            log.exception(
                "Could not inspect Blob role assignments for %s",
                account.name,
            )
            warnings.append(
                f"Could not verify Blob Data roles on account {account.name} "
                f"({type(exc).__name__}: {exc}); set AJ_DEBUG=1 for details."
            )
            continue
        role_ids = {
            str((assignment.get("properties") or {}).get("roleDefinitionId") or "")
            .rsplit("/", 1)[-1]
            .casefold()
            for assignment in assignments
        }
        if not role_ids.intersection(_BLOB_DATA_ROLE_IDS):
            warnings.append(
                f"Managed Identity {identity.client_id} has no visible "
                f"Storage Blob Data role on account {account.name}."
            )
    return warnings


def prepare_workload_identity(
    managed_identity: str,
    service_account: str,
    *,
    namespace: str,
    context: str,
    azure: "AzureClient",
    storage_accounts: Sequence[str] = (),
) -> WorkloadIdentityInfo:
    """Resolve a UAI, ensure its ServiceAccount, and warn about missing FIC."""
    identity = _resolve_managed_identity(managed_identity, azure=azure)
    fic_warning = ""
    if identity.id:
        try:
            fics = _federated_credentials(identity, azure=azure)
        except Exception as exc:
            log.exception(
                "Could not inspect FICs for Managed Identity %s",
                identity.id,
            )
            fics = []
            fic_warning = (
                "Could not verify Federated Identity Credentials "
                f"({type(exc).__name__}: {exc}); set AJ_DEBUG=1 for details."
            )
    else:
        fics = []
    selected_sa = service_account
    if not selected_sa:
        selected_sa = _matching_fic_service_account(
            fics,
            namespace=namespace,
        )
    if not selected_sa and identity.name:
        selected_sa = _sanitize_service_account(identity.name)
    if not selected_sa:
        selected_sa = _find_service_account_by_client_id(
            identity.client_id,
            namespace=namespace,
            context=context,
        )
    if not selected_sa:
        selected_sa = f"amlt-mi-{identity.client_id[:8].lower()}"

    action = _ensure_service_account(
        selected_sa,
        identity.client_id,
        namespace=namespace,
        context=context,
    )
    info = validate_workload_identity(
        selected_sa,
        namespace=namespace,
        context=context,
    )

    warnings: list[str] = []
    if fic_warning:
        warnings.append(fic_warning)
    elif identity.id:
        expected_subject = (
            f"system:serviceaccount:{namespace}:{selected_sa}"
        )
        matching = [
            fic
            for fic in fics
            if (fic.get("properties") or {}).get("subject")
            == expected_subject
        ]
        audiences = {
            audience
            for fic in matching
            for audience in ((fic.get("properties") or {}).get("audiences") or ())
        }
        if not matching:
            warnings.append(
                f"No FIC was found for subject {expected_subject}."
            )
        elif WI_AUDIENCE not in audiences:
            warnings.append(
                f"FIC subject {expected_subject} is missing audience "
                f"{WI_AUDIENCE}."
            )
    warnings.extend(
        _storage_role_warnings(
            identity,
            storage_accounts,
            azure=azure,
        )
    )
    for warning in warnings:
        log.warning(warning)
    return WorkloadIdentityInfo(
        info.service_account,
        info.client_id,
        info.tenant_id,
        action,
        tuple(warnings),
    )


__all__ = [
    "WI_AUDIENCE",
    "WI_CLIENT_ID_ANNOTATION",
    "WI_TENANT_ID_ANNOTATION",
    "WI_USE_LABEL",
    "WorkloadIdentityError",
    "WorkloadIdentityInfo",
    "prepare_workload_identity",
    "validate_workload_identity",
]
