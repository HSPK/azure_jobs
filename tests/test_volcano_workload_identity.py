"""Tests for pre-provisioned Volcano Azure Workload Identity validation."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.server.submit.volcano.workload_identity import (
    WI_AUDIENCE,
    WorkloadIdentityError,
    prepare_workload_identity,
    validate_workload_identity,
)
from azure_jobs.shared.types.azure import ManagedIdentityInfo, StorageAccountInfo

CLIENT_ID = "11111111-2222-3333-4444-555555555555"
TENANT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def test_validate_workload_identity_reads_service_account_annotations() -> None:
    completed = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout=f"{CLIENT_ID}\t{TENANT_ID}",
        stderr="",
    )
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        return_value=completed,
    ) as run:
        info = validate_workload_identity(
            "blob-workload",
            namespace="training",
            context="cluster-a",
        )

    assert info.service_account == "blob-workload"
    assert info.client_id == CLIENT_ID
    assert info.tenant_id == TENANT_ID
    command = run.call_args.args[0]
    assert command[:4] == [
        "kubectl",
        "get",
        "serviceaccount",
        "blob-workload",
    ]
    assert command[command.index("--namespace") + 1] == "training"
    assert command[command.index("--context") + 1] == "cluster-a"


def test_validate_workload_identity_allows_webhook_default_tenant() -> None:
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        return_value=subprocess.CompletedProcess(
            ["kubectl"],
            0,
            stdout=f"{CLIENT_ID}\t",
            stderr="",
        ),
    ):
        info = validate_workload_identity(
            "blob-workload",
            namespace="training",
        )

    assert info.tenant_id == ""


@pytest.mark.parametrize(
    ("stdout", "message"),
    [
        ("\t", "missing annotation"),
        ("not-a-guid\t", "invalid.*client-id"),
        (f"{CLIENT_ID}\tnot-a-guid", "invalid.*tenant-id"),
    ],
)
def test_validate_workload_identity_rejects_bad_annotations(
    stdout: str,
    message: str,
) -> None:
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        return_value=subprocess.CompletedProcess(
            ["kubectl"],
            0,
            stdout=stdout,
            stderr="",
        ),
    ):
        with pytest.raises(WorkloadIdentityError, match=message):
            validate_workload_identity(
                "blob-workload",
                namespace="training",
            )


def test_validate_workload_identity_surfaces_kubectl_failure() -> None:
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        return_value=subprocess.CompletedProcess(
            ["kubectl"],
            1,
            stdout="",
            stderr="serviceaccounts \"missing\" not found",
        ),
    ):
        with pytest.raises(WorkloadIdentityError, match="does not exist"):
            validate_workload_identity("missing", namespace="training")


def _identity() -> ManagedIdentityInfo:
    return ManagedIdentityInfo(
        name="blob-mi",
        resource_group="rg",
        subscription_id="sub",
        id=(
            "/subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.ManagedIdentity/userAssignedIdentities/blob-mi"
        ),
        client_id=CLIENT_ID,
        principal_id="principal",
    )


def _identity_json() -> dict:
    identity = _identity()
    return {
        "name": identity.name,
        "resourceGroup": identity.resource_group,
        "subscriptionId": identity.subscription_id,
        "id": identity.id,
        "properties": {
            "clientId": identity.client_id,
            "principalId": identity.principal_id,
        },
    }


def _arm_responses(azure, fics: list[dict], *extra: dict) -> None:
    azure.get.side_effect = [
        _identity_json(),
        {"value": fics},
        *extra,
    ]


def test_prepare_uses_service_account_from_matching_fic() -> None:
    azure = MagicMock()
    fics = [
        {
            "properties": {
                "subject": "system:serviceaccount:training:fic-sa",
                "audiences": [WI_AUDIENCE],
            }
        }
    ]
    _arm_responses(azure, fics)
    reads = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout=f"{CLIENT_ID}\t{TENANT_ID}",
        stderr="",
    )
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        side_effect=[reads, reads],
    ):
        info = prepare_workload_identity(
            _identity().id,
            "",
            namespace="training",
            context="cluster-a",
            azure=azure,
        )

    assert info.service_account == "fic-sa"
    assert info.action == "existing"
    assert info.warnings == ()


def test_prepare_creates_missing_service_account() -> None:
    azure = MagicMock()
    _arm_responses(azure, [])
    missing = subprocess.CompletedProcess(
        ["kubectl"],
        1,
        stdout="",
        stderr='serviceaccounts "blob-mi" not found',
    )
    applied = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout="serviceaccount/blob-mi configured",
        stderr="",
    )
    ready = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout=f"{CLIENT_ID}\t",
        stderr="",
    )
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        side_effect=[missing, applied, ready],
    ) as run:
        info = prepare_workload_identity(
            _identity().id,
            "",
            namespace="training",
            context="cluster-a",
            azure=azure,
        )

    assert info.service_account == "blob-mi"
    assert info.action == "created"
    assert info.warnings == (
        "No FIC was found for subject "
        "system:serviceaccount:training:blob-mi.",
    )
    apply_call = run.call_args_list[1]
    assert apply_call.args[0][:3] == [
        "kubectl",
        "create",
        "-f",
    ]
    assert CLIENT_ID in apply_call.kwargs["input"]


def test_prepare_patches_service_account_missing_annotation() -> None:
    azure = MagicMock()
    _arm_responses(azure, [])
    empty = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout="\t",
        stderr="",
    )
    patched = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout="serviceaccount/blob-mi patched",
        stderr="",
    )
    ready = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout=f"{CLIENT_ID}\t",
        stderr="",
    )
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        side_effect=[empty, patched, ready],
    ) as run:
        prepare_workload_identity(
            _identity().id,
            "",
            namespace="training",
            context="cluster-a",
            azure=azure,
        )

    assert run.call_args_list[1].args[0][1:3] == [
        "patch",
        "serviceaccount",
    ]


def test_prepare_refuses_to_rebind_existing_service_account() -> None:
    azure = MagicMock()
    _arm_responses(azure, [])
    existing = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout="99999999-2222-3333-4444-555555555555\t",
        stderr="",
    )
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        return_value=existing,
    ):
        with pytest.raises(WorkloadIdentityError, match="different Managed"):
            prepare_workload_identity(
                _identity().id,
                "shared-sa",
                namespace="training",
                context="cluster-a",
                azure=azure,
            )


def test_prepare_client_id_discovers_existing_annotated_service_account() -> None:
    azure = MagicMock()
    azure.uai.list.return_value = []
    listed = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout=f"fic-sa\t{CLIENT_ID}\n",
        stderr="",
    )
    ready = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout=f"{CLIENT_ID}\t",
        stderr="",
    )
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        side_effect=[listed, ready, ready],
    ):
        info = prepare_workload_identity(
            CLIENT_ID,
            "",
            namespace="training",
            context="cluster-a",
            azure=azure,
        )

    assert info.service_account == "fic-sa"
    assert info.action == "existing"


def test_prepare_warns_when_blob_data_role_is_missing() -> None:
    azure = MagicMock()
    fics = [
        {
            "properties": {
                "subject": "system:serviceaccount:training:fic-sa",
                "audiences": [WI_AUDIENCE],
            }
        }
    ]
    azure.sa.list.return_value = [
        StorageAccountInfo(
            name="acct",
            resource_group="rg",
            subscription_id="sub",
        )
    ]
    _arm_responses(azure, fics, {"value": []})
    ready = subprocess.CompletedProcess(
        ["kubectl"],
        0,
        stdout=f"{CLIENT_ID}\t",
        stderr="",
    )
    with patch(
        "azure_jobs.server.submit.volcano.workload_identity.subprocess.run",
        side_effect=[ready, ready],
    ):
        info = prepare_workload_identity(
            _identity().id,
            "",
            namespace="training",
            context="cluster-a",
            azure=azure,
            storage_accounts=["acct"],
        )

    assert info.warnings == (
        f"Managed Identity {CLIENT_ID} has no visible Storage Blob Data role "
        "on account acct.",
    )
