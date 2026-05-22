"""Storage mount setup and datastore management."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

from ..models import SubmitRequest

def _datastore_name(
    account: str, container: str, mount_name: str, mount_dir: str
) -> str:
    digest = hashlib.sha1(
        "\0".join((account, container, mount_name, mount_dir)).encode("utf-8")
    ).hexdigest()[:8]
    return f"ds_{digest}"

if TYPE_CHECKING:
    from azure_jobs.core.az_client import AzureMLClient

def _build_storage_mounts(
    request: SubmitRequest,
    client: AzureMLClient,
) -> tuple[dict[str, Any], dict[str, str], dict[str, str]]:
    outputs: dict[str, Any] = {}
    path_on_compute: dict[str, str] = {}
    dataref_env: dict[str, str] = {}

    if not request.storage:
        return outputs, path_on_compute, dataref_env

    for mount_name, mount_cfg in request.storage.items():
        account = mount_cfg.storage_account_name
        container = mount_cfg.container_name
        mount_dir = mount_cfg.mount_dir or f"/mnt/{mount_name}"
        ds_name = _datastore_name(account, container, mount_name, mount_dir)

        client.datastores.get_or_create(
            name=ds_name,
            account_name=account,
            container_name=container,
            description=f"Created by aj for {mount_name}",
        )

        uri = f"azureml://datastores/{ds_name}/paths/"

        outputs[mount_name] = {
            "jobOutputType": "uri_folder",
            "uri": uri,
            "mode": "ReadWriteMount",
        }
        prop_key = f"AZURE_ML_OUTPUT_PathOnCompute_{mount_name}"
        path_on_compute[prop_key] = mount_dir.rstrip("/") + "/"
        dataref_env[f"AZUREML_DATAREFERENCE_{mount_name}"] = mount_dir

    return outputs, path_on_compute, dataref_env
