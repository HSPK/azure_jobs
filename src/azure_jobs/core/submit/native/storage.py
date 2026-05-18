"""Storage mount setup and datastore management."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..models import SubmitRequest

if TYPE_CHECKING:
    from azure_jobs.core.rest_client import AzureMLClient


def _build_storage_mounts(
    request: SubmitRequest,
    client: AzureMLClient,
) -> tuple[dict[str, Any], dict[str, str], dict[str, str]]:
    """Set up storage mounts via workspace datastores.

    Creates or reuses datastores in the workspace, then builds output dicts
    and PathOnCompute properties that Singularity needs to mount storage.

    Returns:
        (outputs, path_on_compute_properties, datareference_env_vars)
    """
    outputs: dict[str, Any] = {}
    path_on_compute: dict[str, str] = {}
    dataref_env: dict[str, str] = {}

    if not request.storage:
        return outputs, path_on_compute, dataref_env

    for mount_name, mount_cfg in request.storage.items():
        account = mount_cfg.storage_account_name
        container = mount_cfg.container_name
        mount_dir = mount_cfg.mount_dir or f"/mnt/{mount_name}"
        ds_name = f"aj_{mount_name}".replace("-", "_")

        client.resources.get_or_create_datastore(
            name=ds_name,
            account_name=account,
            container_name=container,
            description=f"Created by aj for {mount_name}",
        )

        # Short-form URI — the long ARM-style azureml:// is rejected by Singularity
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
