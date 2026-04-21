"""Storage mount setup and datastore management."""

from __future__ import annotations

from typing import Any

from ._models import SubmitRequest


def _get_or_create_datastore(
    client: Any, ds_name: str, account: str, container: str, mount_name: str,
) -> None:
    """Ensure a blob datastore exists in the workspace (create if missing)."""
    try:
        existing = client.get_datastore(ds_name)
        if existing:
            return
    except Exception:
        pass
    try:
        client.create_or_update_datastore(
            name=ds_name,
            account_name=account,
            container_name=container,
            description=f"Created by aj for {mount_name}",
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to create datastore '{ds_name}' "
            f"(account={account}, container={container}): {exc}"
        ) from exc


def _build_storage_mounts(
    request: SubmitRequest,
    client: Any,
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
        account = mount_cfg.get("storage_account_name", "")
        container = mount_cfg.get("container_name", "")
        mount_dir = mount_cfg.get("mount_dir", f"/mnt/{mount_name}")
        ds_name = f"aj_{mount_name}".replace("-", "_")

        _get_or_create_datastore(client, ds_name, account, container, mount_name)

        # Short-form URI — the long ARM-style azureml:// is rejected by Singularity
        uri = f"azureml://datastores/{ds_name}/paths/{request.name}/"

        outputs[mount_name] = {
            "jobOutputType": "uri_folder",
            "uri": uri,
            "mode": "ReadWriteMount",
        }
        prop_key = f"AZURE_ML_OUTPUT_PathOnCompute_{mount_name}"
        path_on_compute[prop_key] = mount_dir.rstrip("/") + "/"
        dataref_env[f"AZUREML_DATAREFERENCE_{mount_name}"] = mount_dir

    return outputs, path_on_compute, dataref_env
