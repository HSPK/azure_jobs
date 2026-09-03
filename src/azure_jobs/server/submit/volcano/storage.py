"""Mount Azure Blob containers inside Volcano pods with blobfuse2.

The cluster has no ``blob.csi.azure.com`` node plugin registered, so a CSI
volume — inline or through a PersistentVolume — never binds and the pod stays in
``ContainerCreating``. Mounting therefore happens inside the container with
blobfuse2, which opens ``/dev/fuse`` and so needs a privileged security context.
That privilege is requested only for pods that declare storage.

SAS remains the compatibility default. FIC mode binds a pre-provisioned Azure
Workload Identity ServiceAccount and delegates blobfuse configuration and mount
health to ``usm blobmount``. Its default sandbox strategy runs privileged FUSE
inside a native sidecar and exposes storage to the unprivileged main container
over loopback NFS.
"""

from __future__ import annotations

import datetime as _dt
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from azure_jobs.shared.errors import AJError
from azure_jobs.shared.opts import VolcanoBlobMountOpts

if TYPE_CHECKING:
    from azure_jobs.shared.job.spec import StorageMount

_SCRIPTS_DIR = Path(__file__).parent / "scripts"

SECRET_VOLUME_NAME = "aj-blob-secrets"
SECRET_MOUNT_PATH = "/mnt/aj-blob-secrets"
NFS_READY_VOLUME_NAME = "aj-blob-ready"
NFS_READY_MOUNT_PATH = "/mnt/aj-blob-ready"
SIDECAR_CACHE_VOLUME_NAME = "aj-blob-sidecar-cache"
SIDECAR_CACHE_MOUNT_PATH = "/root/.cache/usm"
SIDECAR_MOUNT_ROOT = "/mnt/aj-blob-sidecar"

SAS_PERMISSIONS = "racwdl"
# Azure rejects a user-delegation SAS beyond seven days; stay just inside it.
SAS_MAX_HOURS = 7 * 24 - 1
AZ_SAS_TIMEOUT = 120

class BlobMountError(AJError):
    """A blob mount could not be prepared."""


@dataclass
class BlobMount:
    """One container to mount and the credential file that unlocks it."""

    key: str
    account: str
    container: str
    mount_dir: str
    sas: str

    @property
    def sas_path(self) -> str:
        return f"{SECRET_MOUNT_PATH}/{self.key}"


@dataclass
class BlobMountPlan:
    mounts: list[BlobMount] = field(default_factory=list)
    secret_name: str = ""
    options: VolcanoBlobMountOpts = field(default_factory=VolcanoBlobMountOpts)

    @property
    def enabled(self) -> bool:
        return bool(self.mounts)

    @property
    def uses_fic(self) -> bool:
        return self.enabled and self.options.uses_fic

    @property
    def strategy(self) -> str:
        return self.options.resolved_strategy

    @property
    def requires_secret(self) -> bool:
        return self.enabled and not self.uses_fic

    def secret_manifest(self, namespace: str) -> dict[str, Any]:
        """Build the Secret holding every SAS for this job.

        ``stringData`` lets Kubernetes handle base64 encoding, so the token is
        never re-encoded here.
        """
        if not self.requires_secret:
            raise BlobMountError("FIC Blob mounts do not use a SAS Secret.")
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": self.secret_name, "namespace": namespace},
            "type": "Opaque",
            "stringData": {mount.key: mount.sas for mount in self.mounts},
        }

    def volume(self) -> dict[str, Any]:
        if not self.requires_secret:
            raise BlobMountError("FIC Blob mounts do not use a SAS Secret.")
        return {
            "name": SECRET_VOLUME_NAME,
            "secret": {"secretName": self.secret_name, "defaultMode": 0o400},
        }

    def volume_mount(self) -> dict[str, Any]:
        if not self.requires_secret:
            raise BlobMountError("FIC Blob mounts do not mount a SAS Secret.")
        return {
            "name": SECRET_VOLUME_NAME,
            "mountPath": SECRET_MOUNT_PATH,
            "readOnly": True,
        }

    def setup_lines(self) -> list[str]:
        """Bash that delegates direct mounts to ``usm blobmount``."""
        if not self.mounts:
            return []
        if self.strategy != "direct":
            return []
        lines = self._load_script("usm_blobmount.sh")
        lines += ["", "_aj_install_usm_blobmount || exit 1"]
        for mount in self.mounts:
            auth = "fic" if self.uses_fic else "file"
            credential = "" if self.uses_fic else mount.sas_path
            lines.append(
                "_aj_usm_blobmount "
                f"{shlex.quote(mount.key)} "
                f"{shlex.quote(mount.mount_dir)} "
                f"{shlex.quote(mount.account)} "
                f"{shlex.quote(mount.container)} "
                f"{shlex.quote(auth)} "
                f"{shlex.quote(credential)} || exit 1"
            )
        return lines

    @staticmethod
    def _load_script(name: str) -> list[str]:
        return (_SCRIPTS_DIR / name).read_text(encoding="utf-8").splitlines()

    def sidecar_volumes(self) -> list[dict[str, Any]]:
        if not (self.uses_fic and self.strategy == "sidecar"):
            return []
        return [
            {"name": NFS_READY_VOLUME_NAME, "emptyDir": {}},
            {"name": SIDECAR_CACHE_VOLUME_NAME, "emptyDir": {}},
        ]

    def main_sidecar_volume_mounts(self) -> list[dict[str, Any]]:
        if not (self.uses_fic and self.strategy == "sidecar"):
            return []
        return [
            {
                "name": NFS_READY_VOLUME_NAME,
                "mountPath": NFS_READY_MOUNT_PATH,
            }
        ]

    def sidecar_container(self) -> dict[str, Any] | None:
        if not (self.uses_fic and self.strategy == "sidecar"):
            return None
        lines = [
            "set -euo pipefail",
            *self._load_script("usm_blobmount.sh"),
            *self._load_script("nfs_blob_sidecar.sh"),
            "",
            "rm -f /mnt/aj-blob-ready/ready /mnt/aj-blob-ready/error",
            "_aj_install_usm_blobmount",
            "_aj_install_ganesha",
        ]
        exports: list[str] = []
        for mount in self.mounts:
            internal = f"{SIDECAR_MOUNT_ROOT}/{mount.key}"
            lines.append(
                "_aj_usm_blobmount "
                f"{shlex.quote(mount.key)} "
                f"{shlex.quote(internal)} "
                f"{shlex.quote(mount.account)} "
                f"{shlex.quote(mount.container)} fic ''"
            )
            exports.append(f"{mount.key}:{internal}")
        lines.append(f"_aj_start_ganesha {shlex.quote(';'.join(exports))}")
        return {
            "name": "aj-blob-nfs",
            "image": self.options.sidecar_image,
            "imagePullPolicy": "IfNotPresent",
            "restartPolicy": "Always",
            "startupProbe": {
                "exec": {
                    "command": [
                        "/bin/sh",
                        "-c",
                        f"test -f {NFS_READY_MOUNT_PATH}/ready",
                    ]
                },
                "periodSeconds": 2,
                "failureThreshold": 300,
            },
            "securityContext": {"privileged": True},
            "resources": {
                "requests": {
                    "cpu": self.options.sidecar_cpu,
                    "memory": self.options.sidecar_memory,
                },
                "limits": {
                    "cpu": self.options.sidecar_cpu,
                    "memory": self.options.sidecar_memory,
                },
            },
            "volumeMounts": [
                {
                    "name": NFS_READY_VOLUME_NAME,
                    "mountPath": NFS_READY_MOUNT_PATH,
                },
                {
                    "name": SIDECAR_CACHE_VOLUME_NAME,
                    "mountPath": SIDECAR_CACHE_MOUNT_PATH,
                },
            ],
            "command": ["/bin/bash", "-lc"],
            "args": ["\n".join(lines)],
        }

    def main_sidecar_setup_lines(self) -> list[str]:
        if not (self.uses_fic and self.strategy == "sidecar"):
            return []
        lines = [
            *self._load_script("nfs_blob_client.sh"),
            "",
            "_aj_install_nfs_client || exit 1",
            "_aj_wait_blob_sidecar || exit 1",
        ]
        for mount in self.mounts:
            lines.append(
                "_aj_mount_blob_nfs "
                f"{shlex.quote(mount.key)} "
                f"{shlex.quote(mount.mount_dir)} || exit 1"
            )
        return lines

    def occupied_main_mount_paths(self) -> list[str]:
        paths = [mount.mount_dir for mount in self.mounts]
        if self.requires_secret:
            paths.append(SECRET_MOUNT_PATH)
        if self.uses_fic and self.strategy == "sidecar":
            paths.append(NFS_READY_MOUNT_PATH)
        return paths


def _mint_sas(account: str, container: str, expiry_hours: int) -> str:
    """Mint a user-delegation SAS with the caller's az login."""
    expiry = (
        _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(hours=expiry_hours)
    ).strftime("%Y-%m-%dT%H:%MZ")
    cmd = [
        "az",
        "storage",
        "container",
        "generate-sas",
        "--account-name",
        account,
        "--name",
        container,
        "--permissions",
        SAS_PERMISSIONS,
        "--expiry",
        expiry,
        "--auth-mode",
        "login",
        "--as-user",
        "-o",
        "tsv",
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=AZ_SAS_TIMEOUT, check=False
        )
    except FileNotFoundError as exc:
        raise BlobMountError(
            "Azure CLI ('az') not found on PATH — required to mint a SAS for "
            f"blob container {account}/{container}."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BlobMountError(
            f"`az storage container generate-sas` timed out after {exc.timeout}s "
            f"for {account}/{container}."
        ) from exc

    if result.returncode != 0:
        raise BlobMountError(
            f"Failed to mint a SAS for {account}/{container}: "
            f"{(result.stderr or result.stdout).strip()}"
        )
    sas = result.stdout.strip().strip('"')
    if not sas:
        raise BlobMountError(f"Empty SAS returned for {account}/{container}")
    return sas.lstrip("?")


def build_blob_mount_plan(
    storage: dict[str, "StorageMount"],
    job_name: str,
    expiry_hours: int = SAS_MAX_HOURS,
    options: VolcanoBlobMountOpts | None = None,
) -> BlobMountPlan:
    """Mint credentials and describe how each declared container is mounted."""
    options = options or VolcanoBlobMountOpts()
    plan = BlobMountPlan(
        secret_name=f"{job_name}-blob",
        options=options,
    )
    for key, mount in storage.items():
        account = getattr(mount, "storage_account_name", "")
        container = getattr(mount, "container_name", "")
        mount_dir = getattr(mount, "mount_dir", "") or f"/mnt/{key}"
        if not account or not container:
            raise BlobMountError(
                f"storage.{key} needs both storage_account_name and container_name"
            )
        plan.mounts.append(
            BlobMount(
                key=key,
                account=account,
                container=container,
                mount_dir=mount_dir,
                sas=(
                    ""
                    if options.uses_fic
                    else _mint_sas(account, container, expiry_hours)
                ),
            )
        )
    return plan


def refresh_secret_manifest(
    storage: dict[str, "StorageMount"],
    job_name: str,
    namespace: str,
    expiry_hours: int = SAS_MAX_HOURS,
) -> dict[str, Any]:
    """Mint fresh credentials for an existing job and return its Secret.

    Applying the result extends a running job past the seven-day token limit;
    the pod's refresh loop picks the replacement up without a restart.
    """
    plan = build_blob_mount_plan(storage, job_name, expiry_hours)
    return plan.secret_manifest(namespace)


__all__ = [
    "BlobMount",
    "BlobMountError",
    "BlobMountPlan",
    "NFS_READY_MOUNT_PATH",
    "NFS_READY_VOLUME_NAME",
    "SIDECAR_CACHE_MOUNT_PATH",
    "SIDECAR_CACHE_VOLUME_NAME",
    "build_blob_mount_plan",
    "refresh_secret_manifest",
]
