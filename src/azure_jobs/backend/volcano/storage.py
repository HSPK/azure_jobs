"""Mount Azure Blob containers inside Volcano pods with blobfuse2.

The cluster has no ``blob.csi.azure.com`` node plugin registered, so a CSI
volume — inline or through a PersistentVolume — never binds and the pod stays in
``ContainerCreating``. Mounting therefore happens inside the container with
blobfuse2, which opens ``/dev/fuse`` and so needs a privileged security context.
That privilege is requested only for pods that declare storage.

Credentials are user-delegation SAS tokens minted from the submitting user's
``az login``, matching how the blob code uploader authenticates, so no account
key is required. Azure caps such a token at seven days. The token is therefore
delivered as a mounted Secret rather than an environment variable: values taken
from a Secret through ``env`` are fixed when the container starts, while a
mounted Secret is refreshed in place by the kubelet. Combined with the mount
script's refresh loop, replacing the Secret extends a running job's mount
without restarting it.
"""

from __future__ import annotations

import datetime as _dt
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from azure_jobs.errors import AJError

if TYPE_CHECKING:
    from azure_jobs.job.spec import StorageMount

_SCRIPTS_DIR = Path(__file__).parent / "scripts"

SECRET_VOLUME_NAME = "aj-blob-secrets"
SECRET_MOUNT_PATH = "/mnt/aj-blob-secrets"

SAS_PERMISSIONS = "racwdl"
# Azure rejects a user-delegation SAS beyond seven days; stay just inside it.
SAS_MAX_HOURS = 7 * 24 - 1
AZ_SAS_TIMEOUT = 120

BLOBFUSE_VERSION = "2.5.4"
# How often the pod re-reads its credential file. An hour is far below the
# token lifetime while letting a replaced token take effect promptly.
REFRESH_SECONDS = 3600


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

    @property
    def enabled(self) -> bool:
        return bool(self.mounts)

    def secret_manifest(self, namespace: str) -> dict[str, Any]:
        """Build the Secret holding every SAS for this job.

        ``stringData`` lets Kubernetes handle base64 encoding, so the token is
        never re-encoded here.
        """
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": self.secret_name, "namespace": namespace},
            "type": "Opaque",
            "stringData": {mount.key: mount.sas for mount in self.mounts},
        }

    def volume(self) -> dict[str, Any]:
        return {
            "name": SECRET_VOLUME_NAME,
            "secret": {"secretName": self.secret_name, "defaultMode": 0o400},
        }

    def volume_mount(self) -> dict[str, Any]:
        return {
            "name": SECRET_VOLUME_NAME,
            "mountPath": SECRET_MOUNT_PATH,
            "readOnly": True,
        }

    def setup_lines(self) -> list[str]:
        """Bash that installs blobfuse2 and mounts every declared container."""
        if not self.mounts:
            return []
        script = (_SCRIPTS_DIR / "mount_blobfuse.sh").read_text(encoding="utf-8")
        script = script.replace("{REFRESH_SECONDS}", str(REFRESH_SECONDS))
        script = script.replace("{BLOBFUSE_VERSION}", BLOBFUSE_VERSION)
        lines = script.splitlines()
        lines.append("")
        lines.append("if _aj_install_blobfuse2; then")
        for mount in self.mounts:
            lines.append(
                "    _aj_blob_mount "
                f"{shlex.quote(mount.key)} "
                f"{shlex.quote(mount.mount_dir)} "
                f"{shlex.quote(mount.account)} "
                f"{shlex.quote(mount.container)} "
                f"{shlex.quote(mount.sas_path)} || exit 1"
            )
        lines += [
            "else",
            "    _aj_warn 'blobfuse2 unavailable; blob mounts are required'",
            "    exit 1",
            "fi",
            "",
        ]
        return lines


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
) -> BlobMountPlan:
    """Mint credentials and describe how each declared container is mounted."""
    plan = BlobMountPlan(secret_name=f"{job_name}-blob")
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
                sas=_mint_sas(account, container, expiry_hours),
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
    "build_blob_mount_plan",
    "refresh_secret_manifest",
]
