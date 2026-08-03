"""Mount Azure Blob containers inside Volcano pods with blobfuse2.

The cluster has no ``blob.csi.azure.com`` node plugin registered, so a CSI
volume — inline or through a PersistentVolume — cannot be satisfied and the pod
would stay in ``ContainerCreating``. Mounting therefore happens inside the
container with blobfuse2, which needs ``/dev/fuse`` and so requires a
privileged security context. That privilege is only requested for pods that
actually declare storage.

Credentials are user-delegation SAS tokens minted from the submitting user's
``az login``, matching how the blob code uploader authenticates, so no account
key is needed or stored. Azure caps a user-delegation SAS at seven days: a
longer run outlives its mount, and the token has to be refreshed in the Secret.
"""

from __future__ import annotations

import datetime as _dt
import shlex
import subprocess
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from azure_jobs.errors import AJError

if TYPE_CHECKING:
    from azure_jobs.job.spec import StorageMount

# The SAS is delivered through environment variables rather than a mounted
# Secret file so the mount script stays independent of the pod's volume layout.
SAS_ENV_PREFIX = "AJ_BLOB_SAS_"
ACCOUNT_ENV_PREFIX = "AJ_BLOB_ACCOUNT_"

SAS_PERMISSIONS = "racwdl"
SAS_MAX_DAYS = 7
AZ_SAS_TIMEOUT = 120

BLOBFUSE_VERSION = "2.3.2"
BLOBFUSE_DEB_URL = (
    "https://github.com/Azure/azure-storage-fuse/releases/download/"
    f"blobfuse2-{BLOBFUSE_VERSION}/blobfuse2-{BLOBFUSE_VERSION}-Ubuntu-20.04.x86_64.deb"
)


class BlobMountError(AJError):
    """A blob mount could not be prepared."""


@dataclass
class BlobMount:
    """One container to mount, and the environment variables that unlock it."""

    key: str
    account: str
    container: str
    mount_dir: str
    sas: str
    account_env: str
    sas_env: str


@dataclass
class BlobMountPlan:
    mounts: list[BlobMount] = field(default_factory=list)
    secret_name: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.mounts)

    def secret_manifest(self, namespace: str) -> dict[str, Any]:
        """Build the Secret holding every SAS for this job.

        ``stringData`` lets Kubernetes handle the base64 encoding, so the token
        never passes through an intermediate encoding step here.
        """
        data: dict[str, str] = {}
        for mount in self.mounts:
            data[mount.sas_env] = mount.sas
            data[mount.account_env] = mount.account
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": self.secret_name, "namespace": namespace},
            "type": "Opaque",
            "stringData": data,
        }

    def env_entries(self) -> list[dict[str, Any]]:
        """Container env entries that read each value from the Secret."""
        entries: list[dict[str, Any]] = []
        for mount in self.mounts:
            for name in (mount.account_env, mount.sas_env):
                entries.append(
                    {
                        "name": name,
                        "valueFrom": {
                            "secretKeyRef": {"name": self.secret_name, "key": name}
                        },
                    }
                )
        return entries

    def setup_lines(self) -> list[str]:
        """Bash that installs blobfuse2 once and mounts every container."""
        if not self.mounts:
            return []
        lines = [
            "",
            "# --- aj: mount Azure Blob containers with blobfuse2 ---",
            "_aj_blobfuse_setup() {",
            "    command -v blobfuse2 >/dev/null 2>&1 && return 0",
            "    echo '[aj] installing blobfuse2'",
            "    export DEBIAN_FRONTEND=noninteractive",
            "    apt-get -qq update >/dev/null 2>&1 || true",
            "    apt-get -qq install -y --no-install-recommends "
            "ca-certificates wget fuse3 >/dev/null 2>&1 || true",
            "    _aj_deb=$(mktemp /tmp/blobfuse2-XXXXXX.deb)",
            f"    wget -q -O \"$_aj_deb\" {shlex.quote(BLOBFUSE_DEB_URL)} || return 1",
            "    dpkg -i \"$_aj_deb\" >/dev/null 2>&1 || "
            "apt-get -qq -f install -y >/dev/null 2>&1 || true",
            "    rm -f \"$_aj_deb\"",
            "    command -v blobfuse2 >/dev/null 2>&1",
            "}",
            "",
            "_aj_blob_mount() {",
            "    # $1 mount dir, $2 account, $3 container, $4 sas",
            "    local dir=\"$1\" account=\"$2\" container=\"$3\" sas=\"$4\"",
            "    if mountpoint -q \"$dir\" 2>/dev/null; then",
            "        echo \"[aj] $dir already mounted\"",
            "        return 0",
            "    fi",
            "    local cache=\"/tmp/aj-blobfuse-cache/${account}-${container}\"",
            "    local cfg=\"/tmp/aj-blobfuse-${account}-${container}.yaml\"",
            "    mkdir -p \"$dir\" \"$cache\"",
            "    # The cache directory must start empty or blobfuse2 refuses to mount.",
            "    rm -rf \"${cache:?}/\"* 2>/dev/null || true",
            "    umask 077",
            "    cat >\"$cfg\" <<AJ_BLOBFUSE_CFG",
            "file_cache:",
            "  path: ${cache}",
            "components: [libfuse, file_cache, attr_cache, azstorage]",
            "libfuse:",
            "  attribute-expiration-sec: 120",
            "  entry-expiration-sec: 120",
            "  negative-entry-expiration-sec: 240",
            "attr_cache:",
            "  timeout-sec: 7200",
            "azstorage:",
            "  type: block",
            "  account-name: ${account}",
            "  endpoint: https://${account}.blob.core.windows.net/",
            "  container: ${container}",
            "  mode: sas",
            "  sas: ${sas}",
            "AJ_BLOBFUSE_CFG",
            "    umask 022",
            "    if blobfuse2 mount \"$dir\" --config-file=\"$cfg\" -o allow_other; then",
            "        echo \"[aj] mounted ${account}/${container} at $dir\"",
            "    else",
            "        echo \"[aj] failed to mount ${account}/${container} at $dir\" >&2",
            "        return 1",
            "    fi",
            "}",
            "",
            "if _aj_blobfuse_setup; then",
        ]
        for mount in self.mounts:
            lines.append(
                "    _aj_blob_mount "
                f"{shlex.quote(mount.mount_dir)} "
                f'"${mount.account_env}" '
                f"{shlex.quote(mount.container)} "
                f'"${mount.sas_env}" || true'
            )
        lines += [
            "else",
            "    echo '[aj] blobfuse2 unavailable; blob mounts skipped' >&2",
            "fi",
            "# --- aj: end blob mounts ---",
            "",
        ]
        return lines


def _mint_sas(account: str, container: str, expiry_days: int) -> str:
    """Mint a user-delegation SAS with the caller's az login."""
    expiry = (
        _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=expiry_days)
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


def _env_suffix(key: str) -> str:
    """Turn a storage key into an environment-variable-safe suffix."""
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in key).upper()
    return cleaned.strip("_") or "STORAGE"


def build_blob_mount_plan(
    storage: dict[str, "StorageMount"],
    job_name: str,
    expiry_days: int = SAS_MAX_DAYS,
) -> BlobMountPlan:
    """Mint credentials and describe how each declared container is mounted."""
    plan = BlobMountPlan(secret_name=f"{job_name}-blob")
    seen: set[str] = set()
    for key, mount in storage.items():
        account = getattr(mount, "storage_account_name", "")
        container = getattr(mount, "container_name", "")
        mount_dir = getattr(mount, "mount_dir", "") or f"/mnt/{key}"
        if not account or not container:
            raise BlobMountError(
                f"storage.{key} needs both storage_account_name and container_name"
            )
        suffix = _env_suffix(key)
        if suffix in seen:
            raise BlobMountError(
                f"storage keys collide after normalisation: {key} -> {suffix}"
            )
        seen.add(suffix)
        plan.mounts.append(
            BlobMount(
                key=key,
                account=account,
                container=container,
                mount_dir=mount_dir,
                sas=_mint_sas(account, container, expiry_days),
                account_env=f"{ACCOUNT_ENV_PREFIX}{suffix}",
                sas_env=f"{SAS_ENV_PREFIX}{suffix}",
            )
        )
    return plan


__all__ = [
    "BlobMount",
    "BlobMountError",
    "BlobMountPlan",
    "build_blob_mount_plan",
]
