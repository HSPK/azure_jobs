"""Blob strategy: tar+gzip code, upload to Azure Blob with short-lived SAS,
pod fetches via azcopy. Bypasses kubectl-exec/PVC — use when the kubectl
gateway is unstable."""

from __future__ import annotations

import datetime as _dt
import logging
import os
import subprocess
import tempfile
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests

from azure_jobs.server.submit.archive import (
    ArchiveError,
    ArchiveMetadata,
    create_code_archive,
)
from azure_jobs.shared.job.spec import JobEvent
from azure_jobs.shared.utils.format import format_size

from ._scripts import load_script
from .base import CodeUploader, CodeUploadResult, EmitFn

if TYPE_CHECKING:
    from azure_jobs.shared.job.spec import JobSpec
    from ..config import VolcanoConfig

log = logging.getLogger(__name__)

_AZ_SAS_TIMEOUT = 30
_BLOB_UPLOAD_TIMEOUT = 600
_BLOB_API_VERSION = "2024-11-04"
_DEFAULT_BLOB_SUFFIX = "blob.core.windows.net"
_MAX_SINGLE_PUT_BYTES = 5_000 * 1024 * 1024

@dataclass
class BlobUploadOpts:
    storage_account: str = ""
    container: str = ""
    upload_dir: str = "aj-code"
    sas_expiry_days: int = 1
    pod_download_retries: int = 5

    @classmethod
    def from_extra(cls, extra: dict[str, Any] | None) -> "BlobUploadOpts":
        if not isinstance(extra, dict):
            return cls()
        code_upload = extra.get("code_upload") if isinstance(extra, dict) else None
        blob_raw = (
            code_upload.get("blob")
            if isinstance(code_upload, dict) and isinstance(code_upload.get("blob"), dict)
            else {}
        )
        return cls(
            storage_account=str(blob_raw.get("storage_account", "") or ""),
            container=str(blob_raw.get("container", "") or ""),
            upload_dir=str(blob_raw.get("upload_dir", "aj-code") or "aj-code"),
            sas_expiry_days=int(blob_raw.get("sas_expiry_days", 1) or 1),
            pod_download_retries=int(blob_raw.get("pod_download_retries", 5) or 5),
        )

class BlobUploadError(Exception):
    pass

def _generate_sas_token(
    account_name: str,
    container_name: str,
    blob_name: str,
    permissions: str,
    expiry_days: int,
) -> str:
    """Mint a blob-scoped User-Delegation SAS via the daemon's az login."""
    expiry = (
        _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=expiry_days)
    ).strftime("%Y-%m-%dT%H:%MZ")
    cmd = [
        "az",
        "storage",
        "blob",
        "generate-sas",
        "--account-name",
        account_name,
        "--container-name",
        container_name,
        "--name",
        blob_name,
        "--permissions",
        permissions,
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
            cmd,
            capture_output=True,
            text=True,
            timeout=_AZ_SAS_TIMEOUT,
            check=False,
        )
    except FileNotFoundError as exc:
        raise BlobUploadError(
            "Azure CLI ('az') not found on PATH — required to mint a User-"
            "Delegation SAS for blob upload. Install az CLI or switch back to "
            "_extra.code_upload.strategy: kubectl-exec."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BlobUploadError(
            f"`az storage container generate-sas` timed out after "
            f"{exc.timeout}s. Check your az login and network."
        ) from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        raise BlobUploadError(
            "Failed to generate SAS token via az CLI "
            f"(exit={result.returncode}).\n"
            f"command: {' '.join(cmd)}\n"
            f"stderr: {stderr or '(empty)'}\n"
            f"stdout: {stdout or '(empty)'}\n"
            "hint: run `az login` and ensure your account has 'Storage Blob "
            "Delegator' on the storage account."
        )
    token = result.stdout.strip().strip('"')
    if not token:
        raise BlobUploadError(
            "az generated an empty SAS token — refusing to upload "
            "(would produce an unauthenticated URL)."
        )
    return token

def _tar_gz(
    src_dir: Path,
    ignore: list[str],
    dest_tar: Path,
    emit: EmitFn,
) -> ArchiveMetadata:
    def _on_progress(completed: int, total: int, current: str) -> None:
        if completed == 0 or completed % 50 == 0 or completed == total:
            emit(
                JobEvent(
                    kind="upload",
                    completed=completed,
                    total=total,
                    current="tar.gz" if completed == 0 else current,
                )
            )

    try:
        metadata = create_code_archive(
            src_dir,
            dest_tar,
            ignore_patterns=ignore,
            on_progress=_on_progress,
        )
    except ArchiveError as exc:
        raise BlobUploadError(str(exc)) from exc
    return metadata

def _put_blob(blob_url: str, sas: str, local_path: Path, emit: EmitFn) -> None:
    sep = "&" if "?" in blob_url else "?"
    url = f"{blob_url}{sep}{sas}"
    size = local_path.stat().st_size
    if size > _MAX_SINGLE_PUT_BYTES:
        raise BlobUploadError(
            "Code archive is too large for one Azure Blob upload "
            f"({size} bytes > {_MAX_SINGLE_PUT_BYTES} bytes). "
            "Exclude large artifacts from the submitted code directory."
        )
    emit(
        JobEvent(
            kind="code",
            detail=f"Uploading {format_size(size)} → blob ({local_path.name})",
        )
    )
    last_exc: BaseException | None = None
    for attempt in range(1, 4):
        try:
            with local_path.open("rb") as fh:
                resp = requests.put(
                    url,
                    data=fh,
                    headers={
                        "x-ms-blob-type": "BlockBlob",
                        "x-ms-version": _BLOB_API_VERSION,
                        "Content-Type": "application/gzip",
                        "Content-Length": str(size),
                    },
                    timeout=_BLOB_UPLOAD_TIMEOUT,
                )
        except requests.RequestException as exc:
            safe_message = str(exc).replace(url, blob_url).replace(
                sas,
                "[REDACTED]",
            )
            last_exc = BlobUploadError(
                f"{type(exc).__name__}: {safe_message}"
            )
            log.warning(
                "blob PUT attempt %d failed (%s) — retrying in %ds",
                attempt,
                safe_message,
                attempt * 2,
            )
            time.sleep(attempt * 2)
            continue
        if resp.status_code in (201, 202):
            return
        body = (resp.text or "")[:1500]
        last_exc = BlobUploadError(
            f"blob PUT returned HTTP {resp.status_code} (attempt {attempt}/3)\n"
            f"url: {blob_url}\n"
            f"response: {body or '(empty)'}"
        )
        if resp.status_code in (401, 403, 404):
            break
        time.sleep(attempt * 2)
    assert last_exc is not None
    raise BlobUploadError(
        f"blob PUT failed after 3 attempts: {last_exc}"
    )

def _build_pod_setup(
    blob_url: str,
    sas: str,
    extract_dir: str,
    retries: int,
    expected_sha256: str,
) -> list[str]:
    bootstrap_retries = max(1, int(retries))
    install_lines = load_script(
        "install_azcopy.sh", RETRIES=bootstrap_retries
    )
    download_lines = load_script(
        "blob_download.sh",
        EXTRACT_DIR=extract_dir,
        URL_WITH_SAS=f"{blob_url}?{sas}",
        EXPECTED_SHA256=expected_sha256,
    )
    return [
        *install_lines,
        "",
        *download_lines,
    ]

class BlobUploader(CodeUploader):
    """tar.gz → Azure Blob (SAS) → pod fetches via azcopy."""

    name = "blob"

    def prepare(
        self,
        cfg: "VolcanoConfig",
        request: "JobSpec",
        *,
        namespace: str,
        on_event: EmitFn | None = None,
    ) -> CodeUploadResult:
        emit = on_event or (lambda _ev: None)
        opts = BlobUploadOpts.from_extra(request.extra)

        missing = [
            n
            for n, v in (
                ("storage_account", opts.storage_account),
                ("container", opts.container),
            )
            if not v
        ]
        if missing:
            return CodeUploadResult(
                ok=False,
                error=(
                    "_extra.code_upload.blob is missing required field(s): "
                    f"{', '.join(missing)}. "
                    "Provide them in the template, or fall back to strategy: kubectl-exec."
                ),
            )

        if not cfg.code_dir:
            return CodeUploadResult(
                ok=False,
                error="No code_dir resolved on VolcanoConfig — nothing to upload.",
            )
        code_path = Path(cfg.code_dir).resolve()
        if not code_path.is_dir():
            return CodeUploadResult(
                ok=False,
                error=(
                    f"code_dir is not a directory: {code_path} "
                    f"(cfg.code_dir={cfg.code_dir!r})"
                ),
            )

        account_url = f"https://{opts.storage_account}.{_DEFAULT_BLOB_SUFFIX}"

        tmp_tar: Path | None = None
        try:
            fd, tmp_name = tempfile.mkstemp(
                prefix=f"aj-code-{cfg.name}-",
                suffix=".tgz",
            )
            os.close(fd)
            tmp_tar = Path(tmp_name)
            archive = _tar_gz(
                code_path,
                list(cfg.code_ignore),
                tmp_tar,
                emit,
            )
            blob_rel = (
                f"{opts.upload_dir.strip('/')}/{archive.code_hash}.tgz"
            )
            blob_url = f"{account_url}/{opts.container}/{blob_rel}"
            extract_dir = f"/tmp/aj-code/{archive.code_hash}"
            emit(
                JobEvent(
                    kind="code",
                    detail=(
                        f"Tar.gz ready ({format_size(archive.size_bytes)}) "
                        f"→ {blob_url}"
                    ),
                )
            )

            emit(
                JobEvent(
                    kind="code",
                    detail=(
                        f"Generating upload SAS for "
                        f"{opts.storage_account}/{opts.container}…"
                    ),
                )
            )
            upload_sas = _generate_sas_token(
                opts.storage_account,
                opts.container,
                blob_rel,
                "cw",
                opts.sas_expiry_days,
            )
            _put_blob(blob_url, upload_sas, tmp_tar, emit)
            emit(
                JobEvent(
                    kind="code",
                    detail="Generating read-only SAS for pod download…",
                )
            )
            download_sas = _generate_sas_token(
                opts.storage_account,
                opts.container,
                blob_rel,
                "r",
                opts.sas_expiry_days,
            )
        except BlobUploadError as exc:
            log.exception("Blob upload failed for %s", cfg.name)
            return CodeUploadResult(
                ok=False,
                error=f"Blob upload failed: {exc}",
            )
        except Exception as exc:
            log.exception("Unexpected error during blob upload")
            return CodeUploadResult(
                ok=False,
                error=(
                    f"Blob upload error: {type(exc).__name__}: {exc}\n"
                    f"{traceback.format_exc()}"
                ),
            )
        finally:
            if tmp_tar is not None:
                try:
                    tmp_tar.unlink(missing_ok=True)
                except Exception:
                    log.debug("failed to remove %s", tmp_tar, exc_info=True)

        emit(JobEvent(kind="code", detail=f"Code uploaded to {blob_url}"))

        return CodeUploadResult(
            ok=True,
            pod_setup_lines=_build_pod_setup(
                blob_url,
                download_sas,
                extract_dir,
                opts.pod_download_retries,
                archive.code_hash,
            ),
            code_path=extract_dir,
        )

__all__ = ["BlobUploader", "BlobUploadError"]
