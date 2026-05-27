"""Code upload to a PVC via a transient kubectl-managed pod."""

from __future__ import annotations

import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Callable

import yaml

from azure_jobs.utils.format import format_size
from azure_jobs.utils.fs import CodeFile, walk_code

from azure_jobs.job.models import SubmitEvent
from . import constants as C
from .config import VolcanoConfig

def write_filelist(selected: list[CodeFile]) -> str:
    """Write a NUL-separated list of relpaths for tar --null -T."""
    fd = tempfile.NamedTemporaryFile(
        mode="wb", prefix="aj-upload-list-", suffix=".lst", delete=False
    )
    with fd as f:
        for cf in selected:
            f.write(cf.rel.encode("utf-8"))
            f.write(b"\x00")
    return fd.name

def stream_tar(
    *,
    src_dir: Path,
    filelist_path: str,
    pod_name: str,
    namespace: str,
    dest_dir: str,
    ctx_args: list[str],
    total_files: int,
    emit: Callable[[SubmitEvent], None],
) -> subprocess.CompletedProcess[str]:
    """Stream tar c into kubectl exec tar x, emitting per-file events."""
    tar_proc = subprocess.Popen(
        [
            "tar",
            "cvf",
            "-",
            "-C",
            str(src_dir),
            "--no-recursion",
            "--null",
            "-T",
            filelist_path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    completed = [0]

    def _drain_stderr() -> None:
        assert tar_proc.stderr is not None
        for raw in tar_proc.stderr:
            line = raw.decode("utf-8", errors="replace").rstrip()
            if not line or line.startswith("tar:"):
                continue
            cur = line[2:] if line.startswith("./") else line
            completed[0] += 1
            emit(
                SubmitEvent(
                    kind="upload",
                    completed=completed[0],
                    total=total_files,
                    skipped=0,
                    current=cur,
                )
            )

    drainer = threading.Thread(target=_drain_stderr, daemon=True)
    drainer.start()

    try:
        return subprocess.run(
            [
                "kubectl",
                "exec",
                "-i",
                pod_name,
                f"--namespace={namespace}",
                *ctx_args,
                "--",
                "tar",
                "xf",
                "-",
                "-C",
                dest_dir,
            ],
            stdin=tar_proc.stdout,
            capture_output=True,
            text=True,
            timeout=C.KUBECTL_EXEC_TIMEOUT,
        )
    finally:
        if tar_proc.stdout is not None:
            tar_proc.stdout.close()
        tar_proc.wait()
        drainer.join(timeout=C.STDERR_DRAIN_JOIN_TIMEOUT)

def upload_files_to_pvc(
    *,
    src_dir: Path,
    files: list[CodeFile],
    pvc_name: str,
    pvc_mount_dir: str,
    dest_dir: str,
    pod_name: str,
    namespace: str,
    context: str | None,
    on_event: Callable[[SubmitEvent], None] | None = None,
    status_kind: str = "code",
    label: str = "Files",
) -> bool:
    """Upload files (relative to src_dir) into dest_dir on a PVC."""
    emit = on_event or (lambda _ev: None)

    def status(kind: str, detail: str) -> None:
        emit(SubmitEvent(kind=kind, detail=detail))

    if not files:
        status(status_kind, f"No {label.lower()} to upload")
        return True

    ctx_args = ["--context", context] if context else []
    total_bytes = sum(cf.size for cf in files)
    status(
        status_kind,
        f"Uploading {label.lower()} to PVC {pvc_name}:{dest_dir} "
        f"({len(files)} files, {format_size(total_bytes)})",
    )

    pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": namespace},
        "spec": {
            "restartPolicy": "Never",
            "volumes": [
                {
                    "name": C.PVC_VOLUME_NAME,
                    "persistentVolumeClaim": {"claimName": pvc_name},
                }
            ],
            "containers": [
                {
                    "name": "upload",
                    "image": C.UPLOAD_POD_IMAGE,
                    "resources": C.UPLOAD_POD_RESOURCES,
                    "volumeMounts": [
                        {"name": C.PVC_VOLUME_NAME, "mountPath": pvc_mount_dir}
                    ],
                    "command": [
                        "sh",
                        "-c",
                        f"mkdir -p {dest_dir} && exec sleep {C.UPLOAD_POD_IDLE_SECONDS}",
                    ],
                }
            ],
        },
    }

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=C.POD_YAML_SUFFIX,
        prefix=C.POD_YAML_PREFIX,
        delete=False,
    ) as f:
        f.write(yaml.dump(pod_spec, default_flow_style=False))
        pod_yaml_path = f.name
    filelist_path = write_filelist(files)

    try:
        result = subprocess.run(
            ["kubectl", "apply", "-f", pod_yaml_path, *ctx_args],
            capture_output=True,
            text=True,
            timeout=C.KUBECTL_APPLY_TIMEOUT,
        )
        if result.returncode != 0:
            err = result.stderr.strip() or result.stdout.strip()
            status("error", f"Upload pod creation failed: {err}")
            return False

        result = subprocess.run(
            [
                "kubectl",
                "wait",
                "--for=condition=Ready",
                f"pod/{pod_name}",
                f"--namespace={namespace}",
                f"--timeout={C.KUBECTL_WAIT_TIMEOUT}s",
                *ctx_args,
            ],
            capture_output=True,
            text=True,
            timeout=C.KUBECTL_WAIT_SUBPROCESS_TIMEOUT,
        )
        if result.returncode != 0:
            status("error", "Upload pod failed to become ready")
            return False

        exec_result = stream_tar(
            src_dir=src_dir,
            filelist_path=filelist_path,
            pod_name=pod_name,
            namespace=namespace,
            dest_dir=dest_dir,
            ctx_args=ctx_args,
            total_files=len(files),
            emit=emit,
        )
        if exec_result.returncode != 0:
            err = exec_result.stderr.strip() or exec_result.stdout.strip()
            status("error", f"{label} copy failed: {err}")
            return False

        status(status_kind, f"{label} uploaded to {dest_dir}")
        return True

    except subprocess.TimeoutExpired:
        status("error", f"{label} upload timed out")
        return False
    except Exception as exc:
        status("error", f"{label} upload error: {exc}")
        return False
    finally:
        Path(pod_yaml_path).unlink(missing_ok=True)
        Path(filelist_path).unlink(missing_ok=True)
        subprocess.run(
            [
                "kubectl",
                "delete",
                "pod",
                pod_name,
                f"--namespace={namespace}",
                "--ignore-not-found",
                "--wait=false",
                *ctx_args,
            ],
            capture_output=True,
            text=True,
            timeout=C.KUBECTL_DELETE_TIMEOUT,
        )

def upload_code_to_pvc(
    cfg: VolcanoConfig,
    *,
    namespace: str,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> bool:
    """Upload local code to PVC via a transient kubectl-managed pod."""
    if not cfg.code_dir or not cfg.pvc_name or not cfg.pvc_mount_dir:
        return False

    code_path = Path(cfg.code_dir).resolve()
    if not code_path.is_dir():
        return False

    selected = walk_code(code_path, cfg.code_ignore)
    dest_dir = f"{cfg.pvc_mount_dir}/{C.CODE_UPLOAD_PREFIX}/{cfg.name}"
    pod_suffix = cfg.name[: C.UPLOAD_POD_NAME_SUFFIX_MAX].lower().replace("_", "-")
    pod_name = f"{C.UPLOAD_POD_NAME_PREFIX}{pod_suffix}"

    return upload_files_to_pvc(
        src_dir=code_path,
        files=selected,
        pvc_name=cfg.pvc_name,
        pvc_mount_dir=cfg.pvc_mount_dir,
        dest_dir=dest_dir,
        pod_name=pod_name,
        namespace=namespace,
        context=cfg.context,
        on_event=on_event,
        status_kind="code",
        label="Code",
    )
