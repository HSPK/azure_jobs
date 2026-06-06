"""Code upload to a PVC via a transient kubectl-managed pod."""

from __future__ import annotations

import logging
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Callable

import yaml

from azure_jobs.utils.format import format_size
from azure_jobs.utils.fs import CodeFile, walk_code

from azure_jobs.job.spec import JobEvent
from . import constants as C
from .config import VolcanoConfig

log = logging.getLogger(__name__)

_KUBECTL_OUTPUT_LIMIT = 4000

def _trim(text: str, limit: int = _KUBECTL_OUTPUT_LIMIT) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    head = text[: limit // 2]
    tail = text[-limit // 2 :]
    return f"{head}\n…[truncated {len(text) - limit} chars]…\n{tail}"

def _format_subprocess_failure(
    label: str,
    cmd: list[str],
    result: subprocess.CompletedProcess[str],
) -> str:
    parts = [f"{label} (exit={result.returncode}): {' '.join(cmd[:4])}"]
    stderr = _trim(result.stderr or "")
    stdout = _trim(result.stdout or "")
    if stderr:
        parts.append(f"stderr: {stderr}")
    if stdout:
        parts.append(f"stdout: {stdout}")
    if not stderr and not stdout:
        parts.append("(no stdout/stderr produced)")
    return "\n".join(parts)

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
    emit: Callable[[JobEvent], None],
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """Stream tar c into kubectl exec tar x, emitting per-file events.

    Returns (kubectl_exec_result, tar_stderr_lines). The tar stderr lines are
    collected so the caller can surface a meaningful error if either side of
    the pipe fails (kubectl exit codes alone often hide the real cause).
    """
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
    tar_diagnostics: list[str] = []

    def _drain_stderr() -> None:
        assert tar_proc.stderr is not None
        for raw in tar_proc.stderr:
            line = raw.decode("utf-8", errors="replace").rstrip()
            if not line:
                continue
            if line.startswith("tar:"):
                tar_diagnostics.append(line)
                continue
            cur = line[2:] if line.startswith("./") else line
            completed[0] += 1
            emit(
                JobEvent(
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
        result = subprocess.run(
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
    if tar_proc.returncode and tar_proc.returncode != 0:
        tar_diagnostics.append(f"tar exited with code {tar_proc.returncode}")
    return result, tar_diagnostics

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
    on_event: Callable[[JobEvent], None] | None = None,
    status_kind: str = "code",
    label: str = "Files",
) -> tuple[bool, str]:
    """Upload files (relative to src_dir) into dest_dir on a PVC.

    Returns (ok, error_detail). When ok is True, error_detail is empty;
    otherwise it contains a multi-line message describing exactly what
    failed (kubectl/tar exit codes, stdout, stderr) so the caller can
    surface it to the user without forcing them to enable AJ_DEBUG.
    """
    emit = on_event or (lambda _ev: None)

    def status(kind: str, detail: str) -> None:
        emit(JobEvent(kind=kind, detail=detail))

    if not files:
        status(status_kind, f"No {label.lower()} to upload")
        return True, ""

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
        apply_cmd = ["kubectl", "apply", "-f", pod_yaml_path, *ctx_args]
        result = subprocess.run(
            apply_cmd,
            capture_output=True,
            text=True,
            timeout=C.KUBECTL_APPLY_TIMEOUT,
        )
        if result.returncode != 0:
            detail = _format_subprocess_failure(
                "kubectl apply (upload pod)", apply_cmd, result
            )
            log.error("Upload pod creation failed\n%s", detail)
            status("error", f"Upload pod creation failed: {_trim(result.stderr, 200) or _trim(result.stdout, 200)}")
            return False, detail

        wait_cmd = [
            "kubectl",
            "wait",
            "--for=condition=Ready",
            f"pod/{pod_name}",
            f"--namespace={namespace}",
            f"--timeout={C.KUBECTL_WAIT_TIMEOUT}s",
            *ctx_args,
        ]
        result = subprocess.run(
            wait_cmd,
            capture_output=True,
            text=True,
            timeout=C.KUBECTL_WAIT_SUBPROCESS_TIMEOUT,
        )
        if result.returncode != 0:
            detail = _format_subprocess_failure(
                f"kubectl wait pod/{pod_name}", wait_cmd, result
            )
            describe = _describe_pod(pod_name, namespace, ctx_args)
            if describe:
                detail = f"{detail}\n\nkubectl describe pod:\n{describe}"
            log.error("Upload pod failed to become ready\n%s", detail)
            status(
                "error",
                f"Upload pod {pod_name} failed to become ready "
                f"(see error detail; AJ_DEBUG=1 for full trace)",
            )
            return False, detail

        exec_result, tar_diagnostics = stream_tar(
            src_dir=src_dir,
            filelist_path=filelist_path,
            pod_name=pod_name,
            namespace=namespace,
            dest_dir=dest_dir,
            ctx_args=ctx_args,
            total_files=len(files),
            emit=emit,
        )
        if exec_result.returncode != 0 or tar_diagnostics:
            detail = _format_subprocess_failure(
                f"kubectl exec tar (extract into {dest_dir})",
                ["kubectl", "exec", pod_name, "--", "tar", "xf", "-"],
                exec_result,
            )
            if tar_diagnostics:
                detail = f"{detail}\n\nlocal tar diagnostics:\n" + "\n".join(
                    tar_diagnostics[:50]
                )
            log.error("%s extract failed\n%s", label, detail)
            short = _trim(exec_result.stderr, 200) or _trim(
                exec_result.stdout, 200
            )
            if not short and tar_diagnostics:
                short = tar_diagnostics[0]
            status("error", f"{label} copy failed: {short}")
            if exec_result.returncode != 0:
                return False, detail
            # tar diagnostics only - still report but mark success ambiguous
            return False, detail

        status(status_kind, f"{label} uploaded to {dest_dir}")
        return True, ""

    except subprocess.TimeoutExpired as exc:
        cmd_repr = " ".join(map(str, exc.cmd or [])) if exc.cmd else "<unknown>"
        detail = (
            f"{label} upload timed out after {exc.timeout}s\n"
            f"command: {cmd_repr}\n"
            f"hint: increase the timeout, check kubectl connectivity, or "
            f"run with AJ_DEBUG=1 for a Python traceback"
        )
        if exc.stderr:
            try:
                detail += f"\nstderr (last bytes): {_trim(exc.stderr.decode('utf-8', errors='replace') if isinstance(exc.stderr, (bytes, bytearray)) else exc.stderr, 1000)}"
            except Exception:
                pass
        log.exception("%s upload timed out", label)
        status("error", f"{label} upload timed out after {exc.timeout}s")
        return False, detail
    except Exception as exc:
        log.exception("%s upload error", label)
        detail = (
            f"{label} upload error: {type(exc).__name__}: {exc}\n"
            f"(run with AJ_DEBUG=1 for a Python traceback)"
        )
        status("error", f"{label} upload error: {type(exc).__name__}: {exc}")
        return False, detail
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

def _describe_pod(
    pod_name: str, namespace: str, ctx_args: list[str]
) -> str:
    """Best-effort `kubectl describe pod` for use in error messages."""
    try:
        result = subprocess.run(
            [
                "kubectl",
                "describe",
                "pod",
                pod_name,
                f"--namespace={namespace}",
                *ctx_args,
            ],
            capture_output=True,
            text=True,
            timeout=C.KUBECTL_NAMESPACE_TIMEOUT,
        )
    except Exception as exc:
        log.debug(
            "kubectl describe pod/%s failed", pod_name, exc_info=True
        )
        return f"(kubectl describe failed: {type(exc).__name__}: {exc})"
    if result.returncode != 0:
        return _trim(result.stderr or result.stdout, 1500)
    return _trim(result.stdout, 1500)

def upload_code_to_pvc(
    cfg: VolcanoConfig,
    *,
    namespace: str,
    on_event: Callable[[JobEvent], None] | None = None,
) -> tuple[bool, str]:
    """Upload local code to PVC via a transient kubectl-managed pod.

    Returns (ok, error_detail). On success error_detail is empty; on failure
    it contains a multi-line, user-actionable description of the underlying
    failure (which kubectl subprocess failed, its exit code, captured
    stdout/stderr, and `kubectl describe pod` output when relevant).
    """
    if not cfg.code_dir or not cfg.pvc_name or not cfg.pvc_mount_dir:
        missing = [
            name
            for name, val in (
                ("code_dir", cfg.code_dir),
                ("pvc_name", cfg.pvc_name),
                ("pvc_mount_dir", cfg.pvc_mount_dir),
            )
            if not val
        ]
        return False, (
            "Upload precondition not met: missing "
            f"{', '.join(missing)} in VolcanoConfig"
        )

    code_path = Path(cfg.code_dir).resolve()
    if not code_path.is_dir():
        return False, (
            f"code_dir is not a directory: {code_path} "
            f"(cfg.code_dir={cfg.code_dir!r})"
        )

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
