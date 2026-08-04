"""Legacy strategy: push code into the PVC via ``tar | kubectl exec tar x``."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .. import constants as C
from ..upload import upload_code_to_pvc
from .base import CodeUploader, CodeUploadResult, EmitFn

if TYPE_CHECKING:
    from azure_jobs.shared.job.spec import JobSpec
    from ..config import VolcanoConfig

log = logging.getLogger(__name__)

class KubectlExecUploader(CodeUploader):
    name = "kubectl-exec"

    def prepare(
        self,
        cfg: "VolcanoConfig",
        request: "JobSpec",
        *,
        namespace: str,
        on_event: EmitFn | None = None,
    ) -> CodeUploadResult:
        if not (cfg.code_dir and cfg.pvc_name and cfg.pvc_mount_dir):
            return CodeUploadResult(
                ok=True,
                pod_setup_lines=[],
                code_path="",
            )

        ok, err = upload_code_to_pvc(cfg, namespace=namespace, on_event=on_event)
        if not ok:
            return CodeUploadResult(ok=False, error=err)

        code_path = f"{cfg.pvc_mount_dir}/{C.CODE_UPLOAD_PREFIX}/{cfg.name}"
        return CodeUploadResult(
            ok=True,
            pod_setup_lines=[],
            code_path=code_path,
        )

__all__ = ["KubectlExecUploader"]
