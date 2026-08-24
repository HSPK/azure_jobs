"""Opt-in end-to-end test that submits one minimal real Azure job."""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid
import warnings
from dataclasses import asdict

import pytest

from azure_jobs import connect
from azure_jobs.server.az_client import AzureClient, AzureWorkspaceClient
from azure_jobs.server.runner import Daemon
from azure_jobs.server.submit.azureml.sku_match import select_best_vc
from azure_jobs.shared.config import read_config
from azure_jobs.shared.contract.models import Target
from azure_jobs.shared.job.spec import JobSpec
from azure_jobs.shared.opts.aml import AmlOpts
from azure_jobs.shared.sku import SkuSpec

from .live_e2e import choose_auto_sing, choose_fastest

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.getenv("AJ_LIVE_E2E") != "1",
        reason="set AJ_LIVE_E2E=1 to submit one real Azure job",
    ),
]

TERMINAL = {"Completed", "Failed", "Canceled", "Cancelled"}


class Catalog:
    def __init__(self, target: Target) -> None:
        self.target = target

    def configured(self):
        return self.target

    def discover(self, subscription_id: str = ""):
        return (self.target,)


def _wait_socket(path, timeout: float = 20) -> None:
    from azure_jobs.sdk._transport import _reachable

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _reachable(path):
            return
        time.sleep(0.05)
    raise AssertionError(f"daemon did not answer at {path}")


def _cleanup_job(client, ref: str) -> None:
    """Reconcile an ambiguous submit, cancel if needed, then delete."""
    last_error: Exception | None = None
    for _attempt in range(3):
        try:
            current = client.job.status(ref)
            if current.status not in TERMINAL:
                client.job.cancel(current.ref)
                deadline = time.time() + 120
                while time.time() < deadline:
                    current = client.job.status(ref)
                    if current.status in TERMINAL:
                        break
                    time.sleep(2)
            if current.status in TERMINAL:
                client.job.delete(current.ref)
                return
            last_error = RuntimeError(
                f"cleanup timed out while {ref} was {current.status}"
            )
        except Exception as exc:
            if "404" in str(exc) or "not found" in str(exc).lower():
                return
            last_error = exc
        time.sleep(2)
    if last_error is not None:
        raise last_error


def _find_sing_workspace_and_identity(
    workspaces,
    *,
    requested_workspace: str,
    requested_uai: str,
) -> tuple[dict[str, str] | None, str]:
    """Find a visible workspace that has the requested or any UAI attached."""
    wanted_workspace = requested_workspace.casefold()
    wanted_uai = requested_uai.rstrip("/").casefold()
    for workspace in workspaces:
        if wanted_workspace and workspace.name.casefold() != wanted_workspace:
            continue
        try:
            with AzureWorkspaceClient(
                workspace.subscription_id,
                workspace.resource_group,
                workspace.name,
            ) as workspace_client:
                identities = (
                    (workspace_client.info().get("identity") or {})
                    .get("userAssignedIdentities")
                    or {}
                )
        except Exception:
            continue
        if wanted_uai:
            selected_uai = next(
                (
                    resource_id
                    for resource_id in identities
                    if resource_id.rstrip("/").casefold() == wanted_uai
                ),
                "",
            )
        else:
            selected_uai = next(iter(identities), "")
        if selected_uai:
            return (
                {
                    "name": workspace.name,
                    "resource_group": workspace.resource_group,
                    "subscription_id": workspace.subscription_id,
                },
                selected_uai,
            )
    return None, ""


def test_fastest_available_aml_or_sing_submission(
    tmp_path,
    monkeypatch,
) -> None:
    """Submit a no-op, wait for terminal state, then clean it up."""
    import azure_jobs.server.submit.azureml  # noqa: F401  (register aml/sing)

    monkeypatch.setenv("AJ_SHIP_SSH", "0")
    requested_service = os.getenv("AJ_LIVE_SERVICE", "auto").lower()
    if requested_service not in {"auto", "aml", "sing"}:
        pytest.fail("AJ_LIVE_SERVICE must be auto, aml, or sing")
    configured = read_config().workspace
    configured_fallback = (
        {
            "name": configured.workspace_name,
            "resource_group": configured.resource_group,
            "subscription_id": configured.subscription_id,
        }
        if configured.workspace_name
        else None
    )
    sing_uai = os.getenv("AJ_LIVE_SING_UAI", "")
    requested_workspace = os.getenv("AJ_LIVE_WORKSPACE", "")
    exact_sing_sku = os.getenv("AJ_LIVE_SING_SKU", "")
    sing_tier = os.getenv("AJ_LIVE_SING_TIER", "Premium")
    expected_selection = None

    with AzureClient() as azure:
        workspaces = azure.ws.list()
        sing_workspace, sing_uai = _find_sing_workspace_and_identity(
            workspaces,
            requested_workspace=(
                requested_workspace
                or (configured.workspace_name if configured_fallback else "")
            ),
            requested_uai=sing_uai,
        )
        images = azure.image.list()
        if requested_service == "sing" and exact_sing_sku:
            if not sing_workspace or not sing_uai:
                pytest.skip(
                    "no visible workspace with the requested Singularity UAI"
                )
            vcs = azure.quota.list(include_zero=True, strict=True)
            expected_selection = select_best_vc(
                exact_sing_sku,
                candidates=vcs,
                tier=sing_tier,
                client=azure,
                nodes=1,
                gpus_per_node=1,
            )
            candidate = choose_auto_sing(
                sing_workspace,
                images,
                sku=exact_sing_sku,
            )
        else:
            pairs = [
                {
                    "workspace": asdict(workspace),
                    "computes": [asdict(value) for value in computes],
                }
                for workspace, computes in azure.compute.list_all(
                    workspaces=workspaces
                )
            ]
            candidate = choose_fastest(
                pairs,
                azure.quota.list(include_zero=False),
                images,
                fallback_workspace=sing_workspace or configured_fallback,
                service=requested_service,
                allow_sing=bool(sing_uai),
            )

    if candidate is None:
        pytest.skip(f"no usable {requested_service} live candidate")
    if candidate.service == "sing" and not sing_uai:
        pytest.skip(
            "Singularity requires AJ_LIVE_SING_UAI or an identity attached "
            "to the selected workspace"
        )

    target = Target.create(
        backend="azureml",
        native_id=(
            f"{candidate.subscription_id}/{candidate.resource_group}/"
            f"{candidate.workspace_name}"
        ),
        label=candidate.workspace_name,
        detail=candidate.resource_group,
        metadata={
            "subscription_id": candidate.subscription_id,
            "resource_group": candidate.resource_group,
            "workspace_name": candidate.workspace_name,
        },
    )
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    daemon = Daemon(
        runtime / "daemon.sock",
        target_catalog=Catalog(target),
        shutdown_when_idle=0,
    )
    daemon.bind()
    thread = threading.Thread(target=daemon.serve_forever, daemon=True)
    thread.start()
    _wait_socket(daemon.socket_path)

    code = tmp_path / "code"
    code.mkdir()
    (code / "README.txt").write_text("aj live e2e\n", encoding="utf-8")
    name = f"aj-live-e2e-{uuid.uuid4().hex[:8]}"
    command = (
        ["bash -lc \"printf 'aj-live-e2e-ok\\n'\""]
        if SkuSpec.parse(candidate.sku).is_cpu
        else ["python -c \"print('aj-live-e2e-ok')\""]
    )
    spec = JobSpec(
        name=name,
        sid=uuid.uuid4().hex[:8],
        service=candidate.service,
        image=candidate.image,
        sku=candidate.sku,
        nodes=1,
        gpus_per_node=1,
        processes_per_node=1,
        command=command,
        code_dir=str(code),
        env_vars={
            "AJ_LIVE_E2E": "1",
            **(
                {"_AZUREML_SINGULARITY_JOB_UAI": sing_uai}
                if sing_uai
                else {}
            ),
        },
        backend_spec=AmlOpts(
            subscription_id=candidate.subscription_id,
            resource_group=candidate.resource_group,
            workspace_name=candidate.workspace_name,
            compute=candidate.compute,
            sla_tier=sing_tier,
        ),
    )
    if expected_selection is not None:
        assert spec.backend_spec.compute == ""
        assert spec.sku == exact_sing_sku
        assert spec.nodes == spec.gpus_per_node == spec.processes_per_node == 1

    client = None
    # Reconcile by the requested Azure name even if PUT succeeded but its
    # response was lost and submit() returned a failed outcome.
    ref = name
    current = None
    submit_events = []
    auto_selection_event = None
    timeout = float(os.getenv("AJ_LIVE_TIMEOUT", "600"))
    try:
        client = connect(
            candidate.workspace_name,
            root=tmp_path,
            path=daemon.socket_path,
            autostart=False,
        )
        outcome = client.job.submit(
            spec.to_dict(),
            on_event=submit_events.append,
        )
        assert outcome.succeeded, outcome.error
        ref = outcome.backend_ref or outcome.job_name or name
        if expected_selection is not None:
            auto_selection_event = next(
                (
                    event
                    for event in submit_events
                    if event.detail.startswith("Auto-selected VC ")
                ),
                None,
            )
            assert auto_selection_event is not None, [
                event.detail for event in submit_events
            ]
            assert "matched: 40G1-A100" in auto_selection_event.detail

        deadline = time.time() + timeout
        while time.time() < deadline:
            current = client.job.status(ref)
            if current.status in TERMINAL:
                break
            time.sleep(5)
        else:
            pytest.fail(
                f"{candidate.service} job {ref} on {candidate.compute} "
                f"did not finish within {timeout}s"
            )

        assert current.status == "Completed", current.raw
        log_deadline = time.time() + 120
        last_log_error = ""
        while time.time() < log_deadline:
            downloaded = client.log.download(current.ref)
            content = downloaded.get("content", "")
            last_log_error = downloaded.get("error", "")
            if "aj-live-e2e-ok" in content:
                break
            time.sleep(3)
        else:
            pytest.fail(
                f"job completed but expected log marker was missing "
                f"(last log error: {last_log_error or 'none'})"
            )

        if expected_selection is not None:
            assert auto_selection_event is not None
            actual_compute = str(current.raw.get("compute") or "")
            actual_tier = str(current.raw.get("sla_tier") or "")
            assert actual_compute
            assert actual_tier
            assert f"VC '{actual_compute}'" in auto_selection_event.detail
            assert f"at {actual_tier} " in auto_selection_event.detail
            assert current.raw.get("nodes") == 1
            assert current.raw.get("instance_type")
    finally:
        active_failure = sys.exc_info()[0] is not None
        cleanup_error = None
        if client is not None and ref:
            try:
                _cleanup_job(client, ref)
            except Exception as exc:
                cleanup_error = exc
            client.close()
        daemon.shutdown()
        thread.join(timeout=10)
        if cleanup_error is not None:
            if active_failure:
                warnings.warn(
                    f"live job cleanup failed ({type(cleanup_error).__name__}: "
                    f"{cleanup_error})",
                    RuntimeWarning,
                )
            else:
                raise cleanup_error
