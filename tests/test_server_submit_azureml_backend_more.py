from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

import pytest

import azure_jobs.server.submit.azureml as azureml_backend
from azure_jobs.server.submit.archive import ArchiveMetadata
from azure_jobs.server.submit.azureml import entry as entry_mod
from azure_jobs.server.submit.azureml import image as image_mod
from azure_jobs.server.submit.azureml import target as target_mod
from azure_jobs.server.submit.azureml.workspace import ResolvedTarget
from azure_jobs.shared.errors import RestError
from azure_jobs.shared.job.spec import JobResult, JobSpec, StorageMount
from azure_jobs.shared.opts import AmlOpts
from azure_jobs.shared.template.models import Template
from azure_jobs.shared.types.instance import InstanceTypeInfo


class _AzureCtx:
    def __init__(self, *, vc: object = None) -> None:
        self.quota = SimpleNamespace(get_by_name=MagicMock(return_value=vc))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _WorkspaceCtx:
    def __init__(self, *, returned_job: dict[str, object]) -> None:
        self.returned_job = returned_job
        self.blob = SimpleNamespace(upload_archive=self._upload_archive)
        self.job = SimpleNamespace(create_or_update=MagicMock(return_value=returned_job))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def _upload_archive(self, archive_path, code_hash, *, on_progress=None):
        assert archive_path.is_file()
        assert code_hash == "archive-hash"
        if on_progress is not None:
            on_progress(1, 1, 0, "code.tar.gz")
        return (
            "azureml://datastores/workspaceblobstore/paths/"
            "LocalUpload/archive-hash/code.tar.gz"
        )


def _req(**overrides) -> JobSpec:
    data = {
        "name": "job",
        "service": "aml",
        "expr_name": "exp",
        "sku": "1xG1",
        "nodes": 1,
        "gpus_per_node": 1,
        "processes_per_node": 1,
        "image": "image:latest",
        "code_dir": "/repo/code",
        "code_ignore": ["*.tmp"],
        "env_vars": {"FOO": "bar"},
        "backend_spec": AmlOpts(compute="compute-a", tags=["owner:team"]),
    }
    data.update(overrides)
    return JobSpec(**data)


def test_build_distribution_returns_none_and_pytorch_config() -> None:
    assert target_mod._build_distribution(_req()) is None
    dist = target_mod._build_distribution(_req(nodes=2, processes_per_node=4))
    assert dist == {
        "distributionType": "PyTorch",
        "processCountPerInstance": 4,
    }


def test_build_resources_logs_tier_downgrade_nvlink_and_group_policy() -> None:
    aml = AmlOpts(
        compute="vc-a",
        sla_tier="Premium",
        priority="low",
        group_policy="policy-a",
    )
    request = _req(
        service="sing",
        nodes=2,
        gpus_per_node=4,
        sku="2x80G4-A100-NvLink",
        image="amlt-sing/curated",
        backend_spec=aml,
    )
    logs: list[str] = []
    matched = SimpleNamespace(
        instances=[
            InstanceTypeInfo(
                name="Singularity.NC96ad_A100_v4",
                series_id="NC_A100_v4",
                num_gpus=4,
                accelerator="A100",
                gpu_memory_gb=80,
            )
        ],
        effective_tier="Standard",
        nvlink_satisfied=False,
    )

    with patch(
        "azure_jobs.server.submit.azureml.sku_match.match_instance_type",
        return_value=matched,
    ):
        resources = target_mod._build_resources(
            request,
            "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.MachineLearningServices/virtualclusters/vc-a",
            vc=object(),
            client=MagicMock(),
            on_log=logs.append,
        )

    props = resources["properties"]["AISuperComputer"]
    assert props["groupPolicyName"] == "policy-a"
    assert props["imageVersion"] == "curated"
    assert props["slaTier"] == "Standard"
    assert props["instanceTypes"] == ["Singularity.NC96ad_A100_v4"]
    assert aml.sla_tier == "Standard"
    assert aml.matched_instances == ["80G4-A100"]
    assert logs[0] == "Resolving SKU 2x80G4-A100-NvLink…"
    assert any("SLA tier downgraded: Premium → Standard" in line for line in logs)
    assert any("NVLink unavailable" in line for line in logs)


def test_resolve_sing_identity_returns_none_when_workspace_identity_lacks_client_id() -> None:
    request = _req(
        service="sing",
        env_vars={"_AZUREML_SINGULARITY_JOB_UAI": "/subs/1/uai/train"},
        backend_spec=AmlOpts(workspace_name="ws-a"),
    )
    client = MagicMock()
    client.info.return_value = {
        "identity": {"userAssignedIdentities": {"/subs/1/uai/train": {}}}
    }

    assert target_mod._resolve_sing_identity(request, client) is None


def test_build_identity_returns_none_for_unknown_mode() -> None:
    assert target_mod._build_identity(_req(backend_spec=AmlOpts(identity="none"))) is None


def test_build_environment_returns_cached_id() -> None:
    client = MagicMock()
    client.env.get.return_value = SimpleNamespace(id="cached-env")

    assert image_mod._build_environment(_req(), client) == "cached-env"
    client.env.create_or_update.assert_not_called()


def test_build_environment_cache_lookup_error_falls_back_to_create() -> None:
    client = MagicMock()
    client.env.get.side_effect = RuntimeError("cache miss")
    client.env.create_or_update.return_value = SimpleNamespace(id="created-env")

    assert image_mod._build_environment(_req(expr_name=""), client) == "created-env"
    client.env.create_or_update.assert_called_once_with("aj", ANY, "image:latest")


def test_build_environment_registration_failure_returns_inline_image() -> None:
    client = MagicMock()
    client.env.get.return_value = None
    client.env.create_or_update.side_effect = RuntimeError("boom")

    assert image_mod._build_environment(_req(), client) == ""


def test_get_rest_client_uses_backend_coordinates() -> None:
    aml = AmlOpts(
        subscription_id="sub-a",
        resource_group="rg-a",
        workspace_name="ws-a",
    )
    with patch("azure_jobs.server.az_client.AzureWorkspaceClient") as client_cls:
        entry_mod._get_rest_client(aml)
    client_cls.assert_called_once_with(
        subscription_id="sub-a",
        resource_group="rg-a",
        workspace_name="ws-a",
    )


@pytest.mark.parametrize(
    ("service", "identity_client_id"),
    [("aml", ""), ("sing", "client-id")],
)
def test_aml_and_sing_share_archive_upload_flow(
    monkeypatch,
    service: str,
    identity_client_id: str,
) -> None:
    request = _req(
        service=service,
        sku="2x80G4-A100",
        nodes=2,
        gpus_per_node=4,
        processes_per_node=2,
        backend_spec=AmlOpts(compute="vc-a", tags=["owner:team"]),
        storage={
            "fast": StorageMount(
                storage_account_name="acct",
                container_name="cont",
                mount_dir="/mnt/fast",
            )
        },
    )
    azure = _AzureCtx(vc=SimpleNamespace(name="vc-a"))
    workspace = _WorkspaceCtx(
        returned_job={
            "properties": {"services": {"Studio": {"endpoint": "https://portal/job"}}},
            "name": "",
        }
    )
    events = []

    monkeypatch.setattr(entry_mod, "AzureClient", lambda: azure)
    monkeypatch.setattr(
        entry_mod,
        "resolve_target",
        lambda request, arm_client: ResolvedTarget(
            aml=request.backend_spec,
            vc=SimpleNamespace(name="vc-a") if request.service == "sing" else None,
        ),
    )
    monkeypatch.setattr(entry_mod, "_get_rest_client", lambda aml: workspace)
    monkeypatch.setattr(entry_mod, "_build_distribution", lambda request: {"distributionType": "PyTorch"})
    monkeypatch.setattr(entry_mod, "_build_identity", lambda request: None)
    monkeypatch.setattr(entry_mod, "_resolve_compute", lambda request: "compute-arm-id")
    monkeypatch.setattr(
        entry_mod,
        "_build_resources",
        lambda request, client, compute_id, on_log, vc, match, requested_tier: {
            "properties": {"kind": "resources"}
        },
    )
    monkeypatch.setattr(entry_mod, "_resolve_sing_identity", lambda request, workspace: "client-id")
    monkeypatch.setattr(entry_mod, "generate_runner_script", lambda request, identity_client_id: f"runner:{identity_client_id}")
    monkeypatch.setattr(entry_mod, "_collect_ssh_files", lambda code_root, emit: {"ssh.key": b"data"})
    archive_dirs = []

    def _create_archive(code_root, archive_path, **kwargs):
        assert code_root == "/repo/code"
        assert kwargs["ignore_patterns"] == ["*.tmp"]
        assert kwargs["extra_files"] == {
            entry_mod.RUNNER_FILENAME: f"runner:{identity_client_id}",
            "ssh.key": b"data",
        }
        archive_path.write_bytes(b"archive")
        archive_dirs.append(archive_path.parent)
        kwargs["on_progress"](0, 2, "code.tar.gz")
        kwargs["on_progress"](2, 2, "ssh.key")
        return ArchiveMetadata("archive-hash", 7, 2)

    monkeypatch.setattr(entry_mod, "create_code_archive", _create_archive)
    monkeypatch.setattr(entry_mod, "_build_environment", lambda request, workspace: "env-id")
    monkeypatch.setattr(
        entry_mod,
        "_build_storage_mounts",
        lambda request, workspace: (
            {"fast": {"jobOutputType": "uri_folder"}},
            {"AZURE_ML_OUTPUT_PathOnCompute_fast": "/mnt/fast/"},
            {"AZUREML_DATAREFERENCE_fast": "/mnt/fast"},
        ),
    )
    monkeypatch.setattr(
        entry_mod,
        "_build_env_vars",
        lambda request, dataref_env: {"FOO": "bar", **dataref_env},
    )
    monkeypatch.setattr(entry_mod, "_build_tags", lambda tags: {"owner": "team"})
    def _build_job_body(request_arg, **kwargs):
        return {"payload": {"request": request_arg, **kwargs}}

    monkeypatch.setattr(entry_mod, "_build_job_body", _build_job_body)

    result = entry_mod.submit(request, on_event=events.append)

    assert result == JobResult(
        job_name="job",
        azure_name="job",
        status="submitted",
        portal_url="https://portal/job",
    )
    expected_events = [
        "resolve",
        "auth",
        "command",
        "environment",
        "storage",
        "code",
        "package",
        "package",
        "code",
        "code",
        "upload",
        "submit",
        "done",
    ]
    if service == "sing":
        expected_events.insert(3, "identity")
    assert [event.kind for event in events] == expected_events
    upload_event = next(event for event in events if event.kind == "upload")
    assert (upload_event.completed, upload_event.total, upload_event.current) == (1, 1, "code.tar.gz")
    assert archive_dirs and not archive_dirs[0].exists()
    azure.quota.get_by_name.assert_not_called()
    workspace.job.create_or_update.assert_called_once_with(
        "job",
        {
            "payload": {
                "request": request,
                "env_id": "env-id",
                "code_archive_uri": (
                    "azureml://datastores/workspaceblobstore/paths/"
                    "LocalUpload/archive-hash/code.tar.gz"
                ),
                "code_archive_hash": "archive-hash",
                "compute_id": "compute-arm-id",
                "env_vars": {
                    "FOO": "bar",
                    "AZUREML_DATAREFERENCE_fast": "/mnt/fast",
                    "AJ_CODE_ARCHIVE_SHA256": "archive-hash",
                },
                "distribution": {"distributionType": "PyTorch"},
                "identity": None,
                "resources": {"properties": {"kind": "resources"}},
                "outputs": {"fast": {"jobOutputType": "uri_folder"}},
                "custom_props": {"AZURE_ML_OUTPUT_PathOnCompute_fast": "/mnt/fast/"},
                "tags": {"owner": "team"},
            }
        },
    )


def test_archive_temp_directory_is_cleaned_when_upload_fails(monkeypatch) -> None:
    request = _req()
    azure = _AzureCtx()
    workspace = _WorkspaceCtx(returned_job={})
    archive_dirs = []

    monkeypatch.setattr(entry_mod, "AzureClient", lambda: azure)
    monkeypatch.setattr(
        entry_mod,
        "resolve_target",
        lambda request, arm_client: ResolvedTarget(aml=request.backend_spec),
    )
    monkeypatch.setattr(entry_mod, "_get_rest_client", lambda aml: workspace)
    monkeypatch.setattr(entry_mod, "_build_distribution", lambda request: None)
    monkeypatch.setattr(entry_mod, "_build_identity", lambda request: None)
    monkeypatch.setattr(entry_mod, "_resolve_compute", lambda request: "compute")
    monkeypatch.setattr(
        entry_mod,
        "_build_resources",
        lambda request, client, compute_id, on_log, vc, match, requested_tier: None,
    )
    monkeypatch.setattr(
        entry_mod,
        "generate_runner_script",
        lambda request, identity_client_id: "runner",
    )
    monkeypatch.setattr(
        entry_mod,
        "_collect_ssh_files",
        lambda code_root, emit: {},
    )
    monkeypatch.setattr(
        entry_mod,
        "_build_environment",
        lambda request, workspace: "env",
    )
    monkeypatch.setattr(
        entry_mod,
        "_build_storage_mounts",
        lambda request, workspace: ({}, {}, {}),
    )

    def _create_archive(code_root, archive_path, **kwargs):
        archive_path.write_bytes(b"archive")
        archive_dirs.append(archive_path.parent)
        return ArchiveMetadata("archive-hash", 7, 1)

    monkeypatch.setattr(entry_mod, "create_code_archive", _create_archive)
    workspace.blob.upload_archive = MagicMock(
        side_effect=OSError("upload interrupted")
    )

    result = entry_mod.submit(request)

    assert result.status == "failed"
    assert "upload interrupted" in result.error
    assert archive_dirs and not archive_dirs[0].exists()
    workspace.job.create_or_update.assert_not_called()


def test_submit_rest_error_includes_http_status_and_azure_code() -> None:
    events = []
    with patch.object(
        entry_mod,
        "_submit_impl",
        side_effect=RestError("denied", status_code=403, azure_code="Forbidden"),
    ):
        result = entry_mod.submit(_req(), on_event=events.append)

    assert result.status == "failed"
    assert result.error == "RestError: denied [HTTP 403] [code=Forbidden]"
    assert events[-1].detail == "RestError: denied [HTTP 403] [code=Forbidden]"


def test_azureml_backend_helpers_delegate_and_filter_unknown_fields() -> None:
    request = _req()
    with patch("azure_jobs.server.submit.azureml.entry.submit", return_value=JobResult(job_name="job", status="submitted")) as submit:
        result = azureml_backend._submit_azureml(request)
    assert result.status == "submitted"
    submit.assert_called_once_with(request, on_event=None)

    template = Template.from_dict(
        {
            "target": {
                "name": "compute-a",
                "service": "aml",
                "subscription_id": "sub-a",
                "resource_group": "rg-a",
                "workspace_name": "ws-a",
            },
            "jobs": [{"identity": "user", "tags": ["k:v"]}],
            "code": {"local_dir": "src"},
        }
    )
    built = azureml_backend._build_aml_spec(template)
    assert built == AmlOpts(
        identity="user",
        tags=["k:v"],
        compute="compute-a",
        subscription_id="sub-a",
        resource_group="rg-a",
        workspace_name="ws-a",
        amlt_code_dir="src",
    )

    loaded = azureml_backend._load_aml_spec(
        {"compute": "compute-b", "identity": "managed", "unknown": "ignore-me"}
    )
    assert loaded.compute == "compute-b"
    assert loaded.identity == "managed"
    assert not hasattr(loaded, "unknown")
