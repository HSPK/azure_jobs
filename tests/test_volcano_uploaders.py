"""Tests for the parallel code-upload strategy abstraction (volcano backend)."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.backend.volcano import (
    BlobUploader,
    BlobUploadOpts,
    CodeUploadResult,
    KubectlExecUploader,
    VolcanoOpts,
    build_volcano_job,
    pick_uploader,
    resolve_namespace,
)
from azure_jobs.backend.volcano.config import VolcanoConfig
from azure_jobs.backend.volcano.entry import submit_via_volcano
from azure_jobs.backend.volcano.uploaders import blob as blob_mod
from azure_jobs.backend.volcano.uploaders._scripts import load_script
from azure_jobs.errors import ConfigError
from azure_jobs.job.spec import (
    JobResult,
    JobSpec,
)
from azure_jobs.template.models import Template


def _blob_extra(**blob: object) -> dict:
    """Shortcut: build the JobSpec.extra dict for the blob strategy."""
    return {"code_upload": {"strategy": "blob", "blob": dict(blob)}}


class TestExtraField:
    """_extra in the YAML round-trips into Template + JobSpec.extra."""

    def test_template_keeps_extra_unparsed(self):
        t = Template.from_dict({"_extra": {"code_upload": {"strategy": "blob"}}})
        assert t._extra == {"code_upload": {"strategy": "blob"}}

    def test_template_without_extra_defaults_to_empty(self):
        t = Template.from_dict({})
        assert t._extra == {}

    def test_build_job_spec_passes_extra_through_untouched(self, tmp_path: Path):
        """build_job_spec must passthrough _extra opaquely; backend refines add their own slot."""
        from azure_jobs.job import build_job_spec

        extra = {
            "code_upload": {
                "strategy": "blob",
                "blob": {
                    "storage_account": "mysa",
                    "container": "aj-code",
                    "upload_dir": "snap",
                    "sas_expiry_days": 3,
                    "pod_download_retries": 9,
                },
            },
            # forward-compat: unknown keys must survive the build untouched
            "future_feature": {"foo": "bar"},
        }
        tmpl = Template.from_dict(
            {
                "jobs": [{"name": "j", "sku": "x"}],
                "target": {"service": "volcano"},
                "_extra": extra,
            }
        )
        spec = build_job_spec(
            tmpl,
            name="j",
            sid="sid",
            sku="x",
            user_command="echo",
            user_args=(),
            nodes=1,
            gpus_per_node=1,
            code_dir=str(tmp_path),
        )
        # All template _extra entries survive verbatim.
        assert spec.extra["code_upload"] == extra["code_upload"]
        assert spec.extra["future_feature"] == extra["future_feature"]
        # The Volcano backend's typed config lives in the dedicated
        # backend_spec slot, NOT in extra (extra is _extra passthrough).
        from azure_jobs.backend.volcano.opts import VolcanoOpts

        assert isinstance(spec.backend_spec, VolcanoOpts)
        assert "volcano" not in spec.extra
        # JobSpec must NOT grow strategy-specific top-level fields.
        assert not hasattr(spec, "code_upload")

    def test_default_backend_spec_is_typed(self, tmp_path: Path):
        from azure_jobs.job import build_job_spec

        tmpl = Template.from_dict({"jobs": [{"name": "j", "sku": "x"}]})
        spec = build_job_spec(
            tmpl,
            name="j",
            sid="sid",
            sku="x",
            user_command="echo",
            user_args=(),
            nodes=1,
            gpus_per_node=1,
            code_dir=str(tmp_path),
        )
        # Default service is "aml" → AML build_spec_backend produces a
        # typed AmlOpts in the dedicated backend_spec slot.
        from azure_jobs.backend.azureml.opts import AmlOpts

        assert isinstance(spec.backend_spec, AmlOpts)
        assert spec.backend_spec.identity == "managed"
        # extra is reserved for template _extra passthrough — empty by default.
        assert spec.extra == {}


class TestPickUploader:
    """``pick_uploader`` is the single dispatch entry point."""

    def test_defaults_to_kubectl_exec(self):
        assert isinstance(pick_uploader({}), KubectlExecUploader)
        assert isinstance(pick_uploader(None), KubectlExecUploader)
        assert isinstance(pick_uploader({"code_upload": {}}), KubectlExecUploader)
        assert isinstance(
            pick_uploader({"code_upload": {"strategy": ""}}), KubectlExecUploader
        )

    def test_picks_blob(self):
        assert isinstance(
            pick_uploader({"code_upload": {"strategy": "blob"}}), BlobUploader
        )

    def test_ignores_unrelated_extras(self):
        assert isinstance(
            pick_uploader({"other": {"strategy": "blob"}}), KubectlExecUploader
        )

    def test_returns_fresh_instance(self):
        a = pick_uploader({"code_upload": {"strategy": "blob"}})
        b = pick_uploader({"code_upload": {"strategy": "blob"}})
        assert a is not b

    def test_unknown_strategy_raises_config_error(self):
        with pytest.raises(ConfigError, match="Unknown code-upload strategy"):
            pick_uploader({"code_upload": {"strategy": "nope"}})

    def test_register_uploader_is_gone(self):
        """Two-strategy if/else dispatch — no register_uploader to misuse."""
        import azure_jobs.backend.volcano as _vol
        import azure_jobs.backend.volcano.uploaders as _up

        assert not hasattr(_vol, "register_uploader")
        assert not hasattr(_up, "register_uploader")
        assert not hasattr(_up, "_STRATEGIES")


class TestBlobUploadOptsParser:
    """BlobUploadOpts.from_extra owns the blob schema, not job/build.py."""

    def test_parses_full_blob_section(self):
        opts = BlobUploadOpts.from_extra(
            {
                "code_upload": {
                    "blob": {
                        "storage_account": "mysa",
                        "container": "c",
                        "upload_dir": "snap",
                        "sas_expiry_days": 3,
                        "pod_download_retries": 9,
                    }
                }
            }
        )
        assert opts.storage_account == "mysa"
        assert opts.container == "c"
        assert opts.upload_dir == "snap"
        assert opts.sas_expiry_days == 3
        assert opts.pod_download_retries == 9

    def test_missing_blob_yields_defaults(self):
        opts = BlobUploadOpts.from_extra({})
        assert opts == BlobUploadOpts()
        opts = BlobUploadOpts.from_extra(None)
        assert opts == BlobUploadOpts()

    def test_defaults(self):
        opts = BlobUploadOpts()
        assert opts.sas_expiry_days == 1
        assert opts.upload_dir == "aj-code"
        assert opts.pod_download_retries == 5


class TestUploaderDispatchRemoved:
    """Sanity: the old registry-like API has been deleted."""

    def test_no_register_or_strategies_dict(self):
        import azure_jobs.backend.volcano.uploaders as _up

        for gone in ("_STRATEGIES", "register_uploader", "available_strategies",
                     "get_code_uploader", "strategy_from_extra"):
            assert not hasattr(_up, gone), f"unexpected symbol still present: {gone}"


def _vol_cfg(**over) -> VolcanoConfig:
    base = dict(
        name="j-abc",
        nodes=1,
        gpus_per_node=1,
        cpus_per_node=4,
        memory="8Gi",
        image="img:latest",
        command=["echo hi"],
        code_dir="/some/code",
        pvc_name="pvc",
        pvc_mount_dir="/mnt/pvc",
    )
    base.update(over)
    return VolcanoConfig(**base)


class TestKubectlExecUploader:
    def test_returns_pvc_code_path_and_no_setup(self, tmp_path: Path):
        cfg = _vol_cfg(code_dir=str(tmp_path))
        spec = JobSpec(name="j-abc", service="volcano")
        u = KubectlExecUploader()
        with patch(
            "azure_jobs.backend.volcano.uploaders.kubectl_exec.upload_code_to_pvc",
            return_value=(True, ""),
        ) as m:
            r = u.prepare(cfg, spec, namespace="ns")
        assert m.called
        assert r.ok is True
        assert r.pod_setup_lines == []
        assert r.code_path == "/mnt/pvc/aj_code/j-abc"

    def test_propagates_underlying_error(self):
        cfg = _vol_cfg()
        spec = JobSpec(name="j-abc", service="volcano")
        u = KubectlExecUploader()
        with patch(
            "azure_jobs.backend.volcano.uploaders.kubectl_exec.upload_code_to_pvc",
            return_value=(False, "kubectl apply exit=1\nstderr: Forbidden"),
        ):
            r = u.prepare(cfg, spec, namespace="ns")
        assert r.ok is False
        assert "Forbidden" in r.error

    def test_no_pvc_is_a_silent_noop(self, tmp_path: Path):
        cfg = _vol_cfg(code_dir=str(tmp_path), pvc_name="", pvc_mount_dir="")
        spec = JobSpec(name="j-abc", service="volcano")
        with patch(
            "azure_jobs.backend.volcano.uploaders.kubectl_exec.upload_code_to_pvc"
        ) as m:
            r = KubectlExecUploader().prepare(cfg, spec, namespace="ns")
        assert m.called is False
        assert r.ok is True
        assert r.code_path == ""


class TestBlobUploader:
    def test_rejects_missing_required_fields(self):
        cfg = _vol_cfg()
        spec = JobSpec(name="j", service="volcano", extra={**(_blob_extra())})
        r = BlobUploader().prepare(cfg, spec, namespace="ns")
        assert r.ok is False
        assert "storage_account" in r.error and "container" in r.error

    def test_rejects_missing_code_dir(self):
        cfg = _vol_cfg(code_dir="")
        spec = JobSpec(name="j", service="volcano", extra={**(_blob_extra(storage_account="sa", container="c"))})
        r = BlobUploader().prepare(cfg, spec, namespace="ns")
        assert r.ok is False
        assert "code_dir" in r.error

    def test_rejects_nonexistent_code_dir(self, tmp_path: Path):
        cfg = _vol_cfg(code_dir=str(tmp_path / "does-not-exist"))
        spec = JobSpec(name="j", service="volcano", extra={**(_blob_extra(storage_account="sa", container="c"))})
        r = BlobUploader().prepare(cfg, spec, namespace="ns")
        assert r.ok is False
        assert "not a directory" in r.error

    def test_happy_path_uploads_and_emits_pod_setup(self, tmp_path: Path):
        # Create a tiny code tree
        (tmp_path / "main.py").write_text("print('hi')\n")
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "x.txt").write_text("x\n")

        cfg = _vol_cfg(name="j-xyz", code_dir=str(tmp_path), code_ignore=[])
        spec = JobSpec(name="j-xyz", service="volcano", extra={**(_blob_extra(
                storage_account="mysa",
                container="aj-code",
                upload_dir="snap",
                sas_expiry_days=2,
                pod_download_retries=7,
            ))})

        fake_sas = "sv=2024-01-01&sig=abc%2Bdef"
        with patch.object(
            blob_mod,
            "_generate_sas_token",
            return_value=fake_sas,
        ) as m_sas, patch.object(blob_mod.requests, "put") as m_put:
            m_put.return_value = MagicMock(status_code=201, text="")
            events: list = []
            r = BlobUploader().prepare(
                cfg, spec, namespace="ns", on_event=events.append
            )

        assert r.ok is True, r.error
        m_sas.assert_called_once_with("mysa", "aj-code", 2)
        assert m_put.called
        put_url = m_put.call_args.args[0]
        assert put_url.startswith("https://mysa.blob.core.windows.net/aj-code/snap/j-xyz-")
        assert put_url.endswith(f".tgz?{fake_sas}")

        # pod setup must reference the SAS-laden URL, force-install azcopy,
        # and use azcopy for the download (not raw curl).
        lines = "\n".join(r.pod_setup_lines)
        assert fake_sas in lines
        assert "azcopy copy" in lines
        # azcopy bootstrap is wired to the configured retry count
        assert "seq 1 7" in lines
        assert "downloadazcopy-v10-linux" in lines
        assert r.code_path.startswith("/tmp/aj-code/j-xyz-")
        # extract_dir is referenced in tar xzf
        assert r.code_path in lines

    def test_az_cli_missing_yields_clean_error(self, tmp_path: Path):
        cfg = _vol_cfg(code_dir=str(tmp_path))
        spec = JobSpec(name="j", service="volcano", extra={**(_blob_extra(storage_account="sa", container="c"))})
        with patch.object(
            blob_mod.subprocess, "run", side_effect=FileNotFoundError("az")
        ):
            r = BlobUploader().prepare(cfg, spec, namespace="ns")
        assert r.ok is False
        assert "Azure CLI" in r.error and "kubectl-exec" in r.error

    def test_az_cli_nonzero_exit_surfaces_stderr(self, tmp_path: Path):
        cfg = _vol_cfg(code_dir=str(tmp_path))
        spec = JobSpec(name="j", service="volcano", extra={**(_blob_extra(storage_account="sa", container="c"))})
        fake = subprocess.CompletedProcess(
            args=["az"], returncode=2, stdout="", stderr="AADSTS50076: MFA required"
        )
        with patch.object(blob_mod.subprocess, "run", return_value=fake):
            r = BlobUploader().prepare(cfg, spec, namespace="ns")
        assert r.ok is False
        assert "MFA required" in r.error
        assert "Storage Blob Delegator" in r.error

    def test_blob_put_403_no_retry_storm(self, tmp_path: Path):
        (tmp_path / "f.txt").write_text("x")
        cfg = _vol_cfg(code_dir=str(tmp_path))
        spec = JobSpec(name="j", service="volcano", extra={**(_blob_extra(storage_account="sa", container="c"))})
        with patch.object(
            blob_mod, "_generate_sas_token", return_value="sas"
        ), patch.object(blob_mod.requests, "put") as m_put:
            m_put.return_value = MagicMock(status_code=403, text="Forbidden")
            r = BlobUploader().prepare(cfg, spec, namespace="ns")
        assert r.ok is False
        assert "403" in r.error
        # 401/403/404 should break out immediately, not retry 3x
        assert m_put.call_count == 1


class TestBuildVolcanoJobInjection:
    def test_default_layout_preserves_legacy_script(self):
        cfg = _vol_cfg()
        spec = build_volcano_job(cfg, namespace="ns")
        args = spec["spec"]["tasks"][0]["template"]["spec"]["containers"][0]["args"][0]
        # Legacy PVC path
        assert "/mnt/pvc/aj_code/j-abc" in args
        # No curl/blob script
        assert "curl" not in args

    def test_custom_code_path_and_setup_lines(self):
        cfg = _vol_cfg()
        spec = build_volcano_job(
            cfg,
            namespace="ns",
            code_setup_lines=[
                "echo before",
                "curl -fL -o /tmp/code.tgz 'https://blob/x'",
                "tar xzf /tmp/code.tgz -C /tmp/extract",
            ],
            code_path="/tmp/extract",
        )
        args = spec["spec"]["tasks"][0]["template"]["spec"]["containers"][0]["args"][0]
        assert "echo before" in args
        assert "curl -fL -o /tmp/code.tgz" in args
        # cp -a copies from custom code_path
        assert "cp -a /tmp/extract/. " in args
        # legacy PVC path is no longer baked in
        assert "/mnt/pvc/aj_code/j-abc" not in args


class TestSubmitViaVolcanoStrategyDispatch:
    """End-to-end: submit_via_volcano picks the right uploader per request."""

    @pytest.fixture(autouse=True)
    def _kubectl_present(self):
        with patch(
            "azure_jobs.backend.volcano.entry.shutil.which",
            return_value="/usr/bin/kubectl",
        ):
            yield

    def _run(self, strategy: str, uploader_result: CodeUploadResult) -> JobResult:
        spec = JobSpec(name="j-abc", service="volcano", code_dir="/some/code", extra={"code_upload": {"strategy": strategy}}, backend_spec=VolcanoOpts(namespace="ns-x"))
        fake_uploader = MagicMock()
        fake_uploader.name = strategy
        fake_uploader.prepare.return_value = uploader_result

        with patch(
            "azure_jobs.backend.volcano.entry.pick_uploader",
            return_value=fake_uploader,
        ), patch(
            "azure_jobs.backend.volcano.entry.subprocess.run"
        ) as m_run:
            m_run.return_value = subprocess.CompletedProcess(
                args=["kubectl"], returncode=0, stdout="job created", stderr=""
            )
            return submit_via_volcano(spec)

    def test_dispatches_to_configured_strategy(self):
        r = self._run(
            "blob",
            CodeUploadResult(
                ok=True,
                pod_setup_lines=["curl ..."],
                code_path="/tmp/extract",
            ),
        )
        assert r.status == "submitted"

    def test_failure_includes_strategy_name_and_detail(self):
        r = self._run(
            "blob",
            CodeUploadResult(
                ok=False,
                error="Blob upload failed: az not logged in",
            ),
        )
        assert r.status == "failed"
        assert "blob" in r.error
        assert "az not logged in" in r.error
        # short note also includes strategy
        assert "blob" in r.note

    def test_unknown_strategy_returns_clean_error_not_crash(self):
        spec = JobSpec(name="j-abc", service="volcano", code_dir="/some/code", extra={"code_upload": {"strategy": "hocus-pocus"}}, backend_spec=VolcanoOpts(namespace="ns-x"))
        r = submit_via_volcano(spec)
        assert r.status == "failed"
        assert "hocus-pocus" in r.error
        assert "Available" in r.error


class TestResolveNamespace:
    def test_explicit_namespace_wins(self):
        cfg = _vol_cfg()
        cfg.namespace = "my-ns"
        assert resolve_namespace(cfg) == "my-ns"

    def test_falls_back_to_kubectl_default_when_lookup_fails(self):
        cfg = _vol_cfg()
        cfg.namespace = ""
        with patch(
            "azure_jobs.backend.volcano.config.subprocess.run",
            side_effect=FileNotFoundError("kubectl"),
        ):
            assert resolve_namespace(cfg) == "default"


class TestScriptLoader:
    """Tiny loader for bash snippets under uploaders/scripts/."""

    def test_install_azcopy_round_trip(self):
        lines = load_script("install_azcopy.sh", RETRIES=7)
        text = "\n".join(lines)
        assert "seq 1 7" in text
        assert "downloadazcopy-v10-linux" in text
        assert "{RETRIES}" not in text

    def test_blob_download_round_trip(self):
        lines = load_script(
            "blob_download.sh",
            EXTRACT_DIR="/tmp/aj-code/jx",
            URL_WITH_SAS="https://sa.blob.core.windows.net/c/x.tgz?sv=…&sig=…",
        )
        text = "\n".join(lines)
        assert "azcopy copy" in text
        assert "/tmp/aj-code/jx" in text
        assert "sv=" in text and "sig=" in text
        assert "{EXTRACT_DIR}" not in text
        assert "{URL_WITH_SAS}" not in text

    def test_bash_constructs_are_left_alone(self):
        """Bash's ${var}/$(cmd)/{ … } must survive the {KEY} replacement."""
        lines = load_script("install_azcopy.sh", RETRIES=1)
        text = "\n".join(lines)
        assert "$(uname -m)" in text
        assert "$(mktemp" in text
        # bash command grouping `{ ... }` (with spaces) is not a {KEY} match
        assert "mkdir -p /tmp/aj-bin" in text


class TestRenderAmltDoesNotLeakExtra:
    """JobSpec.extra is aj-only — it must never reach the amlt-bound YAML."""

    def test_extra_does_not_appear_in_rendered_yaml(self, tmp_path: Path):
        from azure_jobs.job import build_job_spec, render_amlt_yaml

        tmpl = Template.from_dict(
            {
                "jobs": [{"name": "j", "sku": "x", "command": ["echo"]}],
                "target": {"service": "volcano"},
                "environment": {"image": "img"},
                "_extra": {
                    "code_upload": {
                        "strategy": "blob",
                        "blob": {"storage_account": "secret", "container": "c"},
                    },
                    "future_feature": {"foo": "bar"},
                },
            }
        )
        spec = build_job_spec(
            tmpl,
            name="j",
            sid="sid",
            sku="x",
            user_command="echo",
            user_args=(),
            nodes=1,
            gpus_per_node=1,
            code_dir=str(tmp_path),
        )
        rendered = render_amlt_yaml(spec)
        import yaml as _yaml

        y = _yaml.safe_dump(rendered, sort_keys=False)
        for needle in ("_extra", "code_upload", "future_feature", "secret"):
            assert needle not in y, f"{needle!r} leaked into amlt YAML"
