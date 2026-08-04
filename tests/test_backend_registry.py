"""Backend-registration tests: each service owns its JobSpec wiring."""

from __future__ import annotations

from typing import Any

import pytest

from azure_jobs.shared.spec import (
    SpecHooks,
    get_spec_hooks,
    known_services,
    register_spec,
)
from azure_jobs.shared.errors import BackendError
from azure_jobs.shared.job import build_job_spec
from azure_jobs.shared.job.spec import JobResult, JobSpec
from azure_jobs.shared.template.models import Template


def _fake_submit(req: JobSpec, *, on_event=None) -> JobResult:  # noqa: ARG001
    return JobResult(job_name=req.name, status="submitted")


def _build_tmpl(**target_fields: Any) -> Template:
    return Template.from_dict(
        {
            "jobs": [{"name": "j", "sku": "x"}],
            "target": target_fields,
        }
    )


class TestBuildSpecBackendIsTheOnlyDispatch:
    """build.py has no service-specific branches; the refine hook does all the work."""

    def test_volcano_hook_sanitises_name_and_populates_extra(self, tmp_path):
        tmpl = _build_tmpl(
            service="volcano",
            namespace="ns-x",
            queue="bonete04",
            context="ctx",
            gpus_per_node=4,
            memory="32Gi",
            labels={"team": "infra"},
        )
        spec = build_job_spec(
            tmpl,
            name="my_job.name",  # underscores + dots are illegal in DNS-1035
            sid="s",
            sku="x",
            user_command="echo",
            user_args=(),
            nodes=1,
            gpus_per_node=1,
            code_dir=str(tmp_path),
        )
        # Volcano build_spec_backend sanitised the name (no _, no .)
        assert "_" not in spec.name and "." not in spec.name
        # And populated extra["volcano"] with a typed VolcanoOpts:
        from azure_jobs.shared.opts.volcano import VolcanoOpts

        vol = spec.backend_spec
        assert isinstance(vol, VolcanoOpts)
        assert vol.namespace == "ns-x"
        assert vol.queue == "bonete04"
        assert vol.context == "ctx"
        assert vol.gpus_per_node == 4
        assert vol.memory == "32Gi"
        assert vol.labels == {"team": "infra"}
        # AJ_NAME reflects the post-mutation name (hook ran before env-vars).
        assert spec.env_vars["AJ_NAME"] == spec.name
        # Slim JobSpec must not have a typed `volcano` slot anymore.
        assert not hasattr(spec, "volcano")

    def test_sing_hook_moves_sub_rg_into_extra_aml(self, tmp_path):
        tmpl = _build_tmpl(
            service="sing",
            subscription_id="sub-1",
            resource_group="rg-1",
            workspace_name="ws-1",
        )
        spec = build_job_spec(
            tmpl,
            name="j",
            sid="s",
            sku="x",
            user_command="echo",
            user_args=(),
            nodes=1,
            gpus_per_node=1,
            code_dir=str(tmp_path),
        )
        aml = spec.backend_spec
        # Singularity: VC sub/rg live under extra["aml"]; top-level cleared.
        assert aml.subscription_id == ""
        assert aml.resource_group == ""
        assert aml.vc_subscription_id == "sub-1"
        assert aml.vc_resource_group == "rg-1"
        assert aml.workspace_name == "ws-1"
        # Slim JobSpec no longer carries any typed AML/sing fields.
        assert not hasattr(spec, "subscription_id")
        assert not hasattr(spec, "workspace_name")
        assert not hasattr(spec, "sing")

    def test_aml_hook_is_noop_for_sub_rg(self, tmp_path):
        tmpl = _build_tmpl(
            service="aml",
            subscription_id="sub-1",
            resource_group="rg-1",
        )
        spec = build_job_spec(
            tmpl,
            name="j",
            sid="s",
            sku="x",
            user_command="echo",
            user_args=(),
            nodes=1,
            gpus_per_node=1,
            code_dir=str(tmp_path),
        )
        aml = spec.backend_spec
        # AML keeps sub/rg under backend_spec.
        assert aml.subscription_id == "sub-1"
        assert aml.resource_group == "rg-1"
        # No sing-specific vc_* set.
        assert aml.vc_subscription_id == ""
        assert aml.vc_resource_group == ""
        # extra is reserved for template _extra (empty here).
        assert spec.extra == {}


class TestBackendRegistryAcceptsBuildSpecBackend:
    """The OCP seam: register a new backend → build_job_spec handles it."""

    def teardown_method(self):
        # The registry is process-global; undo our test registration.
        from azure_jobs.shared.spec import _HOOKS as _REGISTRY

        _REGISTRY.pop("test-foo", None)

    def test_new_backend_with_hooks_is_picked_up_without_editing_build(
        self, tmp_path
    ):
        def _build_foo(template: Template) -> dict:
            return {"seen": True}

        def _normalize_foo(name: str) -> str:
            return f"foo--{name}"

        register_spec(
            "test-foo",
build_spec_backend=_build_foo,
            normalize_job_name=_normalize_foo,
        )

        tmpl = _build_tmpl(service="test-foo")
        spec = build_job_spec(
            tmpl,
            name="j",
            sid="s",
            sku="x",
            user_command="echo",
            user_args=(),
            nodes=1,
            gpus_per_node=1,
            code_dir=str(tmp_path),
        )
        assert spec.name == "foo--j"
        assert spec.backend_spec == {"seen": True}
        # AJ_NAME reflects the normalised name.
        assert spec.env_vars["AJ_NAME"] == "foo--j"

    def test_backend_without_hooks_uses_noop(self, tmp_path):
        register_spec(
            "test-foo",
)  # no hooks

        tmpl = _build_tmpl(service="test-foo", subscription_id="sub")
        spec = build_job_spec(
            tmpl,
            name="j",
            sid="s",
            sku="x",
            user_command="echo",
            user_args=(),
            nodes=1,
            gpus_per_node=1,
            code_dir=str(tmp_path),
        )
        # No-op hooks: name untouched, backend_spec stays None.
        assert spec.name == "j"
        assert spec.backend_spec is None
        assert spec.extra == {}

    def test_unknown_service_raises_backend_error(self, tmp_path):
        tmpl = _build_tmpl(service="does-not-exist")
        with pytest.raises(BackendError, match="No job description registered"):
            build_job_spec(
                tmpl,
                name="j",
                sid="s",
                sku="x",
                user_command="echo",
                user_args=(),
                nodes=1,
                gpus_per_node=1,
                code_dir=str(tmp_path),
            )


class TestBuildPyHasNoServiceSpecificBranches:
    """Static guard: build.py must stay backend-agnostic."""

    def test_no_service_string_literals(self):
        import re
        from pathlib import Path

        import azure_jobs.shared.job.build as build_mod

        src = Path(build_mod.__file__).read_text(encoding="utf-8")
        # Strip comments and docstrings so we only inspect runtime code.
        # (KISS check: no `service ==` / `service in (...)` branching.)
        offending = re.findall(r"\bservice\s*(==|in)\b", src)
        assert offending == [], (
            "build.py must not branch on service. Move service-specific "
            "wiring into the backend's build_spec_backend hook."
        )


class TestSpecHooksShape:
    def test_built_in_backends_have_entries(self):
        assert {"aml", "sing", "volcano"}.issubset(set(known_services()))

    def test_entry_has_build_spec_backend_callable(self):
        e = get_spec_hooks("volcano")
        assert isinstance(e, SpecHooks)
        assert callable(e.build_spec_backend)
