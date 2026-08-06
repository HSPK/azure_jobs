from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

import azure_jobs.shared.job.build as build_mod
import azure_jobs.shared.template.validate as validate_mod
from azure_jobs.shared.errors import ConfigError, TemplateError
from azure_jobs.shared.job.spec import StorageMount
from azure_jobs.shared.template.models import Template


def test_build_helpers_cover_storage_command_and_env_edges(caplog) -> None:
    mount = StorageMount("acct", "cont", "/mnt")
    normalized = build_mod._normalize_storage(
        {
            "typed": mount,
            "mapped": {
                "storage_account_name": "acct-2",
                "container_name": "cont-2",
                "mount_dir": "/mnt-2",
            },
        }
    )

    assert normalized["typed"] is mount
    assert normalized["mapped"].mount_dir == "/mnt-2"
    assert build_mod._normalize_template_commands("echo hi") == ["echo hi"]
    assert build_mod._normalize_template_commands(["a", "b"]) == ["a", "b"]
    assert build_mod._normalize_template_commands(None) == []

    with pytest.raises(TypeError, match="Unsupported storage entry"):
        build_mod._normalize_storage({"bad": 1})

    merged = build_mod._merge_env({"AJ_NAME": "user", "KEEP": "x"}, {"AJ_NAME": "aj"})
    assert merged == {"AJ_NAME": "aj", "KEEP": "x"}
    assert "Template env overridden by aj-injected vars: AJ_NAME" in caplog.text


def test_build_job_spec_dedupes_ignore_uses_default_cwd_and_raises_without_jobs() -> None:
    template = Template.from_dict(
        {
            "target": {"service": "aml", "name": "cpu"},
            "environment": {"image": "python:3.12", "setup": ["pip install -e ."]},
            "jobs": [
                {
                    "sku": "cpu",
                    "command": "echo template",
                    "submit_args": {"env": {"USER_ONLY": "1"}},
                }
            ],
            "code": {"ignore": ["build/", "*.pyc", "build/"]},
            "_extra": {"code_upload": {"mode": "blob"}},
        }
    )

    with (
        patch.object(build_mod, "read_ignore_file", return_value=["*.pyc", ".venv/", "build/"]),
        patch.object(build_mod.os, "getcwd", return_value="/repo/root"),
    ):
        spec = build_mod.build_job_spec(
            template,
            name="job-1",
            sid="sid-1",
            sku="cpu",
            user_command="python",
            user_args=("train.py",),
            template_name="demo",
            experiment="exp",
            nodes=2,
            gpus_per_node=3,
            processes_per_node=4,
            code_dir=None,
        )

    assert spec.code_dir == "/repo/root"
    assert spec.code_ignore == ["build/", "*.pyc", ".venv/"]
    assert spec.command[:3] == [
        "[ -f /tmp/.aj_ssh_env ] && source /tmp/.aj_ssh_env",
        "export PATH=$HOME/.local/bin:$PATH",
        "echo template",
    ]
    assert spec.command[-1].startswith("python")
    assert spec.extra == {"code_upload": {"mode": "blob"}}
    assert spec.env_vars["AJ_PROCESSES"] == "6"
    assert spec.env_vars["AJ_PROCESSES_PER_NODE"] == "4"
    assert spec.env_vars["USER_ONLY"] == "1"

    with pytest.raises(TemplateError, match="missing 'jobs' section"):
        build_mod.build_job_spec(
            Template.from_dict({"target": {"service": "aml"}}),
            name="job-1",
            sid="sid-1",
            sku="cpu",
            user_command="echo",
            user_args=(),
            nodes=1,
        )


def test_validate_template_reports_inheritance_and_structural_issues(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    missing.write_text("base: parent\n", encoding="utf-8")
    with patch.object(validate_mod, "read_conf", side_effect=ConfigError("boom")):
        assert validate_mod.validate_template(missing) == ["inheritance error: boom"]

    plain = tmp_path / "plain.yaml"
    plain.write_text("jobs: []\n", encoding="utf-8")
    with patch.object(validate_mod, "read_conf", return_value={"jobs": []}):
        assert validate_mod.validate_template(plain) == []

    templ = tmp_path / "templ.yaml"
    templ.write_text("base: parent\n", encoding="utf-8")
    with patch.object(validate_mod, "read_conf", return_value={"target": {}}):
        assert validate_mod.validate_template(templ) == [
            "missing 'jobs' key",
            "target missing 'service'",
            "target missing 'name'",
        ]
    with patch.object(validate_mod, "read_conf", return_value={"jobs": [], "target": {}}):
        assert validate_mod.validate_template(templ) == [
            "'jobs' must be a non-empty list",
            "target missing 'service'",
            "target missing 'name'",
        ]
    with patch.object(
        validate_mod,
        "read_conf",
        return_value={"jobs": [{"name": "j"}], "target": "aml"},
    ):
        assert validate_mod.validate_template(templ) == [
            "first job missing 'sku' key",
            "'target' must be a dict",
        ]
