"""Pure and filesystem helpers behind ``aj template init``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import yaml

from azure_jobs.client.cli import _template_init as mod
from azure_jobs.shared.errors import AuthError
from azure_jobs.shared.types.azure import SeriesQuota, SlaTierQuota, VCInfo

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("My VC / H100", "my-vc-h100"),
        ("...", "..."),
        ("***", "default"),
    ],
)
def test_sanitise(raw: str, expected: str) -> None:
    assert mod._sanitise(raw) == expected


def test_sku_for_gpu_and_cpu() -> None:
    assert mod._sku_for("H100", 80) == "{nodes}x80G{processes}-H100"
    assert mod._sku_for("CPU", 0) == "{nodes}xC{processes}"


def test_write_yaml_creates_parent(tmp_path) -> None:
    path = tmp_path / "nested" / "value.yaml"
    mod._write_yaml(path, {"base": None, "config": {"x": 1}})
    assert yaml.safe_load(path.read_text())["config"]["x"] == 1


def test_ensure_script_and_base_files_are_idempotent(tmp_path, monkeypatch) -> None:
    home = tmp_path / ".azure_jobs"
    templates = home / "template"
    monkeypatch.setattr("azure_jobs.shared.const.AJ_HOME", home)
    monkeypatch.setattr("azure_jobs.shared.const.AJ_TEMPLATE_HOME", templates)

    with patch("azure_jobs.client.ui.dim"):
        mod._ensure_install_sh()
        mod._ensure_copy_ssh_sh()
        mod._ensure_environment_base()
        mod._ensure_template_base()

    install = home / "scripts" / "install.sh"
    copy_ssh = home / "scripts" / "copy_ssh.sh"
    environment = home / "environment" / "base.yaml"
    template = templates / "base.yaml"
    assert install.stat().st_mode & 0o111
    assert copy_ssh.stat().st_mode & 0o111
    assert yaml.safe_load(environment.read_text())["config"]["jobs"][0]["identity"]
    assert ".git/" in yaml.safe_load(template.read_text())["config"]["code"]["ignore"]

    install.write_text("custom", encoding="utf-8")
    with patch("azure_jobs.client.ui.dim"):
        mod._ensure_install_sh()
    assert install.read_text() == "custom"


def _quota_client(rows):
    client = MagicMock()
    client.__enter__.return_value = client
    client.quota.list.return_value = rows
    return client


def test_generate_leaves_writes_unique_positive_quota(tmp_path, monkeypatch) -> None:
    templates = tmp_path / "template"
    monkeypatch.setattr("azure_jobs.shared.const.AJ_TEMPLATE_HOME", templates)

    quota = SeriesQuota(
        series="NDH100v5",
        accelerator="H100",
        gpu_memory=80,
        user_limit=SlaTierQuota(limit=2, used=0),
    )
    duplicate = SeriesQuota(
        series="OTHER",
        accelerator="H100",
        gpu_memory=80,
        user_limit=SlaTierQuota(limit=1, used=0),
    )
    ignored = SeriesQuota(
        series="ZERO",
        accelerator="A100",
        gpu_memory=80,
        user_limit=SlaTierQuota(limit=0, used=0),
    )
    vc = VCInfo("fast", "rg", "sub", quotas=[quota, duplicate, ignored])

    with patch("azure_jobs.connect", return_value=_quota_client([vc])):
        rows = mod._generate_leaves(
            base_refs=["base", "environment.sing"],
            workspace={"workspace_name": "ws"},
            force=False,
        )

    assert rows == [
        {
            "leaf": "fast_H100_80",
            "vc": "fast",
            "accelerator": "H100",
            "memory": "80",
            "status": "wrote",
        }
    ]
    payload = yaml.safe_load((templates / "fast_H100_80.yaml").read_text())
    assert payload["config"]["target"]["workspace_name"] == "ws"

    with patch("azure_jobs.connect", return_value=_quota_client([vc])):
        rows = mod._generate_leaves(
            base_refs=["base"],
            workspace={"workspace_name": "ws"},
            force=False,
        )
    assert rows[0]["status"] == "skipped"


def test_generate_leaves_reports_no_usable_quota(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("azure_jobs.shared.const.AJ_TEMPLATE_HOME", tmp_path)
    warning = MagicMock()
    with (
        patch("azure_jobs.connect", return_value=_quota_client([])),
        patch("azure_jobs.client.ui.warning", warning),
    ):
        assert mod._generate_leaves(
            base_refs=[],
            workspace={"workspace_name": "ws"},
            force=False,
        ) == []
    warning.assert_called_once()


def test_generate_leaves_preserves_domain_errors() -> None:
    with patch("azure_jobs.connect", side_effect=AuthError("login")):
        with pytest.raises(AuthError, match="login"):
            mod._generate_leaves(
                base_refs=[],
                workspace={"workspace_name": "ws"},
                force=False,
            )


def test_generate_leaves_surfaces_unexpected_errors() -> None:
    with patch("azure_jobs.connect", side_effect=RuntimeError("broken")):
        with pytest.raises(SystemExit):
            mod._generate_leaves(
                base_refs=[],
                workspace={"workspace_name": "ws"},
                force=False,
            )
