"""Hermetic helper tests for template init, quota, and sku commands."""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest

from azure_jobs.client.cli import _template_init as template_init_mod
from azure_jobs.client.cli import quota as quota_mod
from azure_jobs.client.cli import sku as sku_mod
from azure_jobs.client.ui.console import console as ui_console
from azure_jobs.server.az_client.arm import InstanceTypeInfo
from azure_jobs.shared import const
from azure_jobs.shared.errors import AJError
from azure_jobs.shared.types.azure import SeriesQuota, SlaTierQuota, VCInfo


class _Context(SimpleNamespace):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_pick_row_accepts_numeric_selection() -> None:
    rows = [{"_label": "one", "value": 1}, {"_label": "two", "value": 2}]
    with (
        patch("click.prompt", return_value="2"),
        patch.object(ui_console, "print"),
    ):
        picked = template_init_mod._pick_row("Choice", rows)

    assert picked["value"] == 2


def test_pick_row_rejects_invalid_selection() -> None:
    rows = [{"_label": "one"}]
    with (
        patch("click.prompt", return_value="x"),
        patch.object(ui_console, "print"),
    ):
        with pytest.raises(click.ClickException, match="Invalid selection: x"):
            template_init_mod._pick_row("Choice", rows)


def test_pick_account_uses_discovered_identity(monkeypatch, tmp_path: Path) -> None:
    home = tmp_path / ".azure_jobs"
    monkeypatch.setattr(const, "AJ_HOME", home)
    uai = SimpleNamespace(
        name="My Identity",
        resource_group="rg",
        location="westus",
        id="/subscriptions/s/resourceGroups/rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/MyIdentity",
    )
    conn = _Context(uai=SimpleNamespace(list=MagicMock(return_value=[uai])))
    with (
        patch("azure_jobs.connect", return_value=conn),
        patch("azure_jobs.client.cli._template_init._pick_row", return_value={"uai": uai}),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.ui.dim"),
    ):
        ref = template_init_mod._pick_account()

    assert ref == "account.my-identity"
    payload = (home / "account" / "my-identity.yaml").read_text()
    assert "_AZUREML_SINGULARITY_JOB_UAI" in payload


def test_pick_account_prompts_when_none_are_visible(
    monkeypatch, tmp_path: Path
) -> None:
    home = tmp_path / ".azure_jobs"
    monkeypatch.setattr(const, "AJ_HOME", home)
    conn = _Context(uai=SimpleNamespace(list=MagicMock(return_value=[])))
    with (
        patch("azure_jobs.connect", return_value=conn),
        patch("click.prompt", return_value="/subscriptions/s/.../ManualIdentity"),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.ui.dim"),
    ):
        ref = template_init_mod._pick_account()

    assert ref == "account.manualidentity"
    assert (home / "account" / "manualidentity.yaml").exists()


def test_pick_workspace_exits_when_no_workspaces_are_visible() -> None:
    error = MagicMock()
    conn = _Context(ws=SimpleNamespace(list=MagicMock(return_value=[])))
    with (
        patch("azure_jobs.connect", return_value=conn),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.ui.error", error),
    ):
        with pytest.raises(SystemExit) as exc:
            template_init_mod._pick_workspace()

    assert exc.value.code == 1
    error.assert_called_once_with("No AML workspaces visible to this account.")


def test_run_wizard_ignores_positional_leaf_name() -> None:
    summary = MagicMock()
    success = MagicMock()
    info = MagicMock()
    warning = MagicMock()
    leaves = [
        {
            "leaf": "fast_H100_80",
            "vc": "fast",
            "accelerator": "H100",
            "memory": "80",
            "status": "wrote",
        }
    ]
    with (
        patch("azure_jobs.client.cli._template_init._intro"),
        patch("azure_jobs.client.cli._template_init._ensure_template_base"),
        patch("azure_jobs.client.cli._template_init._step"),
        patch("azure_jobs.client.cli._template_init._pick_account", return_value="account.demo"),
        patch(
            "azure_jobs.client.cli._template_init._pick_environment",
            return_value=("environment.sing", "amlt-sing/image"),
        ),
        patch("azure_jobs.client.cli._template_init._pick_storage", return_value="storage.default"),
        patch(
            "azure_jobs.client.cli._template_init._pick_workspace",
            return_value={
                "workspace_name": "ws",
                "resource_group": "rg",
                "subscription_id": "sub",
            },
        ),
        patch("azure_jobs.client.cli._template_init._generate_leaves", return_value=leaves) as generate,
        patch("azure_jobs.client.cli._template_init._summary", summary),
        patch("azure_jobs.client.ui.success", success),
        patch("azure_jobs.client.ui.info", info),
        patch("azure_jobs.client.ui.warning", warning),
        patch.object(ui_console, "print"),
    ):
        template_init_mod.run_wizard("manual-name", force=True)

    warning.assert_called_once()
    generate.assert_called_once_with(
        base_refs=["base", "account.demo", "storage.default", "environment.sing"],
        workspace={
            "workspace_name": "ws",
            "resource_group": "rg",
            "subscription_id": "sub",
        },
        force=True,
    )
    summary.assert_called_once()
    assert success.call_args.args[0] == "Generated 1 leaf template(s)."
    assert info.call_args_list[-1].args[0] == "Try one:"


def test_intro_step_and_summary_render_without_leaves() -> None:
    console_print = MagicMock()
    with patch.object(ui_console, "print", console_print):
        template_init_mod._intro()
        template_init_mod._step(2, "Environment", "Pick an image")
        template_init_mod._summary(
            account_ref="account.demo",
            env_ref="environment.sing",
            image="amlt-sing/demo",
            storage_ref="storage.default",
            workspace={"workspace_name": "ws", "resource_group": "rg"},
            leaves=[],
        )

    assert console_print.call_count >= 5


def test_pick_account_warns_and_prompts_after_discovery_failure(
    monkeypatch, tmp_path: Path
) -> None:
    home = tmp_path / ".azure_jobs"
    monkeypatch.setattr(const, "AJ_HOME", home)
    warning = MagicMock()
    conn = _Context(uai=SimpleNamespace(list=MagicMock(side_effect=RuntimeError("boom"))))
    with (
        patch("azure_jobs.connect", return_value=conn),
        patch("click.prompt", return_value="/subscriptions/s/.../ManualIdentity"),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.ui.warning", warning),
        patch("azure_jobs.client.ui.dim"),
    ):
        ref = template_init_mod._pick_account()

    assert ref == "account.manualidentity"
    assert "Could not list UAIs" in warning.call_args.args[0]
    assert (home / "account" / "manualidentity.yaml").exists()


def test_pick_environment_uses_discovered_image(monkeypatch, tmp_path: Path) -> None:
    home = tmp_path / ".azure_jobs"
    monkeypatch.setattr(const, "AJ_HOME", home)
    with (
        patch(
            "azure_jobs.client.cli.images._fetch_sing_images",
            return_value=[{"name": "demo"}],
        ),
        patch(
            "azure_jobs.client.cli._template_init._pick_row",
            return_value={"image": "amlt-sing/demo"},
        ),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.cli._template_init._ensure_environment_base") as ensure_base,
        patch("azure_jobs.client.cli._template_init._ensure_copy_ssh_sh") as ensure_copy,
        patch("azure_jobs.client.cli._template_init._ensure_install_sh") as ensure_install,
        patch("azure_jobs.client.ui.dim"),
    ):
        ref, image = template_init_mod._pick_environment()

    assert (ref, image) == ("environment.sing", "amlt-sing/demo")
    assert "amlt-sing/demo" in (home / "environment" / "sing.yaml").read_text()
    ensure_base.assert_called_once_with()
    ensure_copy.assert_called_once_with()
    ensure_install.assert_called_once_with()


def test_pick_environment_prompts_when_image_discovery_fails(
    monkeypatch, tmp_path: Path
) -> None:
    home = tmp_path / ".azure_jobs"
    monkeypatch.setattr(const, "AJ_HOME", home)
    warning = MagicMock()
    with (
        patch(
            "azure_jobs.client.cli.images._fetch_sing_images",
            side_effect=RuntimeError("boom"),
        ),
        patch("click.prompt", return_value="custom/image:latest"),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.ui.warning", warning),
        patch("azure_jobs.client.cli._template_init._ensure_environment_base"),
        patch("azure_jobs.client.cli._template_init._ensure_copy_ssh_sh"),
        patch("azure_jobs.client.cli._template_init._ensure_install_sh"),
        patch("azure_jobs.client.ui.dim"),
    ):
        ref, image = template_init_mod._pick_environment()

    assert (ref, image) == ("environment.sing", "custom/image:latest")
    assert "Could not list images" in warning.call_args.args[0]


def test_pick_storage_supports_multiple_mounts(monkeypatch, tmp_path: Path) -> None:
    home = tmp_path / ".azure_jobs"
    monkeypatch.setattr(const, "AJ_HOME", home)
    sa1 = SimpleNamespace(name="sa-one", resource_group="rg", location="westus")
    sa2 = SimpleNamespace(name="sa-two", resource_group="rg", location="eastus")
    conn = _Context(sa=SimpleNamespace(list=MagicMock(return_value=[sa1, sa2])))
    with (
        patch("azure_jobs.connect", return_value=conn),
        patch(
            "azure_jobs.client.cli._template_init._pick_row",
            side_effect=[{"name": "sa-one"}, {"name": "sa-two"}],
        ),
        patch(
            "click.prompt",
            side_effect=["cont-a", "alias-a", "/mnt/a", "cont-b", "alias-b", "/mnt/b"],
        ),
        patch("click.confirm", side_effect=[True, False]),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.ui.info"),
        patch("azure_jobs.client.ui.dim"),
    ):
        ref = template_init_mod._pick_storage()

    assert ref == "storage.default"
    payload = (home / "storage" / "default.yaml").read_text()
    assert "alias-a" in payload
    assert "alias-b" in payload


def test_pick_storage_prompts_manually_after_discovery_failure(
    monkeypatch, tmp_path: Path
) -> None:
    home = tmp_path / ".azure_jobs"
    monkeypatch.setattr(const, "AJ_HOME", home)
    warning = MagicMock()
    conn = _Context(sa=SimpleNamespace(list=MagicMock(side_effect=RuntimeError("boom"))))
    with (
        patch("azure_jobs.connect", return_value=conn),
        patch(
            "click.prompt",
            side_effect=["manual-sa", "cont-a", "alias-a", "/mnt/a"],
        ),
        patch("click.confirm", return_value=False),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.ui.warning", warning),
        patch("azure_jobs.client.ui.info"),
        patch("azure_jobs.client.ui.dim"),
    ):
        ref = template_init_mod._pick_storage()

    assert ref == "storage.default"
    assert "Could not list storage accounts" in warning.call_args.args[0]


def test_pick_workspace_returns_selected_workspace() -> None:
    ws = SimpleNamespace(
        name="ws-demo",
        resource_group="rg-demo",
        location="westus",
        subscription_id="sub-1",
    )
    conn = _Context(ws=SimpleNamespace(list=MagicMock(return_value=[ws])))
    with (
        patch("azure_jobs.connect", return_value=conn),
        patch("azure_jobs.client.cli._template_init._pick_row", return_value={"ws": ws}),
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        picked = template_init_mod._pick_workspace()

    assert picked == {
        "workspace_name": "ws-demo",
        "resource_group": "rg-demo",
        "subscription_id": "sub-1",
    }


def test_run_wizard_with_no_generated_leaves_skips_follow_up_hint() -> None:
    summary = MagicMock()
    success = MagicMock()
    info = MagicMock()
    with (
        patch("azure_jobs.client.cli._template_init._intro"),
        patch("azure_jobs.client.cli._template_init._ensure_template_base"),
        patch("azure_jobs.client.cli._template_init._step"),
        patch("azure_jobs.client.cli._template_init._pick_account", return_value="account.demo"),
        patch(
            "azure_jobs.client.cli._template_init._pick_environment",
            return_value=("environment.sing", "amlt-sing/image"),
        ),
        patch("azure_jobs.client.cli._template_init._pick_storage", return_value="storage.default"),
        patch(
            "azure_jobs.client.cli._template_init._pick_workspace",
            return_value={
                "workspace_name": "ws",
                "resource_group": "rg",
                "subscription_id": "sub",
            },
        ),
        patch("azure_jobs.client.cli._template_init._generate_leaves", return_value=[]),
        patch("azure_jobs.client.cli._template_init._summary", summary),
        patch("azure_jobs.client.ui.success", success),
        patch("azure_jobs.client.ui.info", info),
        patch.object(ui_console, "print"),
    ):
        template_init_mod.run_wizard(None, force=False)

    summary.assert_called_once()
    success.assert_not_called()
    assert info.call_args.args[0].startswith("Auto-generating one leaf")


class TestQuotaHelpers:
    def test_show_sing_quotas_forwards_full_flag(self) -> None:
        vc = VCInfo(name="vc-1", resource_group="rg", subscription_id="sub")
        conn = _Context(quota=SimpleNamespace(list=MagicMock(return_value=[vc])))
        show_table = MagicMock()
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.show_sing_quota_table", show_table),
        ):
            quota_mod._show_sing_quotas(show_all=True, full=True)

        show_table.assert_called_once_with([vc], full=True)
        conn.quota.list.assert_called_once_with(include_zero=True)

    def test_show_aml_quotas_preserves_domain_errors(self) -> None:
        conn = _Context(ws=SimpleNamespace(computes=MagicMock(side_effect=AJError("login"))))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
        ):
            with pytest.raises(AJError, match="login"):
                quota_mod._show_aml_quotas(show_all=False)

    def test_show_aml_quotas_wraps_unexpected_exceptions(self) -> None:
        error = MagicMock()
        conn = _Context(ws=SimpleNamespace(computes=MagicMock(side_effect=RuntimeError("boom"))))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.error", error),
        ):
            with pytest.raises(SystemExit) as exc:
                quota_mod._show_aml_quotas(show_all=False)

        assert exc.value.code == 1
        assert "Could not discover workspaces" in error.call_args.args[0]

    def test_show_aml_quotas_rejects_empty_discovery(self) -> None:
        error = MagicMock()
        conn = _Context(ws=SimpleNamespace(computes=MagicMock(return_value={"pairs": [], "failures": []})))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.error", error),
        ):
            with pytest.raises(SystemExit) as exc:
                quota_mod._show_aml_quotas(show_all=False)

        assert exc.value.code == 1
        error.assert_called_once_with("No AML workspaces found")

    def test_show_aml_quotas_warns_when_only_empty_clusters_exist(self) -> None:
        warning = MagicMock()
        show_table = MagicMock()
        conn = _Context(
            ws=SimpleNamespace(
                computes=MagicMock(
                    return_value={
                        "pairs": [{"workspace": {"name": "ws-1"}, "computes": []}],
                        "failures": ["ws-x"],
                    }
                )
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.warning", warning),
            patch("azure_jobs.client.ui.show_aml_quota_table", show_table),
        ):
            quota_mod._show_aml_quotas(show_all=False)

        assert "Skipped 1 workspace(s)" in warning.call_args_list[0].args[0]
        assert (
            warning.call_args_list[1].args[0]
            == "No AML compute clusters found in any workspace"
        )
        show_table.assert_not_called()


class TestSkuHelpers:
    def test_sku_catalog_dedupes_regions_and_names(self) -> None:
        catalog_by_region = {
            "westus": [
                InstanceTypeInfo(name="sku-a", series_id="A", num_gpus=1, accelerator="A100"),
                InstanceTypeInfo(name="sku-b", series_id="B", num_gpus=1, accelerator="H100"),
            ],
            "eastus": [
                InstanceTypeInfo(name="sku-b", series_id="B", num_gpus=1, accelerator="H100"),
                InstanceTypeInfo(name="sku-c", series_id="C", num_gpus=0, accelerator="CPU"),
            ],
        }
        d = SimpleNamespace(sku=SimpleNamespace(list=lambda region: list(catalog_by_region[region])))
        vcs = [
            SimpleNamespace(region="westus"),
            SimpleNamespace(region="westus"),
            SimpleNamespace(region="eastus"),
        ]

        catalog, seen_names, seen_regions = sku_mod._sku_catalog(d, vcs)

        assert [item.name for item in catalog] == ["sku-a", "sku-b", "sku-c"]
        assert seen_names == {"sku-a", "sku-b", "sku-c"}
        assert seen_regions == {"westus", "eastus"}

    def test_sku_list_rejects_missing_virtual_clusters(self) -> None:
        error = MagicMock()
        conn = _Context(quota=SimpleNamespace(list=MagicMock(return_value=[])))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.error", error),
        ):
            with pytest.raises(SystemExit) as exc:
                sku_mod.sku_list.callback(show_all=False)

        assert exc.value.code == 1
        error.assert_called_once_with("No Singularity virtual clusters found")

    def test_sku_list_builds_catalog_and_renders_table(self) -> None:
        vc = SimpleNamespace(region="westus")
        info = InstanceTypeInfo(name="sku-a", series_id="A", num_gpus=1, accelerator="A100")
        conn = _Context(
            quota=SimpleNamespace(list=MagicMock(return_value=[vc])),
            sku=SimpleNamespace(list=MagicMock(return_value=[info])),
        )
        show_table = MagicMock()
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.show_sku_table", show_table),
        ):
            sku_mod.sku_list.callback(show_all=True)

        show_table.assert_called_once()
        assert show_table.call_args.args[0] == [vc]
        assert show_table.call_args.kwargs["catalog"] == [info]
