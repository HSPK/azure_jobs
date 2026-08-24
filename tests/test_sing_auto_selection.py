from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.client.ui import set_output_mode
from azure_jobs.client.ui.panels import _request_payload, show_submission_preview
from azure_jobs.server.az_client.arm.vc import VCQuotaAPI
from azure_jobs.server.submit.archive import ArchiveMetadata
from azure_jobs.server.submit.azureml import entry as entry_mod
from azure_jobs.server.submit.azureml.sku_match import (
    match_vc,
    select_best_vc,
)
from azure_jobs.server.submit.azureml.workspace import (
    ResolvedTarget,
    resolve_target,
)
from azure_jobs.shared.errors import ConfigError, SkuResolveError
from azure_jobs.shared.job.spec import JobSpec
from azure_jobs.shared.opts import AmlOpts
from azure_jobs.shared.sku import MatchedInstances
from azure_jobs.shared.template import Template
from azure_jobs.shared.template.validate import validate_template
from azure_jobs.shared.types.azure import SeriesQuota, SlaTierQuota, VCInfo
from azure_jobs.shared.types.instance import InstanceTypeInfo

pytestmark = pytest.mark.azure_unit


def _quota(
    series: str = "ND_A100_v4",
    *,
    accelerator: str = "A100",
    memory: int = 80,
    user: tuple[int, int | None] = (64, 0),
    premium: tuple[int, int | None] = (64, 0),
    standard: tuple[int, int | None] = (0, 0),
    basic: tuple[int, int | None] = (0, 0),
) -> SeriesQuota:
    quota = SeriesQuota(
        series=series,
        accelerator=accelerator,
        gpu_memory=memory,
        user_limit=SlaTierQuota(*user),
    )
    quota.tiers = {
        "Premium": SlaTierQuota(*premium),
        "Standard": SlaTierQuota(*standard),
        "Basic": SlaTierQuota(*basic),
    }
    return quota


def _vc(
    name: str,
    *quotas: SeriesQuota,
    subscription_id: str = "sub",
    resource_group: str = "rg",
    region: str = "westus2",
) -> VCInfo:
    return VCInfo(
        name=name,
        subscription_id=subscription_id,
        resource_group=resource_group,
        locations=[region],
        quotas=list(quotas),
    )


def _gpu(
    *,
    series: str = "ND_A100_v4",
    accelerator: str = "A100",
    memory: int = 80,
    gpus: int = 8,
    nvlink: bool = False,
) -> InstanceTypeInfo:
    return InstanceTypeInfo(
        name=f"Singularity.{series}",
        series_id=series,
        num_gpus=gpus,
        num_cores=96,
        accelerator=accelerator,
        gpu_memory_gb=memory,
        nvlink=nvlink,
    )


class _SkuAPI:
    def __init__(self, rows: list[InstanceTypeInfo]) -> None:
        self.rows = rows
        self.list = MagicMock(side_effect=self._list)

    def _list(
        self,
        region: str,
        *,
        subscription_id: str = "",
        strict: bool = False,
    ):
        return list(self.rows)


def _client(rows: list[InstanceTypeInfo]):
    return SimpleNamespace(sku=_SkuAPI(rows))


@pytest.mark.parametrize("target", [{"service": "sing"}, {"service": "sing", "name": ""}])
def test_sing_template_may_omit_or_empty_target_name(target: dict[str, str]) -> None:
    template = Template.from_dict(
        {"target": target, "jobs": [{"sku": "1x80G8-A100"}]}
    )

    assert AmlOpts.from_template(template).compute == ""


def test_auto_resolve_applies_filters_and_mutates_coordinates() -> None:
    wanted = _vc(
        "wanted",
        _quota(),
        subscription_id="sub-b",
        resource_group="rg-b",
    )
    other = _vc("other", _quota(), subscription_id="sub-a", resource_group="rg-a")
    client = _client([_gpu()])
    client.quota = SimpleNamespace(list=MagicMock(return_value=[other, wanted]))
    client.ws = SimpleNamespace(get=MagicMock())
    aml = AmlOpts(
        compute="",
        vc_subscription_id="sub-b",
        vc_resource_group="rg-b",
        subscription_id="ws-sub",
        resource_group="ws-rg",
        workspace_name="ws",
    )
    request = JobSpec(
        name="job",
        service="sing",
        sku="1x80G8-A100",
        nodes=1,
        gpus_per_node=8,
        backend_spec=aml,
    )

    resolved = resolve_target(request, arm_client=client)

    assert isinstance(resolved, ResolvedTarget)
    assert resolved.auto_selected is True
    assert resolved.vc is wanted
    assert resolved.matched_instances is not None
    assert aml.compute == "wanted"
    assert aml.vc_subscription_id == "sub-b"
    assert aml.vc_resource_group == "rg-b"
    assert aml.sla_tier == "Premium"
    assert aml.matched_instances == ["80G8-A100"]
    client.quota.list.assert_called_once_with(include_zero=True, strict=True)


def test_auto_selector_caches_catalog_per_subscription_and_region() -> None:
    candidates = [_vc("b", _quota()), _vc("a", _quota())]
    client = _client([_gpu()])

    selected = select_best_vc(
        "1x80G8-A100",
        candidates=candidates,
        tier="Premium",
        client=client,
        nodes=1,
        gpus_per_node=8,
    )

    assert selected.vc.name == "a"
    client.sku.list.assert_called_once_with(
        "westus2",
        subscription_id="sub",
        strict=True,
    )


def test_failed_strict_catalog_discovery_is_not_cached() -> None:
    vc = _vc("vc", _quota())
    client = _client([_gpu()])
    client.sku.list.side_effect = [OSError("catalog down"), [_gpu()]]
    cache = {}

    with pytest.raises(OSError, match="catalog down"):
        match_vc(
            "1x80G8-A100",
            vc=vc,
            tier="Premium",
            client=client,
            nodes=1,
            gpus_per_node=8,
            catalog_cache=cache,
        )

    assert cache == {}
    selection = match_vc(
        "1x80G8-A100",
        vc=vc,
        tier="Premium",
        client=client,
        nodes=1,
        gpus_per_node=8,
        catalog_cache=cache,
    )
    assert selection.vc is vc
    assert client.sku.list.call_count == 2


@pytest.mark.parametrize(
    ("row", "sku"),
    [
        (_gpu(accelerator="H100"), "1x80G8-A100"),
        (_gpu(memory=40), "1x80G8-A100"),
    ],
)
def test_catalog_accelerator_and_memory_must_match_exactly(
    row: InstanceTypeInfo,
    sku: str,
) -> None:
    with pytest.raises(SkuResolveError, match="no instance type"):
        match_vc(
            sku,
            vc=_vc("vc", _quota()),
            tier="Premium",
            client=_client([row]),
            nodes=1,
            gpus_per_node=8,
        )


def test_current_user_available_quota_is_required() -> None:
    vc = _vc("vc", _quota(user=(16, 12)))

    with pytest.raises(SkuResolveError, match=r"user quota < 8.*available 4"):
        match_vc(
            "1x80G8-A100",
            vc=vc,
            tier="Premium",
            client=_client([_gpu()]),
            nodes=1,
            gpus_per_node=8,
        )


def test_current_tier_available_quota_is_required() -> None:
    vc = _vc("vc", _quota(premium=(16, 12)))

    with pytest.raises(SkuResolveError, match=r"tier quota available.*Premium=4"):
        match_vc(
            "1x80G8-A100",
            vc=vc,
            tier="Premium",
            client=_client([_gpu()]),
            nodes=1,
            gpus_per_node=8,
        )


def test_cpu_selection_records_concrete_vcpu_capacity() -> None:
    vc = _vc(
        "cpu",
        _quota(
            series="Eadsv5",
            accelerator="CPU",
            memory=0,
            user=(32, 0),
            premium=(32, 0),
        ),
    )
    row = InstanceTypeInfo(
        name="Singularity.E16ads_v5",
        series_id="Eadsv5",
        num_cores=16,
        accelerator="CPU",
    )

    selection = match_vc(
        "1xC1",
        vc=vc,
        tier="Premium",
        client=_client([row]),
        nodes=1,
    )

    assert selection.required_capacity == 16
    assert selection.remaining_capacity == 16


def test_exact_one_gpu_a100_40g_boundary_selects_only_exact_instance() -> None:
    vc = _vc(
        "a100-40g",
        _quota(
            series="NC_A100_40",
            memory=40,
            user=(1, 0),
            premium=(1, 0),
        ),
    )
    rows = [
        _gpu(
            series="NC_A100_40",
            accelerator="A100",
            memory=40,
            gpus=1,
        ),
        _gpu(
            series="NC_A100_40",
            accelerator="A100",
            memory=80,
            gpus=1,
        ),
        _gpu(
            series="NC_A100_40",
            accelerator="A100",
            memory=40,
            gpus=2,
        ),
        _gpu(
            series="NC_A100_40",
            accelerator="H100",
            memory=40,
            gpus=1,
        ),
    ]

    selected = select_best_vc(
        "1x40G1-A100",
        candidates=[vc],
        tier="Premium",
        client=_client(rows),
        nodes=1,
        gpus_per_node=1,
    )

    assert selected.available_capacity == 1
    assert selected.remaining_capacity == 0
    assert [item.shorthand for item in selected.matched_instances.instances] == [
        "40G1-A100"
    ]


def test_exact_one_gpu_a100_40g_falls_back_when_premium_is_full() -> None:
    selected = select_best_vc(
        "1x40G1-A100",
        candidates=[
            _vc(
                "a100-40g",
                _quota(
                    series="NC_A100_40",
                    memory=40,
                    user=(1, 0),
                    premium=(1, 1),
                    standard=(1, 0),
                ),
            )
        ],
        tier="Premium",
        client=_client(
            [
                _gpu(
                    series="NC_A100_40",
                    accelerator="A100",
                    memory=40,
                    gpus=1,
                )
            ]
        ),
        nodes=1,
        gpus_per_node=1,
    )

    assert selected.matched_instances.effective_tier == "Standard"
    assert selected.available_capacity == 1


def test_requested_tier_beats_larger_lower_tier_quota() -> None:
    requested = _vc("premium", _quota(premium=(8, 0)))
    fallback = _vc(
        "standard",
        _quota(premium=(0, 0), standard=(64, 0)),
    )

    selected = select_best_vc(
        "1x80G8-A100",
        candidates=[fallback, requested],
        tier="Premium",
        client=_client([_gpu()]),
        nodes=1,
        gpus_per_node=8,
    )

    assert selected.vc.name == "premium"
    assert selected.matched_instances.effective_tier == "Premium"


def test_greater_remaining_effective_quota_breaks_ties() -> None:
    smaller = _vc("a", _quota(user=(16, 0), premium=(16, 0)))
    larger = _vc("z", _quota(user=(32, 0), premium=(32, 0)))

    selected = select_best_vc(
        "1x80G8-A100",
        candidates=[smaller, larger],
        tier="Premium",
        client=_client([_gpu()]),
        nodes=1,
        gpus_per_node=8,
    )

    assert selected.vc.name == "z"
    assert selected.available_capacity == 32
    assert selected.remaining_capacity == 24


def test_stable_vc_coordinates_are_the_final_tie_break() -> None:
    candidates = [
        _vc("a", _quota(), subscription_id="sub-b", resource_group="rg"),
        _vc("b", _quota(), subscription_id="sub-a", resource_group="rg"),
        _vc("a", _quota(), subscription_id="sub-a", resource_group="rg-z"),
        _vc("a", _quota(), subscription_id="sub-a", resource_group="rg-a"),
    ]

    selected = select_best_vc(
        "1x80G8-A100",
        candidates=candidates,
        tier="Premium",
        client=_client([_gpu()]),
        nodes=1,
        gpus_per_node=8,
    )

    assert (
        selected.vc.name,
        selected.vc.subscription_id,
        selected.vc.resource_group,
    ) == ("a", "sub-a", "rg-a")


def test_requested_nvlink_beats_greater_non_nvlink_capacity() -> None:
    with_nvlink = _vc(
        "nvlink",
        _quota(series="ND_NVLINK", user=(8, 0), premium=(8, 0)),
    )
    without_nvlink = _vc(
        "large",
        _quota(series="ND_PLAIN", user=(64, 0), premium=(64, 0)),
    )
    rows = [
        _gpu(series="ND_NVLINK", nvlink=True),
        _gpu(series="ND_PLAIN", nvlink=False),
    ]

    selected = select_best_vc(
        "1x80G8-A100-NvLink",
        candidates=[without_nvlink, with_nvlink],
        tier="Premium",
        client=_client(rows),
        nodes=1,
        gpus_per_node=8,
    )

    assert selected.vc.name == "nvlink"
    assert selected.matched_instances.nvlink_satisfied is True


def test_premium_non_nvlink_beats_standard_nvlink_across_vcs() -> None:
    premium = _vc(
        "premium-plain",
        _quota(
            series="ND_PLAIN",
            premium=(8, 0),
            standard=(0, 0),
        ),
    )
    standard = _vc(
        "standard-nvlink",
        _quota(
            series="ND_NVLINK",
            premium=(0, 0),
            standard=(64, 0),
        ),
    )
    rows = [
        _gpu(series="ND_PLAIN", nvlink=False),
        _gpu(series="ND_NVLINK", nvlink=True),
    ]

    selected = select_best_vc(
        "1x80G8-A100-NvLink",
        candidates=[standard, premium],
        tier="Premium",
        client=_client(rows),
        nodes=1,
        gpus_per_node=8,
    )

    assert selected.vc.name == "premium-plain"
    assert selected.matched_instances.effective_tier == "Premium"
    assert selected.matched_instances.nvlink_satisfied is False


def test_tier_is_selected_before_nvlink_within_vc() -> None:
    vc = _vc(
        "mixed",
        _quota(
            series="ND_PLAIN",
            premium=(8, 0),
            standard=(0, 0),
        ),
        _quota(
            series="ND_NVLINK",
            premium=(0, 0),
            standard=(64, 0),
        ),
    )
    client = _client(
        [
            _gpu(series="ND_PLAIN", nvlink=False),
            _gpu(series="ND_NVLINK", nvlink=True),
        ]
    )

    selected = match_vc(
        "1x80G8-A100-NvLink",
        vc=vc,
        tier="Premium",
        client=client,
        nodes=1,
        gpus_per_node=8,
    )

    assert selected.matched_instances.effective_tier == "Premium"
    assert selected.matched_instances.nvlink_satisfied is False
    assert selected.matched_instances.instances[0].series_id == "ND_PLAIN"


def test_explicit_name_filters_resolve_ambiguity_compatibly() -> None:
    first = _vc("shared", subscription_id="sub-a", resource_group="rg-a")
    second = _vc("shared", subscription_id="sub-b", resource_group="rg-b")
    api = VCQuotaAPI(SimpleNamespace())
    api.list = MagicMock(return_value=[first, second])

    with pytest.raises(ConfigError, match="ambiguous"):
        api.get_by_name("shared")
    with pytest.raises(ConfigError, match="'123'.*was not found"):
        api.get_by_name(123)  # type: ignore[arg-type]

    assert api.get_by_name("shared", subscription_id="sub-b") is second
    api.list.assert_called_with(
        subscription_ids=["sub-b"],
        include_zero=True,
        strict=False,
    )


def test_explicit_name_and_arm_coordinates_are_case_insensitive() -> None:
    expected = _vc(
        "Shared-VC",
        subscription_id="SUB-A",
        resource_group="Train-RG",
    )
    api = VCQuotaAPI(SimpleNamespace())
    api.list = MagicMock(return_value=[expected])

    assert (
        api.get_by_name(
            "shared-vc",
            subscription_id="sub-a",
            resource_group="train-rg",
        )
        is expected
    )


def test_explicit_resolve_passes_optional_vc_filters() -> None:
    vc = _vc("shared", _quota(), subscription_id="sub-b", resource_group="rg-b")
    client = _client([_gpu()])
    client.quota = SimpleNamespace(get_by_name=MagicMock(return_value=vc))
    client.ws = SimpleNamespace(get=MagicMock())
    aml = AmlOpts(
        compute="shared",
        vc_subscription_id="sub-b",
        vc_resource_group="rg-b",
        subscription_id="ws-sub",
        resource_group="ws-rg",
    )

    resolved = resolve_target(
        JobSpec(
            name="job",
            service="sing",
            sku="1x80G8-A100",
            nodes=1,
            gpus_per_node=8,
            backend_spec=aml,
        ),
        arm_client=client,
    )

    assert isinstance(resolved, ResolvedTarget)
    client.quota.get_by_name.assert_called_once_with(
        "shared",
        subscription_id="sub-b",
        resource_group="rg-b",
        strict=True,
    )


def test_auto_selector_filters_arm_coordinates_case_insensitively() -> None:
    expected = _vc(
        "vc",
        _quota(),
        subscription_id="SUB-A",
        resource_group="Train-RG",
    )

    selected = select_best_vc(
        "1x80G8-A100",
        candidates=[expected],
        tier="Premium",
        client=_client([_gpu()]),
        nodes=1,
        gpus_per_node=8,
        subscription_id="sub-a",
        resource_group="train-rg",
    )

    assert selected.vc is expected


def test_auto_selection_no_visible_vcs_error_is_actionable() -> None:
    with pytest.raises(SkuResolveError) as exc_info:
        select_best_vc(
            "2x80G8-A100",
            candidates=[],
            tier="Premium",
            client=_client([]),
            nodes=2,
            gpus_per_node=8,
            subscription_id="sub-a",
        )

    message = str(exc_info.value)
    assert "2x80G8-A100" in message
    assert "2 node(s), 16 GPU(s) total" in message
    assert "requested tier 'Premium'" in message
    assert "checked 0 VC(s)" in message
    assert "subscription_id=sub-a" in message
    assert "aj quota list --full" in message


def test_auto_resolve_propagates_strict_discovery_failure() -> None:
    client = _client([_gpu()])
    client.quota = SimpleNamespace(
        list=MagicMock(side_effect=OSError("graph unavailable"))
    )
    aml = AmlOpts(
        compute="",
        subscription_id="ws-sub",
        resource_group="ws-rg",
    )

    with pytest.raises(OSError, match="graph unavailable"):
        resolve_target(
            JobSpec(
                name="job",
                service="sing",
                sku="1x80G8-A100",
                nodes=1,
                gpus_per_node=8,
                backend_spec=aml,
            ),
            arm_client=client,
        )

    client.quota.list.assert_called_once_with(include_zero=True, strict=True)


def test_auto_selection_mixed_rejections_are_summarized() -> None:
    no_gpu = _vc(
        "cpu-only",
        _quota(series="Eadsv5", accelerator="CPU", memory=0),
    )
    no_model = _vc(
        "wrong-model",
        _quota(series="ND_H100", accelerator="H100"),
    )
    no_memory = _vc(
        "wrong-memory",
        _quota(series="ND_A100_40", memory=40),
    )
    no_size = _vc("wrong-size", _quota(series="ND_SIZE"))
    no_user = _vc("user-full", _quota(series="ND_USER", user=(8, 8)))
    no_tier = _vc(
        "tier-full",
        _quota(series="ND_TIER", premium=(8, 8)),
    )
    rows = [
        InstanceTypeInfo(
            name="Singularity.Eadsv5",
            series_id="Eadsv5",
            num_gpus=0,
            accelerator="CPU",
        ),
        _gpu(series="ND_H100", accelerator="H100"),
        _gpu(series="ND_A100_40", memory=40),
        _gpu(series="ND_SIZE", gpus=4),
        _gpu(series="ND_USER"),
        _gpu(series="ND_TIER"),
    ]

    with pytest.raises(SkuResolveError) as exc_info:
        select_best_vc(
            "1x80G8-A100",
            candidates=[
                no_gpu,
                no_model,
                no_memory,
                no_size,
                no_user,
                no_tier,
            ],
            tier="Premium",
            client=_client(rows),
            nodes=1,
            gpus_per_node=8,
        )

    message = str(exc_info.value)
    assert "checked 6 VC(s)" in message
    assert "cpu-only" in message and "GPU series" in message
    assert "wrong-model" in message and "A100" in message
    assert "wrong-memory" in message and "80GB per-GPU memory" in message
    assert "wrong-size" in message and "8 GPUs" in message
    assert "user-full" in message and "user quota" in message
    assert "tier-full" in message and "tier quota" in message
    assert "aj quota list --full" in message


def test_auto_selection_bounds_per_vc_rejection_reasons() -> None:
    candidates = [
        _vc(
            f"vc-{index:02d}",
            _quota(
                series=f"CPU_{index}",
                accelerator="CPU",
                memory=0,
            ),
        )
        for index in range(10)
    ]
    rows = [
        InstanceTypeInfo(
            name=f"Singularity.CPU_{index}",
            series_id=f"CPU_{index}",
            num_gpus=0,
            accelerator="CPU",
        )
        for index in range(10)
    ]

    with pytest.raises(SkuResolveError) as exc_info:
        select_best_vc(
            "1x80G8-A100",
            candidates=candidates,
            tier="Premium",
            client=_client(rows),
            nodes=1,
            gpus_per_node=8,
        )

    message = str(exc_info.value)
    assert "vc-07" in message
    assert "vc-08" not in message
    assert "… and 2 more" in message


class _AzureContext:
    def __init__(self, vc: VCInfo, row: InstanceTypeInfo) -> None:
        self.quota = SimpleNamespace(
            list=MagicMock(return_value=[vc]),
            get_by_name=MagicMock(),
        )
        self.sku = _SkuAPI([row])
        self.ws = SimpleNamespace(get=MagicMock())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _WorkspaceContext:
    def __init__(self) -> None:
        self.blob = SimpleNamespace(
            upload_archive=MagicMock(return_value="azureml://code/archive"),
            upload_file=MagicMock(return_value="azureml://code/bootstrap"),
        )
        self.job = SimpleNamespace(
            create_or_update=MagicMock(
                return_value={"name": "job", "properties": {}}
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_entry_reuses_auto_selection_for_compute_and_tier(
    monkeypatch,
    tmp_path,
) -> None:
    vc = _vc(
        "auto-vc",
        _quota(premium=(0, 0), standard=(16, 0)),
        subscription_id="vc-sub",
        resource_group="vc-rg",
    )
    azure = _AzureContext(vc, _gpu())
    workspace = _WorkspaceContext()
    aml = AmlOpts(
        compute="",
        sla_tier="Premium",
        subscription_id="ws-sub",
        resource_group="ws-rg",
        workspace_name="ws",
    )
    request = JobSpec(
        name="job",
        service="sing",
        sku="1x80G8-A100",
        nodes=1,
        gpus_per_node=8,
        image="amlt-sing/image",
        code_dir=str(tmp_path),
        command=["echo hi"],
        backend_spec=aml,
    )
    payload: dict = {}
    events = []

    monkeypatch.setattr(entry_mod, "AzureClient", lambda: azure)
    monkeypatch.setattr(entry_mod, "_get_rest_client", lambda _aml: workspace)
    monkeypatch.setattr(entry_mod, "_build_distribution", lambda _request: None)
    monkeypatch.setattr(entry_mod, "_build_identity", lambda _request: None)
    monkeypatch.setattr(
        entry_mod,
        "_resolve_sing_identity",
        lambda _request, _workspace: "",
    )
    monkeypatch.setattr(
        entry_mod,
        "generate_runner_script",
        lambda _request, _identity: "runner",
    )
    monkeypatch.setattr(entry_mod, "_collect_ssh_files", lambda _root, _emit: {})

    def _archive(_root, path, **_kwargs):
        path.write_bytes(b"archive")
        return ArchiveMetadata("hash", 7, 1)

    monkeypatch.setattr(entry_mod, "create_code_archive", _archive)
    monkeypatch.setattr(entry_mod, "_build_environment", lambda *_args: "env")
    monkeypatch.setattr(
        entry_mod,
        "_build_storage_mounts",
        lambda *_args: ({}, {}, {}),
    )
    monkeypatch.setattr(
        entry_mod,
        "_build_env_vars",
        lambda _request, _dataref: {},
    )
    monkeypatch.setattr(entry_mod, "_build_tags", lambda _tags: {})

    def _body(_request, **kwargs):
        payload.update(kwargs)
        return {"properties": kwargs["resources"]["properties"]}

    monkeypatch.setattr(entry_mod, "_build_job_body", _body)

    with patch(
        "azure_jobs.server.submit.azureml.sku_match.match_instance_type"
    ) as rematch:
        result = entry_mod.submit(request, on_event=events.append)

    assert result.status == "submitted"
    azure.quota.list.assert_called_once_with(include_zero=True, strict=True)
    azure.quota.get_by_name.assert_not_called()
    azure.sku.list.assert_called_once()
    rematch.assert_not_called()
    assert aml.compute == "auto-vc"
    assert aml.sla_tier == "Standard"
    assert aml.matched_instances == ["80G8-A100"]
    assert "/virtualclusters/auto-vc" in payload["compute_id"]
    resources = payload["resources"]["properties"]["AISuperComputer"]
    assert resources["slaTier"] == "Standard"
    assert resources["VirtualClusterArmId"] == payload["compute_id"]
    assert any(
        event.detail
        == (
            "Auto-selected VC 'auto-vc' at Standard "
            "(16 quota available; matched: 80G8-A100)"
        )
        for event in events
    )


def test_validation_allows_empty_sing_name_but_not_aml(
    tmp_path,
    monkeypatch,
) -> None:
    template_path = tmp_path / "template.yaml"
    template_path.write_text("base: parent\n", encoding="utf-8")
    base = {"jobs": [{"sku": "G1"}]}

    monkeypatch.setattr(
        "azure_jobs.shared.template.validate.read_conf",
        lambda _path: {**base, "target": {"service": "sing"}},
    )
    assert validate_template(template_path) == []

    monkeypatch.setattr(
        "azure_jobs.shared.template.validate.read_conf",
        lambda _path: {**base, "target": {"service": "sing", "name": ""}},
    )
    assert validate_template(template_path) == []

    monkeypatch.setattr(
        "azure_jobs.shared.template.validate.read_conf",
        lambda _path: {**base, "target": {"service": "aml", "name": ""}},
    )
    assert validate_template(template_path) == ["target missing 'name'"]

    monkeypatch.setattr(
        "azure_jobs.shared.template.validate.read_conf",
        lambda _path: {**base, "target": {"service": "sing", "name": 123}},
    )
    assert validate_template(template_path) == [
        "'target.name' must be a string"
    ]

    monkeypatch.setattr(
        "azure_jobs.shared.template.validate.read_conf",
        lambda _path: {**base, "target": {"service": "volcano"}},
    )
    assert validate_template(template_path) == []


def test_sing_preview_displays_auto(capsys) -> None:
    set_output_mode("rich")
    request = JobSpec(
        name="job",
        sid="sid",
        service="sing",
        sku="1x80G8-A100",
        backend_spec=AmlOpts(compute=""),
    )

    show_submission_preview(request)

    assert "auto" in capsys.readouterr().out
    assert _request_payload(request)["compute"] == "auto"
