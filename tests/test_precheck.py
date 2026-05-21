"""Tests for utils.cache and core.submit.precheck."""

from __future__ import annotations

from pathlib import Path

import pytest

from azure_jobs.core.submit import precheck
from azure_jobs.core.submit.models import SubmitRequest
from azure_jobs.core.submit.native.precheck import (
    _cached_aml_compute,
    _cached_vc_quotas,
    _instance_to_series,
    check_aml_compute,
    check_singularity,
)
from azure_jobs.utils import cache


@pytest.fixture
def cache_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr("azure_jobs.core.const.AJ_CACHE_HOME", tmp_path / "cache")
    return tmp_path / "cache"


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


def test_cache_roundtrip(cache_home: Path) -> None:
    cache.cache_set("ns", "key1", {"a": 1})
    assert cache.cache_get("ns", "key1", 60) == {"a": 1}


def test_cache_expires(cache_home: Path) -> None:
    cache.cache_set("ns", "k", "v")
    fp = cache_home / "ns" / "k.json"
    import os

    old = fp.stat().st_mtime - 1000
    os.utime(fp, (old, old))
    assert cache.cache_get("ns", "k", 10) is None


def test_cache_missing(cache_home: Path) -> None:
    assert cache.cache_get("ns", "nope", 60) is None


def test_cache_clear(cache_home: Path) -> None:
    cache.cache_set("ns", "a", 1)
    cache.cache_set("ns", "b", 2)
    n = cache.cache_clear("ns")
    assert n == 2


# ---------------------------------------------------------------------------
# precheck helpers
# ---------------------------------------------------------------------------


def test_instance_to_series_known() -> None:
    assert _instance_to_series("ND96amrs_A100_v4") == "NDAMv4"
    assert _instance_to_series("E16ads_v5") == "Eadsv5"
    assert _instance_to_series("not-a-real-sku") is None


# Stub ARM client -----------------------------------------------------------


class _FakeVcQuotaAPI:
    def __init__(self, parent_vc: "_FakeVcAPI"):
        self._vc = parent_vc

    def list(self, subscription_ids=None, *, include_zero=False):
        from azure_jobs.core.sku import parse_managed_quotas

        vcs = self._vc.list(subscription_ids=subscription_ids, with_raw=True)
        for vc in vcs:
            vc.quotas = parse_managed_quotas(vc.raw, include_zero=include_zero)
        return vcs


class _FakeVcAPI:
    def __init__(self, parent: "FakeArm"):
        self._parent = parent
        self.quota = _FakeVcQuotaAPI(self)

    def list(self, subscription_ids=None, *, with_raw=False):
        from azure_jobs.core.sku import VCInfo

        self._parent.vc_calls += 1
        if self._parent.vc_data is None:
            import requests

            raise requests.ConnectionError("no quota")
        return [
            VCInfo(
                name="vc1",
                resource_group="rg",
                subscription_id="sub",
                raw=self._parent.vc_data if with_raw else {},
            )
        ]


class _FakeComputeAPI:
    def __init__(self, parent: "FakeArm"):
        self._parent = parent

    def get(self, sub, rg, ws, name):
        self._parent.compute_calls += 1
        if self._parent.raise_compute:
            import requests

            raise requests.ConnectionError("404")
        return self._parent.compute_data


class FakeArm:
    def __init__(self, vc_data=None, compute_data=None, raise_compute=False):
        self.vc_data = vc_data
        self.compute_data = compute_data
        self.raise_compute = raise_compute
        self.vc_calls = 0
        self.compute_calls = 0
        self.vc = _FakeVcAPI(self)
        self.compute = _FakeComputeAPI(self)


def _vc_payload(series: str, sla: str, limit: int, used: int = 0):
    return {
        "properties": {
            "managed": {
                "defaultGroupPolicyOverallQuotas": {
                    "limits": [
                        {"id": series, "slaTier": sla, "limit": limit, "used": used}
                    ]
                }
            }
        }
    }


def _sing_request(sku="1x80G8-A100-NvLink", sla="Premium"):
    from azure_jobs.core.submit import SingularityOpts

    return SubmitRequest(
        name="x",
        compute="vc1",
        nodes=1,
        service="sing",
        sla_tier=sla,
        sku=sku,
        sing=SingularityOpts(vc_subscription_id="sub", vc_resource_group="rg"),
    )


# ---------------------------------------------------------------------------
# _cached_*
# ---------------------------------------------------------------------------


def test_cached_vc_quotas_uses_cache(cache_home: Path) -> None:
    arm = FakeArm(vc_data=_vc_payload("NDAMv4", "Premium", 8))
    quotas = _cached_vc_quotas(arm, "sub", "rg", "vc1")
    assert quotas and quotas[0].series == "NDAMv4"
    # Second call should hit cache, not ARM
    _cached_vc_quotas(arm, "sub", "rg", "vc1")
    assert arm.vc_calls == 1


def test_cached_vc_quotas_refresh(cache_home: Path) -> None:
    arm = FakeArm(vc_data=_vc_payload("NDAMv4", "Premium", 8))
    _cached_vc_quotas(arm, "sub", "rg", "vc1")
    _cached_vc_quotas(arm, "sub", "rg", "vc1", refresh=True)
    assert arm.vc_calls == 2


def test_cached_aml_compute_caches(cache_home: Path) -> None:
    from azure_jobs.core.az_client import ComputeInfo

    info = ComputeInfo(
        name="c",
        resource_group="r",
        subscription_id="s",
        workspace_name="w",
        location="",
        compute_type="AmlCompute",
    )
    arm = FakeArm(compute_data=info)
    _cached_aml_compute(arm, "s", "r", "w", "c")
    _cached_aml_compute(arm, "s", "r", "w", "c")
    assert arm.compute_calls == 1


# ---------------------------------------------------------------------------
# check_singularity
# ---------------------------------------------------------------------------


def test_check_singularity_ok(cache_home: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "azure_jobs.core.sku.resolve_instance_type",
        lambda *a, **k: ["ND96amrs_A100_v4"],
    )
    arm = FakeArm(vc_data=_vc_payload("NDAMv4", "Premium", 32, used=4))
    res = check_singularity(_sing_request(), arm_client=arm)
    assert res.severity == "ok"


def test_check_singularity_no_match(cache_home: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "azure_jobs.core.sku.resolve_instance_type",
        lambda *a, **k: [],
    )
    arm = FakeArm(vc_data=_vc_payload("NDAMv4", "Premium", 8))
    res = check_singularity(_sing_request(), arm_client=arm)
    assert res.severity == "error"
    assert "no matching instance" in res.title.lower()


def test_check_singularity_no_quota(cache_home: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "azure_jobs.core.sku.resolve_instance_type",
        lambda *a, **k: ["ND96amrs_A100_v4"],
    )
    # VC has quota for a different family
    arm = FakeArm(vc_data=_vc_payload("NDv4", "Premium", 8))
    res = check_singularity(_sing_request(), arm_client=arm)
    assert res.severity == "error"
    assert "no quota" in res.title.lower()


def test_check_singularity_wrong_tier(cache_home: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "azure_jobs.core.sku.resolve_instance_type",
        lambda *a, **k: ["ND96amrs_A100_v4"],
    )
    # Quota on Standard, request Premium
    arm = FakeArm(vc_data=_vc_payload("NDAMv4", "Standard", 8))
    res = check_singularity(
        _sing_request(sla="Premium"),
        arm_client=arm,
    )
    assert res.severity == "error"
    assert "premium" in res.title.lower()


def test_check_singularity_quota_tight(cache_home: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "azure_jobs.core.sku.resolve_instance_type",
        lambda *a, **k: ["ND96amrs_A100_v4"],
    )
    arm = FakeArm(vc_data=_vc_payload("NDAMv4", "Premium", 4, used=4))
    req = _sing_request()
    req.nodes = 2
    res = check_singularity(req, arm_client=arm)
    assert res.severity == "warn"


# ---------------------------------------------------------------------------
# NvLink auto-adjustment
# ---------------------------------------------------------------------------

from azure_jobs.core.submit.native.precheck import _toggle_nvlink


def test_toggle_nvlink_strip() -> None:
    assert _toggle_nvlink("1x80G8-A100-NvLink") == "1x80G8-A100"


def test_toggle_nvlink_add() -> None:
    assert _toggle_nvlink("1x80G8-A100") == "1x80G8-A100-NvLink"


def test_toggle_nvlink_cpu_skip() -> None:
    assert _toggle_nvlink("1xC1") is None


def test_check_singularity_drops_nvlink_for_vc(cache_home: Path, monkeypatch) -> None:
    """User asks NvLink, but VC only has non-NvLink quota → auto-strip."""

    def resolve(sku_raw, **kwargs):
        if "NvLink" in sku_raw:
            return ["ND96amrs_A100_v4"]  # NDAMv4 (no quota)
        return ["NC96ad_A100_v4"]  # NC_A100_v4 (has quota)

    monkeypatch.setattr(
        "azure_jobs.core.sku.resolve_instance_type",
        resolve,
    )
    arm = FakeArm(vc_data=_vc_payload("NC_A100_v4", "Premium", 16))
    req = _sing_request(sku="1x80G8-A100-NvLink")
    res = check_singularity(req, arm_client=arm)
    assert res.severity == "warn"
    assert res.adjusted_sku == "1x80G8-A100"
    assert req.sku == "1x80G8-A100"


def test_check_singularity_adds_nvlink_for_vc(cache_home: Path, monkeypatch) -> None:
    """User omits NvLink, but VC only has NvLink-enabled family → auto-add."""

    def resolve(sku_raw, **kwargs):
        if "NvLink" in sku_raw:
            return ["ND96amrs_A100_v4"]
        return ["NC96ad_A100_v4"]

    monkeypatch.setattr(
        "azure_jobs.core.sku.resolve_instance_type",
        resolve,
    )
    arm = FakeArm(vc_data=_vc_payload("NDAMv4", "Premium", 16))
    req = _sing_request(sku="1x80G8-A100")
    res = check_singularity(req, arm_client=arm)
    assert res.severity == "warn"
    assert res.adjusted_sku == "1x80G8-A100-NvLink"
    assert req.sku == "1x80G8-A100-NvLink"


def test_check_singularity_no_alt_when_both_fail(cache_home: Path, monkeypatch) -> None:
    """If neither NvLink variant fits, keep the original error."""

    def resolve(sku_raw, **kwargs):
        if "NvLink" in sku_raw:
            return ["ND96amrs_A100_v4"]
        return ["NC96ad_A100_v4"]

    monkeypatch.setattr(
        "azure_jobs.core.sku.resolve_instance_type",
        resolve,
    )
    # VC has quota for an unrelated H100 family
    arm = FakeArm(vc_data=_vc_payload("NDH100v5", "Premium", 8))
    req = _sing_request(sku="1x80G8-A100")
    res = check_singularity(req, arm_client=arm)
    assert res.severity == "error"
    assert req.sku == "1x80G8-A100"


# ---------------------------------------------------------------------------
# check_aml_compute
# ---------------------------------------------------------------------------


def _aml_request():
    return SubmitRequest(
        name="x",
        service="aml",
        compute="cluster",
        subscription_id="sub",
        resource_group="rg",
        workspace_name="ws",
    )


def test_check_aml_ok(cache_home: Path) -> None:
    from azure_jobs.core.az_client import ComputeInfo

    arm = FakeArm(
        compute_data=ComputeInfo(
            name="cluster",
            resource_group="rg",
            subscription_id="sub",
            workspace_name="ws",
            location="",
            compute_type="AmlCompute",
            provisioning_state="Succeeded",
            vm_size="STANDARD_D4",
        )
    )
    res = check_aml_compute(_aml_request(), arm_client=arm)
    assert res.severity == "ok"


def test_check_aml_missing(cache_home: Path) -> None:
    arm = FakeArm(raise_compute=True)
    res = check_aml_compute(_aml_request(), arm_client=arm)
    assert res.severity == "error"
    assert "not found" in res.title.lower()


def test_check_aml_skips_when_missing_fields(cache_home: Path) -> None:
    req = SubmitRequest(name="x", service="aml", compute="c")
    res = check_aml_compute(req, arm_client=FakeArm())
    assert res.severity == "warn"


# ---------------------------------------------------------------------------
# precheck dispatch
# ---------------------------------------------------------------------------


def test_precheck_volcano_noop() -> None:
    req = SubmitRequest(name="x", service="volcano")
    assert precheck(req).severity == "ok"
