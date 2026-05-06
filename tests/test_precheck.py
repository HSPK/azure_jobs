"""Tests for utils.cache and core.submit.precheck."""

from __future__ import annotations

from pathlib import Path

import pytest

from azure_jobs.core.submit import precheck
from azure_jobs.core.submit.models import SubmitRequest
from azure_jobs.core.submit.precheck import (
    _cached_aml_compute,
    _cached_vc_quotas_raw,
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


class FakeArm:
    def __init__(self, vc_data=None, compute_data=None, raise_compute=False):
        self.vc_data = vc_data
        self.compute_data = compute_data
        self.raise_compute = raise_compute
        self.vc_calls = 0
        self.compute_calls = 0

    def get_vc_quotas_raw(self, sub, rg, vc):
        self.vc_calls += 1
        if self.vc_data is None:
            raise RuntimeError("no quota")
        return self.vc_data

    def get_workspace_compute(self, sub, rg, ws, name):
        self.compute_calls += 1
        if self.raise_compute:
            raise RuntimeError("404")
        return self.compute_data


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
    return SubmitRequest(
        name="x",
        compute="vc1",
        nodes=1,
        service="sing",
        sla_tier=sla,
        env_vars={"_sku_raw": sku},
        vc_subscription_id="sub",
        vc_resource_group="rg",
    )


# ---------------------------------------------------------------------------
# _cached_*
# ---------------------------------------------------------------------------


def test_cached_vc_quotas_uses_cache(cache_home: Path) -> None:
    arm = FakeArm(vc_data={"x": 1})
    assert _cached_vc_quotas_raw(arm, "s", "r", "v") == {"x": 1}
    # Second call should hit cache, not ARM
    _cached_vc_quotas_raw(arm, "s", "r", "v")
    assert arm.vc_calls == 1


def test_cached_vc_quotas_refresh(cache_home: Path) -> None:
    arm = FakeArm(vc_data={"x": 1})
    _cached_vc_quotas_raw(arm, "s", "r", "v")
    _cached_vc_quotas_raw(arm, "s", "r", "v", refresh=True)
    assert arm.vc_calls == 2


def test_cached_aml_compute_caches(cache_home: Path) -> None:
    arm = FakeArm(compute_data={"name": "c"})
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

from azure_jobs.core.submit.precheck import _toggle_nvlink


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
    assert req.env_vars["_sku_raw"] == "1x80G8-A100"


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
    assert req.env_vars["_sku_raw"] == "1x80G8-A100-NvLink"


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
    assert req.env_vars["_sku_raw"] == "1x80G8-A100"


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
    arm = FakeArm(
        compute_data={
            "properties": {
                "provisioningState": "Succeeded",
                "properties": {"vmSize": "STANDARD_D4"},
            }
        }
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
