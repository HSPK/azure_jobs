"""Singularity SKU resolution — facade.

Submodules:

* :mod:`.spec`       — :class:`SkuSpec` parser for amlt shorthand.
* :mod:`.catalog`    — family + series catalogs loaded from YAML.
* :mod:`.quotas`     — VC quota model + ARM fetch.
* :mod:`.discovery`  — VC discovery + cached available-family lookup.
* :mod:`.resolve`    — public ``resolve_sku`` / ``resolve_instance_type``.

Public names below are stable; underscore-prefixed exports are kept for
internal callers (``precheck`` reaches into ``_FAMILY_MAP`` for reverse
lookup, tests patch ``_fetch_vc_families``).
"""

from .catalog import _FAMILY_MAP, _SERIES_GPU_INFO
from .discovery import (
    VCInfo,
    _fetch_vc_families,
    discover_virtual_clusters,
    discover_vcs_from_template_or_arm,
)
from .quotas import SLA_TIERS, SeriesQuota, SlaTierQuota, fetch_all_vc_quotas, fetch_vc_quotas
from .resolve import _match_family, resolve_instance_type, resolve_sku
from .spec import SkuSpec

__all__ = [
    "SkuSpec",
    "SLA_TIERS",
    "SlaTierQuota",
    "SeriesQuota",
    "VCInfo",
    "fetch_vc_quotas",
    "fetch_all_vc_quotas",
    "discover_virtual_clusters",
    "discover_vcs_from_template_or_arm",
    "resolve_sku",
    "resolve_instance_type",
    # Underscore re-exports for in-repo callers (precheck, tests)
    "_FAMILY_MAP",
    "_SERIES_GPU_INFO",
    "_fetch_vc_families",
    "_match_family",
]
