"""Singularity SKU resolution — facade.

Submodules:

* :mod:`.spec`       — :class:`SkuSpec` parser for amlt shorthand.
* :mod:`.catalog`    — family + series catalogs loaded from YAML.
* :mod:`.resolve`    — public ``resolve_sku`` / ``resolve_instance_type``.

The VC quota models (:class:`VCInfo`, :class:`SeriesQuota`,
:class:`SlaTierQuota`) and the :func:`parse_managed_quotas` parser live
on the ARM client (:mod:`azure_jobs.core.az_client.arm`); they're
re-exported here so SKU-domain callers don't have to reach across
packages.

Public names below are stable; underscore-prefixed exports are kept for
internal callers (``precheck`` reaches into ``_FAMILY_MAP`` for reverse
lookup; tests reach into ``_match_family``).
"""

from azure_jobs.core.az_client.arm import (
    SLA_TIERS,
    SeriesQuota,
    SlaTierQuota,
    VCInfo,
    parse_managed_quotas,
)

from .catalog import _FAMILY_MAP
from .resolve import _match_family, resolve_instance_type, resolve_sku
from .spec import SkuSpec

__all__ = [
    "SkuSpec",
    "SLA_TIERS",
    "SlaTierQuota",
    "SeriesQuota",
    "VCInfo",
    "parse_managed_quotas",
    "resolve_sku",
    "resolve_instance_type",
    # Underscore re-exports for in-repo callers (precheck, tests)
    "_FAMILY_MAP",
    "_match_family",
]
