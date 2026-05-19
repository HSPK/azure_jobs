"""Pure business logic for ``aj``.

This package holds everything that is *not* UI:

Foundation (cross-cutting):

* :mod:`.const`         — path constants derived from ``AJ_HOME``.
* :mod:`.errors`        — domain exception hierarchy (:class:`AJError` etc.).

Transport / user state:

* :mod:`.az_client`     — Azure REST/ARM clients (workspace + tenant-wide).
* :mod:`.config`        — ``aj_config.json`` + workspace detection.

Domain:

* :mod:`.template`      — amlt-style template + ``base`` inheritance + validation.
* :mod:`.submit`        — submission backends (native / amlt / volcano) +
                          :class:`SubmissionRecord` (local ``record.jsonl``).
* :mod:`.jobs`          — Azure-side job query (ids / paginated fetch / fan-out).
* :mod:`.logs`          — log download / live-tail streaming.

Backend catalogs:

* :mod:`.aml`           — AML target compute catalogs (VM size → GPU, cluster fetch).
* :mod:`.sku`           — Singularity SKU shorthand resolution + VC quotas.

Everything here is free of :mod:`click` and :mod:`rich`; CLI / TUI
layers consume it via callbacks. SDK consumers should import from
:mod:`azure_jobs` (top-level façade) when possible.
"""
