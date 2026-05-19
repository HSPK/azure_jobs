"""Pure business logic for ``aj``.

This package holds everything that is *not* UI:

* :mod:`.template`      — amlt-style template + ``base`` inheritance.
* :mod:`.config`        — ``aj_config.json`` + workspace detection.
* :mod:`.record`        — local submission log (``record.jsonl``).
* :mod:`.errors`        — domain exception hierarchy (:class:`AJError` etc.).
* :mod:`.sku`           — Singularity SKU shorthand resolution.
* :mod:`.jobs`          — job query helpers (paginated REST fetch).
* :mod:`.logs`          — log download / live-tail streaming.
* :mod:`.az_client`     — Azure REST client (workspace + ARM).
* :mod:`.submit`        — submission backends (native / amlt / volcano).

Everything here is free of :mod:`click` and :mod:`rich`; CLI / TUI
layers consume it via callbacks. SDK consumers should import from
:mod:`azure_jobs` (top-level façade) when possible.
"""
