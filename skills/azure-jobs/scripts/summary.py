"""Secret-minimized summaries for `aj --json` documents."""

from __future__ import annotations

import shlex
from typing import Any

from redaction import bounded_tail, redact_value


def _pick(value: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: value[key] for key in keys if key in value}


def _diff_paths(value: str) -> list[str]:
    paths: list[str] = []
    for line in value.splitlines():
        if not line.startswith("diff --git "):
            continue
        try:
            parts = shlex.split(line)
        except ValueError:
            continue
        if len(parts) != 4 or not parts[3].startswith("b/"):
            continue
        path = parts[3][2:]
        if path not in paths:
            paths.append(path)
    return paths


def summarize(
    value: dict[str, Any],
    *,
    log_tail: int = 0,
) -> dict[str, Any]:
    safe = _pick(
        value,
        (
            "kind",
            "action",
            "status",
            "sid",
            "name",
            "display_name",
            "azure_name",
            "portal_url",
            "backend",
            "ticket",
            "has_changes",
            "valid_count",
            "invalid_count",
        ),
    )
    for key in ("message", "note", "error"):
        detail = value.get(key)
        if detail:
            safe[key] = bounded_tail(str(detail), 20)

    request = value.get("request")
    if isinstance(request, dict):
        safe["request"] = _pick(
            request,
            (
                "template_name",
                "experiment",
                "service",
                "compute",
                "workspace_name",
                "sku",
                "matched_instances",
                "nodes",
                "gpus_per_node",
                "processes_per_node",
                "total_processes",
                "sla_tier",
            ),
        )

    config = value.get("config")
    if isinstance(config, dict):
        target = config.get("target")
        jobs = config.get("jobs")
        template: dict[str, Any] = {}
        if isinstance(target, dict):
            template["target"] = _pick(
                target,
                (
                    "service",
                    "name",
                    "workspace_name",
                    "namespace",
                    "queue",
                    "context",
                    "gpus_per_node",
                    "cpus_per_node",
                    "memory",
                    "rdma",
                ),
            )
        if isinstance(jobs, list) and jobs and isinstance(jobs[0], dict):
            template["job"] = _pick(
                jobs[0],
                ("name", "sku", "sla_tier", "priority"),
            )
        safe["template"] = template

    rows = value.get("rows")
    if isinstance(rows, list):
        safe["row_count"] = len(rows)

    diff = value.get("diff")
    if isinstance(diff, str) and diff:
        paths = _diff_paths(diff)
        safe["changed_file_count"] = len(paths)
        safe["changed_files"] = paths[:100]
        if len(paths) > 100:
            safe["changed_files_truncated"] = True

    if log_tail:
        content = str(value.get("content") or "")
        if content:
            safe["log_tail"] = bounded_tail(content, log_tail)
        error = str(value.get("error") or "")
        if error:
            safe["log_error"] = bounded_tail(error, 20)

    return redact_value(safe)
