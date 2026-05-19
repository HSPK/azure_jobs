"""Pure functions for parsing Azure ML REST job responses."""

from __future__ import annotations

from typing import Any, TypedDict

from azure_jobs.utils.time import calc_duration, calc_duration_secs, format_time

# Field names that ``extract_rest_job`` always populates (even if to empty
# string / None / 1).  Used by the post-extract assertion below to catch
# silent drift the next time someone refactors the extractor.
_REQUIRED_FIELDS = frozenset(
    {
        "name",
        "display_name",
        "status",
        "compute",
        "portal_url",
        "start_time",
        "end_time",
        "duration",
        "duration_secs",
        "queue_time",
        "queue_secs",
        "experiment",
        "type",
        "description",
        "tags",
        "environment",
        "command",
        "created",
        "created_utc",
        "created_by",
        "error",
        "instance_type",
        "nodes",
        "sla_tier",
        "processes_per_node",
    }
)


class JobInfo(TypedDict, total=False):
    """Lightweight, display-oriented view of an Azure ML job.

    Returned by :func:`extract_rest_job`. ``total=False`` because every
    consumer uses ``.get(key, default)`` to tolerate evolving Azure ML
    REST responses; converting to a strict ``@dataclass`` is a planned
    follow-up that touches the TUI / CLI / stats consumers.

    Until then, :func:`extract_rest_job` asserts post-build that every
    name in :data:`_REQUIRED_FIELDS` is present so renames blow up at
    the extraction boundary instead of silently degrading consumers.
    """

    name: str
    display_name: str
    status: str
    compute: str
    portal_url: str
    start_time: str
    end_time: str
    duration: str
    duration_secs: int | None
    queue_time: str
    queue_secs: int | None
    experiment: str
    type: str
    description: str
    tags: str
    environment: str
    command: str
    created: str
    created_utc: str
    created_by: str
    error: str
    instance_type: str
    nodes: int
    sla_tier: str
    processes_per_node: int


def extract_error_message(err: dict | str | None) -> str:
    """Walk nested ``innerError`` chain and return the most specific message."""
    if not err:
        return ""
    if isinstance(err, dict):
        code = err.get("code", "")
        msg = err.get("message", "")
        inner = err.get("innerError") or err.get("inner_error")
        while inner and isinstance(inner, dict):
            inner_msg = inner.get("message", "")
            if inner_msg:
                msg = inner_msg
            inner = inner.get("innerError") or inner.get("inner_error")
        if code and msg:
            return f"{code}: {msg}"
        return msg or code or str(err)
    return str(err)


def trim_arm_id(arm_id: str) -> str:
    """Extract the trailing name segment from an ARM resource ID."""
    if "/" in arm_id:
        return arm_id.rstrip("/").rsplit("/", 1)[-1]
    return arm_id


def extract_rest_job(raw: dict[str, Any]) -> JobInfo:
    """Convert a REST API job JSON object → lightweight display dict."""
    props = raw.get("properties", {})
    inner_props = props.get("properties", {}) or {}

    name = raw.get("name", "")

    # Timing
    start = inner_props.get("StartTimeUtc", "")
    end = inner_props.get("EndTimeUtc", "")
    duration = calc_duration(start, end)
    duration_secs = calc_duration_secs(start, end)
    start_display = format_time(start)
    end_display = format_time(end)

    # Queue time (created → started)
    queue_time = ""
    queue_secs: int | None = None
    sys_data = raw.get("systemData", {}) or {}
    created_raw = sys_data.get("createdAt", "")
    if created_raw and start:
        queue_time = calc_duration(created_raw[:19], start)
        queue_secs = calc_duration_secs(created_raw[:19], start)

    # Compute — trim ARM ID to short name
    compute = trim_arm_id(props.get("computeId", "") or "")

    # Tags — filter out internal AML system tags
    tags = props.get("tags", {}) or {}
    tags = {k: v for k, v in tags.items() if not k.startswith("_aml_system_")}
    tags_str = ", ".join(f"{k}={v}" for k, v in tags.items()) if tags else ""

    # Environment — trim ARM ID and strip version suffix
    env_str = trim_arm_id(props.get("environmentId", "") or "")
    if ":" in env_str:
        env_str = env_str.rsplit(":", 1)[0]

    # Created (sys_data already extracted above for queue_time)
    created = format_time(created_raw[:19]) if created_raw else ""

    # Created by (user)
    created_by = sys_data.get("createdBy", "") or ""

    # Error
    error_msg = extract_error_message(props.get("error", None))

    # Portal URL
    services = props.get("services", {}) or {}
    studio = services.get("Studio", {}) or {}
    portal_url = studio.get("endpoint", "") or ""

    # Resources — extract instance type, nodes, SLA tier
    resources = props.get("resources", {}) or {}
    aisc = (resources.get("properties") or {}).get("AISuperComputer", {}) or {}
    instance_type = aisc.get("instanceType", "") or ""
    # Strip "Singularity." prefix and comma-separated alternatives
    if instance_type:
        instance_type = instance_type.split(",")[0].strip()
        if instance_type.startswith("Singularity."):
            instance_type = instance_type[len("Singularity.") :]
    nodes = resources.get("instanceCount", 1) or aisc.get("instanceCount", 1)
    sla_tier = aisc.get("slaTier", "") or ""

    # Distribution — process count
    dist = props.get("distribution", {}) or {}
    processes_per_node = dist.get("processCountPerInstance", 0) or 0

    result: JobInfo = {
        "name": name,
        "display_name": props.get("displayName", "") or "",
        "status": props.get("status", ""),
        "compute": compute,
        "portal_url": portal_url,
        "start_time": start_display,
        "end_time": end_display,
        "duration": duration,
        "duration_secs": duration_secs,
        "queue_time": queue_time,
        "queue_secs": queue_secs,
        "experiment": props.get("experimentName", "") or "",
        "type": props.get("jobType", "") or "",
        "description": (props.get("description", "") or "")[:200],
        "tags": tags_str,
        "environment": env_str,
        "command": (props.get("command", "") or "")[:200],
        "created": created,
        "created_utc": created_raw[:19] if created_raw else "",
        "created_by": created_by,
        "error": error_msg,
        "instance_type": instance_type,
        "nodes": nodes,
        "sla_tier": sla_tier,
        "processes_per_node": processes_per_node,
    }
    # Catch silent drift early — if a future refactor drops a field,
    # consumers shouldn't have to discover it via a missing UI cell.
    missing = _REQUIRED_FIELDS - result.keys()
    assert not missing, f"extract_rest_job dropped fields: {sorted(missing)}"
    return result
