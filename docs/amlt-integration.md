# Azure Backend Integration

`aj` is an everyday tool — it must be fast, lightweight, and never make users wait for Python imports. This document describes how to add job lifecycle management (status, cancel, logs) by talking to Azure ML directly, using amlt only as a reference implementation, never as a runtime dependency.

---

## Design Principles

1. **Zero heavy imports at startup.** `aj --help` runs in <100ms today. That's the bar.
2. **No amlt dependency.** amlt pulls in the entire Azure SDK on import (~600ms). We reference its logic but don't import it.
3. **Local-first.** `aj template list`, `aj jobs list`, `aj run --dry-run` read only local files — instant.
4. **Lazy Azure access.** Only commands that actually talk to Azure (`aj run`, `aj jobs show --live`, `aj jobs cancel`, `aj jobs logs`) pay the network cost, and they defer SDK imports to the function body.
5. **Subprocess as the fast path for submission.** `amlt run` is already installed on user machines. Shelling out to it for submission avoids importing its 600ms dependency chain, while our own lightweight SDK code handles status/cancel/logs where amlt's CLI is too slow.

---

## What amlt Actually Does Under the Hood

By reading amlt's source, the core operations reduce to:

| Operation | What amlt calls | Azure SDK surface |
|-----------|-----------------|-------------------|
| **Submit** | `ml_client.jobs.create_or_update(CommandJob(...))` | `azure.ai.ml.MLClient` + `azure.ai.ml.command()` |
| **Status** | Portal REST: `POST https://ml.azure.com/api/{region}/ux/v1.0/entities/crossRegion` | `requests.post()` or `ml_client.jobs.get(job_id)` |
| **Cancel** | `ml_client.jobs.begin_cancel(job_id)` | `azure.ai.ml.MLClient` |
| **Logs** | REST: `GET https://ml.azure.com/api/{region}/artifact/v2.0/.../contentinfo` | `requests.get()` |

amlt wraps these in ~2000 lines of abstraction (code upload, environment building, Docker config, Singularity support, hyperdrive). Most of that is irrelevant to `aj` — we generate the submission YAML and let `amlt run` handle submission complexity. We only need the lightweight operations (status, cancel, logs) ourselves.

---

## Architecture

```
aj CLI (Click) ── 100ms startup, no Azure imports
│
├── aj template list/show/pull    ── local file I/O only
├── aj run --dry-run              ── local file I/O only  
├── aj jobs list                  ── reads record.jsonl (local)
│
├── aj run                        ── subprocess: amlt run <yaml> <sid>
│                                    (amlt handles code upload, env, compute)
│
└── aj jobs show/cancel/logs      ── lazy import azure_backend module
                                     (deferred ~200ms for azure-identity +
                                      azure-ai-ml.MLClient, only when needed)
```

### Module layout

```
src/azure_jobs/
├── cli.py              # Click commands — no Azure imports at top level
├── conf.py             # Template loading and merging — pure YAML
├── const.py            # Path constants
├── record.py           # SubmissionRecord + JSONL read/write (new)
└── backend.py          # Azure ML SDK calls — lazy imports only (new)
```

`backend.py` is the only file that ever touches Azure SDK, and it's never imported at module level by anything — only called inside CLI command functions that need it.

---

## `backend.py` — Lightweight Azure Client

All Azure SDK imports happen inside functions. Importing this module costs 0ms.

```python
"""azure_jobs.backend — Direct Azure ML SDK calls.

Every function lazily imports only what it needs.  This module is
never imported at cli.py top level — only called from command bodies
that actually talk to Azure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LiveJobInfo:
    """Live job status from Azure."""
    job_id: str
    display_name: str
    status: str                          # Running, Completed, Failed, Canceled, etc.
    created_on: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    compute: str | None = None
    portal_url: str | None = None
    tags: dict[str, str] = field(default_factory=dict)


def _get_ml_client(
    subscription_id: str,
    resource_group: str,
    workspace_name: str,
):
    """Create an MLClient with lazy imports (~200ms first call, cached after)."""
    from azure.ai.ml import MLClient
    from azure.identity import DefaultAzureCredential

    return MLClient(
        credential=DefaultAzureCredential(),
        subscription_id=subscription_id,
        resource_group_name=resource_group,
        workspace_name=workspace_name,
    )


def get_job_status(ml_client, job_id: str) -> LiveJobInfo:
    """Get live status for a single job."""
    job = ml_client.jobs.get(job_id)
    portal_url = (
        f"https://ml.azure.com/runs/{job.name}"
        f"?wsid=/subscriptions/{ml_client.subscription_id}"
        f"/resourceGroups/{ml_client.resource_group_name}"
        f"/providers/Microsoft.MachineLearningServices"
        f"/workspaces/{ml_client.workspace_name}"
    )
    return LiveJobInfo(
        job_id=job.name,
        display_name=job.display_name or job.name,
        status=job.status,
        created_on=str(job.creation_context.created_at) if job.creation_context else None,
        start_time=str(getattr(job, "start_time", None)),
        end_time=str(getattr(job, "end_time", None)),
        compute=getattr(job, "compute", None),
        portal_url=portal_url,
        tags=job.tags or {},
    )


def cancel_job(ml_client, job_id: str) -> str:
    """Cancel a job. Returns final status string."""
    ml_client.jobs.begin_cancel(job_id).wait()
    job = ml_client.jobs.get(job_id)
    return job.status


def stream_job_logs(ml_client, job_id: str) -> None:
    """Stream logs for a job to stdout (blocks until job completes)."""
    ml_client.jobs.stream(job_id)


def download_job_logs(
    ml_client,
    job_id: str,
    output_dir: str | Path,
) -> Path:
    """Download all job outputs/logs to a local directory."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ml_client.jobs.download(job_id, download_path=str(output_dir))
    return output_dir
```

**Why this is better than importing amlt:**

| | Import amlt | Direct SDK (backend.py) |
|-|-------------|-------------------------|
| Import cost | ~600ms (`amlt.api.run`) | ~200ms (`azure.ai.ml` + `azure.identity`) |
| Dependencies | amlt + all its deps | `azure-ai-ml`, `azure-identity` (likely already installed) |
| Status check | Goes through 5 abstraction layers | One call: `ml_client.jobs.get(id)` |
| Cancel | 3 abstraction layers | One call: `ml_client.jobs.begin_cancel(id)` |
| Logs | Custom blob download + REST | One call: `ml_client.jobs.stream(id)` |

---

## `record.py` — Local Job Tracking

Extract record management from `cli.py` into its own module. Keep it pure Python (no Azure imports).

```python
"""azure_jobs.record — Local job record storage and querying."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class SubmissionRecord:
    id: str                                     # 8-char hex submission ID
    template: str
    command: str
    args: list[str] = field(default_factory=list)
    nodes: int = 1
    processes: int = 1
    sku: str = ""                               # resolved SKU string
    portal: str = "azure"
    status: str = "submitted"                   # submitted/success/failed/cancelled
    created_at: str = ""
    experiment_name: str = ""                   # amlt experiment name (= sid)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, line: str) -> SubmissionRecord:
        data = json.loads(line)
        # Handle old records missing new fields gracefully
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


def append_record(record_fp: Path, record: SubmissionRecord) -> None:
    record_fp.parent.mkdir(parents=True, exist_ok=True)
    with open(record_fp, "a") as f:
        f.write(record.to_json() + "\n")


def read_records(record_fp: Path) -> list[SubmissionRecord]:
    if not record_fp.exists():
        return []
    records = []
    for line in record_fp.read_text().splitlines():
        line = line.strip()
        if line:
            records.append(SubmissionRecord.from_json(line))
    return records


def find_record(record_fp: Path, job_id: str) -> SubmissionRecord | None:
    """Find a record by ID prefix (supports short IDs)."""
    for rec in read_records(record_fp):
        if rec.id.startswith(job_id):
            return rec
    return None
```

---

## CLI Commands — Performance Targets

### Fast path (no Azure imports, <150ms)

```python
@template_group.command(name="list")
def template_list():
    """List templates — reads local files only."""
    ...  # existing logic, no changes needed

@jobs_group.command(name="list")
@click.option("--last", default=20)
@click.option("--status", default=None)
def jobs_list(last, status):
    """List jobs from local record.jsonl — no network calls."""
    records = read_records(AJ_RECORD)
    if status:
        records = [r for r in records if r.status == status]
    for rec in records[-last:]:
        click.echo(f"{rec.id}  {rec.template:12s}  {rec.status:10s}  {rec.created_at}  {rec.command}")
```

### Azure path (lazy import, ~200ms + network)

```python
@jobs_group.command()
@click.argument("job_id")
def show(job_id):
    """Show live job status — calls Azure ML SDK."""
    rec = find_record(AJ_RECORD, job_id)
    if not rec:
        raise click.ClickException(f"Job {job_id} not found in records")

    from .backend import _get_ml_client, get_job_status  # lazy: ~200ms
    ml_client = _get_ml_client(...)  # read workspace config
    info = get_job_status(ml_client, rec.experiment_name)
    click.echo(f"Job:     {info.job_id}")
    click.echo(f"Status:  {info.status}")
    click.echo(f"Compute: {info.compute}")
    click.echo(f"Portal:  {info.portal_url}")


@jobs_group.command()
@click.argument("job_id")
def cancel(job_id):
    """Cancel a job — calls Azure ML SDK."""
    rec = find_record(AJ_RECORD, job_id)
    from .backend import _get_ml_client, cancel_job
    ml_client = _get_ml_client(...)
    final_status = cancel_job(ml_client, rec.experiment_name)
    click.echo(f"Job {job_id}: {final_status}")


@jobs_group.command()
@click.argument("job_id")
@click.option("--follow", "-f", is_flag=True)
def logs(job_id, follow):
    """Stream or download job logs."""
    rec = find_record(AJ_RECORD, job_id)
    from .backend import _get_ml_client, stream_job_logs, download_job_logs
    ml_client = _get_ml_client(...)
    if follow:
        stream_job_logs(ml_client, rec.experiment_name)
    else:
        path = download_job_logs(ml_client, rec.experiment_name, f".azure_jobs/logs/{rec.id}")
        click.echo(f"Logs downloaded to {path}")
```

---

## Submission Strategy: Keep subprocess

`aj run` continues to shell out to `amlt run` for actual submission. This is intentional:

1. **amlt handles complexity we don't want to own**: code upload, Docker environment resolution, Singularity support, workspace connections, storage mounts.
2. **Import cost doesn't matter at submission time** — the user is about to wait for network I/O anyway.
3. **amlt is already installed** on every user's machine.
4. **Decoupling**: if amlt changes its internal SDK calls, our submission still works.

For the lighter operations (status, cancel, logs), we bypass amlt entirely and call Azure ML SDK directly — fewer layers, faster response, better error messages.

---

## Workspace Configuration

`aj` stores Azure workspace config in `.azure_jobs/azure_config.json` — well-indented JSON, auto-created interactively when missing.

```json
{
  "workspace": {
    "subscription_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "resource_group": "my-rg",
    "workspace_name": "my-workspace"
  }
}
```

When a command needs workspace info and the config doesn't exist, `aj` prompts interactively:

```
Azure workspace not configured. Let's set it up:

  Subscription ID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  Resource group: my-rg
  Workspace name: my-workspace

  ✓ Saved to .azure_jobs/azure_config.json
```

Implementation: `src/azure_jobs/config.py` — `get_workspace_config()` reads or prompts, `write_azure_config()` writes with `indent=2`.

---

## Dependencies

Core `aj` is lightweight but beautiful:

```toml
# pyproject.toml
dependencies = ["click>=8.2.1", "pyyaml>=6.0.2", "rich>=13.0"]

[project.optional-dependencies]
azure = ["azure-ai-ml>=1.0", "azure-identity>=1.0"]
```

- `pip install azure_jobs` — template management, job submission (via amlt subprocess), local job list. **No Azure SDK needed.**
- `pip install azure_jobs[azure]` — adds live status, cancel, logs. Only imported lazily when those commands run.

---

## Performance Budget

| Command | Azure imported? | Target |
|---------|----------------|--------|
| `aj --help` | No | <100ms |
| `aj template list` | No | <100ms |
| `aj template show gpu` | No | <150ms |
| `aj run --dry-run ...` | No | <200ms |
| `aj jobs list` | No | <150ms |
| `aj run ...` (submit) | No (subprocess to amlt) | amlt startup + network |
| `aj jobs show <id>` | Yes (lazy) | ~300ms + network |
| `aj jobs cancel <id>` | Yes (lazy) | ~300ms + network |
| `aj jobs logs <id>` | Yes (lazy) | ~300ms + network |

---

## Migration Checklist

1. [ ] Create `src/azure_jobs/record.py` — extract SubmissionRecord + JSONL helpers from cli.py
2. [ ] Create `src/azure_jobs/backend.py` — lightweight Azure ML SDK wrapper (all lazy imports)
3. [ ] Add `azure` optional dependency group in pyproject.toml
4. [ ] Add workspace config reader (parse `.amltconfig`)
5. [ ] Add `aj jobs list` — reads local record.jsonl only
6. [ ] Add `aj jobs show <id>` — calls `backend.get_job_status()`
7. [ ] Add `aj jobs cancel <id>` — calls `backend.cancel_job()`
8. [ ] Add `aj jobs logs <id>` — calls `backend.stream_job_logs()` / `download_job_logs()`
9. [ ] Restructure templates under `aj template list/show/pull`
10. [ ] Add backward-compat aliases with deprecation warnings
11. [ ] Tests: mock `backend.py` functions in CLI tests
12. [ ] Tests: integration tests with `@pytest.mark.integration`
13. [ ] Update docs: architecture.md, cli-reference.md

---

## amlt Exception → User Message Mapping

When using Azure ML SDK directly, map exceptions at the CLI boundary:

| Exception | User message |
|-----------|-------------|
| `ImportError` (azure-ai-ml) | "Install azure support: `pip install azure_jobs[azure]`" |
| `azure.core.exceptions.HttpResponseError` | "Azure API error: {details}" |
| `azure.core.exceptions.ResourceNotFoundError` | "Job {id} not found in Azure ML" |
| `azure.identity.CredentialUnavailableError` | "Not authenticated — run `az login`" |
| `FileNotFoundError` (.amltconfig) | "No workspace configured — run `amlt project create`" |

