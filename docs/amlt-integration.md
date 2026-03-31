# amlt Python API Integration

This document describes how to replace subprocess calls to the `amlt` CLI with direct use of amlt's internal Python API. This gives us richer error handling, structured return values, and enables the job lifecycle commands (`aj jobs list/show/cancel/logs`) planned in the [roadmap](roadmap.md).

---

## Motivation

The current implementation shells out to `amlt` via `subprocess.run`:

```python
# Current: cli.py line 252
amlt_command = ["amlt", "run", submission_fp, sid]
subprocess.run(amlt_command, check=True)
```

Problems with this approach:

1. **No structured output** — we get an exit code but no job IDs, status, or portal URLs back.
2. **Fragile piping** — the `yes |` workaround required a `Popen` hack.
3. **No lifecycle access** — we can't query status, cancel, or fetch logs without shelling out again.
4. **Error messages are opaque** — subprocess stderr is unstructured text.
5. **Dependency is implicit** — `amlt` must be on PATH; failure is only discovered at submit time.

The amlt package exposes a full Python API under `amlt.api.*` that gives us direct access to all of this.

---

## amlt API Architecture

```
amlt.active_project()
  → ProjectClient
      ├── .experiments
      │     ├── .get(name=...) → ExperimentClient
      │     └── .create(name=...) → ExperimentClient
      └── .jobs → JobsClient (all jobs in project)

ExperimentClient
  └── .jobs → JobsClient
        ├── .by_name([...]) → JobsClient (filtered)
        ├── .by_status([...]) → JobsClient (filtered)
        ├── .cancel(...)
        ├── .logs → LogsClient
        │     ├── .pull(output_path, ...)
        │     └── .tail(lines, follow, ...)
        └── .status → StatusClient
```

Key classes:

| Class | Import | Purpose |
|-------|--------|---------|
| `AMLTConfig` | `amlt.config.core` | Load and validate submission YAML |
| `ConfigRunClient` | `amlt.api.run` | Submit jobs from a config |
| `JobsClient` | `amlt.api.jobs` | Query, filter, cancel, manage jobs |
| `LogsClient` | `amlt.api.logs` | Pull/tail/list log files |
| `JobPortalInfo` | `amlt.api.status` | Structured job status info |
| `AmltStatus` | `amlt.globals` | Status constants (RUNNING, PASS, FAILED, ...) |
| `ProjectClient` | `amlt.api.project` | Project-level operations |
| `ExperimentClient` | `amlt.api.experiment` | Experiment-level operations |

---

## Implementation Plan

### Phase 1: Create an `amlt_client` Module

Create `src/azure_jobs/amlt_client.py` as a thin wrapper around amlt's API. This isolates amlt imports so the rest of the codebase doesn't depend on amlt internals directly, and makes testing easier via mocking.

```python
"""azure_jobs.amlt_client — Thin wrapper around amlt's Python API."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import amlt
from amlt.api.run import ConfigRunClient
from amlt.api.status import JobPortalInfo
from amlt.config.core import AMLTConfig
from amlt.globals import AmltStatus


@dataclass
class JobInfo:
    """Structured job information returned after submission or status query."""
    name: str
    job_id: str
    status: str
    elapsed_time: str | None = None
    portal_url: str | None = None
    retries: int = 0


def get_project() -> amlt.api.project.ProjectClient:
    """Get the active amlt project. Raises if no .amltconfig found."""
    return amlt.active_project()


def submit(
    config_file: Path,
    experiment_name: str,
    *,
    assume_yes: bool = False,
    show_progress: bool = True,
) -> list[JobInfo]:
    """Submit jobs from a config YAML. Returns structured job info."""
    project = get_project()
    experiment = project.experiments.create(name=experiment_name)
    config = AMLTConfig.load(str(config_file))

    run_options = ConfigRunClient.RunOptions(
        show_progress=show_progress,
        assume_yes=assume_yes,
    )
    run_client = ConfigRunClient(
        experiment=experiment,
        config=config,
        run_options=run_options,
    )
    jobs = run_client.run()

    results: list[JobInfo] = []
    for job in jobs:
        results.append(JobInfo(
            name=job.config.name,
            job_id=str(job.config.id),
            status="submitted",
        ))
    return results


def get_status(experiment_name: str) -> list[JobInfo]:
    """Get status of all jobs in an experiment."""
    project = get_project()
    experiment = project.experiments.get(name=experiment_name)
    results: list[JobInfo] = []
    for job in experiment.jobs:
        info = JobPortalInfo.from_job_config(job.config)
        results.append(JobInfo(
            name=info.job_name,
            job_id=info.job_id,
            status=str(info.status),
            elapsed_time=str(info.elapsed_time) if info.elapsed_time else None,
            portal_url=(info.urls or {}).get("portal_url"),
            retries=info.retries,
        ))
    return results


def cancel_jobs(
    experiment_name: str,
    job_names: list[str] | None = None,
) -> int:
    """Cancel jobs in an experiment. Returns number cancelled."""
    project = get_project()
    experiment = project.experiments.get(name=experiment_name)
    jobs = experiment.jobs
    if job_names:
        jobs = jobs.by_name(job_names)
    return jobs.cancel(show_progress=True)


def tail_logs(
    experiment_name: str,
    job_name: str,
    *,
    lines: int = 300,
    follow: bool = False,
) -> None:
    """Tail logs for a single job (prints to stdout)."""
    project = get_project()
    experiment = project.experiments.get(name=experiment_name)
    jobs = experiment.jobs.by_name([job_name])
    jobs.logs.tail(lines=lines, follow=follow, pager=False)


def pull_logs(
    experiment_name: str,
    output_path: str | Path,
    job_names: list[str] | None = None,
) -> None:
    """Download logs for jobs to a local directory."""
    project = get_project()
    experiment = project.experiments.get(name=experiment_name)
    jobs = experiment.jobs
    if job_names:
        jobs = jobs.by_name(job_names)
    jobs.logs.pull(
        output_path=str(output_path),
        show_progress=True,
    )
```

### Phase 2: Update `aj run` to Use the API

Replace the subprocess call in `cli.py` with `amlt_client.submit()`.

**Before (current):**

```python
amlt_command = ["amlt", "run", submission_fp, sid]
try:
    if yes:
        with subprocess.Popen(["yes"], stdout=subprocess.PIPE) as yes_proc:
            subprocess.run(amlt_command, stdin=yes_proc.stdout, check=True)
    else:
        subprocess.run(amlt_command, check=True)
except subprocess.CalledProcessError as exc:
    rec.status = "failed"
    raise click.ClickException(...)
except FileNotFoundError:
    rec.status = "failed"
    raise click.ClickException("amlt is not installed...")
finally:
    log_record(rec)
```

**After:**

```python
from .amlt_client import submit, JobInfo
from amlt.exceptions import AMLTException, MissingProjectConfigError

try:
    jobs = submit(
        config_file=submission_fp,
        experiment_name=sid,
        assume_yes=yes,
    )
    for job in jobs:
        click.echo(f"  Submitted: {job.name} (id={job.job_id})")
except MissingProjectConfigError:
    rec.status = "failed"
    raise click.ClickException(
        "No amlt project found. Run `amlt project create` first."
    )
except AMLTException as exc:
    rec.status = "failed"
    raise click.ClickException(f"amlt submission failed: {exc}")
except ImportError:
    rec.status = "failed"
    raise click.ClickException(
        "amlt is not installed. Install it with: pip install amlt"
    )
finally:
    log_record(rec)
```

Benefits:
- No more subprocess. No more `yes |` piping hack.
- Structured `JobInfo` objects come back — we can store `job_id` and `portal_url` in the record.
- Specific exception types give clear, actionable error messages.

### Phase 3: Extend SubmissionRecord

Add fields returned by the amlt API:

```python
@dataclass
class SubmissionRecord:
    # ... existing fields ...
    sku: str = ""                           # resolved SKU string
    experiment_name: str = ""               # amlt experiment name (= sid)
    job_ids: list[str] = field(default_factory=list)    # amlt job IDs
    portal_url: str | None = None           # Azure portal link
```

These are populated from the `JobInfo` returned by `submit()` and enable all downstream commands.

### Phase 4: Add Job Lifecycle Commands

With `amlt_client.py` in place, the new CLI commands become thin wrappers:

```python
@main.group()
def jobs():
    """Manage submitted jobs."""
    pass

@jobs.command(name="list")
@click.option("--experiment", "-e", default=None)
@click.option("--status", type=click.Choice(["running", "queued", "completed", "failed"]))
@click.option("--last", default=20, help="Number of recent jobs to show")
def jobs_list(experiment, status, last):
    """List submitted jobs."""
    if experiment:
        infos = amlt_client.get_status(experiment)
    else:
        # Read from record.jsonl, optionally refresh with live status
        ...
    # Format and display table


@jobs.command()
@click.argument("job_id")
def show(job_id):
    """Show detailed info for a job."""
    rec = find_record(job_id)  # lookup in record.jsonl
    infos = amlt_client.get_status(rec.experiment_name)
    # Display detailed info


@jobs.command()
@click.argument("job_id")
def cancel(job_id):
    """Cancel a running or queued job."""
    rec = find_record(job_id)
    count = amlt_client.cancel_jobs(rec.experiment_name)
    click.echo(f"Cancelled {count} job(s)")


@jobs.command()
@click.argument("job_id")
@click.option("--follow", "-f", is_flag=True)
@click.option("--tail", "-n", default=300)
def logs(job_id, follow, tail):
    """Fetch or stream job logs."""
    rec = find_record(job_id)
    amlt_client.tail_logs(
        rec.experiment_name,
        rec.command,  # or job name
        lines=tail,
        follow=follow,
    )
```

### Phase 5: Graceful Fallback

Since amlt is installed via pipx in its own venv, it won't be importable from the azure_jobs venv by default. Two options:

**Option A: Add amlt as a dependency**

```toml
# pyproject.toml
dependencies = ["click>=8.2.1", "pyyaml>=6.0.2", "amlt"]
```

This is the simplest approach but couples the install to amlt.

**Option B: Optional dependency with subprocess fallback**

```toml
[project.optional-dependencies]
amlt = ["amlt"]
```

```python
# amlt_client.py
try:
    import amlt
    HAS_AMLT = True
except ImportError:
    HAS_AMLT = False

def submit(config_file, experiment_name, *, assume_yes=False, **kwargs):
    if HAS_AMLT:
        return _submit_api(config_file, experiment_name, assume_yes=assume_yes)
    else:
        return _submit_subprocess(config_file, experiment_name, assume_yes=assume_yes)
```

This keeps the tool functional even without amlt as a Python dependency (it falls back to the CLI), but uses the richer API when available.

**Option C: Import from pipx venv directly**

```python
import sys
import subprocess

def _find_amlt_site_packages() -> str | None:
    """Locate amlt's pipx venv site-packages."""
    result = subprocess.run(
        ["python", "-c", "import amlt; print(amlt.__file__)"],
        capture_output=True, text=True,
        executable=shutil.which("amlt")  # uses amlt's python
    )
    ...
```

This is fragile and not recommended.

**Recommendation:** Option B (optional dependency with fallback). This preserves the current zero-amlt-dependency install for template management (`aj template list/pull`) while unlocking the full API when amlt is co-installed.

---

## Testing Strategy

### Unit Tests

Mock `amlt_client.py` functions in CLI tests — same pattern as the current `patch("azure_jobs.cli.subprocess.run")`:

```python
with patch("azure_jobs.cli.amlt_client.submit") as mock_submit:
    mock_submit.return_value = [
        JobInfo(name="job1", job_id="abc123", status="submitted")
    ]
    result = runner.invoke(main, ["run", "-s", "echo", "hello"])
    assert result.exit_code == 0
    assert "abc123" in result.output
```

### Integration Tests

Test `amlt_client.py` directly against amlt's API (requires amlt installed and a project configured). Mark these with `@pytest.mark.integration` so they don't run in CI without credentials:

```python
@pytest.mark.integration
def test_submit_and_cancel():
    jobs = amlt_client.submit(config_file=..., experiment_name="test_exp")
    assert len(jobs) > 0
    count = amlt_client.cancel_jobs("test_exp")
    assert count == len(jobs)
```

---

## Migration Checklist

1. [ ] Create `src/azure_jobs/amlt_client.py` with `submit`, `get_status`, `cancel_jobs`, `tail_logs`, `pull_logs`
2. [ ] Add `amlt` as optional dependency in `pyproject.toml`
3. [ ] Update `cli.py` `run()` to call `amlt_client.submit()` instead of `subprocess.run`
4. [ ] Extend `SubmissionRecord` with `experiment_name`, `job_ids`, `portal_url`, `sku`
5. [ ] Store experiment name and job IDs in `record.jsonl` after submission
6. [ ] Add `aj jobs list` command (reads record.jsonl + optionally refreshes from amlt API)
7. [ ] Add `aj jobs show <id>` command
8. [ ] Add `aj jobs cancel <id>` command
9. [ ] Add `aj jobs logs <id>` command
10. [ ] Add subprocess fallback path for when amlt is not importable
11. [ ] Update existing tests to mock `amlt_client` instead of `subprocess`
12. [ ] Add new tests for `amlt_client` functions (mocked amlt internals)
13. [ ] Add integration test markers for real amlt tests
14. [ ] Update docs: architecture.md, cli-reference.md

---

## amlt Exception Reference

These are the exceptions to catch at the CLI boundary:

| Exception | When | User message |
|-----------|------|-------------|
| `ImportError` | amlt not installed | "Install amlt: pip install amlt" |
| `MissingProjectConfigError` | No `.amltconfig` in tree | "Run `amlt project create` to set up a project" |
| `MissingProjectError` | Project deleted/moved | "amlt project not found" |
| `ExperimentNotFoundException` | Experiment doesn't exist | "Experiment {name} not found" |
| `JobNotFoundException` | Job not in experiment | "Job {id} not found in experiment" |
| `NotRunnableException` | Config has no runnable jobs | "No runnable jobs found in config" |
| `AuthenticationException` | Auth failed | "Authentication failed — run `az login`" |
| `AuthorizationException` | No permission | "Permission denied for this operation" |
| `BackendNotReachableException` | Network issue | "Cannot reach Azure backend — check network" |
| `UserAbort` | User cancelled prompt | (exit silently) |

---

## amlt Status Constants

```python
from amlt.globals import AmltStatus

AmltStatus.PASS         # completed successfully
AmltStatus.FAILED       # job failed
AmltStatus.KILLED       # cancelled
AmltStatus.RUNNING      # currently executing
AmltStatus.QUEUED       # waiting for resources
AmltStatus.PREPARING    # being set up
AmltStatus.PAUSED       # paused
AmltStatus.EXPIRED      # timed out

AmltStatus.TERMINAL_STATUSES   # {PASS, FAILED, KILLED, EXPIRED}
AmltStatus.RUNNING_STATUSES    # {RUNNING, PREPARING}
```

Map these to user-friendly display strings in the CLI output.
