# SDK

`aj` is also a Python library. The same engine the CLI uses is exposed at the package root, so you can build and submit jobs from your own scripts without shelling out.

```python
from azure_jobs import (
    Template,
    JobSpec,
    JobResult,
    JobEvent,
    build_job_spec,
    write_amlt_yaml,
    submit_via,
    submit_via_amlt,
    get_workspace_config,
)
```

## Pieces

| Symbol | Purpose |
|--------|---------|
| `Template.from_conf_path(path)` | Load a YAML template (resolving its `base` chain) into a `Template` |
| `Template.from_dict(conf)` | Build a `Template` from an already-merged dict |
| `get_workspace_config()` | Read `.azure_jobs/aj_config.json` → `AJWorkspace` |
| `build_job_spec(template, *, name, sid, sku, user_command, user_args, workspace, code_dir=None, ...)` | Translate `Template` + CLI-equivalent params → `JobSpec`. `code_dir` defaults to `os.getcwd()` and is the local directory backends upload from. |
| `JobSpec` | Backend-agnostic, normalized job spec |
| `submit_via(spec, *, on_event=None)` | Dispatch to the backend registered for `spec.service` (`aml`/`sing` → AML REST, `volcano` → kubectl) |
| `submit_via_amlt(spec, *, on_event=None)` | Delegate to the `amlt` CLI (reads `spec.submission_path`); selected by the CLI's `--amlt` flag, not by `spec.service` |
| `write_amlt_yaml(spec, *, dry_run=False) -> Path` | Render the amlt-style YAML to `AJ_SUBMISSION_HOME` (or `AJ_DRYRUN_HOME`); stamps `spec.submission_path`. Required before `submit_via_amlt`. |
| `JobResult` | `{job_name, azure_name, status, portal_url, error}` |
| `JobEvent` | Progress event consumed by `on_event` callbacks |

## Example

```python
import uuid
from azure_jobs import (
    Template,
    build_job_spec,
    submit_via,
    get_workspace_config,
)

template = Template.from_conf_path(".azure_jobs/template/gpu.yaml")

spec = build_job_spec(
    template,
    name="my-job",
    sid=uuid.uuid4().hex[:8],
    sku="2xA100-80GB",
    user_command="train.py",
    user_args=("--lr", "1e-3"),
    workspace=get_workspace_config(),
    template_name="gpu",
    nodes=2,
    processes=8,           # GPUs per node
    processes_per_node=1,  # launcher procs per node
    code_dir="/path/to/project",  # files to upload; defaults to os.getcwd()
)

def on_event(ev):
    print(ev.kind, ev.detail)

result = submit_via(spec, on_event=on_event)
print(result.status, result.portal_url)
```

For the dashboard's progress UI (Live spinner, upload counter, error handling, `record.jsonl`), wrap the call with `submit_and_record`:

```python
from azure_jobs import submit_and_record
from azure_jobs import JobRecord

rec = JobRecord(id=spec.sid, name=spec.name, ...)
submit_and_record(
    lambda on_event: submit_via(spec, on_event=on_event),
    rec,
    spec.name,
    backend_label=spec.service,
)
```

## Notes

- `submit_via` / `submit_via_amlt` are pure — no global state, no `record.jsonl` write. Use `submit_and_record` for the CLI-style UX.
- Each backend's submit fn shares `(spec, *, on_event=None) -> JobResult` and self-registers; you can also dispatch explicitly with `get_backend(spec.service).fn(spec, on_event=...)`.
- `on_event=None` makes the call silent.
- Only `submit_via_amlt` needs an on-disk YAML — call `write_amlt_yaml(spec)` first. The other backends submit straight from the in-memory spec.
- `spec.service` maps `"aml"`/`"sing"` → `backend.azureml`, `"volcano"` → `backend.volcano`. `amlt` is *not* a `service` value — it's picked by the CLI's `--amlt` flag and dispatched directly via `submit_via_amlt`.
