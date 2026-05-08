# SDK

`aj` is also a Python library. The same engine the CLI uses is exposed at the package root, so you can build and submit jobs from your own scripts without shelling out.

```python
from azure_jobs import (
    Template,
    SubmitRequest,
    SubmitResult,
    SubmitEvent,
    build_submit_request,
    submit_via_native,
    submit_via_volcano,
    submit_via_amlt,
    get_workspace_config,
)
```

## Pieces

| Symbol | Purpose |
|--------|---------|
| `Template.from_conf_path(path)` | Load a YAML template (resolving its `base` chain) into a `Template` |
| `Template.from_dict(conf)` | Build a `Template` from an already-merged dict |
| `get_workspace_config()` | Read `~/.azure_jobs/config.toml` → `AJWorkspace` |
| `build_submit_request(template, *, name, sid, sku, user_command, user_args, workspace, ...)` | Translate `Template` + CLI-equivalent params → `SubmitRequest` |
| `SubmitRequest` | Backend-agnostic, normalized job spec |
| `submit_via_native(request, on_event=...)` | Submit to AML / Singularity via REST |
| `submit_via_volcano(request, on_event=...)` | Submit to a Volcano cluster via `kubectl` |
| `submit_via_amlt(request, on_event=...)` | Delegate to the `amlt` CLI |
| `SubmitResult` | `{job_name, azure_name, status, portal_url, error}` |
| `SubmitEvent` | Progress event consumed by `on_event` callbacks |

## Example

```python
import uuid
from azure_jobs import (
    Template,
    build_submit_request,
    submit_via_native,
    get_workspace_config,
)

template = Template.from_conf_path(".azure_jobs/template/gpu.yaml")

request = build_submit_request(
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
)

def on_event(ev):
    print(ev.kind, ev.detail)

result = submit_via_native(request, on_event=on_event)
print(result.status, result.portal_url)
```

For the dashboard's progress UI (Live spinner, upload counter, error handling, `record.jsonl`), wrap the call with `submit_and_record`:

```python
from azure_jobs import submit_and_record
from azure_jobs.core.record import SubmissionRecord

rec = SubmissionRecord(id=request.sid, name=request.name, ...)
submit_and_record(
    lambda on_event: submit_via_native(request, on_event=on_event),
    rec,
    request.name,
    backend_label="native",
)
```

## Notes

- `submit_via_*` is pure — no global state, no `record.jsonl` write. Use `submit_and_record` if you want the CLI-style UX.
- `on_event` is optional. Passing `None` makes the call silent.
- Backend dispatch on `request.service` (`"aml"` / `"sing"` → native, `"volcano"` → volcano) is handled by `azure_jobs.core.submit.submit()` / `submit_and_record()` if you don't want to pick the backend yourself.
