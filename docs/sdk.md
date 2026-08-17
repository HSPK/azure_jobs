# Python SDK

The public SDK is the same daemon client used by the CLI and dashboard. It
holds no Azure credential, imports no server implementation, and runs no Azure
commands.

```python
from azure_jobs import connect

with connect() as d:
    print(d.auth.status())
    for job in d.job.list(limit=20):
        print(job.name, job.status)
```

`connect()` starts the local daemon when needed. If startup or protocol
negotiation fails, it raises an actionable transport error. There is no
in-process fallback.

See [Client and daemon](design/client-daemon.md) for transport, context, queue,
and watcher ownership.

## Connection lifecycle

```python
from pathlib import Path
from azure_jobs import connect

with connect(
    "workspace-name",            # optional pinned workspace
    root=Path(".azure_jobs"),    # project AJ_HOME
    autostart=True,
) as d:
    print(d.info())
```

`AjClient` is a context manager. Call `close()` when not using `with`.
Workspace scopes share the root transport and do not own a second connection.

## Workspace scoping

Root workspace namespaces use the workspace configured for the calling
project:

```python
with connect() as d:
    d.job.list(limit=20)
    d.ds.list()
```

Scope the same operations by name:

```python
with connect() as d:
    other = d.ws("other-workspace")
    jobs = other.job.list(limit=20)
    stores = other.ds.list()
```

`d.ws()` returns the root workspace object, so `d.ws().job is d.job`.

## Namespace map

Account/daemon scope:

| Namespace | Main methods |
| --- | --- |
| `d.auth` | `status()` |
| `d.subscription` | `list()` |
| `d.ws` | `list()`, `current()`, `get(name)`, callable scope |
| `d.sku` | `list(region=..., subscription_id=...)` |
| `d.sa`, `d.uai`, `d.image` | `list(subscription_id=...)` |
| `d.quota` | `list(include_zero=..., subscription_id=...)` |
| `d.compute` | `list(resource_group=..., workspace=..., subscription_id=...)` |

Workspace scope:

| Namespace | Main methods |
| --- | --- |
| `d.job` | `page()`, `list()`, `status()`, `cancel()`, `delete()`, `submit()`, `queue()` |
| `d.log` | `list()`, `pick_default()`, `open()`, `download()` |
| `d.ds` | `list()`, `get(name)` |
| `d.env` | `list()`, `versions(name)` |
| `d.workspace.compute` | `list()` |
| `d.workspace.quota` | `list()` |
| `d.queue` | `list()`, `get(ticket)`, `cancel(ticket)` |
| `d.watch` | `add()`, `remove()`, `list()`, `subscribe()` |
| `d.workspace` | `info()` |

Rows are typed `Job`, `Target`, `CatalogItem`, `QueuedJob`, and related shared
contract values. Resource-specific fields remain available in `.raw`.

## Jobs and progress

```python
from azure_jobs import connect

def progress(event):
    print(event.kind, event.detail, event.completed, event.total)

payload = job_spec.to_dict()

with connect() as d:
    outcome = d.job.submit(payload, on_event=progress)
    if not outcome.succeeded:
        raise RuntimeError(outcome.error)
```

Submit progress is correlated over the daemon's SSE stream while the mutation
runs over HTTP. `submit()` returns a `SubmitOutcome`; a backend-reported
failure is represented in that outcome.

Queue work that should outlive the client:

```python
with connect() as d:
    entry = d.job.queue(job_spec.to_dict(), name=job_spec.name)
    print(entry.ticket)
```

## Job references

Most job methods accept a string or a `JobRef`:

```python
with connect() as d:
    job = d.job.status("azure-job-name")
    d.job.cancel(job.ref)
```

Use `JobRef` when backend identity or incarnation must be preserved, especially
in long-lived UIs.

## Range-based logs

```python
with connect() as d:
    files = d.log.list("azure-job-name")
    path = d.log.pick_default(files)
    reader = d.log.open("azure-job-name", path)

    tail = reader.tail(64 * 1024)
    next_chunk = reader.read_after(tail.end, 64 * 1024)
    exact = reader.read_range(0, 1024)
    reader.close()
```

Each read is an independent HTTP Range request. `LogChunk` carries bytes,
`start`, `end`, and `total_size`; no server-side handle is left behind.

## Watches

```python
with connect() as d:
    d.watch.add("azure-job-name")
    unsubscribe = d.watch.subscribe(
        lambda note: print(note.topic, note.title, note.body)
    )
    # Keep the process alive while notifications are needed.
    unsubscribe()
```

The daemon persists watched jobs; subscription only controls this client's SSE
listener.

## Errors and retries

Typed server errors are reconstructed on the client, including `RestError`
fields such as `status_code` and `azure_code`. Unknown remote types degrade to
`RemoteError` without losing the message.

Transport failures are never automatically replayed. Reads may be retried by
the caller when appropriate. For `submit()`, `queue()`, cancel, or delete, a
lost response is ambiguous: reconcile remote state before retrying to avoid
duplicate work.

Start or restart the daemon with `AJ_DEBUG=1` to allow server traceback detail
across the local transport.
