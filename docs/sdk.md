# Python SDK

The Python SDK is the same client used by the CLI and dashboard. It talks to
the local daemon over HTTP on a Unix socket; it does not construct Azure
clients or run `az`.

```python
from azure_jobs import connect

with connect() as d:
    print(d.auth.status())
    jobs = d.job.list(limit=20)
    stores = d.ds.list()
```

`connect()` starts the daemon when needed. If the daemon cannot authenticate
or start, it raises an actionable `DaemonUnavailable` error; there is no
in-process fallback.

## Workspace scoping

Root workspace resources use the workspace configured for the current project:

```python
with connect() as d:
    d.job.list(limit=20)
    d.ds.list()
```

Use `d.ws(name)` to scope the same namespaces to another workspace:

```python
with connect() as d:
    other = d.ws("FastAML")
    other.job.list(limit=20)
    other.ds.list()
```

`d.ws()` returns the root workspace object, so `d.job is d.ws().job`.

## Namespaces

| Namespace | Scope | Examples |
|---|---|---|
| `d.auth` | daemon | `status()` |
| `d.subscription` | account | `list()` |
| `d.ws` | account | `list()`, `current()`, `get(name)` |
| `d.sku` | account | `list(region=...)` |
| `d.sa` | account | `list()` |
| `d.uai` | account | `list()` |
| `d.image` | account | `list()` |
| `d.quota` | account | `list()` |
| `d.job` | workspace | `list()`, `status(id)`, `cancel(id)`, `delete(id)`, `submit()`, `queue()` |
| `d.log` | workspace | `list(id)`, `open(id, path)`, `download(id)` |
| `d.ds` | workspace | `list()`, `get(name)` |
| `d.env` | workspace | `list()`, `versions(name)` |
| `d.queue` | workspace | `list()`, `get(ticket)`, `cancel(ticket)` |
| `d.watch` | workspace | `add(id)`, `remove(id)`, `list()`, `subscribe()` |

## Failure and retry

Transport failures are not replayed automatically. Replaying an ambiguous
`submit()` or `queue()` response can create duplicate work, so callers decide
whether a particular read is safe to retry.
