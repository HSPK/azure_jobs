# Architecture

`aj` is a daemon-backed CLI and Python SDK. The daemon is the only execution
path; there is no in-process fallback.

## Source layout

```text
src/azure_jobs/
├── shared/                 transport-neutral values and pure logic
│   ├── contract/           HTTP constants, wire models, typed errors/codecs
│   ├── job/                JobSpec build/render/materialisation
│   ├── template/           YAML inheritance, merge and validation
│   ├── config/             aj_config.json models and persistence
│   ├── opts/               typed service-specific options
│   ├── logs.py             shared log ordering/selection policy
│   └── utils/
│
├── sdk/                    public daemon client
│   ├── __init__.py         AjClient and connect()
│   ├── _transport.py       HTTP over UDS, daemon startup and SSE
│   ├── account.py          account-scoped namespaces
│   ├── workspace.py        workspace/job/resource namespaces
│   └── logs.py             HTTP Range log reader
│
├── client/                 presentation frontends
│   ├── cli/                Click commands
│   ├── tui/                Textual dashboard
│   └── ui/                 Rich rendering
│
└── server/                 daemon-only execution
    ├── app.py              FastAPI resource routes
    ├── runner.py           uvicorn lifecycle on a Unix socket
    ├── context.py          per-project/workspace resources, queue and watcher
    ├── resources.py        Azure rows → wire-model adapters
    ├── az_client/          pure-HTTP Azure account/workspace clients
    ├── submit/             submission backend registry and implementations
    ├── queue.py
    └── watch.py
```

Layering is enforced by `tests/test_api_architecture.py`:

| Rule | Reason |
|---|---|
| `shared` imports neither frontend nor server | values and pure logic stay reusable |
| `sdk` imports neither frontend nor server | the public client knows only HTTP |
| `client` never imports `server` | all execution crosses the wire |
| `server` never imports `client` | the daemon runs headless |
| Azure HTTP clients live only under `server/` | only the daemon owns Azure credentials |
| Rich/Textual live only under `client/` | rendering never enters the daemon |

## Request path

The SDK and server use the same resource-shaped namespaces:

```text
d.job.list()
  → GET /v2/workspaces/{ws}/jobs:fetch
  → Context.job.list()
  → AzureWorkspaceClient.job.fetch()

d.sku.list()
  → GET /v2/instance-types
  → AzureClient.sku.list()
```

FastAPI and the SDK use direct HTTP strings. OpenAPI is the drift guard:
`tests/test_openapi_contract.py` drives every SDK operation through a recording
transport and checks its path, method and query parameters against the live
FastAPI schema.

`Context` owns only state with a real lifetime:

- resolved workspace and resource namespaces;
- persistent submission queue;
- persistent job watcher;
- idle expiry and deterministic close.

There is no generic backend/catalog facade between routes and resources.
`resources.py` remains only where translation is required: Azure rows to wire
`Job`/`CatalogItem`, Range readers, submission payload reconstruction, and
cross-workspace aggregation.

## Public SDK

```python
from azure_jobs import connect

with connect() as d:
    d.auth.status()
    d.sku.list()
    d.job.list(limit=20)              # configured workspace
    d.ws("FastAML").ds.list()         # explicit workspace
```

`d.ws()` returns the root workspace object, so `d.job is d.ws().job`.
Account resources (`subscription`, `ws`, `sku`, `sa`, `uai`, `image`,
`quota`, `compute`) live on the root. Workspace resources (`job`, `log`, `ds`,
`env`, `compute`, `quota`, `queue`, `watch`) live on `WorkspaceClient`.

Transport failures are not replayed automatically. Replaying an ambiguous
`submit()` or `queue()` response could create duplicate work; retry policy
belongs to the caller or a future server-side idempotency key.

## HTTP transport

FastAPI/uvicorn serves ordinary HTTP on a private Unix domain socket; httpx is
the client.

| Concern | Implementation |
|---|---|
| versioning | `/v2/...`, advertised by `/v2/info` |
| socket security | `0600` socket in a `0700` user-owned directory |
| project identity | `X-AJ-Root` on every local request |
| log windows | HTTP `Range`, `206`, `Content-Range` |
| server push | SSE at `/v2/events` |
| inspection | `curl --unix-socket ... http://d/v2/info` and `/openapi.json` |

The CLI retains `/v1/info` and `/v1/retire` only so `aj daemon restart` can
replace an old process. Normal resources never fall back to v1.

HTTP is stateless: connections do not own contexts. Contexts are cached by
`(project root, workspace target)` and reaped when idle and not busy.

## Azure clients

The server's Azure clients mirror the public SDK:

```text
AzureClient                     AzureWorkspaceClient
├── subscription               ├── job
├── ws (callable scope)        ├── log
├── sku                        ├── ds
├── sa                         ├── env
├── uai                        ├── blob
├── image                      └── info()
├── quota
└── compute
```

They use `requests` plus `azure-identity`, not `azure-ai-ml`. `AuthSession`
owns retry, token cache and authenticated sessions. ARM, ML data-plane and
storage tokens are cached per scope with early refresh.

Temporary clients are context-managed and closed deterministically.

## Submission

The client builds a `JobSpec` from local templates; the daemon executes it:

```text
CLI args + template
  → shared.job.build_job_spec()
  → d.job.submit(spec.to_dict()) or d.job.queue(...)
  → /v2/workspaces/{ws}/submissions
  → Context.submission
  → server.submit registry
  → azureml / volcano / amlt implementation
```

Description and execution registries are intentionally separate:

- `shared/spec.py` registers typed option loaders and name normalizers needed
  on both sides;
- `server/submit/__init__.py` registers daemon-only execution functions.

`shared/job/build.py` must stay service-agnostic. Service-specific config lives
in typed `Opts`; opaque aj-only template data lives under `_extra`.

### Code upload

Code hashing uses `(relative path, sha256(content))` pairs sorted by path.
Ignore rules combine built-ins, template `code.ignore`, `.codeignore` and
`.amltignore`.

- Azure ML uploads content-addressed blobs and reuses matching versions.
- Volcano uses `kubectl-exec` or blob upload selected by its uploader seam.
- Pod-side scripts live as resources, not large inline strings.

## TUI

`AjDashboard` is a composition root over the same SDK used by the CLI.
Controllers perform I/O; stores own immutable UI state and publish typed
events; view ports isolate Textual widgets.

`TaskRunner` provides a bounded worker pool and cancellation. `ResourceHandle`
leases defer closing workspace sessions and Range readers until active work
finishes. Logs use one-shot timer-driven Range reads so idle polling does not
occupy a worker.

## Remote HTTP feasibility

Read-only TCP exposure is mechanically easy because the app already speaks
HTTP. Cross-machine submission is not just a different bind address:

| Local assumption | Remote requirement |
|---|---|
| socket permissions authenticate the caller | TLS plus mTLS or OIDC and authorization |
| `X-AJ-Root` is a trusted local path | authorized project ID |
| `JobSpec.code_dir` is visible to the daemon | content-addressed artifact upload |
| journals live under project root | server-owned tenant/project state |
| one user receives all SSE events | principal/project-filtered streams |
| daemon Azure CLI identity is the user | explicit service or delegated identity model |
| local submit may collect `~/.ssh` | remote mode must never read server-home SSH keys |

Recommended flow:

1. upload/touch a source artifact by content hash;
2. submit explicit workspace coordinates, artifact ID and `Idempotency-Key`;
3. return `202 Accepted` with a queue ticket;
4. observe through authorized GET/SSE resources.

UDS remains the zero-configuration default. A remote URL should be explicit,
must not autostart a process, and should be added only after artifact storage,
authentication, authorization, tenant isolation and event filtering exist.
