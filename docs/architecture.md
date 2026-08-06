# Architecture

`aj` has four layers and one execution path. Frontends and the public SDK speak
HTTP to the local daemon; only the daemon talks to Azure or submission tools.

## Source layout

```text
src/azure_jobs/
├── shared/
│   ├── contract/       wire models, HTTP constants, typed errors/codecs
│   ├── template/       YAML inheritance, merging, validation
│   ├── job/            JobSpec build, command, render, write
│   ├── config/         aj_config.json models and persistence
│   ├── opts/           typed backend option objects
│   ├── spec.py         shared backend description hooks
│   └── utils/          pure filesystem, naming, time, stats helpers
├── sdk/
│   ├── _transport.py   httpx over UDS, autostart, versioning, SSE
│   ├── account.py      account-scoped namespaces
│   ├── workspace.py    workspace/job/queue/watch namespaces
│   └── logs.py         HTTP Range reader
├── client/
│   ├── cli/            Click commands
│   ├── tui/            Textual dashboard
│   ├── skill_manager.py local Agent Skill lifecycle and ownership
│   └── ui/             Rich/JSON presentation
└── server/
    ├── app.py          FastAPI `/v2` routes and SSE
    ├── runner.py       uvicorn and UDS lifecycle
    ├── context.py      per-project/workspace contexts
    ├── resources.py    wire adapters and submission reconstruction
    ├── az_client/      daemon-only Azure HTTP clients
    └── submit/         execution registry and backends
```

## Layering contract

`tests/test_api_architecture.py` enforces:

| Rule | Consequence |
| --- | --- |
| `shared` imports neither client nor server | shared values stay transport-neutral |
| `sdk` imports neither client nor server | public SDK knows only HTTP |
| client never imports server | no hidden in-process execution |
| server never imports client | daemon remains headless |
| Azure clients live under server | client runs no Azure commands or token acquisition |
| Rich/Textual live under client | presentation cannot enter shared/server code |

The daemon is mandatory for Azure operations. If it cannot start or negotiate
API compatibility, callers receive recovery instructions instead of a fallback
implementation. Host-only filesystem operations such as `aj skill` remain
client-local and never authenticate to Azure.

## Request flow

```text
Click / Textual / user Python
  → public SDK namespace
  → httpx request over private UDS
  → FastAPI `/v2` route
  → Context resource namespace
  → Azure REST adapter or submission backend
```

Example:

```text
d.job.list(limit=20)
  → GET /v2/workspaces/_/jobs:fetch
  → Context.job.list(...)
  → AzureWorkspaceClient.job.fetch(...)
```

Workspace resolution is daemon-side. The client sends a workspace name and
the absolute `AJ_HOME` in `X-AJ-Root`.

## Local JobSpec build

`aj run` performs pure/local description work before crossing the socket:

```text
CLI args
  → read template and recursive bases
  → merge config
  → resolve node/GPU SKU placeholders
  → build_job_spec()
  → JobSpec.to_dict()
```

`shared/job/build.py` is service-agnostic. It asks `shared.spec` for:

- `build_spec_backend(template)` to produce typed `AmlOpts` or `VolcanoOpts`;
- `normalize_job_name(name)` for backend name constraints;
- `load_spec_backend(data)` to reconstruct typed options after HTTP.

`JobSpec.backend_spec` contains typed backend configuration.
`JobSpec.extra` contains only verbatim template `_extra`. Build never inspects
`extra`.

## Submission flow

```text
POST /v2/workspaces/{ws}/submissions
  → resources._spec_from_payload()
  → server.submit.get_backend(spec.service)
  → backend.fn(spec, on_event=...)
  → SubmitOutcome
```

Shared description hooks and daemon execution registration are separate:

- `shared/spec.py` is imported on both sides;
- `server/submit/__init__.py` owns executable backend functions.

This keeps `shared/job/build.py` and client entry points closed to
service-specific branches.

## Context lifecycle

HTTP connections do not own state. `ContextRegistry` caches contexts by:

```text
(absolute AJ_HOME, resolved target ID)
```

A context owns:

- resolved target and workspace resource adapters;
- Azure sessions and credential state;
- a persistent, single-worker submission queue;
- a persistent job watcher.

Configured-workspace lookup is not cached, so `aj ws set` takes effect without
daemon restart. Named resolution uses a bounded cache. Idle, non-busy contexts
are reaped; queues or active watches keep a context alive. Daemon retire waits
for accepted submissions unless forced.

## Transport and API

FastAPI/uvicorn serves HTTP over a UDS; httpx pools client connections.

| Concern | Contract |
| --- | --- |
| version | `/v2`, range advertised by `/v2/info` |
| project identity | `X-AJ-Root` |
| client version | `X-AJ-Client` |
| push events | SSE `/v2/events` |
| log windows | HTTP Range and `206` |
| inspection | `/openapi.json` and `curl --unix-socket` |
| local security | `0700` runtime directory, `0600` socket |

SDK and server paths are direct strings. OpenAPI drift tests verify that every
SDK method maps to a served method/path/query contract. Details are in
[API](api.md).

## Azure clients

The daemon's Azure clients mirror the public namespace style:

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

They use `requests` and `azure-identity`, with ARM, ML data-plane, and storage
token scopes. Temporary clients are context-managed and closed deterministically.

## Native AML and Sing backend

The native backend:

1. resolves workspace/compute or Sing VC;
2. resolves identity, environment, storage, distribution, and resources;
3. creates one deterministic code archive;
4. uploads/reuses one content-addressed workspace Blob;
5. submits an Azure ML CommandJob by REST.

For Sing with no target name, the daemon discovers visible VCs, filters
optional subscription/resource group, matches GPU count exactly and applies
exact accelerator/memory filters when present, checks current user and tier
quota, applies a per-VC NVIDIA preference when accelerator is omitted and both
vendors match, then ranks tier, NVLink, remaining quota, and stable
coordinates. The chosen match is carried through payload construction without
a second lookup.

See [Submitting](submitting.md) for the archive/bootstrap contract and
[Templates](templates.md) for target selection.

## Volcano backend

Volcano renders a Kubernetes `batch.volcano.sh/v1alpha1` Job and submits with
`kubectl create`.

- typed Kubernetes resource options live in `VolcanoOpts`;
- `pick_uploader(extra)` selects `kubectl-exec` or `blob` with a small literal
  branch;
- the PVC strategy uses a helper pod plus `tar | kubectl exec`;
- the Blob strategy uses a deterministic archive, SAS, `azcopy`, and SHA-256;
- declared Blob storage uses blobfuse2 and a mounted Secret;
- pod-side dependencies are installed or fail loudly.

The backend normalizes names to DNS-1035. CPU-only jobs request neither GPU nor
RDMA unless explicitly configured.

## `amlt` backend

`--amlt` keeps aj's template resolution but executes `amlt run` in the daemon.
Raw template fields are preserved where possible, resolved job fields are
overlaid, `_extra` is removed, and shell dollars are escaped for amlt.

## TUI

`AjDashboard` is a composition root over the public SDK:

- controllers own I/O and workflows;
- stores own canonical immutable UI state;
- typed events connect features;
- view ports isolate Textual widgets;
- `TaskRunner` bounds workers and cancellation;
- session handles defer close until active leases finish;
- log polling uses short Range reads rather than occupying a worker.

The TUI never imports Azure clients.

## Remote boundary

Remote HTTP is not implemented. A TCP bind would invalidate assumptions about
socket identity, trusted local paths, code visibility, journals, events,
credentials, and SSH collection.

A future remote design needs authenticated artifact upload, authorization,
tenant isolation, server-owned state, filtered SSE, explicit identity, and
idempotency. UDS remains the only supported transport.
