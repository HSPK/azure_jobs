# Architecture

## Source layout

```
src/azure_jobs/
├── cli/             # Click commands — thin orchestration over the sibling packages
├── template/        # YAML template loader, merge engine, validator
├── config/          # aj_config.json (workspace, defaults, dashboard)
├── job/             # JobSpec spec lifecycle: build + render + materialise
├── backend/         # Submission backends (amlt, azureml, volcano)
├── az_client/       # Pure-REST Azure clients (ARM + ML)
├── tui/             # `aj dash` — Textual dashboard
├── utils/           # ui, fs, ignore, time, text, dataclass helpers
├── journal.py       # Append-only record.jsonl + short-id resolution
├── errors.py        # Typed exception hierarchy
└── const.py         # Path constants (AJ_HOME-derived)
```

The CLI never reaches into ARM/ML SDKs; it goes through these sibling packages.
The TUI is one more consumer with its own state and controllers.

## Submission engine

The submission engine splits into two peers: `job/` owns the pure spec
lifecycle (data model + serialization), and `backend/` owns the three
submission executors. Every backend self-registers under a `service` name
(`aml`, `sing`, `amlt`, `volcano`), takes a `JobSpec`, returns a
`JobResult`, and emits `JobEvent`s for progress UI.

```
job/                 # data + spec lifecycle, no network I/O
├── spec.py          # JobSpec / JobResult / JobEvent / *Opts
├── build.py         # Template + CLI params → JobSpec
├── command.py       # `.py` → `uv run`, `.sh` → `bash`
├── render.py        # JobSpec → amlt-style YAML
└── write.py         # Write YAML to AJ_SUBMISSION_HOME; stamp submission_path

backend/             # submission backends + registry
├── __init__.py      # BackendEntry / register_backend / get_backend / submit_via
├── amlt.py          # Shells out to the amlt CLI
├── azureml/         # Pure-REST native submit for `aml` and `sing`
│   ├── __init__.py  # Registers aml/sing via lazy trampoline
│   └── workspace.py, entry.py, sku.py, target.py, …
└── volcano/         # K8s Volcano target via kubectl
    └── entry.py, config.py, upload.py, constants.py
```

`JobRecord` and the append-only `record.jsonl` I/O live in
`journal.py` — local-journal state, not submission state.

Every backend exposes `(request, *, on_event) -> JobResult` and self-registers
via `register_backend(...)` at import time. The CLI dispatches by
`request.service` — `aml`/`sing` → `backend.azureml`, `volcano` →
`backend.volcano`, `amlt` → `backend.amlt` (also forced when `--amlt` is passed).

## Submit flow

```
aj run -t gpu -n 4 -p 8 train.py
  ├─ read_conf()              resolve base chain (template/engine.py)
  ├─ merge_confs()            dicts recurse; lists-of-dicts merge by index;
  │                           scalar lists concatenate; scalars last-wins
  ├─ apply -n / -p / --ppn
  ├─ build_job_spec →   JobSpec
  ├─ get_backend(service)
  │     azureml → resolve target → upload code → PUT /jobs/{name}
  │              (`aml`/`sing` share backend.azureml)
  │     volcano → render manifest → upload code → kubectl apply
  │     amlt    → materialise YAML → exec amlt run
  └─ log_record() →           append JobRecord to record.jsonl
```

The native backend (`backend.azureml`) orders work as **read-only validation
first, remote writes last**: resolve target → auth → command + SKU resolve →
Singularity UAI preflight → assemble runner script and ssh files → only then
register environment, mount storage, upload code, submit.

## Code upload

`utils/fs.walk_code()` excludes `__pycache__`, `.git`, `.venv`, `node_modules`
and all of `.azure_jobs/` except `scripts/`, then applies the template's
`code.ignore` plus `.codeignore` / `.amltignore`.

`compute_code_hash()` is sha256 over `(rel_path, sha256(content))` pairs sorted
by path. Native uses the hash as the blob prefix, so identical inputs reuse
the prior upload without re-PUTting blobs.

`AJ_*` runtime values (`AJ_NAME`, `AJ_NODES`, `AJ_PROCESSES`, ...) flow via
`environmentVariables` rather than the runner script — keeping the script body
content-stable across runs and preserving the upload dedup.

## Volcano backend

Each Job's container script:

1. `cp -a` the (immutable) PVC code asset into a pod-local `emptyDir` at
   `/mnt/aj-workdir/<job>/wd`. Runtime writes never hit shared storage.
2. `cd $AJ_WORKDIR`, run setup commands, then run the user command.

Code upload uses a short-lived "tar" pod with the PVC mounted:
`tar cv --null -T <filelist> | kubectl exec tar xf - -C <dest>` streams files
in. The pod's main process is `exec sleep` so SIGTERM forwards cleanly;
teardown uses `kubectl delete --wait=false`.

## REST client

`az_client/` is workspace-agnostic ARM + workspace-scoped ML, split into
two sub-packages on top of a shared transport:

```
az_client/
├── auth.py          # TokenCache, retry session, WorkspaceCoords,
│                    # raise_for_rest_error, AuthSession base
├── arm/
│   ├── compute.py / image.py / graph.py / identity.py / instance_types.py
│   ├── storage.py / subscriptions.py / vc.py / workspace.py
│   └── __init__.py  # AzureClient: .subscription/.ws/.sku/.sa/.uai/
│                    # .image/.quota/.compute
└── ml/
    ├── context.py   # RestContext (= AuthSession + WorkspaceCoords + dual scopes)
    ├── jobs.py / environments.py / datastores.py / blob.py / logs.py
    ├── run_history.py
    ├── extract.py   # Pure REST→JobInfo parsers
    └── __init__.py  # AzureWorkspaceClient: .job/.env/.ds/.blob/.log/.info()
```

The server-side client follows the same account-root → callable workspace scope
→ resource namespace shape as the public SDK. `azure.ws(sub, rg, name)` returns
an `AzureWorkspaceClient`; there is no legacy plural alias surface to maintain.

Highlights:

- **Pure `requests`** with retry+backoff (idempotent + write methods, honours
  `Retry-After`); `azure-identity` is the only auth dependency.
- **Two-plane**: ARM control plane (`management.azure.com`) for jobs/env/code
  registration; ML data plane (discovered from `properties.discoveryUrl`,
  `ml.azure.com/.default`) for Run History (error details, log URLs).
- **Token cache** per scope (ARM, data-plane, storage), with a 60 s refresh
  leeway, persisted `0600` under `AJ_CACHE_HOME` so a second `aj` process does
  not shell out to `az` again. A cache file with loose permissions is ignored.

## Client / server split

Three top-level layers, enforced by `tests/test_api_architecture.py`:

```
src/azure_jobs/
├── shared/          vocabulary both sides speak — imports neither side
│   ├── contract/      wire models, errors, routes, typed codec
│   ├── types/         Azure value objects (quota rows, workspaces, SKUs)
│   ├── opts/          typed backend options + spec-hook registration
│   ├── spec.py        how a job is *described*
│   ├── job/           JobSpec and how to build one
│   ├── template/, config/, sku.py, journal.py, utils/
│
├── sdk/             public SDK — resource namespaces + UDS transport
│   ├── __init__.py    AjClient, connect(), public namespace exports
│   ├── _transport.py  httpx over UDS + daemon autostart
│   ├── account.py     account-scoped namespaces
│   ├── workspace.py   workspace-scoped namespaces
│   └── logs.py
│
├── client/          frontends — directly consume azure_jobs.sdk
│   ├── discovery.py   interactive setup, fed by the daemon
│   ├── cli/, tui/, ui/
│
└── server/          executes — never imports client/
    ├── app.py         FastAPI routes
    ├── runner.py      uvicorn on a Unix socket + lifecycle
    ├── context.py     per (root, workspace) backend/queue/watcher
    ├── discovery/     the only place `az` is executed
    ├── targets.py     workspace name → target
    ├── main.py
    ├── backend.py     the Azure implementation of the contract
    ├── azure.py, queue.py, watch.py, concurrent.py
    ├── az_client/     the SDK layer
    └── submit/        how a job is *run*
```

The invariants, each with a test:

| Rule | Why |
|---|---|
| `client` never imports `server` | everything it needs is on the wire |
| `sdk` imports neither frontend nor server | it is a public peer, not a CLI detail |
| `server` never imports `client` | the daemon runs headless |
| `shared` imports neither | it is the vocabulary, not a participant |
| Azure adapters appear only under `server/` | only the daemon talks to Azure |
| `rich`/`textual` appear only under `client/` | rendering is not the daemon's job |

Measured after the split: `client → shared` 88 edges, `server → shared` 83,
`client ↔ server` **zero**. The package root re-exports across layers as a lazy
`__getattr__` facade, which is the public SDK surface rather than a dependency.

**Describing a job vs running one.** The client builds a `JobSpec` from a local
template, so the typed `Opts` and the name rules live in `shared/spec.py`; the
submit functions live in `server/submit`. Splitting the old combined registry
this way is what let `job/build.py` stay shared without dragging the SDK along.
`sku.py` split the same way: parsing shorthand is shared, matching it against
real instance types needs ARM and stayed server-side.

**Everything runs in the daemon.** `aj run` submits through the daemon rather
than in the CLI process, with progress streamed back as correlated
`submit.progress` events — a callback cannot cross a socket, and dropping it
would have made `aj run` less informative than before.

## The client: one SDK, two frontends

The CLI and the dashboard call the same object, organised as resource
namespaces:

```python
from azure_jobs import connect

with connect() as d:
    d.auth.status()
    d.sku.list()
    d.job.list(limit=20)              # the configured workspace
    d.ws("other").job.list(limit=20)  # a different one
    d.ws("other").ds.list()
```

Namespaces mirror the CLI groups, so `aj ds list` and `d.ds.list()` are the
same operation under two names rather than two implementations — the reason
the dashboard and the CLI cannot drift apart. Anything scoped to a workspace
is reachable both at the root (meaning "the configured workspace") and through
`d.ws(name)`; `d.job` **is** `d.ws().job`, not a copy. Anything scoped only to
a subscription (`d.sku`, `d.sa`, `d.uai`, `d.ws`) lives at the root alone,
because there is no workspace to narrow it to — which is what lets `aj init`
run before one is configured.

The top-level `sdk/` owns both the small transport and what requests mean. The
frontends import `connect()` directly; there is no CLI context-manager wrapper,
session factory, workspace-catalog adapter, or reconnect proxy in between. A
namespace never builds a URL by hand: it calls the shared route table, so a
path typo is an import error rather than a 404 at runtime.

**Why the SDK is written rather than generated.** FastAPI publishes
`/openapi.json`, and `openapi-python-client` would turn it into a client — but
into flat per-operation functions, not `d.ws(name).job.list()`, and with
`dict[str, Any]` results, because the handlers return plain dicts. The
namespaces *are* the product here; generating them away to regain them by hand
would leave a build step and no ergonomics.

The schema earns its keep as a *check* instead. `routes.py` single-sources the
path, but not the verb, the query parameters, or which operations exist —
`params={"regoin": ...}` against a server declaring `region` type-checks, unit
tests fine, and silently ignores the filter. So
`tests/test_openapi_contract.py` drives every namespace against a recording
transport and asserts each request it produces exists in the served schema,
with a completeness check so a new operation cannot skip it.

**No automatic replay.** A transport failure reaches the caller. Automatically
replaying an ambiguous `submit()` or `queue()` response can create duplicate
jobs, so retry policy belongs to the caller (or to a future server-side
idempotency key), not to a generic proxy.

## Transport: HTTP over a Unix socket

The same shape `dockerd` exposes. FastAPI/uvicorn serves, httpx calls.

| | |
|---|---|
| Versioning | by path (`/v2/...`), so an older client keeps working |
| Resources | one path per resource, so `/openapi.json` can describe the response |
| Log windows | plain HTTP `Range`, answered `206` — what Range is for |
| Server push | Server-Sent Events on `/v2/events` |
| Debugging | `curl --unix-socket … http://d/v2/info`, plus a generated `/openapi.json` |
| Security | socket `0600` inside a `0700` dir; the client refuses a socket it does not own |

Replacing the hand-rolled JSON-RPC loop was not about the wire format. That
loop handled one request at a time per connection (so a delete polling a
long-running operation froze the dashboard), accumulated a thread per CLI
invocation, and raced on shutdown. Those are exactly the parts a real server
already solves.

**Stateless, so there is less to get wrong.** Requests are addressed by
workspace *name* (`/v2/workspaces/{name}/...`); the daemon resolves the name
against the config of the project root in `X-AJ-Root` — not its own — and
caches a *context* per `(project root, workspace)`, created on first use and
expired on idle. Named lookups are cached because they cost an `az` call; the
configured workspace is not, so `aj ws set` takes effect without a restart. Nothing ties a context to a connection, which deleted the
session-token and refcount bookkeeping the old transport needed — and with it
the lifecycle bugs that lived there.

**The client never runs `az`.** Resolving a workspace name, listing workspaces
and reading the active subscription are all `az` calls, so they are endpoints
(`/v2/workspaces`, `/v2/workspaces/{name}`, `/v2/auth/status`) rather than
client-side subprocesses. Credential health is part of the same auth snapshot,
because the daemon's token is the one that matters. That is why
`shared/config` holds only local file config: everything that shells out lives
in `server/discovery/`, guarded by `tests/test_api_architecture.py` with no
exemptions.

**Signing in is `az login`.** `aj auth` is read-only: the daemon is a separate
process with its own credential, so a client-side login would authenticate the
wrong one. Instead the daemon verifies at startup that it can acquire a token
and exits with status 2 if it cannot. The client keeps the spawned process's
output in `<runtime>/daemon.log` and reports it, so an autostart that refuses
reads as "run az login" rather than as a spawn timeout.

A domain failure keeps its type across the wire: an unresolvable workspace
arrives as `WorkspaceError`, not `TransportError`; neither is automatically
replayed.

## Remote HTTP feasibility

The FastAPI application already speaks ordinary HTTP, so exposing **read-only**
resources over TCP is mechanically straightforward. Accepting a **submit from
another machine** is feasible, but is not a socket configuration change:

| Current local assumption | Remote requirement |
|---|---|
| Unix socket permissions authenticate the caller | TLS plus mTLS or OIDC bearer authentication and per-resource authorization |
| `X-AJ-Root` is a trusted path on the daemon host | an authorized project ID; the client must never choose server filesystem paths |
| `JobSpec.code_dir` points at local files visible to the daemon | upload a content-addressed source artifact first, then submit by artifact ID |
| submission may collect the local user's `~/.ssh` keys | remote mode must never read server-home SSH material; use explicit client credentials or workload identity |
| queue/watch journals live under `<root>/daemon` | server-owned state keyed by principal/project/workspace |
| one local user may receive every SSE event | event streams filtered by authenticated principal and project |
| `/retire`, daemon status and auth status are local controls | separate admin surface; never expose them to normal remote clients |
| the daemon's Azure CLI credential represents the user | explicit service-identity model, or delegated user identity with token isolation |

Recommended remote shape:

```text
SDK RemoteHttpTransport
  -> TLS/OIDC reverse proxy
  -> existing FastAPI resource API
  -> artifact store + durable queue/state
  -> Azure service identity
```

The submission flow should be asynchronous and idempotent:

1. upload/touch an artifact by content hash;
2. `POST` a submission with explicit workspace coordinates, `artifact_id`, and
   `Idempotency-Key`;
3. return `202 Accepted` plus a queue ticket;
4. observe it through scoped `GET`/SSE endpoints.

SSE and HTTP Range already work over TCP. UDS should remain the zero-config
default; a remote URL should be explicit and must not autostart a process.
Implement the transport only after project identity, artifact upload,
authorization and event isolation exist—otherwise the server would either read
the wrong filesystem or expose one user's jobs and secrets to another.
Remote submission must also force `AJ_SHIP_SSH=0` semantics on the server:
credentials are never inherited from the service host.

## Azure client notes
- **Blob credential fallback**: SAS → SharedKey → AAD bearer. If the storage
  account disables shared-key access, it skips straight to AAD; on a 403 mid-
  upload it retries once with bearer.
- **China cloud** auto-detected from `*.cn` URLs (data-plane scope and blob host
  switched).
- **Pure parsers** (`ml/extract.py`, `arm/vc.py:parse_managed_quotas`) keep most
  of the package directly unit-testable without HTTP mocks.

## TUI

`aj dash` is built on Textual. `AjDashboard` is only the composition root: it
wires registered features to immutable stores, typed events, narrow view
ports, a bounded worker pool, and the same SDK namespaces as the CLI.

```
tui/
├── app.py           # composition root only
├── bindings.py      # validated commands + context predicates + help
├── features.py      # feature lifecycle and optional Screen registration
├── events.py        # typed, breadth-first cross-feature events
├── models.py        # TUI-only request identities and view enums
├── state.py         # immutable JobsState / LogsState / TargetState snapshots
├── stores.py        # all jobs/target transitions (UI-thread guarded)
├── log_store.py     # exact-byte log windows and projections
├── runtime.py       # bounded worker pool, cancellation, resource leases
├── view_ports.py    # Jobs / Logs / Target / Shell view Protocols
├── ui.py            # separate Textual adapter for each view port
├── components/      # shell layout, widgets, searchable modals
└── controllers/
    ├── workspace.py # target discovery + session ownership
    ├── jobs/        # I/O and commands; mutations go through JobsStore
    └── logs/        # one-shot I/O and presentation over LogsStore
```

Controllers cannot assign state fields and receive only their feature's view
Protocol. Stores assert UI-thread ownership, publish typed events, and expose
immutable snapshots. A fixed six-worker daemon pool bounds blocking I/O;
cancelled queued work is skipped, while session/reader leases defer closing an
in-use resource. Shutdown cancellation happens before a non-UI finalizer joins
workers.

Live logs are timer-driven one-shot reads, so idle polling does not occupy a
worker. `LogChunk` carries raw bytes and authoritative start/end/total offsets;
decoding happens only for display. Azure's adapter derives ranges from
`Content-Range`, slices locally when a server ignores Range, treats EOF 416 as
idle, and reports transport failures. Backfill uses an isolated reader.
Per-target/job/file windows are capped at 16 MiB (32 MiB globally); oversized
visual records are split at 256 KiB without changing remote offsets.

Jobs use opaque IDs and value-comparable cursors. The dashboard initially loads
the requested `--last` scope (50 by default); reaching the final loaded page
and pressing `→` consumes another cursor page without a fixed total cap. Local
filters apply to the jobs loaded so far. Namespace capability flags drive
command availability. Deleting a job updates the JobsStore atomically and
emits `JobDeleted` so cached logs are evicted.

## Design notes

- **Pure REST.** Only `azure-identity` + `requests`. No `azure-ai-ml`, no `amlt`
  runtime.
- **Layered templates.** `account` / `storage` / `environment` building blocks
  composed via `base` chains.
- **Content-addressed artifacts.** Both code blobs and environment versions
  reuse on hash match.
- **Append-only `record.jsonl`.** One submission per line.
- **Lazy imports** in the CLI keep cold start fast; the SDK surface at the
  package root uses `__getattr__` lazy loading.
- **Pure / impure split** throughout — template merging, REST parsing,
  quota parsing, SKU resolution are all functions on plain data, tested
  without I/O.
