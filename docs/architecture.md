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
│   ├── compute.py / graph.py / identity.py / instance_types.py
│   ├── storage.py / subscriptions.py / vc.py / workspace.py
│   ├── models.py    # VCInfo, WorkspaceInfo, SeriesQuota, …
│   └── __init__.py  # AzureARMClient (namespaces: .vc, .workspace, .graph, …)
└── ml/
    ├── context.py   # RestContext (= AuthSession + WorkspaceCoords + dual scopes)
    ├── jobs.py / environments.py / datastores.py / blob.py / logs.py
    ├── run_history.py
    ├── extract.py   # Pure REST→JobInfo parsers
    └── __init__.py  # AzureMLClient (namespaces: .jobs, .environments,
                     # .datastores, .blob, .logs)
```

Highlights:

- **Pure `requests`** with retry+backoff (idempotent + write methods, honours
  `Retry-After`); `azure-identity` is the only auth dependency.
- **Two-plane**: ARM control plane (`management.azure.com`) for jobs/env/code
  registration; ML data plane (discovered from `properties.discoveryUrl`,
  `ml.azure.com/.default`) for Run History (error details, log URLs).
- **Token cache** per scope (ARM, data-plane, storage), with a 60 s refresh
  leeway.
- **Blob credential fallback**: SAS → SharedKey → AAD bearer. If the storage
  account disables shared-key access, it skips straight to AAD; on a 403 mid-
  upload it retries once with bearer.
- **China cloud** auto-detected from `*.cn` URLs (data-plane scope and blob host
  switched).
- **Pure parsers** (`ml/extract.py`, `arm/vc.py:parse_managed_quotas`) keep most
  of the package directly unit-testable without HTTP mocks.

## TUI

`aj dash` is built on Textual. `AjDashboard` is only the composition root: it
wires registered features to immutable stores, typed events, narrow view ports,
a bounded worker pool, and capability-oriented backend ports.

```
tui/
├── app.py           # composition root only
├── bindings.py      # validated commands + context predicates + help
├── features.py      # feature lifecycle and optional Screen registration
├── events.py        # typed, breadth-first cross-feature events
├── models.py        # Target / Job / JobRef / request identities
├── state.py         # immutable JobsState / LogsState / TargetState snapshots
├── stores.py        # all jobs/target transitions (UI-thread guarded)
├── log_store.py     # exact-byte log windows and projections
├── ports.py         # capability ports + Cursor / LogChunk(bytes)
├── runtime.py       # bounded worker pool, cancellation, resource leases
├── view_ports.py    # Jobs / Logs / Target / Shell view Protocols
├── ui.py            # separate Textual adapter for each view port
├── adapters/
│   └── azureml.py   # strict Azure implementation (including HTTP Range)
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
filters apply to the jobs loaded so far. Optional actions/log capabilities
drive command availability, so a new backend may implement only the features
it supports. Permanent deletion is a separate `JobDelete`
capability rather than part of cancel/detail actions; deleting a job updates
the JobsStore atomically and emits `JobDeleted` so cached logs are evicted.

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
