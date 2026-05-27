# Architecture

## Source layout

```
src/azure_jobs/
├── cli/             # Click commands — thin orchestration over core/
├── core/
│   ├── template/    # YAML template loader, merge engine, validator
│   ├── config/      # aj_config.json (workspace, defaults, dashboard)
│   ├── submit/      # Submission engine + backends + record.jsonl
│   ├── az_client/   # Pure-REST Azure clients (ARM + ML)
│   ├── aml/         # AML compute / VM-GPU helpers built on az_client
│   ├── jobs.py      # Cross-workspace job aggregation
│   ├── errors.py    # Typed exception hierarchy
│   └── const.py     # Path constants (AJ_HOME-derived)
├── tui/             # `aj dash` — Textual dashboard
└── utils/           # ui, fs, ignore, time, text, dataclass helpers
```

The CLI never reaches into ARM/ML SDKs; it goes through `core/`. The TUI is one
more `core/` consumer with its own state and controllers.

## Submission engine

`core/submit/` decomposes into a backend-agnostic core plus two submission
methods. The native method has three target clusters as sub-clients. All build
a `SubmitRequest`, run it, return a `SubmitResult`, and emit `SubmitEvent`s for
progress UI.

```
submit/
├── models.py        # SubmitRequest / SubmitResult / SubmitEvent / *Opts
├── build.py         # Template + CLI params → SubmitRequest
├── script_runner.py # `.py` → `uv run`, `.sh` → `bash`
├── render.py        # SubmitRequest → amlt-style YAML
├── materialise.py   # Write YAML to AJ_SUBMISSION_HOME; stamp submission_path
├── dispatch.py      # Backend registry (register_backend / get_backend)
├── amlt.py          # Shells out to the amlt CLI
└── native/          # We build the REST request ourselves
    ├── __init__.py  # Service router — dispatch by request.service
    ├── azureml/     # `aml` (Azure ML) + `sing` (Singularity) targets
    └── volcano/     # K8s Volcano target via kubectl
```

`SubmissionRecord` and the append-only `record.jsonl` I/O live in
`core/journal.py` — it is local-journal state, not submission state.

Every backend exposes `(request, *, on_event) -> SubmitResult` and self-registers
via `register_backend(...)` at import time. The CLI dispatches by
`request.service` — `aml`/`sing` → native(azureml), `volcano` → native(volcano),
`amlt` → amlt (also forced when `--amlt` is passed).

## Submit flow

```
aj run -t gpu -n 4 -p 8 train.py
  ├─ read_conf()              resolve base chain (core/template/engine.py)
  ├─ merge_confs()            dicts recurse; lists-of-dicts merge by index;
  │                           scalar lists concatenate; scalars last-wins
  ├─ apply -n / -p / --ppn
  ├─ build_submit_request →   SubmitRequest
  ├─ get_backend(service)
  │     native  → resolve target → upload code → PUT /jobs/{name}
  │              (aml/sing via azureml client, volcano via kubectl)
  │     amlt    → materialise YAML → exec amlt run
  └─ log_record() →           append SubmissionRecord to record.jsonl
```

The native backend orders work as **read-only validation first, remote writes
last**: resolve target → auth → command + SKU resolve → Singularity UAI
preflight → assemble runner script and ssh files → only then register
environment, mount storage, upload code, submit.

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

`core/az_client/` is workspace-agnostic ARM + workspace-scoped ML, split into
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

`aj dash` is built on Textual. The `AjDashboard` app is a thin dispatcher: every
key binding forwards to a controller method. Controllers own their state slice
and one concern each.

```
tui/
├── app.py           # AjDashboard: bindings + action_* delegation only
├── state.py         # JobsState / LogsState / WorkspaceState / Widgets
├── components/      # LogViewer (vim-nav RichLog), InfoScroll, modals
└── controllers/
    ├── base.py      # Controller[S] (app + state slice + spawn helper)
    ├── workspace.py # detect / pick / switch
    ├── jobs/        # fetch · view · filters · cancel (one JobsState)
    └── logs/        # stream · buffer · view (one LogsState)
```

REST I/O runs on Textual thread workers via `Controller.spawn(...)`. Stale
results are dropped by comparing a per-controller `session_seq` captured at
spawn time; logs additionally guard on `state.job == azure_name`. Workspace
switches bump the seq and `cancel_all()` outstanding workers.

Log streaming uses HTTP Range requests: initial `tail(64 KiB)` then `poll()`
with exponential backoff (3 s → 30 s); scroll-up triggers single-flight
backfill of 1 MiB chunks; `g` jumps to byte 0 (capped at 32 MiB). Per-job
buffers (`MAX_BUFFER_LINES=50 000`) are LRU-evicted (`MAX_SNAPSHOTS=32`) with
the active job protected from eviction.

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
- **Pure / impure split** throughout `core/` — template merging, REST parsing,
  quota parsing, SKU resolution are all functions on plain data, tested
  without I/O.
