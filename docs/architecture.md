# Architecture

## Layout

```
src/azure_jobs/
├── cli/            # Click commands (one module per command group)
├── core/
│   ├── submit/     # Submission engine (backend-agnostic + per-backend)
│   ├── rest_client/# Azure ARM + Azure ML REST client
│   ├── conf.py     # YAML template loading + merge engine
│   ├── config.py   # aj_config.json reader/writer
│   ├── const.py    # paths derived from $AJ_HOME
│   ├── record.py   # SubmitRecord + record.jsonl
│   ├── sku.py      # Singularity SKU shorthand → instance type
│   └── log_*.py    # Job log download / streaming
├── tui/            # Textual dashboard (`aj dash`)
└── utils/
    ├── ui.py       # Rich console helpers
    ├── fs.py       # walk_code, compute_code_hash, .codeignore
    ├── ignore.py   # Compiled gitignore-style matcher
    └── time.py     # Timezone + duration formatting
```

## Submission engine

`core/submit/` is split into a backend-agnostic core plus three backends. All three implement the same contract: build a normalized `SubmitRequest`, submit it, return a `SubmitResult`, and emit `SubmitEvent`s for progress UI.

```
submit/
├── models.py       # SubmitRequest, SubmitResult, SubmitEvent, StorageMount
├── config.py       # Template + CLI args → SubmitRequest (merge logic)
├── runner.py       # Backend dispatch (`submit(request, on_event)`)
├── native/         # AML / Singularity via REST
│   ├── submit.py     # main flow
│   ├── command.py    # runner script (aj_runner.sh)
│   ├── compute.py    # distribution / identity / resources
│   ├── environment.py# environment registration
│   ├── storage.py    # datastore + outputs
│   └── precheck.py   # SKU / quota / compute pre-flight
├── volcano/        # Kubernetes Volcano via kubectl
│   ├── submit.py
│   ├── config.py     # SubmitRequest → Volcano Job spec
│   ├── upload.py     # PVC upload via `kubectl exec` + tar
│   └── constants.py
└── amlt/           # Shell out to `amlt run`
```

### Flow

```
aj run -t gpu -n 4 -p 8 train.py
  │
  ├─ 1. read_conf()        recursively resolve `base` chain
  ├─ 2. merge_confs()      dicts merge, lists zip, scalars last-wins
  ├─ 3. CLI overrides      -n/-p/--ppn applied on top
  ├─ 4. build_submit_request()
  │       → SubmitRequest (compute, sku, image, env_vars, code, …)
  │
  ├─ 5. dispatch by request.service
  │     ├─ native  → register env → upload code → PUT /jobs/{name}
  │     ├─ volcano → kubectl exec tar (PVC) → kubectl create
  │     └─ amlt    → write YAML → exec amlt run
  │
  └─ 6. record.jsonl ← SubmitRecord
```

### Code upload

`utils/fs.walk_code()` walks the project respecting:

- Built-in excludes: `__pycache__`, `.git`, `.venv`, `node_modules`, plus everything under `.azure_jobs/` except `scripts/`.
- User patterns from the template's `code.ignore` plus `.codeignore` / `.amltignore`.

`compute_code_hash()` produces a deterministic SHA-256 over `(rel_path, sha256(content))` pairs sorted by path. The native backend uses this hash as the blob path (`LocalUpload/{hash}/...`), so identical inputs reuse the prior upload and the registered code asset.

All AJ_* runtime values (`AJ_NAME`, `AJ_ID`, `AJ_NODES`, `AJ_PROCESSES`, ...) are passed via `environmentVariables` rather than baked into the runner script — the script body stays content-stable across runs.

### Volcano backend

Each Volcano `Job` runs a script that:

1. Materializes the (immutable, shared) PVC code asset into a pod-local `emptyDir` at `/mnt/aj-workdir/<job>/wd` via `cp -a`. Runtime writes never hit shared storage.
2. Sets `AJ_WORKDIR` and `cd`s into it.
3. Runs the template's setup commands and the user command.

Code upload uses a short-lived "tar" pod with the PVC mounted; `tar cv --null -T <filelist> | kubectl exec tar xf - -C <dest>` streams the files in. The pod's main process is `exec sleep` so SIGTERM is forwarded; cleanup uses `kubectl delete --wait=false`.

## REST client

`core/rest_client/` is split by surface:

- `auth.py` — `AzureCliCredential` token cache (60-second early refresh).
- `arm.py` — `AzureARMClient`: cross-subscription ARM operations, Resource Graph queries, VC quotas, AML compute lookups.
- `client.py` — `AzureMLClient`: workspace-scoped namespaces (`.jobs`, `.environments`, `.datastores`, `.blob`, `.run_history`).
- `api/` — per-namespace implementations.
- `factory.py` / `context.py` — wire credentials and workspace context together.

China cloud is auto-detected from management URLs (`*.cn`) and switches the data-plane scope and blob hostname accordingly.

## Design notes

- **Pure REST, no SDK.** Only `azure-identity` (for `AzureCliCredential`) plus `requests`. No `azure-ai-ml`, no `amlt` runtime.
- **Lazy imports.** `aj --help` runs in ~160 ms because Rich/Textual/Azure modules are imported only when their commands run.
- **Layered templates.** Templates compose `account` / `storage` / `environment` building blocks via `base` chains; cluster-specific bits stay in the leaf template.
- **Content-addressed artifacts.** Code and environment versions are SHA-deduped; re-submits skip the upload.
- **Append-only history.** `record.jsonl` is greppable, never lossy.
- **Single config file.** `aj_config.json` holds defaults, `repo_id`, and workspace credentials.
