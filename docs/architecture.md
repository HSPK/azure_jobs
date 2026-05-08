# Architecture

## Layout

```
src/azure_jobs/
├── cli/            # Click commands
├── core/
│   ├── submit/     # Submission engine
│   ├── rest_client/# Azure ARM + ML REST
│   ├── conf.py     # YAML loader + merge
│   ├── config.py   # aj_config.json
│   ├── record.py   # record.jsonl
│   └── sku.py      # SKU shorthand resolution
├── tui/            # `aj dash`
└── utils/          # ui, fs, ignore, time
```

## Submission engine

`core/submit/` has a backend-agnostic core plus three backends — all build a `SubmitRequest`, run it, return a `SubmitResult`, and emit `SubmitEvent`s for progress UI.

```
submit/
├── models.py       # SubmitRequest / Result / Event
├── config.py       # template + CLI → SubmitRequest
├── runner.py       # backend dispatch
├── native/         # AML + Singularity via REST
├── volcano/        # K8s Volcano via kubectl
└── amlt/           # shells out to amlt
```

## Flow

```
aj run -t gpu -n 4 -p 8 train.py
  ├─ read_conf()          resolve `base` chain
  ├─ merge_confs()        dicts merge, lists zip, scalars last-wins
  ├─ apply -n / -p / --ppn
  ├─ build_submit_request → SubmitRequest
  ├─ dispatch by request.service
  │     native  → register env → upload code → PUT /jobs/{name}
  │     volcano → kubectl exec tar (PVC) → kubectl create
  │     amlt    → write YAML → exec amlt run
  └─ append SubmitRecord to record.jsonl
```

## Code upload

`utils/fs.walk_code()` excludes `__pycache__` / `.git` / `.venv` / `node_modules` and `.azure_jobs/` (except `scripts/`), then applies the template's `code.ignore` + `.codeignore` / `.amltignore`.

`compute_code_hash()` is sha256 over `(rel_path, sha256(content))` pairs sorted by path. Native uses this as the blob path, so identical inputs reuse the prior upload.

AJ_* runtime values (`AJ_NAME`, `AJ_NODES`, `AJ_PROCESSES`, ...) flow via `environmentVariables` rather than the runner script — keeps the script body content-stable across runs.

## Volcano backend

Each Job's container script:

1. `cp -a` the (immutable) PVC code asset into a pod-local `emptyDir` at `/mnt/aj-workdir/<job>/wd`. Runtime writes don't hit shared storage.
2. `cd $AJ_WORKDIR`, run setup commands, run the user command.

Code upload uses a short-lived "tar" pod with the PVC mounted. `tar cv --null -T <filelist> | kubectl exec tar xf - -C <dest>` streams files in. Pod main process is `exec sleep` so SIGTERM forwards; cleanup uses `kubectl delete --wait=false`.

## REST client

`core/rest_client/` is split by surface:

- `auth.py` — token cache (60 s early refresh).
- `arm.py` — `AzureARMClient`: cross-subscription ARM, Resource Graph, VC quotas, AML compute lookups.
- `client.py` + `api/` — `AzureMLClient` with namespaces: `.jobs`, `.environments`, `.datastores`, `.blob`, `.run_history`.

China cloud auto-detected from `*.cn` URLs (data-plane scope and blob host switched).

## Design notes

- Pure REST. Only `azure-identity` + `requests`. No `azure-ai-ml`, no `amlt` runtime.
- Lazy imports → `aj --help` in ~160 ms.
- Layered templates: `account` / `storage` / `environment` building blocks composed via `base` chains.
- Content-addressed code + environment artifacts.
- Append-only `record.jsonl`.
