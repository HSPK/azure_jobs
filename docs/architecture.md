# Architecture

## Overview

Azure Jobs (`aj`) is a CLI tool for submitting and managing Azure ML jobs. It provides template-based job configuration with inheritance and sensible defaults, submits jobs via pure REST APIs, and supports both Azure ML compute clusters and Singularity virtual clusters.

## Module Layout

The codebase is organized into four packages under `src/azure_jobs/`:

**`cli/`** — Command layer. Each file maps to one or more CLI commands, all built with Click.

- `__init__.py` — defines the `main` Click group and registers all subcommands
- `run.py` — job submission (`aj run`): config validation, SKU resolution, command building
- `jobs.py` — cloud job management (`aj job list/show/cancel/logs`) and local history (`aj list`)
- `templates.py` — template operations (`aj template list/show/validate/pull/push`)
- `workspace.py` — workspace detection and switching (`aj ws list/show/set`)
- `auth.py` — authentication status and `az login`/`logout` delegation (`aj auth`)
- `env.py` — environment inspection (`aj env list/show`)
- `ds.py` — datastore inspection (`aj ds list/show`)
- `quota.py` — compute quota and availability (`aj quota list`)
- `experiment.py` — experiment browsing (`aj exp list/show`)
- `images.py` — Singularity curated image listing (`aj image list`)
- `config.py` — tool configuration (`aj config show/timezone`)
- `dashboard.py` — interactive TUI dashboard (`aj dash`)
- `pull.py` — shared helpers for template repository sync

**`core/`** — Business logic. No CLI framework dependencies.

- `rest_client.py` — `AzureARMClient` (generic ARM) and `AzureMLJobsClient` (workspace-scoped) with token caching
- `submit.py` — `SubmitRequest` → code upload → environment registration → job body → REST PUT
- `conf.py` — YAML template loading with recursive base inheritance and merge logic
- `config.py` — reads/writes `aj_config.json` (defaults, workspace, repo_id)
- `const.py` — path constants derived from `AJ_HOME` environment variable
- `record.py` — `SubmitRecord` dataclass and JSONL logging
- `log_download.py` — job log download via Run History API
- `distributed.py` — multi-node preamble generation (MPI rank mapping, NCCL tuning)
- `sku.py` — Singularity SKU/instance type resolution
- `client.py` — shared utilities: log line filtering, JSON error extraction
- `auth.py` — credential health checks

**`tui/`** — Textual-based interactive dashboard.

- `app.py` — main `AjDashboard` app with job table, filtering, keyboard shortcuts
- `modals.py` — reusable modal dialogs (job detail, confirmation)
- `log_viewer.py` — log display modal
- `workspace.py` — workspace selector modal
- `helpers.py` — UI helper functions

**`utils/`** — Shared output helpers.

- `ui.py` — Rich console: panels, tables, spinners, styled messages
- `time.py` — timezone conversion, duration formatting

## Submission Flow

A job submission follows this path:

```
aj run -t template -n 4 -p 2 python train.py
│
├─ 1. Resolve template ─── look up from -t flag or aj_config.json defaults
├─ 2. Load config ──────── read_conf() recursively resolves base chain
├─ 3. Merge ────────────── merge_confs() combines inheritance chain
├─ 4. Apply overrides ──── CLI flags > template _extra > aj_config defaults
├─ 5. Resolve SKU ──────── string templates or range-based dict matching
├─ 6. Build SubmitRequest ─ dataclass with all job parameters
│
├─ 7. Authenticate ──────── AzureCliCredential → ARM token
├─ 8. Register environment ─ SHA-based dedup, PUT /environments/{name}/versions/{ver}
├─ 9. Create datastores ─── ensure blob datastores exist for storage mounts
├─ 10. Upload code ──────── zip → blob storage → register code version
├─ 11. Build command ────── env exports + setup + distributed preamble + user command
├─ 12. Build job body ───── plain dict with all REST properties
├─ 13. Submit ───────────── PUT /workspaces/{ws}/jobs/{name}
│
└─ 14. Record ───────────── append SubmitRecord to record.jsonl, print portal URL
```

### Environment Management

Environments register Docker images as versioned assets in Azure ML:

- Version is derived from `SHA256(image_url)[:16]` for deterministic dedup
- If the version already exists, registration is skipped
- Singularity curated images (`amlt-sing/` prefix) register a dummy MCR image; the actual image is passed via the job's `imageVersion` resource property

### Datastore Management

Datastores provide named references to Azure Blob containers for job storage mounts:

- Created on-demand during job submission (credential-less, using workspace managed identity)
- Mount paths are translated to `azureml://` URIs in the job output spec
- `PathOnCompute` properties and `AZUREML_DATAREFERENCE_*` env vars give jobs filesystem access to mounted data

### Code Upload

1. Zip code directory (respecting `.gitignore`-like patterns, always skipping `__pycache__`, `.git`, `.venv`)
2. Compute SHA256 hash of the zip for content-based dedup
3. Get workspace default blob storage info from ARM properties
4. Upload zip to `azureml-blobstore-{workspace_id}/LocalUpload/{hash}/code.zip`
5. Register code version via `PUT /codes/aj-code/versions/{hash}`
6. Reference the code ARM ID in the job body

## Design Decisions

- **Pure REST** — no dependency on `azure-ai-ml` SDK or `amlt`. All Azure interactions are direct HTTP calls via `requests`. Only `azure-identity` is used (for `AzureCliCredential`).
- **Fast startup** — `aj --help` runs in ~160ms. Heavy imports (Rich, Textual, Azure) are deferred to command execution.
- **Layered templates** — concerns are separated into account, storage, and environment layers. Templates compose these via `base` chains. Cluster-specific config lives in the template itself.
- **Single config file** — `aj_config.json` stores defaults, repo_id, and workspace credentials.
- **Append-only job log** — `record.jsonl` is simple, greppable, and never loses data.
- **Content-addressed artifacts** — code versions and environment versions use SHA hashes for dedup.
- **Dual compute support** — both AML clusters and Singularity VCs are first-class citizens, with SKU resolution, identity handling, and resource configuration tailored to each.
