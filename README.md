# Azure Jobs

A fast, lightweight CLI for submitting Azure ML jobs through pure REST APIs — no `azure-ai-ml` SDK and no `amlt` runtime required.

`aj run` adds a template inheritance layer on top of three submission backends:

- **native** — direct Azure ML REST (default for AML / Singularity).
- **amlt** — delegates to the `amlt` CLI for compatibility.
- **volcano** — submits to a Kubernetes Volcano cluster via `kubectl`.

## Install

```bash
pipx install azure_jobs
```

Requires `az login`. The volcano backend additionally needs `kubectl` configured against your cluster.

## Quickstart

```bash
mkdir my-project && cd my-project
aj init                          # scaffold .azure_jobs/, register workspace
aj pull <user>/<repo>            # (optional) clone shared templates
aj run -t gpu train.py           # submit using the "gpu" template
```

`.py` scripts run via `uv run`, `.sh` via `bash`. Drop a `.codeignore` (or `.amltignore`) at the project root to exclude paths from the upload.

## `aj run`

```bash
aj run -t gpu train.py           # submit via REST
aj run train.py                  # reuse last template
aj run -t gpu -n 4 -p 8 train.py # 4 nodes × 8 GPUs/node
aj run -d train.py               # dry run — print config, don't submit
aj run -L train.py               # run locally
aj run --amlt -t gpu train.py    # submit via amlt instead
```

| Flag | Purpose |
|------|---------|
| `-t` | Template name |
| `-n` | Number of nodes |
| `-p` | GPUs per node (drives SKU + `AJ_PROCESSES`) |
| `--ppn` | Launcher processes per node (e.g. `torchrun --nproc-per-node`) |
| `-d` | Dry run |
| `-y` | Skip confirmation |
| `-L` | Run locally |
| `--amlt` | Submit via amlt |

Positional args after the script are forwarded verbatim to your command.

### How it works

1. Resolve the template, walk the `base` chain, merge configs.
2. Apply CLI overrides (`-n` / `-p` / `--ppn`).
3. Build a normalized `SubmitRequest`.
4. Dispatch by backend:
   - **native** — register environment (SHA-deduped) → upload code (content-addressed) → `PUT /jobs/{name}`.
   - **volcano** — render Volcano Job YAML → upload code to a PVC via `kubectl exec` + tar → `kubectl create`.
   - **amlt** — write a submission YAML and shell out to `amlt run`.
5. Append a `SubmitRecord` to `record.jsonl` and print the portal URL.

Code uploads are content-addressed: identical (template + command + code) → identical hash → re-runs reuse the prior asset.

## `AJ_*` environment variables

Exported into every job. Read them in your training script.

| Variable | Meaning |
|----------|---------|
| `AJ_NAME` | Job display name |
| `AJ_ID` | Submission ID (matches `record.jsonl`) |
| `AJ_TEMPLATE` | Template name used |
| `AJ_NODES` | Number of nodes |
| `AJ_GPUS_PER_NODE` | `-p` value |
| `AJ_PROCESSES` | `AJ_NODES × AJ_GPUS_PER_NODE` |
| `AJ_PROCESSES_PER_NODE` | `--ppn` value |
| `AJ_SUBMIT_TIMESTAMP_UTC` | Submission timestamp |

Example — `torchrun` with whatever the user requested:

```bash
torchrun \
  --nnodes=$AJ_NODES \
  --nproc_per_node=$AJ_GPUS_PER_NODE \
  --node_rank=$RANK \
  --master_addr=$MASTER_ADDR \
  train.py
```

These variables flow via the job's `environmentVariables` (native), the container `env` list (volcano), or `submit_args.env` (amlt). They are **not** baked into the runner script body, so the uploaded code asset stays content-addressable across submissions.

## Documentation

| Document | Contents |
|----------|----------|
| [Commands](docs/commands.md) | `aj job`, `aj template`, `aj quota`, `aj sku`, `aj dash`, ... |
| [Architecture](docs/architecture.md) | Module layout, submission flow, backends |
| [Configuration](docs/configuration.md) | Templates, inheritance, merge rules, SKU formats |
| [REST API](docs/rest-api.md) | REST client design, endpoints, job body shape |
| [Comparison](docs/comparison.md) | aj vs amlt feature matrix |
| [Roadmap](docs/roadmap.md) | Planned features |
