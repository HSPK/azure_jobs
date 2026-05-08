# Azure Jobs

A fast, lightweight CLI for submitting and managing Azure ML jobs through pure REST APIs — no `azure-ai-ml` SDK and no `amlt` runtime required.

`aj` adds a template inheritance layer on top of three submission backends:

- **native** — direct Azure ML REST (default for AML / Singularity).
- **amlt** — delegates to the `amlt` CLI for compatibility.
- **volcano** — submits to a Kubernetes Volcano cluster via `kubectl`.

## Install

```bash
pipx install azure_jobs
```

Requires `az login`. For the volcano backend you also need `kubectl` configured against your cluster.

## Quickstart

```bash
mkdir my-project && cd my-project
aj init                          # scaffold .azure_jobs/, register workspace
aj pull <user>/<repo>            # (optional) clone shared templates
aj run -t gpu train.py           # submit using the "gpu" template
```

`.py` scripts run via `uv run`, `.sh` via `bash`. Drop a `.codeignore` (or `.amltignore`) at the project root to exclude paths from the upload.

### amlt compatibility

```bash
aj init amlt                     # scaffold amlt config
aj run --amlt -t gpu train.py    # submit via amlt instead of REST
```

## Common commands

### Submit

```bash
aj run -t gpu train.py           # submit via REST
aj run train.py                  # reuse last template
aj run -d train.py               # dry run — print the config, don't submit
aj run -L train.py               # run locally
aj run -n 4 -p 8 train.py        # 4 nodes × 8 GPUs/node
aj code stats -t gpu             # what would be uploaded? (count, size, hash)
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

### Manage jobs

```bash
aj job list                      # recent cloud jobs
aj job list -s Running           # filter by status
aj job show <id>                 # detail panel
aj job cancel <id>
aj job logs <id>                 # download + display logs
aj job stats                     # GPU-hours, success rate, by experiment/compute/user
aj list                          # local submission history (record.jsonl)
aj dash                          # interactive TUI
```

### Templates

```bash
aj template list                 # available templates
aj template show <name>          # resolved config (after inheritance)
aj template validate             # check all templates
aj template diff                 # local edits vs upstream
aj template pull <repo>          # clone a template repo
aj template push -m "msg"        # commit + push
```

### Workspace, quota, SKUs

```bash
aj ws list / set                 # workspaces in subscription
aj auth status / login           # credential health, az login
aj quota list                    # Singularity VC quota
aj quota list --aml              # AML cluster quota
aj sku list                      # SKUs by VC
aj sku check -t <template>       # pre-flight: SKU/quota/compute
aj env list / show <name>        # registered environments
aj ds list / show <name>         # datastores
aj image list                    # Singularity curated images
aj exp list                      # experiments (aggregated from jobs)
```

### Config

```bash
aj config show
aj config timezone Asia/Shanghai
aj config experiment <name>
```

## How submission works

1. Resolve the template, walk the `base` inheritance chain, merge configs.
2. Apply CLI overrides (`-n` / `-p` / `--ppn`).
3. Build a normalized `SubmitRequest`.
4. Backend dispatch:
   - **native** — register environment (SHA-deduped) → upload code (content-addressed) → `PUT /jobs/{name}` via ARM REST.
   - **volcano** — render Volcano Job YAML → upload code to a PVC via `kubectl exec` + tar → `kubectl create`.
   - **amlt** — write a submission YAML and shell out to `amlt run`.
5. Append a `SubmitRecord` to `record.jsonl` and print the portal URL.

Code uploads are content-addressed: identical (template + command + code) yields the same hash, so re-runs reuse the prior asset and skip the upload entirely.

## Documentation

| Document | Contents |
|----------|----------|
| [Architecture](docs/architecture.md) | Module layout, submission flow, backends |
| [Configuration](docs/configuration.md) | Templates, inheritance, merge rules, SKU formats |
| [REST API](docs/rest-api.md) | REST client design, endpoints, job body shape |
| [Comparison](docs/comparison.md) | aj vs amlt feature matrix |
| [Roadmap](docs/roadmap.md) | Planned features |
