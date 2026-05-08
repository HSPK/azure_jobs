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

## Templates

Templates live under `.azure_jobs/template/` as YAML files. There's no `aj template create` — bring them in one of three ways:

1. **`aj init`** — scaffolds `.azure_jobs/` and (optionally) pulls a starter template repo.
2. **`aj template pull <user>/<repo>`** — clone a shared template repo into `.azure_jobs/`.
3. **Hand-author** — drop a YAML file into `.azure_jobs/template/`.

Minimal leaf template (`.azure_jobs/template/gpu.yaml`):

```yaml
base: [account.default, storage.default, environment.aml]
config:
  target:
    name: my-cluster
  jobs:
    - name: train
      sku: "{nodes}xA100-80GB"
```

`base` chains other YAML files in `.azure_jobs/` (dotted name → `.azure_jobs/<dir>/<name>.yaml`); `{nodes}` / `{processes}` are substituted from CLI flags. Inheritance, merge rules, and SKU formats are documented in [docs/configuration.md](docs/configuration.md).

```bash
aj template list                # see what's available
aj template show <name>         # resolved config (after inheritance)
aj template validate            # sanity check
aj template push -m "msg"       # commit + push back upstream
```

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

## `aj dash`

Interactive TUI dashboard for browsing and managing cloud jobs.

```bash
aj dash
```

| Key | Action |
|-----|--------|
| `↑` `↓` | Move selection |
| `←` `→` | Prev / next page |
| `enter` / `i` | Job detail panel |
| `l` | Open logs (auto-streams if the job is running) |
| `o` | Pick a different log file |
| `c` | Cancel the selected job |
| `r` | Refresh |
| `f` / `e` / `w` | Filter by status / experiment / workspace |
| `/` | Search |
| `F` | Clear all filters |
| `esc` | Help overlay |
| `q` | Quit |

## Use as a Python SDK

The same engine the CLI uses is exposed at the package root, so you can build and submit jobs from your own scripts:

```python
from azure_jobs import (
    Template,
    build_submit_request,
    submit_via_native,   # also: submit_via_volcano, submit_via_amlt
    get_workspace_config,
)

template = Template.from_conf_path(".azure_jobs/template/gpu.yaml")
request = build_submit_request(
    template,
    name="my-job", sid="abc123", sku="2xA100-80GB",
    user_command="train.py", user_args=(),
    workspace=get_workspace_config(),
    template_name="gpu", nodes=2, processes=8,
    code_dir="/path/to/project",  # defaults to os.getcwd()
)
result = submit_via_native(request)
print(result.status, result.portal_url)
```

See [docs/sdk.md](docs/sdk.md) for the full surface and a `submit_and_record` example.

## Documentation

| Document | Contents |
|----------|----------|
| [Commands](docs/commands.md) | `aj job`, `aj template`, `aj quota`, `aj sku`, `aj dash`, ... |
| [SDK](docs/sdk.md) | Programmatic submission API |
| [Architecture](docs/architecture.md) | Module layout, submission flow, backends |
| [Configuration](docs/configuration.md) | Templates, inheritance, merge rules, SKU formats |
| [REST API](docs/rest-api.md) | REST client design, endpoints, job body shape |
| [Comparison](docs/comparison.md) | aj vs amlt feature matrix |
| [Roadmap](docs/roadmap.md) | Planned features |
