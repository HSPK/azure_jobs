# Tutorial

A guided walkthrough — from an empty directory to a tracked, iterating
Azure ML training job — in ten minutes.

---

## 1 · Install

`aj` is distributed on PyPI. Install via [`pipx`](https://pipx.pypa.io/)
so the CLI lives in its own isolated environment:

```bash
sudo apt install pipx          # or: python3 -m pip install --user pipx
pipx ensurepath

pipx install azure_jobs
```

Verify:

```bash
aj --version
```

![aj version command](_assets/aj_version.png)

### Authenticate

`aj` uses the standard Azure credential chain. A one-time `az login` is
enough for most users:

```bash
az login                       # opens a browser
aj auth status                 # check the credential is picked up
```

![aj auth success](_assets/aj_auth_success.png)

The **Volcano** backend additionally requires `kubectl` configured
against your cluster.

---

## 2 · Scaffold

`aj init` is interactive. Its first prompt is for a **shared template
repo** (e.g. `user/repo` or `git@github.com:…`). It then walks you
through subscription → resource group → workspace → experiment, and
writes `.azure_jobs/aj_config.json`.

Pick the path that matches your situation:

=== "I have a shared template repo"

    ```bash
    mkdir my-project && cd my-project
    aj init
    # → Template repo URL: <user>/<repo>
    # → walks you through workspace + experiment
    ```

![init from template](_assets/init_from_template.png)

=== "I'm starting from scratch"

    Pre-create an empty `.azure_jobs/template/` so `aj init` skips the
    repo prompt and only configures the workspace:

    ```bash
    mkdir -p my-project/.azure_jobs/template
    cd my-project
    aj init                       # workspace + experiment only
    ```

    You'll author your first template in [§3](#3-author-a-template).

You now have:

```
.azure_jobs/
├── aj_config.json
├── record.jsonl
└── template/
    ├── account/        # populated by `aj pull`
    ├── storage/        # populated by `aj pull`
    ├── environment/    # populated by `aj pull`
    └── …               # leaf templates
```

> **Tip** — you can pull a shared repo later with
> `aj pull <user>/<repo>`; it merges into the existing `.azure_jobs/`.

---

## 3 · Author a template

A template is just YAML. `aj` composes a final job spec by walking the
`base:` chain, so a typical project splits concerns into four files:

```
.azure_jobs/template/
├── account/
│   └── default.yaml          # subscription + workspace
├── storage/
│   └── default.yaml          # blob mounts
├── environment/
│   └── aml.yaml              # service + image + setup
└── gpu.yaml                  # leaf — what `aj run -t gpu` picks up
```

Create each file as follows.

### 3.1 · Account — subscription & workspace

```yaml
# .azure_jobs/template/account/default.yaml
target:
  service: aml                            # aml | sing | volcano
  subscription_id: 00000000-0000-0000-0000-000000000000
  resource_group: my-rg
  workspace_name: my-workspace
```

> `aj init` already wrote these three values into
> `.azure_jobs/aj_config.json`. Hard-coding them in a template only
> matters if you want a single project to address multiple workspaces.

### 3.2 · Storage — blob mounts

```yaml
# .azure_jobs/template/storage/default.yaml
storage:
  data:                                   # mount alias, free-form
    storage_account_name: mydataacct
    container_name: datasets
    mount_dir: /mnt/data                  # path inside the container
  ckpt:
    storage_account_name: mydataacct
    container_name: checkpoints
    mount_dir: /mnt/ckpt
```

`aj` creates the matching datastore in the workspace on first submit
and exposes each mount as a read/write path in the job container.

### 3.3 · Environment — image & setup

```yaml
# .azure_jobs/template/environment/aml.yaml
environment:
  image: nvcr.io/nvidia/pytorch:24.07-py3
  registry: nvcr.io                       # optional, only if private
  setup:                                  # commands run before user cmd
    - pip install -U pip
    - pip install -r requirements.txt
```

### 3.4 · Leaf — what you actually submit

```yaml
# .azure_jobs/template/gpu.yaml
description: 1–8× A100 training
base: [account.default, storage.default, environment.aml]
target:
  name: my-a100-cluster                   # the AML compute name
jobs:
  - name: train
    sku: "{nodes}xA100-80GB"              # {nodes} comes from `-n`
```

That's it. `aj run -t gpu` resolves the `base:` chain left-to-right,
overlays this leaf, and submits.

### Inspect what you wrote

```bash
aj template list                          # available leaves
aj template show gpu                      # resolved config, post-inheritance
aj template validate                      # schema sweep across all templates
```

### Things to know

- Dotted `base:` names resolve to `.azure_jobs/<dir>/<name>.yaml`;
  plain names resolve next to the current file.
- `{nodes}` / `{processes}` are substituted from `-n` / `-p` at submit.
- Merge: dicts recurse, lists-of-dicts by index, scalar lists concat,
  scalars last-wins. See [configuration.md](configuration.md).
- The leaf is the only file `aj run -t <name>` looks at; everything
  else is reachable only via `base:`.

---

## 4 · Dry-run before you spend

```bash
aj run -t gpu -d -n 2 -p 8 train.py
```

`-d` renders the full submission YAML to `.azure_jobs/dryrun/<sid>.yaml`
without uploading or submitting. Always do this after touching a
template.

Curious what gets uploaded?

```bash
aj code stats -t gpu          # file count, total size, content hash
aj code stats -t gpu -n 20    # 20 largest files
```

> A `.codeignore` (or `.amltignore`) at the project root prunes the
> upload. `.git`, `__pycache__`, `.venv`, `node_modules`, and
> `.azure_jobs/` (except `scripts/`) are always excluded.

---

## 5 · Pre-flight

Catch SKU and quota issues before submitting:

```bash
aj sku check -t gpu -n 2 -p 8
aj quota list                 # Singularity VC quota
aj quota list --aml           # AML cluster availability
```

---

## 6 · Submit

```bash
aj run -t gpu -n 2 -p 8 train.py --lr 1e-3
```

| Flag    | Meaning                                                   |
|---------|-----------------------------------------------------------|
| `-t`    | Template (omit to reuse the last default)                 |
| `-n`    | Nodes                                                     |
| `-p`    | GPUs per node — drives SKU + `AJ_GPUS_PER_NODE`           |
| `--ppn` | Launcher processes per node (e.g. `torchrun --nproc-per-node`) |
| `-d`    | Dry run                                                   |
| `--amlt`| Submit via the `amlt` CLI instead of native REST          |

Anything after the script forwards verbatim to your command. `.py` runs
via `uv run`, `.sh` via `bash`.

Every submission is appended to `.azure_jobs/record.jsonl` with a short
ID (`AJ_ID`) for later lookup.

### Inside your training script

```python
import os
nodes = int(os.environ["AJ_NODES"])
gpus = int(os.environ["AJ_GPUS_PER_NODE"])
run_id = os.environ["AJ_ID"]
```

```bash
torchrun --nnodes $AJ_NODES --nproc-per-node $AJ_GPUS_PER_NODE train.py
```

Full env contract: [env_vars.md](env_vars.md).

---

## 7 · Track

```bash
aj list                       # local submission history
aj job list                   # cloud jobs in the current workspace
aj job list -s Running
aj job show <id>              # detail panel (short ID works)
aj job logs <id>              # stream + download
aj job cancel <id>
aj job stats                  # GPU-hours · success rate · breakdowns
```

For interactive triage:

```bash
aj dash                       # TUI dashboard
```

---

## 8 · Iterate

```bash
aj template diff              # local edits vs upstream
aj template push -m "bump to A100-80GB"
```

---

## 9 · Where to go next

- **Compose** richer setups by splitting `account.*`, `storage.*`,
  `environment.*` blocks.
- **Automate** by passing `--json` to any command — one envelope per
  call with a `kind=…` discriminator.
- **Skip SSH upload** with `AJ_SHIP_SSH=0` (ships only `.ssh/.keep`).
- **Embed in Python** via the SDK — see [sdk.md](sdk.md).

---

## See also

| Doc                                       | What it covers                         |
|-------------------------------------------|----------------------------------------|
| [commands.md](commands.md)                | Full CLI reference                     |
| [configuration.md](configuration.md)      | Template syntax · merge rules · SKUs   |
| [env_vars.md](env_vars.md)                | `AJ_*` runtime contract + client flags |
| [sdk.md](sdk.md)                          | Python SDK surface                     |
| [architecture.md](architecture.md)        | Internals                              |
