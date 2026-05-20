# Tutorial

A guided walkthrough — from an empty directory to a tracked, iterating
Azure ML training job — in ten minutes.

> **Prerequisites**
> `pipx install azure_jobs` · `az login` · (optional) `kubectl` for the
> Volcano backend.

---

## 1 · Scaffold

```bash
mkdir my-project && cd my-project
aj init
```

`aj init` walks you through subscription → resource group → workspace
and writes `.azure_jobs/aj_config.json`. To start from a team's shared
templates, layer them on top:

```bash
aj pull <user>/<repo>
```

You now have:

```
.azure_jobs/
├── aj_config.json
├── record.jsonl
└── template/
    ├── account/
    ├── storage/
    ├── environment/
    └── …
```

---

## 2 · Author a template

A leaf template composes building blocks via `base` and overrides only
what it needs:

```yaml
# .azure_jobs/template/gpu.yaml
base: [account.default, storage.default, environment.aml]
config:
  target:
    name: my-cluster
  jobs:
    - name: train
      sku: "{nodes}xA100-80GB"
      command:
        - pip install -r requirements.txt
```

**Things to know**

- Dotted `base` names resolve to `.azure_jobs/<dir>/<name>.yaml`.
- `{nodes}` / `{processes}` are substituted from `-n` / `-p` at submit.
- Merge: dicts recurse, lists-of-dicts by index, scalar lists concat,
  scalars last-wins. See [configuration.md](configuration.md).

Inspect what you wrote:

```bash
aj template list
aj template show gpu          # resolved config, post-inheritance
aj template validate          # schema sweep
```

---

## 3 · Dry-run before you spend

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

## 4 · Pre-flight

Catch SKU and quota issues before submitting:

```bash
aj sku check -t gpu -n 2 -p 8
aj quota list                 # Singularity VC quota
aj quota list --aml           # AML cluster availability
```

---

## 5 · Submit

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

## 6 · Track

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

## 7 · Iterate

```bash
aj template diff              # local edits vs upstream
aj template push -m "bump to A100-80GB"
```

---

## 8 · Where to go next

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
