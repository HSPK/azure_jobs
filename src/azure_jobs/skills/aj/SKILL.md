---
name: aj
description: >-
  Authoritative reference for `aj` (azure_jobs) — a CLI/SDK for submitting and
  managing Azure ML jobs via pure REST. Use when the user needs to submit a
  training job, list/inspect/cancel jobs, check VC quota or SKU availability,
  pick a workspace, write or validate YAML templates, browse experiment stats,
  or open the TUI dashboard. Trigger on phrases like "submit this", "run on
  the cluster", "check my job", "what GPUs are free", "fix this template",
  even when the user does not mention `aj` by name.
---

# aj CLI Reference

`aj` submits Azure ML jobs through composable YAML templates. It talks to
Azure over pure REST (no `azure-ai-ml`, no `amlt` runtime), with a built-in
SDK, TUI dashboard, and three backends (native AML/Singularity, Volcano, and
delegation to `amlt`).

## ⚠️ Unsafe commands

These are destructive. **Do not execute without explicit user confirmation.**

- `aj job cancel <id>`
- `aj template push -m "..."`           (commits + pushes upstream)
- `aj template pull <repo>`             (overwrites local templates)
- `aj run ...` (any submission)         (creates real cloud jobs, costs money)
- `aj ws set` interactively             (changes the active workspace)

For read-only inspection (`list / show / stats / status / logs / quota /
sku / code stats / template show / template validate`), no confirmation
needed.

## How to use this skill

1. **`aj init` is only needed before `aj run`** (to register the active
   workspace + at least one template). Read-only commands (`job list /
   show / logs / stats`, `quota`, `sku`, `ws list`, `env list`,
   `ds list`, `exp list`, `template list`, …) work without it. If the
   user wants to submit and `.azure_jobs/` is missing, stop and ask them
   to run `aj init` themselves — it is interactive (picks a workspace).
2. Identify intent (submit / inspect / quota / template / dashboard).
3. If a template name is needed, run `aj template list` first.
4. Use `--json` on any command for machine-readable output:
   ```bash
   aj job list --json | jq '.rows[] | select(.status=="Running")'
   ```
5. Prefer the SDK (`from azure_jobs import ...`) when scripting more than
   2–3 chained commands.

## Example prompts

> "Submit a single-GPU smoke-test AML job on `gpu-a100` that prints all
> `AJ_*` env vars."

```bash
cat > smoke.sh <<'EOF'
#!/bin/bash
env | grep ^AJ_ | sort
EOF
aj run -t gpu-a100 -n 1 -p 1 smoke.sh
```

> "Show me running jobs in this workspace."

```bash
aj job list -s Running
```

## Setup

```bash
pipx install azure_jobs                 # end users
aj init                                 # scaffold .azure_jobs/, pick workspace
aj init amlt                            # additionally configure amlt
aj auth status                          # verify credentials
```

`AJ_HOME` defaults to `./.azure_jobs/`. Workspace config lives at
`.azure_jobs/aj_config.json`. Submission history is appended to
`.azure_jobs/record.jsonl`.

## Submitting jobs

```bash
aj run -t <template> [-n <nodes>] [-p <gpus_per_node>] [--ppn <procs>] \
       <script.py|script.sh> [-- args...]
```

- `.py` scripts run under `uv run`; `.sh` scripts run under `bash`.
- Override the backend with `--amlt` (delegate to `amlt run`) or
  `--volcano` (submit to a K8s Volcano cluster).
- `-n` × `-p` populates `AJ_NODES`, `AJ_GPUS_PER_NODE`, `AJ_PROCESSES`.
- Dry-run: `aj run -t <t> --dry-run <cmd>` writes the rendered YAML to
  `.azure_jobs/dryrun/` and does **not** submit.

Stable job-side env vars (do not rename without a major bump):
`AJ_NAME`, `AJ_ID`, `AJ_TEMPLATE`, `AJ_NODES`, `AJ_GPUS_PER_NODE`,
`AJ_PROCESSES`, `AJ_PROCESSES_PER_NODE`, `AJ_SUBMIT_TIMESTAMP_UTC`.

## Job management

```bash
aj job list [-s Running|Completed|Failed] [-e <exp>] [--ws <name>]
aj job show <id>                        # detail panel
aj job logs <id>                        # fetch + render logs
aj job cancel <id>                      # UNSAFE
aj job stats                            # GPU-hours, success rate, grouped
aj list                                 # local record.jsonl
aj dash                                 # interactive TUI
aj exp list                             # experiments aggregated from jobs
```

`<id>` accepts the full Azure name or the 8-char `AJ_ID` short prefix.

## Workspace · quota · SKU · resources

```bash
aj ws list                              # workspaces in subscription
aj ws set                               # interactive picker (UNSAFE: switches active)
aj quota list                           # Singularity VC quota
aj quota list --aml                     # AML cluster availability
aj sku list                             # SKUs by VC
aj sku check -t <template>              # pre-flight: SKU/quota/compute,
                                        # auto-toggles `-NvLink`
aj env list / aj env show <name>        # registered environments
aj ds list  / aj ds show  <name>        # datastores
aj image list                           # Singularity curated images
aj uai list                             # user-assigned managed identities
aj sa list                              # storage accounts
```

## Templates

Templates are YAML files under `.azure_jobs/template/` with a `base:` chain
that composes `account · storage · environment` building blocks.

```bash
aj template list
aj template show <name>                 # resolved (post-inheritance) config
aj template validate                    # check all templates
aj template diff                        # local edits vs upstream
aj template init                        # wizard to scaffold leaves
aj template pull <repo>                 # UNSAFE (overwrites local)
aj template push -m "msg"               # UNSAFE (commits + pushes)
```

Merge rules (`core/template/engine.py:merge_confs`): dicts recurse; lists
where any element is a dict merge **by index**; lists of pure scalars
**concatenate**; scalars take the last value.

SKU formats:

```yaml
sku: "{nodes}xA100-80GB"               # template with {nodes}/{processes}
sku:                                   # or a range dict
  "1":   "1xA100-80GB"
  "2-4": "{nodes}xA100-80GB"
  "8+":  "8xA100-80GB-NvLink"
```

## Code upload

`aj` walks the cwd, content-hashes it (sha256), and uploads only if the
hash is new. `.codeignore` / `.amltignore` excludes paths; always-excluded:
`__pycache__`, `.git`, `.venv`, `node_modules`, all of `.azure_jobs/`
except `scripts/`.

```bash
aj code stats -t <template>             # file count, total size, hash
aj code stats -t <template> --list-all
aj code stats -t <template> -n 20       # top-N largest
```

`~/.ssh` whitelist (`id_rsa`, `id_ed25519`, `id_ecdsa`, `config`,
`known_hosts`) ships only when `AJ_SHIP_SSH=1`. Otherwise only
`.ssh/.keep` is uploaded.

## Tool config

```bash
aj config show
aj config timezone Asia/Shanghai
aj config experiment <name>
```

## SDK

```python
from azure_jobs import (
    Template, build_submit_request,
    submit_via_native, submit_via_amlt, submit_via_volcano,
    materialise_submission, get_workspace_config,
)

template = Template.from_conf_path(".azure_jobs/template/gpu.yaml")
request = build_submit_request(
    template, name="my-job", sid="abc12345", sku="2xA100-80GB",
    user_command="train.py", user_args=(),
    workspace=get_workspace_config(),
    template_name="gpu", nodes=2, processes=8, processes_per_node=1,
)
result = submit_via_native(request)
print(result.status, result.portal_url)
```

All backends share `(request, *, on_event=None) -> SubmitResult`.
Only `submit_via_amlt` needs an on-disk YAML — call
`materialise_submission(request)` first.

## Output modes

Every command supports `--json` (single envelope with a `kind`
discriminator). Set globally with `AJ_OUTPUT=json` or
`aj --json <cmd>`. Default is Rich tables.

## Pointers (when you need to read source)

- Template engine: `src/azure_jobs/core/template/engine.py`
- Submission pipeline: `src/azure_jobs/core/submit/{build,render,materialise,dispatch}.py`
- Native backend: `src/azure_jobs/core/submit/native/orchestrate.py`
- REST clients: `src/azure_jobs/core/az_client/{arm,ml}/`
- TUI: `src/azure_jobs/tui/`
- Docs site: `docs/` (built with `mkdocs build`)
- Tests: `uv run pytest -q` (434+ tests, all REST mocked)
