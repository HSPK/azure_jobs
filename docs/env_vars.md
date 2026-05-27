# `AJ_*` environment variables

`aj run` injects the following environment variables into every submitted
job's container. These are part of the **stable** runtime contract — user
training scripts may rely on them for distributed setup, run identification,
and template tracking.

| Variable | Type | Source | Purpose |
|---|---|---|---|
| `AJ_NAME` | `str` | `request.name` | Display name of the job (e.g. `my-project_abc12345`). |
| `AJ_ID` | `str` (8 hex) | `request.sid` | Short aj submission ID — also used in the short-ID lookups by `aj job status <id>` / `aj job logs <id>`. |
| `AJ_TEMPLATE` | `str` | `request.template_name` | Name of the template that produced this submission (e.g. `gpu-a100`). |
| `AJ_SUBMIT_TIMESTAMP_UTC` | ISO 8601 | submit time | UTC timestamp at which `build_submit_request` was called. |
| `AJ_NODES` | `int` (str-coerced) | CLI `-n` | Number of nodes. |
| `AJ_PROCESSES` | `int` (str-coerced) | `nodes * gpus_per_node` | Total processes (≈ total GPUs) across the job. Convenience for `torchrun --nnodes $AJ_NODES --nproc-per-node $AJ_GPUS_PER_NODE`. |
| `AJ_GPUS_PER_NODE` | `int` (str-coerced) | CLI `-p` / `--gpn` | GPUs per node — drives SKU resolution. |
| `AJ_PROCESSES_PER_NODE` | `int` (str-coerced) | CLI `--ppn` | Launcher processes per node (e.g. value to pass to `torchrun --nproc-per-node`). Independent of GPU count. |

## Stability

The names and meanings above are **stable** across patch releases. New
variables may be added (always prefixed `AJ_`); existing variables will
not be renamed or removed without a major version bump.

## Source of truth

These are populated in `job/build.py:build_submit_request`. Tests
in `tests/test_submit.py` lock the contract.

## Example use in a training script

```python
import os

run_id = os.environ["AJ_ID"]
nodes = int(os.environ["AJ_NODES"])
gpus_per_node = int(os.environ["AJ_GPUS_PER_NODE"])
ckpt_dir = f"/mnt/ckpt/{os.environ['AJ_TEMPLATE']}/{run_id}"
```

```bash
# torchrun via the aj-provided env
torchrun \
    --nnodes $AJ_NODES \
    --nproc-per-node $AJ_GPUS_PER_NODE \
    train.py
```

## Opt-out / opt-in env vars

Client-side flags, not injected into the job container:

| Variable | Default | Purpose |
|---|---|---|
| `AJ_SHIP_SSH` | `1` | Set to `0`/`false`/`no`/`off` to skip shipping `~/.ssh` whitelist (`id_rsa`, `id_ed25519`, `id_ecdsa`, `config`, `known_hosts`). Only `.ssh/.keep` is uploaded instead. A vendored `<code_dir>/.ssh` always wins. |
| `AJ_DEBUG` | unset | Truthy enables stderr `DEBUG` logging. |

## Note on `$$`

Azure ML's runtime pre-processes shell commands and replaces `$$` with `$`
before bash execution. The `aj` CLI escapes user-provided `$` to `$$` when
rendering the amlt YAML — this is why YAMLs on disk may look like `$$HOME`
even though the job container sees `$HOME`.
