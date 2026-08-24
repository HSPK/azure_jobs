# Environment variables

There are two different contracts:

1. variables injected into submitted jobs;
2. variables controlling the local client or daemon.

## Job naming

Set `AJ_NAME` on the local `aj` command to choose the task-name base:

```bash
AJ_NAME=pretrain aj run -t gpu -n 2 -p 8 python train.py
```

aj constructs the name in this order:

1. use local `AJ_NAME` when set;
2. otherwise use the current directory, optionally followed by the command
   file stem when `COMMAND` directly names an existing file;
3. append `_<8-character-AJ-ID>`;
4. apply backend normalization.

For example, `AJ_NAME=pretrain` may become `pretrain_a1b2c3d4`. AML and Sing
use that final name. Volcano converts it to a DNS-1035 `generateName` stem;
Kubernetes appends another suffix, so the submission result's `azure_name` is
the authoritative resource name.

The submitted process receives the normalized pre-suffix value as runtime
`AJ_NAME`. A template `submit_args.env.AJ_NAME` does not name the task because
aj overwrites all stable runtime variables.

## Stable job runtime contract

Every built job receives:

| Variable | Meaning |
| --- | --- |
| `AJ_NAME` | final normalized job name injected into the task |
| `AJ_ID` | short aj submission ID used by local records |
| `AJ_TEMPLATE` | selected leaf template name |
| `AJ_SUBMIT_TIMESTAMP_UTC` | ISO 8601 build timestamp |
| `AJ_NODES` | resolved CLI/template node count |
| `AJ_GPUS_PER_NODE` | compatibility name for resolved `-p`; literal only for GPU SKUs |
| `AJ_PROCESSES` | `AJ_NODES × -p`; compatibility SKU-unit total |
| `AJ_PROCESSES_PER_NODE` | resolved CLI/template launcher count |

Example:

```bash
torchrun \
  --nnodes "$AJ_NODES" \
  --nproc-per-node "$AJ_PROCESSES_PER_NODE" \
  train.py
```

`AJ_PROCESSES` is not `--ppn`. For GPU SKUs it commonly equals total GPUs; for
CPU SKUs it is only a compatibility SKU-unit value. Use
`AJ_PROCESSES_PER_NODE` for actual launcher processes.

Template values with these names are overwritten by aj.

## Heterogeneous Volcano runtime

Heterogeneous Tasks derive topology from YAML and override scalar job values
per Pod:

| Variable | Meaning |
| --- | --- |
| `AJ_TASK_NAME` | current Volcano Task |
| `AJ_TASK_INDEX` | zero-based replica index inside that Task |
| `AJ_TASK_REPLICAS` | replicas in that Task |
| `AJ_NODE_RANK` | unique node rank across all Tasks |
| `AJ_GPU_NODES` | total replicas with GPUs |
| `AJ_TOTAL_GPUS` | total GPUs across all Tasks |
| `AJ_GPUS_PER_NODE` | current Task's GPU count |
| `AJ_PROCESSES_PER_NODE` | current Task's process count |

Rank order is `master`, then remaining Task names lexicographically.
`AJ_NODE_RANK` is the Task's rank base plus `AJ_TASK_INDEX`.

Volcano supplies `VK_TASK_INDEX`; aj derives and exports `AJ_TASK_INDEX`,
`AJ_NODE_RANK`, `NODE_RANK`, and `RANK` before the user command. These are
environment variables, not launcher arguments. Pass one explicitly when a
launcher requires it:

```bash
torchrun --node-rank "$RANK" ...
```

Existing `RANK`, `NODE_RANK`, `WORLD_SIZE`, `MASTER_ADDR`, and `MASTER_PORT`
values take precedence. By default, `WORLD_SIZE` is total Pods and
`MASTER_ADDR` points to `master-0`.

## Local client and daemon controls

| Variable | Default | Purpose |
| --- | --- | --- |
| `AJ_HOME` | `./.azure_jobs` | project state, templates, config, records, submissions |
| `AJ_CACHE_HOME` | `~/.cache/azure_jobs` | private token and response caches |
| `AJ_RUNTIME_DIR` | `$XDG_RUNTIME_DIR/aj` or a user-scoped fallback | daemon socket, spawn lock, log |
| `AJ_DEBUG` | unset | debug logging and full local/remote tracebacks |
| `AJ_OUTPUT` | `rich` | force `rich` or `json` output |
| `AJ_NAME` | derived from directory/command | local job-name base; aj appends the short ID and normalizes it |
| `AJ_SHIP_SSH` | enabled | set `0`, `false`, `no`, or `off` to stop native SSH whitelist shipping |
| `AJ_AMLT_TIMEOUT` | `1800` | seconds allowed for daemon-side `amlt run` |
| `AJ_LOG_LEVEL` | `WARNING` | daemon Python log level |

Command-line `--json` sets output mode for that process and is clearer than
`AJ_OUTPUT=json` in scripts.

`AJ_AMLT_TIMEOUT`, `AJ_SHIP_SSH`, `AJ_LOG_LEVEL`, server-side `AJ_DEBUG`, and
other daemon controls are read in the daemon process. Export them before
`aj daemon start`, or restart the daemon after changing them:

```bash
export AJ_SHIP_SSH=0
export AJ_DEBUG=1
aj daemon restart
```

## Project paths

With the default `AJ_HOME`:

```text
.azure_jobs/
├── aj_config.json
├── record.jsonl
├── submission/
├── logs/
├── scripts/
└── template/
```

The SDK sends the absolute `AJ_HOME` in `X-AJ-Root` on local HTTP requests so
one daemon can isolate multiple checkouts.

## SSH behavior

For native AML/Sing, a project-owned `.ssh/` directory takes precedence and is
archived as project code. Only when the project has no `.ssh/` may aj add this
home-directory whitelist:

```text
id_rsa
id_ed25519
id_ecdsa
config
known_hosts
```

Set `AJ_SHIP_SSH=0` to disable copying the home-directory whitelist; aj adds an
empty `.ssh/.keep` only when the project has no `.ssh/`. This setting does not
exclude a project-owned `.ssh/`. Remote HTTP, if implemented later, must never
read server-home SSH files.

## Backend-internal variables

Backends may add internal values such as `AJ_CODE_ARCHIVE_SHA256` or
`AJ_WORKDIR`. They are implementation details, not the stable user contract.

## Test-only variables

`AJ_LIVE_E2E`, `AJ_LIVE_SERVICE`, `AJ_LIVE_SING_*`,
`AJ_LIVE_WORKSPACE`, and `AJ_LIVE_TIMEOUT` only control opt-in tests. See
[Testing](testing.md).

## Stability

The eight job runtime variables above are stable across patch releases. New
`AJ_*` runtime variables may be added. Renaming or changing the meaning of a
stable variable requires a major-version compatibility decision.

Local operational variables and backend-internal variables may evolve as the
daemon implementation changes.
