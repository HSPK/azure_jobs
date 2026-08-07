# Job operations

Use this for submission, AML/Sing management, logs, queues, statistics,
experiments, watches, and dashboard work.

## Run flags
```bash
aj run [OPTIONS] COMMAND [ARGS]...
```

All aj options precede `COMMAND`.
JSON submit/queue is noninteractive and requires a configured experiment:

```bash
aj --json config experiment EXPERIMENT
```

| Flag | Meaning |
| --- | --- |
| `-t`, `--template` | leaf template or saved default |
| `-n`, `--nodes` | node count; saved default or `1` |
| `-p`, `--gpn`, `--gpus-per-node` | GPUs per node |
| `--ppn`, `--processes-per-node` | launcher processes per node; default `1` |
| `-d`, `--dry-run` | render only |
| `--queue` | persist work in daemon queue |
| `--amlt` | compatibility path through daemon-side amlt |

## Job name and runtime environment

Set the task name base on the local `aj` process:

```bash
AJ_NAME=pretrain aj run -t TEMPLATE_NAME -n 1 -p 1 \
  python train.py
```

Naming is deterministic:

1. `AJ_NAME` is the base when it is set.
2. Otherwise the base is the current directory name; when `COMMAND` directly
   names an existing file, aj also appends that file's stem.
3. aj appends `_<8-character-AJ-ID>`.
4. The backend normalizes the result. Volcano converts it to a DNS-1035 stem
   and Kubernetes appends a generated resource suffix; the returned
   `azure_name` is authoritative for Kubernetes operations.

Example: `AJ_NAME=pretrain` may resolve to `pretrain_a1b2c3d4`. The job sees
that normalized value as runtime `AJ_NAME`; it does not see only `pretrain`.
The wrapper inherits the same local variable:

```bash
AJ_NAME=pretrain python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
  aj --json run -t TEMPLATE_NAME -d -n 1 -p 1 --ppn 1 \
  python train.py
```

Every backend receives this stable runtime contract:

| Variable | Meaning |
| --- | --- |
| `AJ_NAME` | final normalized job name |
| `AJ_ID` | eight-character AJ submission ID |
| `AJ_TEMPLATE` | selected leaf template |
| `AJ_SUBMIT_TIMESTAMP_UTC` | ISO 8601 build timestamp |
| `AJ_NODES` | requested nodes |
| `AJ_GPUS_PER_NODE` | CLI `-p` value |
| `AJ_PROCESSES` | `AJ_NODES × AJ_GPUS_PER_NODE` compatibility total |
| `AJ_PROCESSES_PER_NODE` | CLI `--ppn` value |

These injected values override same-named `jobs[0].submit_args.env` entries.
Set the task name with the local `AJ_NAME=... aj run ...` prefix, not template
environment YAML.

Dry run:

```bash
python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
  aj --json run -t TEMPLATE_NAME -d -n 2 -p 8 --ppn 1 \
  python train.py --epochs 5
```

The wrapper keeps raw stdout/stderr private and emits a redacted allowlist.
Raw dry-run JSON embeds rendered environment and command values; never print
or paste it directly.

After the chargeable-action gate, submit directly or queue:

```bash
python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
  aj --json run -t TEMPLATE_NAME -n 2 -p 8 --ppn 1 \
  python train.py --epochs 5
```

Queue with the same private-output pattern:

```bash
AJ_SUMMARY="$(
  python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
    aj --json run --queue -t TEMPLATE_NAME -n 2 -p 8 --ppn 1 \
    python train.py --epochs 5
)"
printf '%s\n' "$AJ_SUMMARY"
TICKET="$(
  printf '%s' "$AJ_SUMMARY" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["ticket"])'
)"
aj queue show "$TICKET"
```

Template, nodes, and GPU count become local defaults.

## Command behavior
Arguments are shell-quoted while preserving boundaries; template
`jobs[0].command` entries run first.

- Existing `.py` becomes `uv run FILE`; the image must contain `uv`.
- Existing `.sh` becomes `bash FILE`.
- Other file extensions are rejected.
- Use `python train.py` when only Python is guaranteed.
- Use `bash -lc '...'` only for intentional shell composition.

## Archive preflight
Native AML/Sing select the working tree with built-ins, template ignores, and
the root ignore file; create one deterministic archive; upload/reuse one
content-addressed Blob; verify SHA-256; extract once; run `aj_runner.sh`.

`code stats` covers project-selected files, but not the optional home SSH
whitelist injected later by the daemon. Default agent behavior:

```bash
aj daemon status
AJ_SHIP_SSH=0 aj daemon start
# If a daemon is already running and has no active submissions:
AJ_SHIP_SSH=0 aj daemon restart
find . -type d -name .ssh -prune -print
```

Do not restart across active work. `AJ_SHIP_SSH=0` disables home-directory
copying; project `.ssh/` still ships unless removed or ignored. Enable home SSH
shipping only after explicit confirmation.

```bash
aj --json code stats -t TEMPLATE_NAME
aj --json template validate TEMPLATE_NAME
```

See [templates](templates.md) for ignore/setup rules.

## IDs and local records
Every attempt gets an eight-character AJ ID (`sid`). AML/Sing commands accept
that ID or the full Azure job name.

```bash
aj --json list
aj --json list -n 50
aj --json list -t TEMPLATE_NAME
aj --json list -s failed
```

Local records help reconciliation but are not authoritative cloud state.

## AML/Sing jobs
Record the resolved workspace from the template/submission. Pass it explicitly;
the active workspace may be different.

```bash
aj --json job list --ws WORKSPACE
aj --json job list -n 100 -s Running -e EXPERIMENT --ws WORKSPACE
aj --json job list -T Command --tag PURPOSE --ws WORKSPACE
aj --json job show JOB_OR_AJ_ID --ws WORKSPACE
aj --json job show JOB_NAME --ws WORKSPACE
aj --json job status JOB_OR_AJ_ID --ws WORKSPACE
```

`-s`/`-e` filter status/experiment; `-T`/`--tag` are server-side filters;
`-a` includes archived jobs. Volcano is not managed by these APIs.

## Logs

Queued, not-started, provisioning, or preparing jobs may have no logs. Poll
with delay/deadline:

```bash
aj --json job status JOB_OR_AJ_ID --ws WORKSPACE
sleep 15
```

Capture logs privately and print only a bounded redacted tail:

```bash
python3 <SKILL_DIR>/scripts/run-aj-json.py --log-tail 200 -- \
  aj --json job logs JOB_OR_AJ_ID --ws WORKSPACE
```

A failed log read is safe to retry. A lost submit response is ambiguous and
must be reconciled. Dashboard/SDK additionally support file selection and
HTTP Range reads.

## Cancellation/deletion

After confirmation:

```bash
aj --json job cancel JOB_OR_AJ_ID --ws WORKSPACE
```

CLI exposes cancellation, not permanent deletion. TUI can delete a terminal
job after confirmation; active jobs must first be canceled. SDK also exposes
AML/Sing deletion. Never infer delete permission from inspect/cancel intent.

## Statistics and experiments

```bash
aj --json job stats
aj --json job stats --days 30 --all
aj --json job stats -n 500 --ws WORKSPACE
aj --json exp list
aj --json exp list --days 30 --all
aj --json exp show EXPERIMENT -n 100 --ws WORKSPACE
aj --json config experiment
aj --json config experiment EXPERIMENT
```

Cross-workspace results may include partial failures.

## Queue lifecycle

```bash
aj queue list
aj queue show TICKET
aj queue wait TICKET --timeout 3600
```

Pending work persists per project/workspace. If restart interrupts active
submission, its remote outcome is unknown. Only pending entries can be
canceled; after confirmation:

```bash
aj queue cancel TICKET
```

Once AML/Sing has submitted, use `aj job cancel`; Volcano uses Kubernetes.

## Watches

```bash
aj watch add JOB_NAME
aj watch list
aj watch listen --no-desktop --timeout 600
aj watch remove JOB_NAME
```

The daemon polls after the shell exits. Do not remove a watch unless requested.

## Dashboard

```bash
aj dash
aj dash -n 500 --page-size 40
aj dash --mouse
```

TUI uses the same daemon and confirms cancel/delete. Prefer CLI JSON for
automation.

## Ambiguous response

After a lost submit/queue response:

```bash
aj --json list -n 20
aj --json job list -n 100 -e EXPERIMENT --ws WORKSPACE
aj queue list
```

Match AJ ID, generated name, experiment, template, and timestamp. Retry only
after establishing that no cloud job or queue entry exists. Never blind retry.
