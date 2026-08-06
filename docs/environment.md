# Environment variables

There are two different contracts:

1. variables injected into submitted jobs;
2. variables controlling the local client or daemon.

## Stable job runtime contract

Every built job receives:

| Variable | Meaning |
| --- | --- |
| `AJ_NAME` | final display/job name after backend normalization |
| `AJ_ID` | short aj submission ID used by local records |
| `AJ_TEMPLATE` | selected leaf template name |
| `AJ_SUBMIT_TIMESTAMP_UTC` | ISO 8601 build timestamp |
| `AJ_NODES` | requested node count |
| `AJ_GPUS_PER_NODE` | CLI `-p` value |
| `AJ_PROCESSES` | `AJ_NODES × AJ_GPUS_PER_NODE`; compatibility total |
| `AJ_PROCESSES_PER_NODE` | CLI `--ppn` value |

Example:

```bash
torchrun \
  --nnodes "$AJ_NODES" \
  --nproc-per-node "$AJ_PROCESSES_PER_NODE" \
  train.py
```

`AJ_PROCESSES` is not `--ppn`. It retains the historical total-GPU meaning.
The same history explains why SKU `{processes}` means GPUs per node.

Template values with these names are overwritten by aj.

## Local client and daemon controls

| Variable | Default | Purpose |
| --- | --- | --- |
| `AJ_HOME` | `./.azure_jobs` | project state, templates, config, records, submissions |
| `AJ_CACHE_HOME` | `~/.cache/azure_jobs` | private token and response caches |
| `AJ_RUNTIME_DIR` | `$XDG_RUNTIME_DIR/aj` or a user-scoped fallback | daemon socket, spawn lock, log |
| `AJ_DEBUG` | unset | debug logging and full local/remote tracebacks |
| `AJ_OUTPUT` | `rich` | force `rich` or `json` output |
| `AJ_NAME` | derived from directory/command | override the job-name base; aj still adds the short ID |
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
