# Submitting jobs

`aj run` builds a local `JobSpec`; all real execution then crosses HTTP to the
daemon. There is no in-process submission fallback.

## Command form

```bash
aj run [OPTIONS] COMMAND [ARGS]...
```

Common examples:

```bash
aj run -t gpu train.py
aj run -t gpu -n 4 -p 8 --ppn 1 train.py --lr 1e-3
aj run -t gpu -d train.py
aj run -t gpu --queue train.py
aj run -t gpu --amlt train.py
```

Place all `aj run` options before `COMMAND`. Remaining arguments are
shell-quoted and assembled into the remote command while preserving argument
boundaries.

| Option | Contract |
| --- | --- |
| `-t`, `--template` | leaf template; omitted means the saved default |
| `-n`, `--nodes` | node count; default is saved value or `1` |
| `-p`, `--gpn`, `--gpus-per-node` | GPUs per node; drives SKU and runtime variables |
| `--ppn`, `--processes-per-node` | launcher processes per node; default `1` |
| `-d`, `--dry-run` | render YAML; no upload or submission |
| `--queue` | persist work in the daemon and return a ticket |
| `--amlt` | render compatibility YAML and run external `amlt` in the daemon |

The selected template, node count, and GPU count become future defaults.

## Script handling

If `COMMAND` names an existing file:

- `.py` becomes `uv run <file>`;
- `.sh` becomes `bash <file>`;
- another extension fails before submission.

The selected image or setup commands must provide `uv` when using the `.py`
shorthand. Use `aj run ... python train.py` when only Python is guaranteed.
Non-file commands are shell-quoted. Template `jobs[0].command` entries run
first, followed by the user command and arguments. `AJ_*` variables override
same-named template environment entries; see [Environment](environment.md).

## Dry run

```bash
aj run -t gpu -d -n 2 -p 8 train.py
```

Dry run resolves inheritance, SKU placeholders, backend options, environment,
and command construction, then writes an amlt-shaped YAML for inspection. It
does not prove that Azure quota, permissions, images, or Kubernetes resources
are available.

Use these pre-flight views:

```bash
aj template show gpu
aj template validate gpu
aj code stats -t gpu
aj quota list
aj sku list
```

## Native AML and Sing code flow

Native submission never registers an Azure ML Code Asset and never uploads
files individually:

1. The daemon selects files using built-ins, template ignores, and the root
   ignore file.
2. It adds generated `aj_runner.sh` and the allowed SSH payload.
3. It writes a deterministic `tar.gz` with normalized metadata and hashes the
   complete archive with SHA-256.
4. It checks and, if absent, uploads one blob to
   `workspaceblobstore/LocalUpload/<hash>/code.tar.gz`.
5. The CommandJob receives that blob as a downloaded `uri_file` input.
6. Each node verifies SHA-256. Local processes coordinate with an
   `AJ_ID`-scoped lock, so extraction and tool installation happen once per
   node.
7. Every process enters the extracted tree and runs `bash aj_runner.sh`.

Identical archive bytes reuse the existing blob. Uploads stream from disk and
reject archives above Azure Blob's single-request limit with an actionable
error.

## Native target resolution

AML resolves `target.name` to a workspace compute. Sing either uses the
explicit VC or performs daemon-side automatic selection as described in
[Templates](templates.md#sing-example-and-auto-selection).

The daemon also prepares the environment, storage mounts, identity,
distribution, resources, tags, and final CommandJob body before the Azure REST
`PUT`.

## Queue mode

```bash
aj run --queue -t gpu train.py
aj queue show <ticket>
aj queue wait <ticket>
```

The daemon journals pending work and runs submissions one at a time. Pending
entries survive the shell and daemon restart; they are restored lazily when a
later request reopens that project/workspace context. If the daemon stops
during an active submission, that queue entry is restored as failed because
the remote outcome is unknown; reconcile cloud state before retrying. A
pending ticket can be canceled with `aj queue cancel`; after an AML/Sing
submission use `aj job cancel`. Volcano is submission-only, so manage it with
Kubernetes tools.

`aj queue wait` exits `0` only for a completed queue entry, exits `1` for a
failed or canceled entry, and errors on timeout.

## `amlt` compatibility

```bash
aj init amlt
aj run --amlt -t gpu train.py
```

The client still resolves aj inheritance and overrides. The daemon strips
`_extra`, writes submission YAML under `.azure_jobs/submission/`, and runs:

```bash
amlt run <submission-yaml> <experiment> -y
```

The default daemon-side timeout is 1,800 seconds and can be changed with
`AJ_AMLT_TIMEOUT`. Export it before daemon startup, then run
`aj daemon restart`.

## Failure semantics

- Daemon startup, protocol, and Azure domain errors retain their concrete type
  and detail across the wire.
- Direct submission returns nonzero when the backend reports failure.
- Every direct attempt appends a local journal record, including failures.
- Unexpected errors show `Type: message`; start/restart the daemon with
  `AJ_DEBUG=1` for server tracebacks.
- The SDK does not automatically replay ambiguous mutations. A lost response
  may follow a successful remote mutation, so callers must reconcile before
  retrying.
