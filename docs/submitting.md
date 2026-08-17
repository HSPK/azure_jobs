# Submitting jobs

`aj run` builds a local `JobSpec`; all real execution then crosses HTTP to the
daemon. There is no in-process submission fallback.

See [Submission backends](design/submission-backends.md) for registry,
artifact, and failure semantics.

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
| `-t`, `--template` | leaf template; omitted only when a configured template exists |
| `-n`, `--nodes` | nodes; otherwise `jobs[0].instance_count` is required |
| `-p`, `--gpn`, `--gpus-per-node` | GPUs per node; otherwise `target.gpus_per_node` is required |
| `--ppn`, `--processes-per-node` | launcher processes; template value, then `1` |
| `-d`, `--dry-run` | render YAML; no upload or submission |
| `--queue` | persist work in the daemon and return a ticket |
| `--amlt` | render compatibility YAML and run external `amlt` in the daemon |

`aj run` never saves template or resource arguments. CLI values apply only to
that invocation. For homogeneous jobs, each node/GPU value must come from the
current command or YAML; missing values fail before upload.

For `_extra.volcano.tasks`, YAML owns topology. Omit `-n`, `-p`, and `--ppn`;
aj derives totals from Tasks. Heterogeneous Volcano Tasks also reject
`--amlt`.

## Task name

Set the local `AJ_NAME` environment variable when submitting:

```bash
AJ_NAME=pretrain aj run -t gpu train.py
AJ_NAME=pretrain aj run --queue -t gpu train.py
```

`AJ_NAME` is a base, not the complete identifier. aj appends the
eight-character AJ ID and applies backend normalization. Without it, aj uses
the current directory and, for a direct existing-file command, the file stem.
The final value is injected into the task as runtime `AJ_NAME`.

Volcano uses that value as a DNS-1035 `generateName` stem and Kubernetes adds
a suffix. Use the returned `azure_name` for subsequent `kubectl` operations.
Do not put `AJ_NAME` in template `submit_args.env`: stable injected `AJ_*`
values override template entries. See [Environment](environment.md).

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
