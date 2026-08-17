---
name: azure-jobs
description: >-
  Use when a task involves `aj` / Azure Jobs: installation; template creation,
  inheritance, validation, pull, or publication; submission to Azure ML (AML),
  Singularity VC, or Volcano/Kubernetes; jobs, logs, queues, watches, quota,
  SKUs, and failure analysis; or related Azure resource inspection.
---

# Azure Jobs

Operate Azure Jobs conservatively from a local checkout. Prefer implemented
`aj` commands and structured output over hand-built API requests.

## Operating contract

- `aj` provides a CLI, TUI (`aj dash`), and Python SDK.
- All clients use the local daemon over a private Unix socket.
- Only the daemon authenticates to and executes against Azure.
- There is no supported in-process fallback or remote HTTP/TCP endpoint.
- Prefer global `aj --json ...` for agent-readable discovery and results. The
  flag must precede the command; do not parse Rich tables when JSON exists.
- Reads may be retried when appropriate. Never automatically replay an
  ambiguous mutation.

## Safety gates

Read-only discovery may proceed: inspect tool/auth/config/templates, cloud
resources, jobs/logs, quota/SKUs, diffs, dry runs, code statistics, and
Kubernetes resources.

Require confirmation immediately before these actions unless the user already
explicitly requested that exact action:

- direct chargeable submission or queueing a submission;
- job/queue cancellation, job deletion, or `kubectl delete`;
- `aj k8s delete` / `aj k delete`;
- `aj template push`;
- `gh repo create --public` or another public-repository publication.

Also confirm before:

- installing or upgrading `aj` when the user did not request installation;
- `aj k8s install`, because it may use sudo, install host tools, and add an
  apt repository;
- any template pull into a non-empty shareable tree, because normal pull
  overwrites matching files and `--force` also removes stale files;
- destructive recovery such as `aj daemon stop --force`.

Never expose or persist in public templates:

- passwords, tokens, SAS values, storage keys, or credentialed URLs;
- UAI credentials or real tenant-specific identity values;
- daemon queue/watch journals, local records, logs, or submissions;
- project/home `.ssh/`, private keys, certificates, or local SSH material.

Use placeholders in public examples and review staged content before publish.

Template show/diff, dry-run, submit, and logs may contain environment or
command secrets. Run those commands through the bundled
[`run-aj-json.py`](scripts/run-aj-json.py), which captures both streams
privately and emits only a bounded, redacted allowlist. Use
[`summarize-aj-json.py`](scripts/summarize-aj-json.py) only for an already
private JSON file. Resolve scripts relative to this skill, not the user's
working directory.

For native AML/Sing, default agent behavior is to disable home SSH shipping.
If the daemon is not running, start it with `AJ_SHIP_SSH=0`. If it is already
running, check for active submissions and restart with that setting only when
safe. A project-owned `.ssh/` is still archived; inspect/remove it explicitly.
Enable home SSH shipping only after explicit confirmation.

## Backend boundary

| Backend | Service | Submit | Manage afterward |
| --- | --- | --- | --- |
| Native AML | `aml` | `aj run` | `aj job`, `aj exp`, `aj watch`, `aj dash` |
| Native Singularity | `sing` | `aj run` | Same AML workspace job APIs |
| Volcano | `volcano` | `aj run` | Kubernetes/Volcano tools only |
| amlt compatibility | `--amlt` | Optional | Not this skill's focus |

`target.name` behavior:

- AML: required AML compute target.
- Sing: optional; omit/empty for daemon VC auto-selection.
- Volcano: not a compute name; use `namespace`, `queue`, `context`, and
  resource fields instead.

Do not use `aj job` APIs to manage Volcano.

## Decision workflow

### 1. Inspect local context

```bash
pwd
git status --short
command -v aj
aj --version
aj daemon status
aj --json auth status
aj --json config show
aj --json config experiment
aj --json ws show
aj --json template list
```

If absent and installation was requested or confirmed:

```bash
uv tool install azure-jobs
command -v aj || uv tool update-shell
aj --version
```

Python 3.10+ is required. If authentication is missing, have the user complete
`az login`; never request credentials. Then verify the daemon's credential
with `aj --json auth status`.

Before JSON submit/queue, ensure an experiment is configured:

```bash
aj --json config experiment EXPERIMENT
```

Choose the value with the user when it is not already implied by the task.

### 2. Choose the backend

- AML when the user names an AML workspace compute.
- Sing for native VC scheduling; prefer auto-selection unless a VC is required.
- Volcano only with a working context, CRD, queue, namespace, and upload path.
- For Podman/Docker inside Volcano, use the nested-runtime section in the
  Volcano reference; never add `SYS_ADMIN` without a stated requirement.

Read [templates](references/templates.md) before authoring YAML and
[Volcano](references/volcano.md) before Kubernetes submission.
For an existing failed/Pending Kubernetes task, read
[Kubernetes analysis](references/kubernetes-analysis.md) before running
diagnostics.

### 3. Establish the template

For an empty shareable tree:

```bash
aj --json template pull OWNER/REPOSITORY
```

If `.azure_jobs/template`, `account`, `environment`, `storage`, or `scripts`
already contain files, review Git/local state and obtain confirmation before
pulling. To author locally:

```bash
mkdir -p .azure_jobs/template
aj template init TEMPLATE_NAME
```

`template init` is interactive and rejects JSON mode. Before public repository
work, read [public repository](references/public-repository.md).

```bash
aj --json template validate TEMPLATE_NAME
python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
  aj --json template show TEMPLATE_NAME
```

### 4. Preflight

For native AML/Sing, ensure SSH behavior is deliberate before code inspection:

```bash
AJ_SHIP_SSH=0 aj daemon start
# If already running and idle: AJ_SHIP_SSH=0 aj daemon restart
find . -type d -name .ssh -prune -print
```

Do not restart a daemon with active work without agreement.

```bash
aj --json code stats -t TEMPLATE_NAME
aj --json quota list --aml --all
aj --json quota list --sing --all --full
aj --json sku list --all
aj --json image list
```

Run only backend-relevant discovery. Volcano uses the preflight in its
reference. Render without upload/submission. Raw dry-run JSON contains the
full rendered config, so never print it directly:

For homogeneous jobs, nodes and GPUs must come from this command or explicit
`jobs[0].instance_count` and `target.gpus_per_node` YAML. `aj run` has no
resource memory; never infer a prior value or silently assume `1x1`.

```bash
AJ_NAME=TRAIN_NAME python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
  aj --json run -t TEMPLATE_NAME -d -n 1 -p 1 --ppn 1 \
  python train.py
```

For `_extra.volcano.tasks`, omit `-n`, `-p`, and `--ppn` in dry-run,
submission, and queue commands. Never use `--amlt`; YAML defines the full
topology.

Set `AJ_NAME` on the local `aj` process when the user wants a specific task
name. It is the base name, not the complete remote identifier: aj appends
`_<8-character-AJ-ID>`, then applies backend normalization. Without
`AJ_NAME`, the base is the current directory, plus the command file stem when
`COMMAND` directly names an existing file. Do not set task naming through
template `submit_args.env.AJ_NAME`; injected runtime variables override it.

The remote job receives the normalized name as `AJ_NAME`, plus `AJ_ID`,
`AJ_TEMPLATE`, `AJ_SUBMIT_TIMESTAMP_UTC`, `AJ_NODES`,
`AJ_GPUS_PER_NODE`, `AJ_PROCESSES`, and `AJ_PROCESSES_PER_NODE`. Read
[job operations](references/job-operations.md#job-name-and-runtime-environment)
before submission.

Heterogeneous Volcano Pods additionally receive Task name/index/replica count,
global node rank, GPU-node count, and total GPUs. `AJ_TASK_INDEX` is local to
one Task; `AJ_NODE_RANK` is global.

All `aj run` flags must precede `COMMAND`.

Do not invent or transfer flags between command groups. In particular,
`quota list` owns `--aml`, `--sing`, and `--full`; `sku list` supports
`--all` but not those quota flags. If uncertain, run
`aj <group> <command> --help`.

### 5. Summarize before mutation

State the requested name base and resolved job name, backend/target, template,
experiment, command, nodes, GPUs per node, processes per node, image, storage,
identity, code size/ignores, expected quota/queue behavior, and
direct-versus-queued mode. Obtain confirmation if submission or queueing was
not already explicitly requested.

### 6. Submit and capture identity

Direct:

```bash
AJ_NAME=TRAIN_NAME python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
  aj --json run -t TEMPLATE_NAME -n 1 -p 1 --ppn 1 \
  python train.py
```

Queued:

```bash
AJ_SUMMARY="$(
  AJ_NAME=TRAIN_NAME python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
    aj --json run --queue -t TEMPLATE_NAME -n 1 -p 1 --ppn 1 \
    python train.py
)"
printf '%s\n' "$AJ_SUMMARY"
TICKET="$(
  printf '%s' "$AJ_SUMMARY" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["ticket"])'
)"
aj queue show "$TICKET"
```

Capture the eight-character `sid`/AJ ID, cloud job name, portal URL, resolved
workspace when available, or ticket. For AML/Sing, record the template/compute
workspace explicitly; management commands must use the same `--ws`. For
Volcano, capture returned `azure_name`, which is the generated Kubernetes
resource name; the normalized stem is not the full name.

### 7. Poll and read logs

AML/Sing:

```bash
aj --json job status JOB_OR_AJ_ID --ws WORKSPACE
python3 <SKILL_DIR>/scripts/run-aj-json.py --log-tail 200 -- \
  aj --json job logs JOB_OR_AJ_ID --ws WORKSPACE
```

Always capture the resolved workspace at submission. The active workspace may
differ from the template/compute workspace.

Queue:

```bash
aj queue show TICKET
aj queue wait TICKET --timeout 3600
```

Volcano uses narrow `kubectl get` fields and sanitized logs. Do not use raw
Job/pod `describe` or full YAML/JSON; Blob command arguments contain a read
SAS, and PVC jobs can still contain sensitive values.

For diagnosis, separate observed status/log evidence from hypotheses. Read
[FAQ and quota](references/faq.md) for common answers and
[Kubernetes analysis](references/kubernetes-analysis.md) for a layered K8s
workflow.

Prefer `aj k` for client installation, login, status, queue, Job, pod, log,
event, and exact delete operations. `aj k install` installs client tools once;
`aj k login` configures access to an existing cluster without rerunning apt.
Neither deploys Kubernetes control-plane or worker nodes. Read the Volcano
reference first.

### 8. Cleanup only when requested

Cancellation/deletion is never implicit. Re-state the exact job, ticket, or
Kubernetes resource and obtain confirmation unless already requested.

## Command and archive behavior

- Existing `.py` files become `uv run FILE`; use `python FILE` when only Python
  is guaranteed. Existing `.sh` files become `bash FILE`.
- Other commands/arguments are shell-quoted while preserving boundaries.
- Native AML/Sing create one deterministic content-addressed archive, upload or
  reuse one Blob, verify SHA-256, extract once, and run `aj_runner.sh`.
- Review ignores and archive size before submit; see
  [templates](references/templates.md) and
  [job operations](references/job-operations.md).

## Ambiguous mutation recovery

If a submit, queue, cancel, delete, or connection response is lost, assume the
mutation may have succeeded. Before retry:

1. inspect `aj --json list` for local attempt/AJ ID;
2. inspect `aj --json job list --ws WORKSPACE` for AML/Sing;
3. inspect `aj queue list` for a ticket;
4. inspect Volcano by namespace, creation time, and `app` label;
5. compare timestamps/names/IDs and retry only if no mutation occurred.

Never blind retry a chargeable or destructive command.

## Reference routing

- [Templates](references/templates.md): YAML, inheritance, SKU, storage,
  ignores, setup, `_extra`, validation, and archive inputs.
- [Public repository](references/public-repository.md): safe create, pull,
  edit, diff, and publish workflow.
- [Job operations](references/job-operations.md): run, queue, jobs, logs,
  stats, experiments, watches, dashboard, and AJ IDs.
- [Volcano](references/volcano.md): Kubernetes preflight, upload strategies,
  generated names, pod logs, storage, and deletion.
- [Resources](references/resources.md): auth, daemon, workspace, quota, SKU,
  image, datastore, environment, identity, storage, config, and code.
- [Troubleshooting](references/troubleshooting.md): daemon, protocol, template,
  quota, archive, log, ambiguous mutation, or Kubernetes failures.
- [FAQ and quota](references/faq.md): quota decisions and concise replies to
  common user questions.
- [Kubernetes analysis](references/kubernetes-analysis.md): read-only Volcano
  Job, pod, event, scheduling, storage, and log analysis.
