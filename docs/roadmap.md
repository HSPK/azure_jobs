# Development Roadmap

This document outlines the next phase of development for Azure Jobs. It covers CLI restructuring, job lifecycle management, template improvements, and quality-of-life features.

---

## 1. Optimize Job Submission — `aj run`

### 1.1 Pre-submission Validation

Before calling `amlt`, verify the submission will likely succeed.

- **Check `amlt` is installed** — run `amlt --version` at CLI startup; show a clear install instruction if missing.
- **Validate SKU names** — maintain a known-SKU list (or query Azure) and warn on unrecognized SKUs during dry-run.
- **Validate script exists remotely** — if the command references a file, confirm it's tracked by git (it will be uploaded by amlt).

### 1.2 Interactive Submission Preview

Enhance `--dry-run` to show a human-readable summary instead of raw YAML:

```
╭─ Submission Preview ──────────────────────╮
│ Job name : project_train_a1b2c3d4         │
│ Template : gpu                            │
│ SKU      : Standard_NC4s_v3               │
│ Nodes    : 4                              │
│ Procs    : 8 (2 × 4)                     │
│ Command  : uv run train.py --lr 0.01      │
│ File     : .azure_jobs/submission/a1b2.yaml│
╰───────────────────────────────────────────╯
```

This could also be shown on every non-dry-run submission before confirming (unless `-y`).

### 1.3 Resubmit Support

Add `aj run --resubmit <job_id>` to re-run a previous submission:

- Look up the original SubmissionRecord from `record.jsonl`.
- Reload the submission YAML from `AJ_SUBMISSION_HOME/{id}.yaml`.
- Generate a new `sid` and resubmit with the same config.
- Useful for retrying failed or preempted jobs.

### 1.4 Job Tags and Metadata

Allow attaching key-value tags at submission time:

```bash
aj run -t gpu --tag experiment=ablation --tag seed=42 python train.py
```

- Store tags in the `SubmissionRecord`.
- Enable filtering by tag in `aj jobs list` (see §2).
- Pass tags as environment variables (`AJ_TAG_EXPERIMENT=ablation`) into the job.

### 1.5 Multi-job Submission

Currently only `conf["jobs"][0]` is used. Support submitting all jobs in a template:

- Iterate over every entry in `jobs` list.
- Apply SKU resolution and command building to each.
- Submit as a single amlt experiment or as separate submissions (configurable).

### 1.6 Config Overrides from CLI

Allow overriding arbitrary config values without editing templates:

```bash
aj run -t gpu --set jobs.0.sku=Standard_NC8s_v3 --set _extra.nodes=8 python train.py
```

Parse dot-separated keys and apply them after the merge step, before submission.

---

## 2. Job Lifecycle Management

The biggest gap today: after `aj run`, users must leave the tool entirely. This section adds full lifecycle commands.

### 2.1 `aj jobs list`

List submitted jobs with their current status.

```bash
aj jobs list                    # all jobs from record.jsonl
aj jobs list --status running   # filter by status
aj jobs list --template gpu     # filter by template
aj jobs list --tag experiment=ablation
aj jobs list --last 10          # most recent N
aj jobs list --since 2026-03-01 # since date
```

**Implementation:**

1. Read `record.jsonl` and display a table:

```
ID        Template  Nodes  Status     Submitted            Command
────────  ────────  ─────  ─────────  ───────────────────  ──────────────
a1b2c3d4  gpu       4      Running    2026-03-31 10:30     uv run train.py
x9y8z7w6  cpu       1      Completed  2026-03-30 14:15     bash run.sh
f3e2d1c0  gpu       8      Failed     2026-03-30 09:00     python eval.py
```

2. Optionally query live status from amlt for recent jobs:

```bash
aj jobs list --live     # refresh status from amlt before displaying
```

This calls `amlt status <experiment_id>` for each job and updates `record.jsonl` with the latest status.

### 2.2 `aj jobs show <job_id>`

Show detailed information about a single job.

```bash
aj jobs show a1b2c3d4
```

Output:

```
Job: a1b2c3d4
  Template    : gpu
  SKU         : Standard_NC4s_v3
  Nodes       : 4
  Processes   : 8
  Status      : Running
  Submitted   : 2026-03-31T10:30:45Z
  Queue time  : 3m 22s
  Run time    : 1h 45m
  Command     : uv run train.py --lr 0.01
  Tags        : experiment=ablation, seed=42
  Config file : .azure_jobs/submission/a1b2c3d4.yaml
```

Pulls live data from `amlt status` and merges with local record.

### 2.3 `aj jobs cancel <job_id>`

Cancel a running or queued job.

```bash
aj jobs cancel a1b2c3d4
aj jobs cancel --all --status queued   # cancel all queued jobs
```

**Implementation:**
- Map the `aj` job ID to the amlt experiment name (the `sid`).
- Call `amlt cancel <experiment_id>`.
- Update status in `record.jsonl` to `"cancelled"`.

### 2.4 `aj jobs logs <job_id>`

Stream or fetch logs for a job.

```bash
aj jobs logs a1b2c3d4              # fetch latest logs
aj jobs logs a1b2c3d4 --follow     # stream logs in real-time
aj jobs logs a1b2c3d4 --tail 100   # last 100 lines
```

**Implementation:**
- Delegate to `amlt logs <experiment_id>`.
- For `--follow`, use `amlt logs --follow` or poll periodically.
- Consider saving logs locally to `AJ_HOME/logs/{id}.log` for offline access.

### 2.5 `aj jobs retry <job_id>`

Alias for resubmit that also copies tags and metadata.

```bash
aj jobs retry a1b2c3d4               # resubmit with same config
aj jobs retry a1b2c3d4 -n 8          # resubmit with different nodes
```

---

## 3. Template Management — `aj template`

Move template-related commands under a dedicated subgroup.

### 3.1 `aj template list`

Replace the current `aj list` with `aj template list`.

```bash
aj template list
```

Enhanced output showing descriptions and defaults:

```
Name     Base        Nodes  Processes  SKU Pattern
───────  ──────────  ─────  ─────────  ─────────────────────
gpu      base_env    2      4          Standard_NC{nodes}s_v3
cpu      base_env    1      1          Standard_D{nodes}s_v5
a100     gpu         4      8          Standard_ND96s_v4
```

Read `_extra` and `sku` from each template to populate the table.

### 3.2 `aj template show <name>`

Inspect a template's resolved configuration.

```bash
aj template show gpu
```

Shows the fully merged config (after base resolution) as formatted YAML, so users can see exactly what will be submitted.

### 3.3 `aj template pull`

Move the current `aj pull` to `aj template pull`.

```bash
aj template pull https://github.com/org/templates.git
aj template pull --force   # re-clone
```

### 3.4 `aj template validate <name>`

Validate a template without submitting.

```bash
aj template validate gpu
```

- Resolve full inheritance chain.
- Check required keys (`jobs`, `sku`).
- Verify SKU template syntax.
- Report warnings for missing optional fields.

### 3.5 `aj template diff <a> <b>`

Compare two templates after inheritance resolution.

```bash
aj template diff gpu a100
```

Shows a unified diff of the resolved YAML, making it easy to see what changes between template variants.

### 3.6 `aj template create <name>`

Scaffold a new template interactively.

```bash
aj template create my_experiment
```

Prompts for base template, SKU, nodes, processes, and pre-commands. Writes a new YAML file to `AJ_TEMPLATE_HOME`.

### 3.7 Backward Compatibility

Keep `aj list` and `aj pull` working as aliases for `aj template list` and `aj template pull` for at least one minor version. Print a deprecation warning:

```
Warning: `aj list` is deprecated. Use `aj template list` instead.
```

---

## 4. Job Metrics and Insights

### 4.1 Queue Time Tracking

Track how long jobs wait before execution starts.

**Data model extension — add to `SubmissionRecord`:**

```python
@dataclass
class SubmissionRecord:
    # ... existing fields ...
    queued_at: str | None = None      # when amlt accepted the job
    started_at: str | None = None     # when execution began
    completed_at: str | None = None   # when job finished
    queue_duration_s: float | None = None
    run_duration_s: float | None = None
```

**Collection strategy:**
- `queued_at` = timestamp when `amlt run` returns successfully.
- `started_at` and `completed_at` = fetched from `amlt status` on demand.
- Queue duration = `started_at - queued_at`.
- Run duration = `completed_at - started_at`.

### 4.2 `aj jobs stats`

Aggregate statistics across submissions.

```bash
aj jobs stats                      # overall stats
aj jobs stats --template gpu       # stats for a template
aj jobs stats --since 2026-03-01   # stats since date
```

Output:

```
Jobs Summary (last 30 days)
  Total submissions : 47
  Succeeded         : 38  (81%)
  Failed            : 6   (13%)
  Cancelled         : 3   (6%)

Timing
  Avg queue time    : 4m 12s
  Median queue time : 2m 30s
  P95 queue time    : 18m 45s
  Avg run time      : 2h 15m

By Template
  gpu   : 28 jobs, avg queue 3m 50s
  a100  : 12 jobs, avg queue 6m 20s
  cpu   : 7 jobs,  avg queue 0m 45s

By SKU
  Standard_NC4s_v3  : 20 jobs, avg queue 2m 10s
  Standard_ND96s_v4 : 12 jobs, avg queue 8m 30s
```

### 4.3 Cost Estimation

Provide rough cost estimates before submission.

```bash
aj run -t gpu -n 4 --estimate python train.py
```

```
Estimated cost: ~$12.80/hr (4x Standard_NC4s_v3 @ $3.20/hr each)
```

Requires a local SKU-to-price mapping table (could be pulled from Azure pricing API or maintained as a config file).

### 4.4 Job History Search

```bash
aj jobs search "train.py --lr"    # search by command substring
aj jobs search --failed           # all failed jobs
aj jobs search --today            # today's jobs
```

---

## 5. Quality of Life

### 5.1 Rich Output

Add optional rich/colored terminal output using the `rich` library (optional dependency).

- Tables for `aj jobs list`, `aj template list`.
- Progress spinners for long operations.
- Syntax-highlighted YAML in `aj template show`.
- Graceful fallback to plain text when `rich` is not installed or output is piped.

### 5.2 Shell Completions

Generate shell completions for bash, zsh, and fish:

```bash
aj --install-completion bash
aj --install-completion zsh
```

Click supports this natively via `click.shell_completion`.

### 5.3 Configuration File

Support a project-level `.aj.yaml` or `aj.toml` config for defaults:

```yaml
# .aj.yaml
default_template: gpu
default_nodes: 2
default_processes: 4
auto_confirm: true
tags:
  project: my-research
  team: ml-infra
```

These act as defaults that CLI flags can still override.

### 5.4 Job Notifications

Notify when a job completes or fails.

```bash
aj run -t gpu --notify python train.py
```

Options:
- Desktop notification (via `notify-send` on Linux, `osascript` on macOS).
- Webhook (Slack, Teams) via a configured URL in `.aj.yaml`.
- Requires a background polling loop or integration with amlt's notification system.

### 5.5 Submission Cleanup

```bash
aj clean                        # remove old submission YAMLs
aj clean --older-than 30d       # keep last 30 days
aj clean --keep 50              # keep last 50 submissions
```

Removes files from `AJ_SUBMISSION_HOME` to prevent unbounded growth.

### 5.6 Migrate Record Storage

The current `record.jsonl` is simple but hard to query efficiently. Consider migrating to SQLite:

- Same append-only semantics.
- Enables fast filtering, sorting, aggregation.
- Support `aj jobs list --status running` without scanning the full file.
- Provide a one-time `aj migrate` command to convert existing `record.jsonl` → SQLite.

---

## 6. CLI Structure Summary

After all changes, the command tree looks like:

```
aj
├── run [OPTIONS] COMMAND [ARGS]        # submit a job (enhanced)
│   ├── -t, --template
│   ├── -n, --nodes
│   ├── -p, --processes
│   ├── -d, --dry-run
│   ├── -y, --yes
│   ├── -L, --run-local
│   ├── -s, --skip-ssh-check
│   ├── --tag KEY=VALUE                 # new
│   ├── --set KEY=VALUE                 # new
│   ├── --resubmit JOB_ID              # new
│   └── --estimate                      # new
│
├── jobs                                # new command group
│   ├── list [--status] [--template] [--tag] [--last N] [--since DATE] [--live]
│   ├── show <job_id>
│   ├── cancel <job_id> [--all --status STATUS]
│   ├── logs <job_id> [--follow] [--tail N]
│   ├── retry <job_id> [-n NODES] [-p PROCS]
│   ├── stats [--template] [--since DATE]
│   └── search <query> [--failed] [--today]
│
├── template                            # new command group
│   ├── list
│   ├── show <name>
│   ├── pull [REPO_URL] [-f]
│   ├── validate <name>
│   ├── diff <a> <b>
│   └── create <name>
│
├── clean [--older-than DURATION] [--keep N]    # new
│
├── list                                # deprecated alias → template list
└── pull                                # deprecated alias → template pull
```

---

## 7. Implementation Priority

Roughly ordered by user impact and implementation complexity.

### Phase A — Core Job Management

| Item | Section | Complexity | Notes |
|------|---------|------------|-------|
| `aj template list` (move + enhance) | §3.1 | Low | Restructure Click groups |
| `aj template show` | §3.2 | Low | Reuse `read_conf` |
| `aj template pull` (move) | §3.3 | Low | Alias existing code |
| `aj jobs list` (local records) | §2.1 | Low | Read JSONL, format table |
| `aj jobs show` | §2.2 | Medium | Needs amlt status integration |
| `aj jobs cancel` | §2.3 | Medium | Needs amlt cancel integration |
| `aj jobs logs` | §2.4 | Medium | Needs amlt logs integration |
| Backward-compat aliases | §3.7 | Low | Deprecation warnings |

### Phase B — Submission Enhancements

| Item | Section | Complexity | Notes |
|------|---------|------------|-------|
| Submission preview | §1.2 | Low | Format existing data |
| Pre-submission validation | §1.1 | Low | Check amlt, file existence |
| Resubmit | §1.3 | Medium | Reload from submission YAML |
| Job tags | §1.4 | Medium | Extend SubmissionRecord |
| Config overrides `--set` | §1.6 | Medium | Dot-path parser |

### Phase C — Metrics and Polish

| Item | Section | Complexity | Notes |
|------|---------|------------|-------|
| Queue time tracking | §4.1 | Medium | Extend data model + polling |
| `aj jobs stats` | §4.2 | Medium | Aggregate JSONL data |
| `aj template validate` | §3.4 | Low | Reuse existing validation |
| `aj clean` | §5.5 | Low | File cleanup |
| Shell completions | §5.2 | Low | Click built-in |

### Phase D — Advanced Features

| Item | Section | Complexity | Notes |
|------|---------|------------|-------|
| SQLite migration | §5.6 | High | New storage backend |
| Rich output | §5.1 | Medium | Optional dependency |
| Cost estimation | §4.3 | Medium | Needs pricing data |
| Job notifications | §5.4 | High | Background process or webhook |
| Multi-job submission | §1.5 | High | Significant refactor |
| `aj template diff` | §3.5 | Medium | YAML diff logic |
| Project config `.aj.yaml` | §5.3 | Medium | Config layering |

---

## 8. Data Model Evolution

The current `SubmissionRecord` will need to grow. Proposed extended schema:

```python
@dataclass
class SubmissionRecord:
    # Identity
    id: str                                 # 8-char hex
    template: str
    command: str
    args: list[str] = field(default_factory=list)

    # Resources
    nodes: int = 1
    processes: int = 1
    sku: str = ""                           # new: resolved SKU string

    # Lifecycle
    portal: str = "azure"
    status: str = "submitted"               # submitted → queued → running → completed/failed/cancelled
    created_at: str = ""
    queued_at: str | None = None            # new
    started_at: str | None = None           # new
    completed_at: str | None = None         # new

    # Timing (seconds)
    queue_duration_s: float | None = None   # new
    run_duration_s: float | None = None     # new

    # Metadata
    tags: dict[str, str] = field(default_factory=dict)  # new
    config_file: str = ""                   # new: path to submission YAML
    amlt_experiment: str = ""               # new: amlt experiment name for status queries
```

**Backward compatibility:** new fields all have defaults, so existing `record.jsonl` entries will deserialize correctly.
