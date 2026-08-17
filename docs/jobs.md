# Jobs, queues, watches, and dashboard

Cloud state, local submission records, and daemon work are separate views.
The `job`, log, watch, and dashboard resources are Azure ML workspace APIs and
therefore manage AML/Sing jobs. Volcano currently supports submission only.

## Cloud jobs

```bash
aj job list
aj job list -n 100 -s Running -e training
aj job list --type Command --tag team
aj job show <job-or-aj-id>
aj job status <job-or-aj-id> --ws <workspace>
aj job logs <job-or-aj-id> --ws <workspace>
aj job cancel <job-or-aj-id> --ws <workspace>
```

`job list` reads the configured workspace unless `--ws NAME` is available and
specified. Job type and tag filters are sent to Azure; status and experiment
filters are applied by the daemon while fetching.

Short eight-character aj IDs are resolved through the local journal. A full
Azure job name also works. Pass `--ws` when the template or compute resolves
to a workspace other than the active project workspace.

`aj job logs` checks status, downloads the selected aggregate output and error
text, and reports when a queued or provisioning job has no logs yet. The SDK
and dashboard additionally support file lists and byte-range reads.

The CLI cancels but does not expose permanent deletion. In `aj dash`, `d`
deletes a terminal job after confirmation; active jobs must be canceled first.
The SDK exposes `d.job.delete()` for AML/Sing jobs.

## Local records

```bash
aj list
aj list -n 50
aj list -t <template>
```

`.azure_jobs/record.jsonl` is append-only and newest-first when read. It records
the built request, result status, backend name, portal URL, timestamp, and
failure note. It is local history, not the authoritative cloud status.

## Statistics and experiments

```bash
aj job stats
aj job stats --days 30
aj job stats --all

aj exp list
aj exp list --days 30 --all
aj exp show <experiment>
```

Statistics summarize duration, GPU-hours, success rate, experiment, compute,
workspace, and user where data is available. `--all` fans out across visible
workspaces and reports partial failures rather than hiding successful results.

Set the default experiment with:

```bash
aj config experiment training
```

## Submission queue

```bash
aj run --queue -t <template> train.py
aj queue list
aj queue show <ticket>
aj queue wait <ticket> --timeout 3600
aj queue cancel <ticket>
```

Queue entries are daemon-owned and persisted per project/workspace context.
Only pending entries can be canceled through the queue.

## Watches

```bash
aj watch add <job>
aj watch list
aj watch listen
aj watch listen --no-desktop --timeout 600
aj watch remove <job>
```

The daemon keeps polling after the invoking shell exits. Watch journals
survive daemon restart and are restored lazily when a later request reopens
that project/workspace context. Watches are removed automatically after a
terminal transition. `watch listen` receives server-sent events; desktop
notifications use the platform notifier when available.

## Dashboard

```bash
aj dash
aj dash -n 500 --page-size 40
aj dash --mouse
```

`--last` controls the initial fetch, not a permanent pagination cap. The TUI
can request later Azure pages.

Important keys:

| Keys | Action |
| --- | --- |
| `up`, `down` | select job |
| `left`, `right` | previous/next page |
| `i`, `l` | info/log view |
| `r` | refresh |
| `c`, `d` | cancel/delete |
| `f`, `e`, `/`, `F` | status, experiment, search, clear filters |
| `w` | switch workspace |
| `o` | select log file |
| `L`, `s`, `Ctrl-S` | live tail, auto-scroll, save buffer |
| `Esc`, `q` | manual/quit |

The dashboard uses the same SDK and daemon as the CLI. Controllers perform
I/O; stores own state; HTTP Range reads keep log tailing bounded.

See [TUI dashboard design](design/tui.md) for state, concurrency, and
destructive-action contracts.
