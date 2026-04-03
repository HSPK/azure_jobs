# Azure Backend Integration

Design for adding job lifecycle commands (status, cancel, logs) by talking to Azure ML directly. Uses amlt as reference only, never as a runtime dependency.

## Principles

1. **Zero heavy imports at startup** — `aj --help` stays under 200ms
2. **No amlt dependency** — amlt imports cost ~600ms; we only reference its logic
3. **Local-first** — template and job list commands read local files only
4. **Lazy Azure access** — only commands that talk to Azure pay the import cost
5. **Subprocess for submission** — `amlt run` handles code upload complexity; we handle status/cancel/logs directly

## What amlt Does

| Operation | Azure SDK call |
|-----------|---------------|
| Submit | `ml_client.jobs.create_or_update(CommandJob(...))` |
| Status | `ml_client.jobs.get(job_id)` |
| Cancel | `ml_client.jobs.begin_cancel(job_id)` |
| Logs | REST artifact API + blob storage |

We delegate submission to `amlt run` via subprocess but handle status/cancel/logs ourselves for speed.

## Planned Module: core/backend.py

A lightweight wrapper around `azure-ai-ml` with all imports inside function bodies.

Key functions:
- `get_job_status(job_id)` — returns status string from Azure
- `cancel_job(job_id)` — cancels a running job
- `stream_job_logs(job_id)` — streams logs to terminal
- `download_job_logs(job_id, path)` — downloads log files

Authentication uses `DefaultAzureCredential` (supports `az login`, env vars, managed identity). Workspace details come from `aj_config.json`.

## Performance Budget

| Path | Target | How |
|------|--------|-----|
| `aj --help` | <200ms | No heavy imports at module level |
| `aj list`, `aj jobs list` | <200ms | Local file I/O only |
| `aj run` (submission) | <200ms + amlt | Subprocess, no SDK import |
| `aj jobs show/cancel/logs` | ~300ms + network | Lazy `azure-ai-ml` import |

## Dependencies

Core `aj` stays lightweight: `click`, `pyyaml`, `rich`.

Azure features will be an optional extra:

    pip install azure_jobs[azure]

Which adds `azure-ai-ml` and `azure-identity`.
