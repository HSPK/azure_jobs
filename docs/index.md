---
hide:
  - navigation
  - toc
---

# Azure Jobs

<p style="font-size: 1.25rem; margin-top: -0.5rem;">
Fast, lightweight CLI for submitting Azure ML jobs through pure REST APIs —
no <code>azure-ai-ml</code> SDK, no <code>amlt</code> runtime.
</p>

<div class="grid cards" markdown>

-   :material-rocket-launch: **Zero-friction submits**

    ---

    `aj run -t gpu train.py` and you're on the cluster. SKU resolution,
    code upload, env injection — all handled.

-   :material-file-tree: **Composable templates**

    ---

    YAML inheritance via `base` chains. Compose `account · storage ·
    environment` building blocks; override only what you need.

-   :material-puzzle: **Three backends, one CLI**

    ---

    Native REST (AML / Singularity), `amlt`, or Kubernetes Volcano —
    behind a single dispatch surface.

-   :material-chart-box: **Built-in tracking**

    ---

    `aj job list / show / logs / stats`, an interactive TUI dashboard,
    and a local `record.jsonl` for every submit.

</div>

---

## Install

```bash
pipx install azure_jobs
```

Requires `az login`. The Volcano backend additionally needs `kubectl`.

## Quickstart

```bash
mkdir my-project && cd my-project
aj init                          # scaffold .azure_jobs/, register workspace
aj pull <user>/<repo>            # (optional) clone shared templates
aj run -t gpu train.py           # submit
```

`.py` runs via `uv run`, `.sh` via `bash`. Drop a `.codeignore` at the
project root to prune the upload.

[Open the full tutorial :material-arrow-right:](tutorial.md){ .md-button .md-button--primary }
[See all commands](commands.md){ .md-button }

---

## Where to go

| If you want to…                              | Read                                        |
|----------------------------------------------|---------------------------------------------|
| Submit your first job end-to-end             | [Tutorial](tutorial.md)                     |
| Look up a specific command or flag           | [Commands](commands.md)                     |
| Write or extend templates                    | [Configuration](configuration.md)           |
| Use `aj` as a Python library                 | [SDK](sdk.md)                               |
| Understand how it all fits together          | [Architecture](architecture.md)             |
| Compare against `amlt` / `azure-ai-ml`       | [Comparison](comparison.md)                 |
