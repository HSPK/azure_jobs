# Azure Jobs

`aj` is a daemon-backed CLI, TUI, and Python SDK for submitting jobs to Azure
Machine Learning, native Singularity, and Kubernetes Volcano. Cloud job
management applies to AML/Sing workspace jobs; Volcano currently supports
submission only.

Azure operations run only in the local daemon. Clients use HTTP over a private
Unix domain socket; there is no in-process fallback.

## Install

```bash
uv tool install azure-jobs
command -v aj || uv tool update-shell
az login
aj auth status
```

Python 3.10+ is required. Volcano also requires `kubectl`; `aj run --amlt`
requires `amlt`.

Optional Agent Skill integration:

```bash
aj skill install                 # Copilot, Codex, and Claude Code
aj skill status
```

Kubernetes/Volcano client setup and task management:

```bash
aj k install                     # one-time kubectl/Krew/OIDC installation
aj k login                       # kubeconfig merge + OIDC authentication
aj k status
aj k jobs
```

## Quickstart

```bash
mkdir training && cd training
aj init                         # pull templates, choose workspace, set experiment
aj template list
aj run -t <template> -d -n 1 -p 1 python -c "print('hello')"
aj run -t <template> -n 1 -p 1 python -c "print('hello')"
aj job list
aj job logs <job-or-aj-id>
aj dash
```

Use `aj ws set` plus a hand-written `.azure_jobs/template/*.yaml` file instead
of `aj init` when no shared template repository is available.

## Core features

- YAML templates with recursive `base` inheritance and explicit merge rules.
- Native AML/Sing submission through Azure REST, with one deterministic,
  content-addressed code archive.
- Automatic Singularity VC selection from exact hardware requirements and
  current quota.
- Volcano submission with PVC/`kubectl exec` or Blob archive code transfer.
- Persistent submit queue, job watches, HTTP Range logs, local records, and TUI.
- OpenAI-style SDK namespaces such as `d.job.list()` and `d.ws(name).ds.list()`.
- Managed Agent Skill installation for Copilot, Codex, and Claude Code.
- Kubernetes OIDC client setup and narrow Volcano job/pod/log management.
- `amlt` compatibility through `aj run --amlt`.

Start with the [documentation](https://hspk.github.io/azure_jobs/), especially
[Getting started](https://hspk.github.io/azure_jobs/getting-started/) and
[Templates](https://hspk.github.io/azure_jobs/templates/).

## Development

```bash
uv sync --dev
uv run pytest -q
uv run mkdocs build --strict
```

See [Development](https://hspk.github.io/azure_jobs/development/) and
[Testing](https://hspk.github.io/azure_jobs/testing/).
