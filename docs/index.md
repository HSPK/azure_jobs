# Azure Jobs

Azure Jobs (`aj`) provides one local interface for template-driven jobs on
Azure Machine Learning, native Singularity, Kubernetes Volcano, and `amlt`.

The CLI, Textual dashboard, and public Python SDK are clients. Azure
authentication, discovery, submission, queueing, watching, and log access run
in a background daemon. Communication is HTTP over a private Unix domain
socket; no client falls back to running Azure work in-process.

## Install

```bash
uv tool install azure-jobs
az login
aj auth status
```

Requirements:

- Python 3.10 or newer;
- Azure CLI for authentication and daemon-side discovery;
- `kubectl` for Volcano;
- `amlt` only for `aj run --amlt`.

## Five-minute flow

```bash
mkdir training && cd training

# Interactive: pull a template repository, choose a workspace, set experiment.
aj init

aj template list
aj template show <template>
aj run -t <template> -d python -c "print('hello')"
aj run -t <template> python -c "print('hello')"

aj job status <job-or-aj-id>
aj job logs <job-or-aj-id>
aj job cancel <job-or-aj-id>
```

No shared templates? Configure the workspace and author one locally:

```bash
mkdir -p .azure_jobs/template
aj ws set
aj config experiment training
```

Continue with [Getting started](getting-started.md) for a complete first
submission and [Templates](templates.md) for copyable YAML.

## Daily workflow

```bash
aj run --queue -t <template> python -c "print('hello')"
aj queue list
aj watch add <job>
aj watch listen
aj dash

aj quota list
aj sku list
aj ds list
aj env list
```

Use `aj --help` as the command source of truth. Its top-level groups are:

| Group | Commands |
| --- | --- |
| Getting Started | `init`, `run`, `dash` |
| Jobs | `job`, `exp`, `queue`, `watch`, `list` |
| Azure Resources | `ws`, `ds`, `env`, `image`, `sku`, `quota`, `sa`, `uai` |
| Kubernetes | `k8s` (`k`) |
| Project | `template`, `config`, `code`, `skill` |
| System | `auth`, `daemon` |

## Documentation map

| Need | Page |
| --- | --- |
| Install, authenticate, and submit once | [Getting started](getting-started.md) |
| Author AML, Sing, or Volcano YAML | [Templates](templates.md) |
| Understand flags and upload behavior | [Submitting](submitting.md) |
| Query, log, cancel, queue, watch, or use the TUI | [Jobs](jobs.md) |
| Find resource, config, auth, and daemon commands | [Resources](resources.md) |
| Install the Agent Skill for Copilot, Codex, or Claude | [Agent Skill](skills.md) |
| Set up Kubernetes access and manage Volcano tasks | [Kubernetes](kubernetes.md) |
| Automate through Python | [SDK](sdk.md) |
| Look up `AJ_*` variables | [Environment](environment.md) |
| Understand HTTP/OpenAPI and Azure REST clients | [API](api.md) |
| Understand layering and backend flow | [Architecture](architecture.md) |
| Change or test the repository | [Development](development.md), [Testing](testing.md) |
| Give an agent repository facts and constraints | [Agent guide](agent-guide.md) |
