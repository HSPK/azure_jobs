# Getting started

This path installs `aj`, authenticates the daemon, configures a project, and
submits one job.

## 1. Install

Install the published CLI in an isolated environment:

```bash
uv tool install azure-jobs
command -v aj || uv tool update-shell
aj --version
```

Python 3.10+ is required. Volcano users also need a working `kubectl` context.
Install `amlt` only if you plan to use `aj run --amlt`.

Optionally install the bundled Agent Skill for Copilot, Codex, and Claude Code:

```bash
aj skill install
aj skill status
```

See [Agent Skill](skills.md) for project-scoped installation and updates.

## 2. Authenticate Azure

Sign in with the Azure CLI, then ask the daemon to verify its credential:

```bash
az login
aj auth status
```

The daemon starts on demand. It refuses to start without a usable sign-in
because it is the only Azure execution path. The CLI never obtains tokens or
runs Azure discovery itself.

Useful recovery commands:

```bash
aj daemon status
aj daemon restart              # use after upgrading aj
AJ_DEBUG=1 aj daemon restart   # daemon inherits debug mode
AJ_DEBUG=1 aj auth status
```

## 3. Create a project

### Shared-template path

`aj init` is interactive. In a new directory it asks for a Git template
repository, selects an Azure ML workspace, and sets an experiment:

```bash
mkdir training && cd training
aj init
```

Repository shorthand such as `owner/repository` expands to a GitHub SSH URL.
Use a private repository when templates contain sensitive resource metadata.

### Local-template path

If no shared repository exists, create the project state directory and choose
a workspace directly:

```bash
mkdir training && cd training
mkdir -p .azure_jobs/template
aj ws set
aj config experiment training
```

`aj ws set [NAME]` saves subscription, resource group, and workspace in
`.azure_jobs/aj_config.json`. Check it with:

```bash
aj ws show
aj config show
```

## 4. Get or write a template

Pull a template repository:

```bash
aj template pull owner/repository
aj template list
aj template show <template>
aj template validate <template>
```

Or write `.azure_jobs/template/first.yaml`:

```yaml
base:
config:
  target:
    service: aml
    name: <AML_COMPUTE>
    gpus_per_node: 1
  environment:
    image: mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest
  code:
    ignore:
      - data/
      - outputs/
  jobs:
    - name: train
      sku: G1
      instance_count: 1
      identity: managed
```

Replace `<AML_COMPUTE>` with a compute target visible in the selected
workspace. See [Templates](templates.md) for Sing auto-selection, storage,
Volcano, inheritance, and SKU syntax.

## 5. Add a command

```bash
cat > train.py <<'PY'
import os

print("hello from", os.environ["AJ_NAME"])
print("nodes:", os.environ["AJ_NODES"])
PY
```

When the positional command is an existing `.py` file, `aj` runs it as
`uv run train.py`. Existing `.sh` files run as `bash script.sh`. Other command
names are passed through unchanged.

## 6. Dry run

```bash
aj run -t first -d python train.py
```

Dry run resolves inheritance, CLI counts, command handling, runtime variables,
and backend options, then writes the rendered submission YAML. It does not
upload code or submit a cloud job.

Preview the native upload selection separately:

```bash
aj code stats -t first
```

## 7. Submit and inspect

```bash
aj run -t first python train.py
aj list
aj job list
aj job status <job-or-aj-id>
aj job logs <job-or-aj-id>
```

The short `AJ_ID` printed by `aj run` is recorded locally and can be used by
the status, logs, and cancel commands. Continue with
[Submitting](submitting.md) and [Jobs](jobs.md).
