# Azure Jobs

A fast, lightweight CLI for submitting and managing Azure ML jobs via pure REST APIs. Uses YAML template inheritance, sensible defaults, and rich terminal output. Supports both Azure ML compute clusters and Singularity virtual clusters.

## Install

    pipx install azure_jobs

For development:

    uv pip install -e .

## Quick Start

```bash
aj ws set                             # configure workspace (interactive)
aj template pull user/templates       # clone shared templates
aj template list                      # show available templates
aj run -t gpu python train.py         # submit a job
aj job list                           # monitor jobs
aj job logs <id>                      # view logs
```

## Commands

### Job Submission

    aj run -t gpu python train.py       # submit using "gpu" template
    aj run python train.py              # re-use last template
    aj run -d python train.py           # dry run — inspect config without submitting
    aj run -L python train.py           # run locally for testing

| Flag | Purpose |
|------|---------|
| `-t` | Template name |
| `-n` | Number of nodes |
| `-p` | Processes per node |
| `-d` | Dry run |
| `-y` | Skip confirmation |
| `-L` | Run locally |

### Job Management

    aj job list                         # list recent cloud jobs
    aj job list -s Running              # filter by status
    aj job show <id>                    # show job details
    aj job cancel <id>                  # cancel a running job
    aj job logs <id>                    # download and display logs
    aj list                             # show local submission history

### Templates

    aj template list                    # list available templates
    aj template show <name>             # display resolved config after inheritance
    aj template validate                # check all templates for errors
    aj template pull <repo>             # clone template repository
    aj template push -m "msg"           # commit and push local changes

### Workspace & Auth

    aj ws list                          # list workspaces in subscription
    aj ws set                           # set active workspace (interactive picker)
    aj auth status                      # show login and credential status
    aj auth login                       # delegate to az login

### Resources

    aj env list                         # list registered environments
    aj env show <name>                  # show environment versions and images
    aj ds list                          # list datastores
    aj ds show <name>                   # show datastore details
    aj quota list                       # show Singularity VC quota and availability
    aj quota list --aml                 # show AML compute cluster availability
    aj exp list                         # list experiments (aggregated from jobs)
    aj image list                       # list Singularity curated base images

### Other

    aj config show                      # print all configuration
    aj config timezone Asia/Shanghai    # set display timezone
    aj dash                             # open interactive TUI dashboard

## How It Works

1. **Templates** define reusable job configs as YAML with optional base inheritance
2. **`aj run`** loads a template, resolves the inheritance chain, merges configs, and applies CLI overrides
3. **Code upload** — zips your code, uploads to workspace blob storage, and registers a code asset
4. **Environment** — registers Docker images as Azure ML environment versions (with SHA-based dedup)
5. **Submission** — builds a REST job body and submits via `PUT /jobs/{name}` to Azure Resource Manager
6. **Tracking** — records each submission locally to `record.jsonl` and prints the portal URL

No dependency on `amlt` or the `azure-ai-ml` SDK. All Azure interactions use lightweight REST APIs with `AzureCliCredential`.

## Documentation

| Document | Contents |
|----------|----------|
| [Architecture](docs/architecture.md) | Module layout, data flow, design decisions |
| [Configuration](docs/configuration.md) | Templates, inheritance, merge rules, environment variables |
| [REST API](docs/rest-api.md) | REST client design, endpoints, authentication |
| [Roadmap](docs/roadmap.md) | Planned features and milestones |

## Testing

    uv run pytest
