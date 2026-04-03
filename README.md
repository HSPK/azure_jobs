# Azure Jobs

A fast, lightweight CLI for submitting and managing Azure ML jobs. Wraps `amlt` with YAML template inheritance, sensible defaults, and rich terminal output.

## Install

    pipx install azure_jobs

For development:

    uv pip install -e .

## Usage

    aj pull user/templates          # clone shared templates (supports user/repo shorthand)
    aj list                         # show available templates
    aj run -t gpu python train.py   # submit a job using the "gpu" template
    aj run python train.py          # re-use last template (saved in aj_config.json)
    aj run -d python train.py       # dry run — inspect YAML without submitting
    aj run -L python train.py       # run locally for testing

Common options for `aj run`:

| Flag | Purpose |
|------|---------|
| `-t` | Template name |
| `-n` | Number of nodes |
| `-p` | Processes per node |
| `-d` | Dry run |
| `-y` | Skip confirmation |
| `-L` | Run locally |

## How It Works

1. **Templates** define reusable job environments as YAML with optional inheritance
2. **`aj run`** loads a template, resolves the inheritance chain, merges configs, applies CLI overrides, and generates a submission YAML
3. **`amlt`** handles the actual Azure submission (code upload, Docker, storage mounts)
4. **`aj_config.json`** stores your defaults (template, nodes, processes) and workspace config

## Documentation

| Document | Contents |
|----------|----------|
| [Architecture](docs/architecture.md) | Module layout, data flow, design decisions |
| [Configuration](docs/configuration.md) | `aj_config.json`, templates, inheritance, merge rules |
| [Roadmap](docs/roadmap.md) | Planned features and milestones |
| [Azure Integration](docs/amlt-integration.md) | Backend design for direct Azure SDK access |

## Testing

    uv run pytest

