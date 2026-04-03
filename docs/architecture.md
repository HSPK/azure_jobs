# Architecture

## Overview

Azure Jobs (`aj`) is a CLI tool that sits between the user and Azure ML. It provides template-based job configuration with inheritance and sensible defaults, then delegates actual submission to `amlt`.

## Module Layout

The codebase is organized into three packages under `src/azure_jobs/`:

**`cli/`** — Command layer. Each file is one CLI command, all built with Click.

- `__init__.py` — defines the `main` Click group and registers commands
- `run.py` — job submission (`aj run`), including config validation, SKU resolution, command building
- `pull.py` — template repository cloning (`aj pull`)
- `templates.py` — template listing (`aj list`)

**`core/`** — Business logic. No CLI framework dependencies in core logic.

- `const.py` — path constants derived from `AJ_HOME` environment variable
- `conf.py` — YAML template loading with recursive base inheritance and merge logic
- `config.py` — reads/writes `aj_config.json` (defaults, workspace, repo_id)
- `record.py` — `SubmissionRecord` dataclass and JSONL logging

**`utils/`** — Shared output helpers.

- `ui.py` — Rich console: panels, tables, spinners, styled messages

## Data Flow

A job submission follows this path:

1. **Resolve template** — look up template name from `-t` flag or `aj_config.json` defaults
2. **Load config** — `read_conf()` reads the YAML template and recursively resolves its `base` chain
3. **Merge** — `merge_confs()` combines all configs in the inheritance chain (base first, child last)
4. **Apply overrides** — CLI flags (`-n`, `-p`) override template `_extra`, which overrides `aj_config.json` defaults
5. **Resolve SKU** — string templates get formatted, dict templates do range matching by node count
6. **Build command list** — env exports + template commands + user command
7. **Write submission YAML** — final merged config written to `.azure_jobs/submission/`
8. **Submit** — delegates to `amlt run` via subprocess
9. **Log** — appends a `SubmissionRecord` to `record.jsonl`

## Design Decisions

- **Performance first** — `aj --help` runs in ~160ms. All heavy imports (Rich, Azure SDK) are lazy.
- **`amlt` for submission only** — `amlt` handles code upload, Docker environments, Singularity, and storage mounts. We delegate to it for submission but plan to bypass it for status/cancel/logs.
- **Template inheritance over duplication** — templates compose via `base` chains instead of copy-paste.
- **Single config file** — `aj_config.json` stores everything: defaults, repo_id, workspace credentials.
- **Append-only job log** — `record.jsonl` is simple, greppable, and never loses data.
- **Minimal dependencies** — only `click`, `pyyaml`, and `rich`. Azure SDK will be an optional extra.
