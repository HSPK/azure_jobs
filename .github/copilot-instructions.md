# Copilot Instructions

## Project Overview

Azure Jobs (`aj`) is a CLI tool for submitting and managing Azure Machine Learning jobs via templates. It wraps the `amlt` CLI and adds YAML-based template inheritance, configuration merging, and job tracking.

## Build & Run

```bash
# Install (editable, with uv)
uv pip install -e .

# Install for end users
pipx install azure_jobs

# Run CLI
aj --help
aj run -t <template> <command> [args...]
aj pull <repo-url>
aj list
```

```bash
# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_merge_confs.py

# Run a single test by name
uv run pytest tests/test_cli.py::TestRunCommand::test_dry_run_creates_submission_file -v
```

## Architecture

**Entry point:** `aj` CLI → `azure_jobs.cli:main` (Click group)

**Core modules:**
- `cli.py` — All CLI commands (`run`, `pull`, `list`). Job submission builds a YAML config, writes it to `AJ_SUBMISSION_HOME`, and delegates to `amlt run`.
- `conf.py` — Configuration engine. `read_conf()` loads YAML templates with recursive base inheritance. `merge_confs()` merges dicts recursively, zips lists by index, and last-value-wins for scalars.
- `const.py` — Path constants derived from `AJ_HOME` env var (defaults to `./.azure_jobs`).
- `template.py`, `toml.py` — Empty placeholders for planned features.

**Data flow:** CLI args → load template YAML → resolve `base` inheritance chain via `read_conf()` → merge configs → generate submission YAML → `amlt run` → log to `record.jsonl`.

## Key Conventions

- **CLI framework:** Click with `ignore_unknown_options=True` and `allow_extra_args=True` to pass arbitrary arguments through to job commands.
- **Configuration merging** follows specific rules: dicts merge recursively, lists merge by index (zip), scalars are replaced by the last value. All scalar replacements use deep copy.
- **Base template resolution:** dotted names like `subdir.filename` resolve to `AJ_HOME/subdir/filename.yaml`; plain names resolve relative to the current directory.
- **Script detection:** `.py` files are executed via `uv run`, `.sh` files via `bash`.
- **Job records** are appended as JSONL to `AJ_RECORD` using the `SubmissionRecord` dataclass.
- **SKU templates** support two formats: string templates with `{nodes}`/`{processes}` substitution, and dict templates with range-based matching (e.g., `"1-2": "sku_a"`, `"4+": "sku_b"`).
- **Dependencies are minimal by design** — only `click` and `pyyaml`. Python 3.10+.
