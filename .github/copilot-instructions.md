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
- **Client/server split is the organising principle.** `shared/` holds the
  vocabulary both sides speak, `client/` drives and renders, `server/` executes.
  Neither side may import the other; the SDK lives only in `server/`, and
  `rich`/`textual` only in `client/`. Enforced by `tests/test_api_architecture.py`.
- **The daemon is the only execution path.** There is no in-process mode: a
  second path drifts from the first. Transport is HTTP over a Unix domain
  socket (FastAPI/uvicorn server, httpx client), the same shape dockerd
  exposes, so it is `curl --unix-socket` debuggable and versioned by path.
- **Dependencies serve the split.** The old "minimal by design" rule is
  retired: a hand-rolled transport cost more in lifecycle bugs than the
  dependencies save. Python 3.10+.

## Design Discipline

Follow SOLID — design decisions in new code must be defensible against these. Reviewers may push back on any change that violates them without justification.

- **S — Single Responsibility.** One reason to change per module/class/function. If a Volcano upload class also formats Rich output, split them.
- **O — Open/Closed.** Extension is preferred over modification. New submission backends are added by registering against an existing seam (`register_backend(..., build_spec_backend=..., normalize_job_name=...)`), **not** by editing `if/elif` chains in shared entry points. Each backend owns its typed `Opts` and any name-shape constraints via narrow hooks. New template sections / per-strategy schemas live next to the consumer (e.g. `BlobUploadOpts.from_extra` lives in `backend/volcano/uploaders/blob.py`, not in `job/build.py`).
- **L — Liskov Substitution.** Any concrete `CodeUploader` / submission backend / API namespace must satisfy its `Protocol` / abstract contract — same return shape, same error semantics, no surprise side effects. Tests assert against the contract, not against a single implementation.
- **I — Interface Segregation.** Keep `Protocol`s narrow. `CodeUploader.prepare(...)` returns a `CodeUploadResult`; it does not also have `cleanup()`, `validate()`, `render_yaml()`. Add new optional behaviour as a separate Protocol the caller can opt into.
- **D — Dependency Inversion.** Entry points depend on abstractions, not concretions. `submit_via_volcano` only knows about `pick_uploader(extra)` → `CodeUploader`; it must not import `BlobUploader` directly.

### Concrete consequences for this repo

- **No large inline scripts.** Any bash/Python/YAML snippet ≥ ~15 lines that is injected into a job pod, an amlt config, or a generated file must live as a standalone resource (e.g. `backend/volcano/uploaders/scripts/install_azcopy.sh`). Load it via `_scripts.load_script(name, **subs)`. Placeholders use literal `{KEY}` substitution — same convention as `backend/volcano/distributed_preamble.sh`. Keep the loader tiny (DRY/KISS); graduate to a real templating engine only if/when a script genuinely needs conditionals or loops.
- **Pod-side bash that depends on a binary must force-install it.** Don't assume `azcopy`, `uv`, `jq`, etc. exist in the container — install or fail loudly.
- **Backend dispatch is a registry; uploader dispatch is inline if/else (KISS by size).** Backends self-register via `register_backend(name, fn, *, label, build_spec_backend, normalize_job_name)` because they're sub-package boundaries that may grow over time. Volcano's two uploaders (`kubectl-exec`, `blob`) are picked by a single `pick_uploader(extra)` function with literal if/else in `backend/volcano/uploaders/__init__.py` — registries and even literal `{name: factory}` dicts are over-engineering for ≤ ~3 strategies. The OCP invariant either way: `entry.py` never branches on backend/strategy name — it only calls `get_backend(...)` / `pick_uploader(...)`. Add a new strategy = add another if/else branch; if/when there are ~4+ strategies, switch to a registry.
- **`job/build.py` is service-agnostic.** It must never branch on `JobSpec.service` (regression-guarded by `tests/test_backend_registry.py::TestBuildPyHasNoServiceSpecificBranches`). Service-specific wiring lives in two narrow, single-purpose hooks on the backend: `build_spec_backend(template) -> Any` returns the typed backend `Opts` (lands on `spec.backend_spec`), and `normalize_job_name(name) -> str` canonicalises the job name (defaults to identity). Both are pure functions; build.py constructs the JobSpec directly from their results — no intermediate dict merging.
- **JobSpec splits backend-specific config from template passthrough.** `JobSpec.backend_spec` holds the typed Opts the backend's `build_spec_backend` produced (`AmlOpts` / `VolcanoOpts`). `JobSpec.extra` is a pure passthrough of the template's `_extra` block (user-defined sub-keys like `code_upload`). Consumers read `spec.backend_spec or AmlOpts()` for backend config; uploaders/user hooks read `spec.extra["<their_key>"]`. Build never inspects `extra`. Adding a new backend Opts field = touch only the Opts dataclass + its `from_template` classmethod.
- **Template fields that `amlt` doesn't understand go under `_extra`.** Templates carry `_extra: dict[str, Any]`; `job/build.py` copies it verbatim to `JobSpec.extra`. Each backend's own typed config goes to `backend_spec`, not `extra`. `render_amlt_yaml` strips `_extra` before handing the raw YAML to amlt. New aj-only template fields = either declare on the backend's Opts dataclass (typed) or use `_extra.<your_key>` (opaque passthrough).
- **Errors carry actionable detail.** Every broad `except` calls `log.exception(...)`, surfaces `({type}: {msg})` plus a hint to set `AJ_DEBUG=1` for a full traceback. Subprocess failures must include the captured `stdout` / `stderr` and the command that ran. Never reduce a real error to a generic "X failed" string.
