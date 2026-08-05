# Copilot Instructions

## Project Overview

Azure Jobs (`aj`) is a daemon-backed CLI and Python SDK for submitting and
managing Azure ML, Singularity and Volcano jobs through templates.

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
uv run pytest tests/test_cli_run.py::TestRunCommand::test_dry_run_creates_submission_file -v
```

## Architecture

**Entry point:** `aj` CLI → `azure_jobs.client.cli:main` (Click group)

**Core modules:**
- `shared/` — wire models, templates, JobSpec building and pure utilities.
- `sdk/` — public `AjClient`, resource namespaces and HTTP-over-UDS transport.
- `client/cli`, `client/tui`, `client/ui` — Click, Textual and Rich frontends.
- `server/app.py`, `server/context.py`, `server/resources.py` — FastAPI routes,
  resource lifetimes and wire adapters.
- `server/az_client/` — daemon-only pure-HTTP Azure account/workspace clients.
- `server/submit/` — daemon-only submission backend registry.

**Data flow:** CLI args → build `JobSpec` → `azure_jobs.connect()` → HTTP over
UDS → FastAPI workspace resource → submission queue/backend → Azure/Volcano.

## Key Conventions

- **CLI framework:** Click with `ignore_unknown_options=True` and `allow_extra_args=True` to pass arbitrary arguments through to job commands.
- **Configuration merging** follows specific rules: dicts merge recursively, lists merge by index (zip), scalars are replaced by the last value. All scalar replacements use deep copy.
- **Base template resolution:** dotted names like `subdir.filename` resolve to `AJ_HOME/subdir/filename.yaml`; plain names resolve relative to the current directory.
- **Script detection:** `.py` files are executed via `uv run`, `.sh` files via `bash`.
- **Job records** are appended as JSONL to `AJ_RECORD` using the `SubmissionRecord` dataclass.
- **SKU templates** support two formats: string templates with `{nodes}`/`{processes}` substitution, and dict templates with range-based matching (e.g., `"1-2": "sku_a"`, `"4+": "sku_b"`).
- **The four-layer split is the organising principle.** `shared/` is
  transport-neutral, top-level `sdk/` knows only HTTP, `client/` renders and
  drives, `server/` executes. Client/server never import each other; Azure
  clients live only in `server/`; Rich/Textual only in `client/`. Enforced by
  `tests/test_api_architecture.py`.
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
- **O — Open/Closed.** Extension is preferred over modification. New execution
  backends register with `server.submit.register_backend`; shared description
  hooks register separately with `shared.spec.register_spec`. New strategy
  schemas live next to the consumer (for example
  `server/submit/volcano/uploaders/blob.py`), not in `shared/job/build.py`.
- **L — Liskov Substitution.** Any concrete `CodeUploader` / submission backend / API namespace must satisfy its `Protocol` / abstract contract — same return shape, same error semantics, no surprise side effects. Tests assert against the contract, not against a single implementation.
- **I — Interface Segregation.** Keep `Protocol`s narrow. `CodeUploader.prepare(...)` returns a `CodeUploadResult`; it does not also have `cleanup()`, `validate()`, `render_yaml()`. Add new optional behaviour as a separate Protocol the caller can opt into.
- **D — Dependency Inversion.** Entry points depend on abstractions, not concretions. `submit_via_volcano` only knows about `pick_uploader(extra)` → `CodeUploader`; it must not import `BlobUploader` directly.

### Concrete consequences for this repo

- **No large inline scripts.** Any bash/Python/YAML snippet ≥ ~15 lines that is injected into a job pod, an amlt config, or a generated file must live as a standalone resource (e.g. `server/submit/volcano/uploaders/scripts/install_azcopy.sh`). Load it via `_scripts.load_script(name, **subs)`. Placeholders use literal `{KEY}` substitution — same convention as `server/submit/volcano/distributed_preamble.sh`. Keep the loader tiny (DRY/KISS); graduate to a real templating engine only if/when a script genuinely needs conditionals or loops.
- **Pod-side bash that depends on a binary must force-install it.** Don't assume `azcopy`, `uv`, `jq`, etc. exist in the container — install or fail loudly.
- **Backend dispatch is a registry; uploader dispatch is inline if/else (KISS by size).** Execution backends self-register in `server/submit`; Volcano's two uploaders (`kubectl-exec`, `blob`) are selected by `pick_uploader(extra)` in `server/submit/volcano/uploaders/__init__.py`. Add a registry only if the strategy count grows beyond the small literal dispatch.
- **`shared/job/build.py` is service-agnostic.** It must never branch on
  `JobSpec.service` (regression-guarded by
  `tests/test_backend_registry.py::TestBuildPyHasNoServiceSpecificBranches`).
  Service-specific description hooks live in `shared.spec`; execution remains
  in `server.submit`.
- **JobSpec splits backend-specific config from template passthrough.** `JobSpec.backend_spec` holds the typed Opts the backend's `build_spec_backend` produced (`AmlOpts` / `VolcanoOpts`). `JobSpec.extra` is a pure passthrough of the template's `_extra` block (user-defined sub-keys like `code_upload`). Consumers read `spec.backend_spec or AmlOpts()` for backend config; uploaders/user hooks read `spec.extra["<their_key>"]`. Build never inspects `extra`. Adding a new backend Opts field = touch only the Opts dataclass + its `from_template` classmethod.
- **Template fields that `amlt` doesn't understand go under `_extra`.** Templates carry `_extra: dict[str, Any]`; `shared/job/build.py` copies it verbatim to `JobSpec.extra`. Each backend's own typed config goes to `backend_spec`, not `extra`. `render_amlt_yaml` strips `_extra` before handing the raw YAML to amlt. New aj-only template fields = either declare on the backend's Opts dataclass (typed) or use `_extra.<your_key>` (opaque passthrough).
- **Errors carry actionable detail.** Every broad `except` calls `log.exception(...)`, surfaces `({type}: {msg})` plus a hint to set `AJ_DEBUG=1` for a full traceback. Subprocess failures must include the captured `stdout` / `stderr` and the command that ran. Never reduce a real error to a generic "X failed" string.
