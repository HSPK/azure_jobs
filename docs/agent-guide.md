# Agent guide

Machine-oriented repository facts and constraints.

## Source-of-truth order

1. Current `src/azure_jobs/` source.
2. Generated `aj --help` and subcommand help.
3. Contract/regression tests under `tests/`.
4. `pyproject.toml` and workflows.
5. Documentation.

When they disagree, follow code/tests and update docs. Never infer availability
from old examples or roadmap text.

## Runtime facts

- Entry point: `azure_jobs.client.cli:main`.
- Python: 3.10+.
- Azure execution: daemon only; no in-process mode.
- Transport: HTTP over private UDS; API `/v2`; schema `/openapi.json`.
- Project scope: `X-AJ-Root` is absolute `AJ_HOME`.
- Client performs no Azure token acquisition, discovery command, or submission.
- Public SDK: `from azure_jobs import connect`.
- Services: native `aml`/`sing`, `volcano`, compatibility `amlt`.
- Remote HTTP/TCP: not implemented.

## Hard invariants

### Layers

```text
shared imports neither client nor server
sdk imports neither client nor server
client never imports server or Azure clients
server never imports client
Azure clients exist only under server
Rich/Textual exist only under client
```

Guard: `tests/test_api_architecture.py`.

### Daemon

- No `AJ_NO_DAEMON` or alternative client.
- Daemon failure is actionable, never bypassed.
- Public transport does not replay requests automatically.
- Ambiguous mutations require reconciliation before caller retry.
- `.azure_jobs/daemon/` journals may contain full queued `JobSpec.env_vars`;
  template sync must always treat that directory as local-only.

Guard: `tests/test_api_daemon_only.py`.

### JobSpec and backends

```text
backend_spec = typed backend Opts
extra        = verbatim template _extra
template     = source Template for amlt rendering
```

- `shared/job/build.py` never branches on service.
- Description hooks register in `shared/spec.py`.
- Execution functions register in `server/submit/`.
- `_extra` is stripped before amlt.
- Do not add strategy-specific top-level `JobSpec` fields.

Guard: `tests/test_backend_registry.py`.

### API

- SDK and FastAPI intentionally use direct path strings.
- Add every SDK operation to `tests/test_openapi_contract.py`.
- Submit/watch progress uses SSE.
- Logs use HTTP Range.
- Typed errors survive the wire.
- `X-AJ-Root` scopes state; UDS permissions authenticate locally.

### Scripts and errors

- New injected script around 15+ lines: standalone resource.
- New parameterized resources use the owning `_scripts.load_script`;
  placeholders are literal `{KEY}`. Existing Volcano preamble/blobfuse loaders
  are legacy exceptions.
- Force-install pod dependencies or fail loudly.
- Broad catch calls `log.exception`.
- User error includes type/message and `AJ_DEBUG=1` guidance.
- Subprocess failure includes command, status, stdout, and stderr.

## File map

| Change | Primary location |
| --- | --- |
| CLI tree/commands | `client/cli/__init__.py`, `client/cli/` |
| Agent Skill lifecycle | `client/skill_manager.py`, `skills/azure-jobs/` |
| Rich/JSON output | `client/ui/` |
| SDK namespaces | `sdk/account.py`, `sdk/workspace.py`, `sdk/logs.py` |
| UDS/SSE | `sdk/_transport.py` |
| HTTP routes | `server/app.py` |
| contexts/queue/watch | `server/context.py`, `server/queue.py`, `server/watch.py` |
| wire adapters | `server/resources.py` |
| Azure HTTP | `server/az_client/arm/`, `server/az_client/ml/` |
| template engine | `shared/template/` |
| JobSpec build | `shared/job/` |
| backend hooks/options | `shared/spec.py`, `shared/opts/` |
| native submit/archive | `server/submit/azureml/`, `server/submit/archive.py` |
| Volcano | `server/submit/volcano/` |
| amlt | `server/submit/amlt.py`, `shared/job/render.py` |
| TUI | `client/tui/` |

## Change recipes

### New daemon capability

```text
shared wire type if needed
→ server resource method
→ FastAPI route
→ SDK namespace direct path
→ OpenAPI CALLS inventory
→ CLI/TUI presentation
```

Never import server code from SDK/client.

### New backend

```text
typed Opts + from_template/load
→ register_spec(build/load/normalize)
→ daemon implementation
→ register_backend
→ backend/registry tests
```

Never add a service branch to shared build or client entry points.

### New template field

```text
backend behavior → typed backend Opts
opaque aj feature → _extra.<consumer>
amlt compatibility → raw template/render overlay
```

Consumer schemas do not belong in `shared/job/build.py`.

### New Volcano uploader

Implement `CodeUploader.prepare`, keep its parser/scripts beside it, add one
literal `pick_uploader` branch, and test uploader plus entry flow. Keep literal
dispatch while strategy count is small.

### New TUI workflow

Add command metadata, controller I/O, store transition, typed event, and view
port. Worker threads cannot mutate bound stores directly.

## Backend facts

Native AML/Sing:

- archive current working directory with built-in and user ignores;
- generated `aj_runner.sh` overrides a matching project path;
- project `.ssh/` takes precedence; otherwise an optional home whitelist is
  added unless `AJ_SHIP_SSH=0`;
- deterministic tar/gzip; SHA-256 addresses one Blob at
  `LocalUpload/<hash>/code.tar.gz`;
- Azure ML downloads one `uri_file`;
- node verifies SHA-256 and extracts once through `AJ_ID` lock/ready state;
- process runs `bash aj_runner.sh`;
- no Code Asset and no per-file native upload.

Sing auto-selection:

- missing/empty `target.name` is allowed for `service: sing`;
- target subscription/resource group are optional VC filters;
- exact GPU count; exact accelerator/per-GPU memory when specified;
- current user and tier quota must cover capacity;
- ranking: tier, per-VC NVIDIA preference when accelerator is omitted, NVLink,
  remaining quota, stable coordinates;
- explicit VC uses the same matching/tier fallback.

Volcano:

- `kubectl-exec`/PVC and Blob archive are the two upload strategies;
- uploader dispatch is literal, not a registry;
- pod binaries are installed or submission fails;
- CPU-only means no GPU/RDMA unless explicit.

## Verification matrix

| Area | Minimum tests |
| --- | --- |
| templates/JobSpec | `test_merge_confs.py`, `test_read_conf.py`, `test_submit.py` |
| backend hooks | `test_backend_registry.py` |
| SDK/API | `test_sdk.py`, `test_openapi_contract.py`, `test_api_contract.py` |
| transport | `test_api_transport.py`, `test_api_daemon_only.py` |
| native archive | `test_server_submit_archive.py`, `test_server_azureml_bootstrap_payload.py` |
| Sing | `test_sing_auto_selection.py`, `test_sku.py`, `test_quota.py` |
| Volcano | `test_volcano_uploaders.py`, `test_volcano_storage.py` |
| CLI JSON | `test_json_audit.py` plus relevant CLI test |
| Agent Skill | `test_skill_manager.py`, `test_cli_skill.py`, `test_skill_assets.py` |
| TUI | `test_tui_architecture.py` plus relevant feature test |
| docs | `uv run mkdocs build --strict` |

Full suite:

```bash
uv run pytest -x -q \
  --cov=azure_jobs --cov-branch --cov-report=term-missing
```

Coverage threshold: 95% combined line/branch.

## Prohibited shortcuts

- In-process fallback or client-side Azure execution.
- Shared route builder that hides SDK/OpenAPI drift.
- Service branch in `shared/job/build.py`.
- Backend typed config in `extra`; `_extra` parsing in shared build.
- Native Azure ML Code Asset or per-file upload.
- Large inline pod scripts or assumed pod binaries.
- Flattened errors, swallowed subprocess output, or automatic mutation replay.
- TCP exposure without authentication, authorization, artifact upload,
  isolation, filtered SSE, explicit identity, and idempotency.
- Documentation of unimplemented behavior.
