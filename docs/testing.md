# Testing

## Default hermetic suite

```bash
uv run pytest -q
```

The normal suite does not require Azure access and does not submit real jobs.
Daemon integration tests run a real FastAPI/uvicorn server over a Unix socket
with deterministic fake resources.

CI runs Python 3.10 and 3.12:

```bash
uv run pytest -x -q \
  --cov=azure_jobs \
  --cov-branch \
  --cov-report=term-missing
```

`pyproject.toml` enforces at least 95% combined line/branch coverage.

## Test tiers

| Tier | Marker or command | Contract |
| --- | --- | --- |
| unit/integration | default | hermetic; fake Azure; no real submission |
| Azure HTTP unit | `azure_unit` | mocked HTTP for real Azure resolvers |
| stress | `stress` | local concurrency/load; submission explicitly unused |
| live | `live` plus `AJ_LIVE_E2E=1` | one real Azure job; opt-in and chargeable |

Useful targeted runs:

```bash
uv run pytest tests/test_merge_confs.py
uv run pytest tests/test_cli_run.py::TestRunCommand::test_dry_run_creates_submission_file -v
uv run pytest -q tests/test_openapi_contract.py tests/test_api_architecture.py
```

## Contract and drift tests

Important guard suites:

- `test_api_architecture.py`: client/server/shared/sdk dependency boundaries;
- `test_api_daemon_only.py`: no in-process mode;
- `test_openapi_contract.py`: direct SDK strings match FastAPI OpenAPI;
- `test_backend_registry.py`: service-agnostic `shared/job/build.py`;
- `test_server_submit_archive.py`: deterministic safe archive;
- `test_sing_auto_selection.py`: exact matching and ranking;
- `test_json_audit.py`: structured CLI output;
- `test_cli_template_init_quota_sku.py`: interactive template bootstrap against
  real SDK contract models;
- `test_tui_architecture.py`: bounded workers, state ownership, lifecycle.

### Use real contract models in consumer tests

When a client consumes an SDK result, tests must return the same shared model
as production (`Target`, `CatalogItem`, `Job`, and so on). Prefer constructing
the model through `from_json(...)`, optionally round-tripping `to_json()`, when
the production SDK also decodes that wire shape.

Do not use `SimpleNamespace` or an unrestricted `MagicMock` as the returned
resource object. Those doubles can retain removed attributes and let a client
keep using an obsolete SDK shape without failing. Mock the namespace or
transport call, but keep its return value contract-accurate.

For metadata-backed `Target` consumers, cover both the metadata fields and the
documented `label`/`detail` fallbacks. When changing a shared contract, search
all SDK consumers and run their CLI/TUI tests in addition to the model and
transport suites.

## Stress tests

```bash
uv run pytest -q -m stress tests/test_stress.py
```

They exercise:

- 500 concurrent requests through one pooled HTTP client;
- 120 short-lived clients sharing one workspace context;
- concurrent failures mixed with healthy requests;
- bounded event fan-out to slow subscribers;
- an explicit assertion that submission was never called.

Stress tests never submit cloud jobs.

## Live Azure E2E

The live test submits one minimal job and can incur cost:

```bash
AJ_LIVE_E2E=1 \
uv run pytest -q -s -m live tests/test_live_submit.py
```

Automatic selection prefers:

1. AML compute with an idle node;
2. Singularity with available user quota;
3. busy AML compute;
4. AML compute that may need to scale.

Force one service:

```bash
AJ_LIVE_E2E=1 AJ_LIVE_SERVICE=aml \
uv run pytest -q -s -m live tests/test_live_submit.py

AJ_LIVE_E2E=1 \
AJ_LIVE_SERVICE=sing \
AJ_LIVE_SING_UAI=/subscriptions/.../userAssignedIdentities/... \
uv run pytest -q -s -m live tests/test_live_submit.py
```

Exercise server-side automatic VC selection with an exact 1-node, 1-GPU
A100 40GB request:

```bash
AJ_LIVE_E2E=1 \
AJ_LIVE_SERVICE=sing \
AJ_LIVE_SING_SKU=1x40G1-A100 \
AJ_LIVE_SING_TIER=Premium \
AJ_LIVE_WORKSPACE=<workspace-with-attached-UAI> \
AJ_LIVE_SING_UAI=/subscriptions/.../userAssignedIdentities/... \
uv run pytest -q -s -m live tests/test_live_submit.py
```

Exercise a one-node CPU size-tier request:

```bash
AJ_LIVE_E2E=1 \
AJ_LIVE_SERVICE=sing \
AJ_LIVE_SING_SKU=1xC1 \
AJ_LIVE_SING_TIER=Basic \
AJ_LIVE_WORKSPACE=<workspace-with-attached-UAI> \
AJ_LIVE_SING_UAI=/subscriptions/.../userAssignedIdentities/... \
uv run pytest -q -s -m live tests/test_live_submit.py
```

CPU live jobs use a `bash` marker because a valid Sing CPU image may not expose
`python` on `PATH`. `C1` is a CPU size tier, not one GPU or one launcher
process.

In that mode `JobSpec.backend_spec.compute` is deliberately empty. The test
verifies the auto-selection progress event, selected VC/tier/instance
metadata, terminal completion, expected stdout marker, and cleanup.

`AJ_LIVE_TIMEOUT` sets the terminal wait, default 600 seconds. Sing uses the
explicit UAI when provided, otherwise the first user-assigned identity attached
to the selected workspace. Without a usable identity, automatic mode falls
back to AML.

The payload is one tiny file and one `python -c` print. SSH shipping is
disabled. Cleanup reconciles ambiguous submission, then attempts cancel and
delete in `finally`.

## Documentation build

```bash
uv run mkdocs build --strict
```

Run this after any documentation or navigation change.
