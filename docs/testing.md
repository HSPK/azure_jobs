# Testing

## Default suite

```bash
uv run pytest -q
```

The normal suite is hermetic. It uses a real FastAPI/uvicorn daemon over a Unix
socket with fake Azure resources. The live submission test is collected but
skipped.

## Coverage

```bash
uv run pytest -q \
  --cov=azure_jobs \
  --cov-branch \
  --cov-report=term-missing
```

CI enforces at least 78% combined line/branch coverage. Tests explicitly cover
typed exception round-trips, unknown remote errors, debug traceback behavior,
HTTP decode failures, protocol mismatch, Azure data-plane fallbacks and
resource cleanup.

## Stress tests

Stress tests do **not** submit jobs:

```bash
uv run pytest -q -m stress tests/test_stress.py
```

They exercise:

- 500 concurrent requests through one pooled HTTP client;
- 120 short-lived clients sharing one workspace context;
- concurrent errors mixed with healthy requests;
- bounded event fan-out to slow subscribers;
- an explicit assertion that submission was never called.

## Live Azure E2E

The live test submits one real minimal job and can incur Azure cost. It is
disabled unless explicitly enabled:

```bash
AJ_LIVE_E2E=1 \
uv run pytest -q -s -m live tests/test_live_submit.py
```

The selector compares available configurations:

1. an AML compute with an idle node;
2. a Singularity VC with available user quota;
3. a busy AML compute;
4. an AML compute that may need to scale.

Force one service when diagnosing a backend:

```bash
AJ_LIVE_E2E=1 AJ_LIVE_SERVICE=aml  uv run pytest -q -s -m live
AJ_LIVE_E2E=1 AJ_LIVE_SERVICE=sing \
AJ_LIVE_SING_UAI=/subscriptions/.../userAssignedIdentities/... \
uv run pytest -q -s -m live
```

`AJ_LIVE_TIMEOUT` controls the terminal-state timeout (default 600 seconds).
For Singularity, the test uses `AJ_LIVE_SING_UAI` when set, otherwise the first
user-assigned identity attached to the configured workspace. Without either,
auto mode selects AML.
The command is a single `python -c` print, code payload is one tiny file,
server-side SSH-key shipping is disabled, and the test attempts cancel/delete
cleanup in `finally`.
