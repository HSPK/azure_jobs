# Client and daemon

## Status

**State:** Implemented since `v0.4.0`.

## Context

CLI invocations are short-lived, while authentication, queues, watches, and
cloud clients benefit from a long-lived owner. Multiple frontends also need one
consistent execution path.

## Goals

- Keep Azure credentials and execution out of clients.
- Serve CLI, TUI, and Python through one public SDK.
- Preserve project and workspace isolation.
- Support concurrent requests, queues, watches, and progress events.
- Make the local protocol inspectable and versioned.

## Non-goals

- Remote TCP access.
- An in-process fallback.
- Client-side Azure discovery or submission.
- Automatic replay of mutations.

## Design

### Request path

```text
Click / Textual / Python
  → SDK resource namespace
  → httpx over UDS
  → FastAPI `/v2`
  → resource adapter
  → Azure client or submission backend
```

SDK methods use direct HTTP paths. FastAPI exposes the same contract through
OpenAPI. `tests/test_openapi_contract.py` checks both sides for drift.

### Transport

| Concern | Contract |
| --- | --- |
| endpoint | private Unix domain socket |
| API version | `/v2`, negotiated through `/v2/info` |
| project scope | absolute `AJ_HOME` in `X-AJ-Root` |
| client identity | version in `X-AJ-Client` |
| progress | server-sent events |
| log windows | HTTP Range and `206` |
| local permissions | `0700` runtime directory, `0600` socket |

`connect()` can start the daemon. Startup is lock-protected so concurrent
clients do not spawn competing processes. The SDK verifies socket ownership
before connecting.

### Resource namespaces

The SDK exposes account resources and workspace-scoped resources. Workspace
names cross the wire; the daemon resolves Azure coordinates and owns all Azure
clients.

```text
AjClient
├── auth, subscription, ws, sku, quota, image, ...
└── workspace scope
    ├── job and log
    ├── submission and queue
    ├── watch
    └── datastore, environment, compute, quota
```

### Context lifetime

`ContextRegistry` keys contexts by:

```text
(absolute AJ_HOME, resolved target ID)
```

A context owns resource adapters, Azure sessions, one submission queue, and
one watcher. Named workspace resolution is cached for a bounded period;
configured-workspace lookup is not, so configuration changes take effect
without a daemon restart.

Idle contexts are reaped only when no queue or active watch needs them. Normal
daemon retirement waits for accepted submissions.

### Queue and watches

Each context has a persistent single-worker submission queue. Serializing the
upload-and-submit phase avoids local resource races; remote jobs may run
concurrently after acceptance.

Watch journals survive daemon restart. A reopened context restores them,
polls status in the daemon, and publishes transitions over SSE.

## Invariants

- Client code imports neither `server` nor Azure clients.
- Server code imports no client or presentation package.
- SDK imports neither client nor server.
- `shared` imports neither side.
- HTTP connections do not own context lifetime.
- No request bypasses the daemon after transport failure.

## Failure handling

- Startup and version errors include a recovery command.
- Typed server errors are reconstructed by the SDK.
- Read-only calls may be retried by callers.
- Lost mutation responses are reconciled before any retry.
- SSE reconnect restores observation, not the mutation that created work.
- Forced daemon shutdown is explicit because it can abandon accepted work.

## Evolution

Remote HTTP requires a separate security model: authenticated artifact upload,
authorization, tenant isolation, server-owned paths, filtered events, and
idempotency keys. The local UDS contract must not be exposed directly over TCP.
