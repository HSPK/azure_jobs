# TUI dashboard

## Status

**State:** Implemented.

## Context

The dashboard combines long-running I/O, mutable navigation, log streaming,
workspace changes, and destructive actions. Coupling these directly to
Textual widgets caused state drift, blocked workers, and focus-dependent
commands.

## Goals

- Keep canonical state independent of widgets.
- Keep blocking I/O off the UI thread.
- Make commands and capabilities explicit.
- Bound worker and log memory use.
- Preserve correct behavior across workspace changes and stale responses.

## Non-goals

- A second backend implementation.
- Widget-owned business state.
- Unbounded worker threads or full-log polling.
- Treating cancel and delete as one capability.

## Design

### Composition

`AjDashboard` is the composition root:

```text
AjDashboard
  ├── FeatureRegistry
  ├── controllers
  ├── immutable state + transition-owning stores
  ├── typed EventBus
  ├── narrow view ports
  ├── TaskRunner
  └── SDK session handles
```

The TUI uses the public SDK. It imports no Azure client or server module.

### State and controllers

Immutable state records describe jobs, logs, and workspace selection. Stores
own every state transition and run on the UI thread. Controllers perform I/O
and request transitions; they do not own a second mutable copy.

Typed events connect features. The event bus dispatches breadth-first so a
handler cannot recursively corrupt an in-flight transition.

### Presentation

View ports expose only what each controller needs. Textual adapters implement
those ports. `CommandRegistry` is the single source for key, visibility,
context predicate, handler, and help text.

The fixed status bar is independent of widget focus. Widget-local bindings
preserve navigation where Textual requires focus-specific handlers.

### Concurrency and sessions

`TaskRunner` uses a bounded pool and cooperative cancellation. Worker results
marshal through the UI dispatcher before touching stores.

Resource handles use leases. A workspace switch retires the old session;
close is deferred until active work releases its lease. New work cannot enter
a retired session.

### Logs

Log reads use short HTTP Range requests. `LogsStore` owns exact byte windows,
visual projections, deduplication, and memory accounting. Live tailing never
holds a worker indefinitely.

### Destructive actions

Cancel and delete are separate controller capabilities. Delete requires:

1. a terminal selected job;
2. user confirmation;
3. a fresh identity/status check;
4. the same workspace generation;
5. explicit handling of accepted-but-unknown outcomes.

## Invariants

- Only stores mutate canonical state.
- Background workers never mutate widgets or stores directly.
- Every command has validated metadata and a handler.
- Events are typed and non-recursive.
- Session closure waits for active leases.
- Logs are byte-bounded and range-based.

## Failure handling

- Generation tokens discard stale refresh, log, and workspace results.
- Workspace changes cancel or invalidate old work.
- User-visible errors are formatted at the presentation boundary.
- Ambiguous cancel/delete outcomes remain uncertain, never false success.
- Cleanup failures are logged and surfaced when they affect ownership.

## Evolution

Add a feature through state, store transitions, controller I/O, typed events,
a narrow view port, and feature registration. New screens remain optional
features rather than expanding the application root.
