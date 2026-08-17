# Templates and JobSpec

## Status

**State:** Implemented.

## Context

Templates are user-authored and backend-specific. The HTTP contract needs one
stable, typed job description without teaching shared code every backend.

## Goals

- Resolve reusable YAML deterministically.
- Build `JobSpec` before any remote mutation.
- Keep common fields backend-neutral.
- Carry typed backend options across HTTP.
- Preserve raw template data only where compatibility requires it.

## Non-goals

- Executing submissions in the template engine.
- Interpreting consumer-owned `_extra` in shared build code.
- Representing multiple independent `jobs[]` entries.
- Exposing server implementations to clients.

## Design

### Template resolution

```text
leaf YAML
  → resolve recursive `base`
  → merge `config`
  → construct typed Template
```

Plain base names resolve beside the current file. Dotted names such as
`storage.default` resolve under `AJ_HOME/storage/default.yaml`.

| Values at one key | Merge rule |
| --- | --- |
| dictionaries | recursive |
| lists containing dictionaries | by index; retain unmatched items |
| scalar-only lists | concatenate |
| scalar or mixed values | last value wins by deep copy |

Missing bases and cycles fail before submission.

### Local build

```text
Template + CLI shape + command
  → require nodes/GPU from this invocation or YAML
  → resolve SKU
  → backend description hook
  → normalize name
  → inject stable `AJ_*` variables
  → JobSpec
```

`shared/job/build.py` handles common fields only. It never branches on
`service`.

Homogeneous nodes resolve from `-n` or `jobs[0].instance_count`; GPUs per node
resolve from `-p` or `target.gpus_per_node`. `aj run` does not persist either
value. Heterogeneous backends may derive their complete shape through the same
generic hook.

### JobSpec boundaries

| Field | Purpose |
| --- | --- |
| common scalar fields | name, shape, image, code, command, storage, env |
| `backend_spec` | typed `AmlOpts`, `VolcanoOpts`, or another backend model |
| `extra` | verbatim template `_extra` for its owning consumer |
| `template` | merged source retained for amlt compatibility rendering |

`backend_spec` and `extra` are deliberately separate. Typed backend behavior
does not become an opaque dictionary; opaque strategy data does not grow
top-level `JobSpec`.

### Description registry

Each service registers in `shared/spec.py`:

```text
build_spec_backend(template) → typed options
load_spec_backend(dict)      → typed options after HTTP
normalize_job_name(name)     → backend-safe name
```

Execution registration lives separately in `server/submit/`. Adding a backend
does not modify the shared builder.

### amlt compatibility

The amlt renderer starts from `Template.raw`, overlays aj-resolved job fields,
escapes shell dollars, and removes `_extra`. This path preserves compatibility
without making raw YAML the native execution contract.

## Invariants

- `JobSpec` is serializable and transport-neutral.
- Stable `AJ_*` values override conflicting template environment values.
- Backend options are typed and loaded symmetrically on both sides.
- `_extra` is copied verbatim and never inspected by shared build.
- One `jobs[0]` entry is the current native job contract.

## Failure handling

- Structural, inheritance, and backend-option errors fail before upload.
- Unknown services fail with the registered service list.
- Invalid names fail or normalize through the owning backend hook.
- Dry-run uses the same build path as submission.

## Evolution

New common behavior should use a narrow backend-neutral hook. Backend-only
fields belong in its typed options. Consumer-specific strategy data belongs
under `_extra.<consumer>`.
