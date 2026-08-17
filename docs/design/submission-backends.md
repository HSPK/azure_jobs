# Submission backends

## Status

**State:** Implemented for native AML, Singularity, Volcano, and amlt
compatibility.

## Context

Backends share job description and progress contracts but differ in target
resolution, artifact transfer, payloads, and failure semantics.

## Goals

- Dispatch execution without service branches in clients or shared build.
- Reuse deterministic artifact handling.
- Keep backend-specific dependencies inside the daemon.
- Report progress and actionable failures through one contract.
- Make new backends additive.

## Non-goals

- Hiding backend capabilities behind a lowest-common-denominator payload.
- Client-side execution.
- Blind retry of submit requests.
- A registry for every small strategy choice.

## Design

### Registry and flow

```text
POST submission
  → reconstruct JobSpec and backend_spec
  → get_backend(spec.service)
  → backend(spec, on_event)
  → JobResult
```

`shared/spec.py` registers description hooks. `server/submit/__init__.py`
registers executable functions. The split keeps wire description independent
from daemon-only libraries.

### Deterministic code archive

Native AML/Sing and Volcano Blob upload use one backend-neutral archive:

1. select files using built-in, template, and ignore-file rules;
2. add generated runtime files;
3. write deterministic tar and gzip metadata;
4. compute SHA-256;
5. upload or reuse a content-addressed object;
6. verify and extract once in the job.

Archive paths are validated. Generated `aj_runner.sh` wins over a project file
with the same path.

### Native AML and Singularity

The native backend resolves workspace, compute or VC, identity, environment,
storage, distribution, and resources before mutation. It uploads one archive
as a `uri_file` and submits an Azure ML CommandJob through REST.

Singularity can select a VC and tier when `target.name` is empty. Selection
uses exact GPU and optional accelerator/memory requirements, current user and
tier quota, then stable ranking.

### Volcano

The Volcano backend builds one `batch.volcano.sh/v1alpha1` Job and runs
`kubectl create`.

Code upload uses the narrow `CodeUploader` protocol:

| Strategy | Path |
| --- | --- |
| `kubectl-exec` | helper Pod and PVC, then `tar \| kubectl exec` |
| `blob` | archive to Blob, Pod downloads with `azcopy` and verifies SHA-256 |

Two strategies use direct dispatch in `pick_uploader`; a registry is
unnecessary at this size. Blob storage mounts use blobfuse2 and a short-lived
SAS in a Kubernetes Secret.

Templates may expand one Job into typed heterogeneous Tasks with independent
Pod specs. See [Heterogeneous Volcano tasks](volcano-heterogeneous-tasks.md).

### amlt

The compatibility backend writes resolved YAML and invokes `amlt run` in the
daemon. It is not the native data or management path.

## Invariants

- Only the daemon imports Azure clients or invokes submission tools.
- Backend functions satisfy the same `JobResult` and event contract.
- `shared/job/build.py` contains no service-specific branch.
- Uploaders return `CodeUploadResult` and do not own unrelated behavior.
- Large injected scripts are standalone package resources.
- Pod-side binary dependencies are installed or fail loudly.

## Failure handling

- Read-only resolution precedes artifact upload and remote mutation.
- Subprocess errors include command, status, stdout, and stderr.
- Archive and checksum mismatch errors stop execution.
- Volcano credential Secrets are removed when Job creation fails.
- A lost response after submission is ambiguous; callers reconcile by ID,
  name, and timestamp before retrying.

## Evolution

Add a backend through typed options, shared description registration, one
daemon implementation, execution registration, and contract tests. Add an
uploader registry only when literal dispatch stops being clear.
