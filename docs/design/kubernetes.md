# Kubernetes management

## Status

**State:** Implemented with a hybrid boundary. Daemon-backed task management is
planned in [issue #15](https://github.com/HSPK/azure_jobs/issues/15).

## Context

Cluster login modifies the host and requires an interactive terminal.
Submission, however, belongs to the same daemon execution path as other
backends. The first management release prioritized safe narrow commands over a
new daemon API.

## Goals

- Separate one-time installation from daily login.
- Preserve the user's kubeconfig transactionally.
- Provide bounded, job-scoped Volcano operations.
- Keep credential-bearing output redacted.
- Make destructive scope exact.

## Non-goals

- Creating Kubernetes control-plane or worker nodes.
- A generic `kubectl` passthrough.
- Silent sudo or package changes.
- Broad selectors or namespace-wide deletion.

## Design

### Current boundary

| Operation | Owner | Reason |
| --- | --- | --- |
| `aj k install` | client | sudo, apt, Krew, host files |
| `aj k login` | client | device-code prompt and kubeconfig |
| status/jobs/pods/logs/events/delete | client | current hybrid implementation |
| `aj run` with Volcano | daemon | shared submission contract |

`aj k setup` is a compatibility alias for login and does not install tools.

### Install

Install checks existing tools before changing the host. It detects conflicting
apt sources, requires an explicit override, and installs `kubectl`, Krew, and
`oidc-login`. Dry-run exposes the plan without mutation.

### Login

Login resolves the OIDC plugin, builds a profile, and merges kubeconfig
atomically:

1. preserve the existing file and namespace;
2. write a candidate configuration;
3. expose the device-code prompt directly;
4. verify identity with `kubectl auth whoami`;
5. derive the matching namespace group;
6. restore the previous file on failure.

Authentication happens before namespace authorization. An `Unauthorized`
response is therefore not caused by choosing the wrong namespace.

### Task operations

Commands expose a narrow resource model:

- context, namespace, and kubeconfig are explicit;
- jobs, pods, queues, and events return selected fields;
- logs and events are bounded and redacted;
- pod operations are scoped to one Job;
- delete names one Job exactly and cleans only its referenced aj credential
  Secret.

Raw Job YAML and broad `describe` output are avoided because command arguments
may contain short-lived credentials.

## Invariants

- Host installation and login remain client-local.
- Volcano submission remains daemon-backed.
- No command accepts an unrestricted resource selector.
- Output redaction runs before terminal or JSON rendering.
- Delete requires an exact kind, namespace, and name.
- Setup changes are previewable and explicit.

## Failure handling

- Package, plugin, and subprocess failures include the command and output.
- Login timeout leaves no partial kubeconfig.
- Failed identity verification restores the prior configuration.
- Read-only commands do not hide partial cluster errors.
- Delete reports Job and Secret outcomes separately.

## Evolution

Move status and task operations behind typed `/v2` routes while keeping
install and login local. Preserve context, namespace, redaction, exact-delete,
JSON, and OpenAPI contracts during that migration.
