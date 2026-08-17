# Agent Skill lifecycle

## Status

**State:** Implemented for GitHub Copilot, OpenAI Codex, and Claude Code.

## Context

The bundled Azure Jobs Skill must reach several agent-specific directories.
Those copies can be outdated, locally modified, unmanaged, or unsafe to
traverse. Installation must not overwrite user work.

## Goals

- Ship one canonical Skill with the package.
- Support user and project scope.
- Detect ownership and local modification.
- Update and uninstall transactionally.
- Reject unsafe filesystem entries.

## Non-goals

- Downloading Skill content at runtime.
- Editing unmanaged directories.
- Merging arbitrary local changes.
- Following symbolic links inside a managed tree.

## Design

### Canonical source and targets

`skills/azure-jobs/` is packaged with Azure Jobs. `AgentSpec` maps each agent
to its user and project destination. `--root` changes only the resolved root;
the agent-specific suffix remains controlled by aj.

### Ownership manifest

Each installed copy contains `.aj-skill.json` with:

- manifest schema;
- Azure Jobs version;
- agent and scope;
- digest of managed content.

The digest covers relative paths, executable bits, and file contents.

| State | Meaning |
| --- | --- |
| `current` | manifest and content match the bundled Skill |
| `outdated` | unchanged older managed copy |
| `modified` | managed copy changed locally |
| `unmanaged` | destination has no valid aj ownership |
| `invalid` | unsafe link or entry |

### Transactions

Install writes a complete candidate before placement. Update detaches the
existing managed tree to a unique backup, places the new copy atomically, and
restores the backup on failure.

Uninstall first detaches the exact verified tree, rechecks ownership, then
removes it. Missing destinations are idempotent. Modified copies require
explicit force; unmanaged and invalid copies are never removed.

## Invariants

- Only bundled files become managed content.
- Ownership is verified before replacement or deletion.
- Directory traversal uses `lstat` and rejects links.
- Project and user scope never share a destination accidentally.
- A failed update preserves one recoverable original copy.

## Failure handling

- Concurrent destination changes abort replacement.
- Failed placement restores the backup where possible.
- An unrestored backup path is reported explicitly.
- Unsupported scope, agent, manifest, or entry type fails without mutation.
- Multi-agent operations report each target independently.

## Evolution

Add an agent by defining one `AgentSpec` and exercising the shared status,
install, update, and uninstall contract. Change the manifest schema only when
ownership semantics change.
