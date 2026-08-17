# Agent Skill

`aj` ships an `azure-jobs` Agent Skill with templates, job operations, quota,
log, and Kubernetes diagnosis guidance. It supports GitHub Copilot, OpenAI
Codex, and Claude Code.

See [Agent Skill lifecycle design](design/agent-skill.md) for ownership,
transaction, and filesystem-safety rules.

## Install

The default target is all Agents in the current user's home:

```bash
aj skill install
aj skill status
```

Select one Agent:

```bash
aj skill install copilot
aj skill install codex
aj skill install claude
```

Project scope installs at the detected Git root:

```bash
aj skill install --project
aj skill status --project
```

Paths:

| Agent | User scope | Project scope |
| --- | --- | --- |
| GitHub Copilot | `~/.copilot/skills/azure-jobs` | `.github/skills/azure-jobs` |
| OpenAI Codex | `~/.agents/skills/azure-jobs` | `.agents/skills/azure-jobs` |
| Claude Code | `~/.claude/skills/azure-jobs` | `.claude/skills/azure-jobs` |

Claude user scope honors `CLAUDE_CONFIG_DIR`. `--root PATH` overrides the
detected Git root or user home and appends the Agent-specific directory:

```bash
aj skill install copilot --project --root /path/to/repository
aj skill install codex --user --root /path/to/home
```

## Status and update

```bash
aj skill status [all|copilot|codex|claude]
aj skill update [all|copilot|codex|claude]
```

Status values:

| Status | Meaning |
| --- | --- |
| `not_installed` | destination does not exist |
| `current` | installed files match this `aj` version |
| `outdated` | an unchanged older managed copy is installed |
| `modified` | files changed after installation |
| `unmanaged` | destination exists without a matching aj manifest |
| `invalid` | destination contains an unsafe link or non-regular entry |

Upgrade `azure-jobs` first to obtain a newer bundled Skill, then run
`aj skill update`. A modified managed copy is preserved unless replacement is
explicit:

```bash
aj skill update copilot --force
```

Missing Agent copies are left unchanged; use `install` to add them.

## Uninstall

```bash
aj skill uninstall
aj skill uninstall claude --project
```

Uninstall is idempotent for missing copies. It refuses unmanaged or invalid
directories, even with `--force`. A locally modified aj-managed copy requires:

```bash
aj skill uninstall codex --force
```

## JSON

Place the global flag first:

```bash
aj --json skill status
aj --json skill install copilot --project
```

Status emits a table envelope. Install, update, and uninstall emit one
`command_result` envelope with a result for each selected Agent. Manager
failures also emit a failed `command_result` and exit nonzero.
