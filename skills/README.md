# Agent Skills

Azure Jobs ships the self-contained `azure-jobs` Agent Skill in every wheel.
Install it for all supported agents in the current user's home:

```bash
aj skill install
aj skill status
```

Supported targets:

| Agent | User | Project |
| --- | --- | --- |
| GitHub Copilot | `~/.copilot/skills/azure-jobs` | `.github/skills/azure-jobs` |
| OpenAI Codex | `~/.agents/skills/azure-jobs` | `.agents/skills/azure-jobs` |
| Claude Code | `~/.claude/skills/azure-jobs` | `.claude/skills/azure-jobs` |

Select one Agent and project scope:

```bash
aj skill install copilot --project
aj skill install codex --project
aj skill install claude --project
```

`--root PATH` overrides the detected Git root or user home. `status`, `update`,
and `uninstall` accept the same `AGENT`, scope, and root arguments:

```bash
aj skill update
aj skill uninstall
```

Copies contain an aj management manifest. Update protects local edits unless
`--force` is explicit, and uninstall never removes an unmanaged directory.
The canonical development source remains `skills/azure-jobs/`.
