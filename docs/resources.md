# CLI resources and configuration

Run `aj --help` for the authoritative command tree and
`aj <group> <command> --help` for flags.

## Top-level groups

These headings and commands match the current `aj --help`:

| Help group | Commands |
| --- | --- |
| Getting Started | `init`, `run`, `dash` |
| Jobs | `job`, `exp`, `queue`, `watch`, `list` |
| Azure Resources | `ws`, `ds`, `env`, `image`, `sku`, `quota`, `sa`, `uai` |
| Kubernetes | `k8s` (`k`) |
| Project | `template`, `config`, `code`, `skill` |
| System | `auth`, `daemon` |

There is no `aj resources` command.

## Azure resource map

| Command | Purpose |
| --- | --- |
| `aj ws list` | list Azure ML workspaces in the active subscription |
| `aj ws set [NAME]` | select and persist the active workspace |
| `aj ws show [NAME]` | show configured or named workspace coordinates |
| `aj ds list [--ws NAME]` | list workspace datastores |
| `aj ds show NAME [--ws NAME]` | inspect one datastore |
| `aj env list [--ws NAME]` | list workspace environments |
| `aj env show NAME [-n N] [--ws NAME]` | list environment versions |
| `aj image list [-f TEXT]` | list/filter native Singularity images |
| `aj sku list [--all]` | list Singularity instance types by visible VC |
| `aj quota list [--sing] [--all] [--full]` | list Singularity quota |
| `aj quota list --aml [--all]` | list AML compute availability |
| `aj sa list` | list storage accounts across accessible subscriptions |
| `aj uai list [--full]` | list user-assigned managed identities |

The daemon performs Azure discovery and REST calls. The CLI only requests and
renders results.

## Project commands

```bash
aj init
aj init -f
aj init amlt

aj template list
aj template show <name>
aj template validate [name]
aj template init [name]
aj template pull [repository]
aj template diff
aj template push -m "message"

aj config show
aj config timezone [IANA_NAME]
aj config experiment [NAME]

aj code stats -t <template>
aj code stats --list-all

aj skill status
aj skill install [all|copilot|codex|claude]
aj skill update [all|copilot|codex|claude]
aj skill uninstall [all|copilot|codex|claude]
```

`aj init` and `aj template init` are interactive. Template pull/push preserves
local-only `aj_config.json`, `record.jsonl`, submissions, logs, and
`daemon/` queue/watch journals. The journals can contain job environment
values and are never copied to the template repository.

Skill commands are local-only and do not contact the daemon or cloud. See
[Agent Skill](skills.md) for user/project paths, `--root`, update protection,
and JSON output.

Kubernetes commands are also local-only and call `kubectl` directly:

```bash
aj k install
aj k login
aj k status
aj k jobs
aj k pods --job <generated-job-name>
aj k logs <pod>
aj k events <pod>
aj k delete <generated-job-name>
```

See [Kubernetes](kubernetes.md) for setup side effects, confirmation, context,
namespace, redaction, and deletion rules.

Configuration is stored in `.azure_jobs/aj_config.json`:

```json
{
  "defaults": {"template": "gpu"},
  "workspace": {
    "subscription_id": "...",
    "resource_group": "...",
    "workspace_name": "..."
  },
  "experiment": "training",
  "repo_id": "git@github.com:owner/templates.git",
  "timezone": "UTC",
  "dashboard": {"page_size": 50}
}
```

`aj run` does not write defaults. Legacy `defaults.nodes` and
`defaults.processes` values are ignored; node/SKU shape comes from current CLI
arguments or template YAML.

## Auth and daemon

```bash
aj auth status

aj daemon status
aj daemon start
aj daemon stop
aj daemon stop --force
aj daemon restart
```

Authentication itself remains `az login`. `auth status` reports the account
and token health as seen by the daemon.

Normal daemon stop drains accepted submissions. `--force` can lose the
outcome of work already running; use it only when necessary.

## JSON output

Place the global flag before the command:

```bash
aj --json ws list
aj --json job list | jq '.rows'
aj --json template show gpu | jq '.config'
```

Resource tables emit one document containing `title`, `columns`, `rows`, and
`metadata`. Detail views use `fields` and `data`. Action-oriented commands use
envelopes such as `kind: command_result`, `template_detail`, or
`submission_result`.

`AJ_OUTPUT=json` selects the same mode. Interactive `aj init` and
`aj template init` reject JSON mode instead of prompting. JSON output is the
automation surface; do not parse Rich text.
