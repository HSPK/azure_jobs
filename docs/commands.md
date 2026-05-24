# Commands

`aj run` is documented in the [Tutorial](tutorial.md) and on the [home page](index.md). Everything else lives here.

## Job management

```bash
aj job list                      # recent cloud jobs
aj job list -s Running           # filter by status
aj job show <id>                 # detail panel
aj job cancel <id>
aj job logs <id>                 # download + display logs
aj job stats                     # GPU-hours, success rate, by experiment/compute/user
aj list                          # local submission history (record.jsonl)
aj dash                          # interactive TUI dashboard
```

## Templates

```bash
aj template list                 # available templates
aj template show <name>          # resolved config (after inheritance)
aj template validate             # check all templates
aj template diff                 # local edits vs upstream
aj template pull <repo>          # clone a template repo
aj template push -m "msg"        # commit + push
```

See [configuration.md](configuration.md) for template syntax, inheritance, and merge rules.

## Workspace & auth

```bash
aj ws list                       # workspaces in subscription
aj ws set                        # interactive picker
aj auth status                   # credential health
aj auth login                    # delegate to az login
```

## Compute, quota, SKUs

```bash
aj quota list                    # Singularity VC quota
aj quota list --aml              # AML cluster availability
aj sku list                      # SKUs by VC
aj sku check -t <template>       # pre-flight: SKU/quota/compute (auto-toggles -NvLink)
aj env list                      # registered environments
aj env show <name>
aj ds list                       # datastores
aj ds show <name>
aj image list                    # Singularity curated images
aj exp list                      # experiments (aggregated from jobs)
aj uai list                      # user-assigned managed identities
aj sa list                       # storage accounts in the active subscription
```

## Code upload preview

```bash
aj code stats -t gpu             # file count, total size, content hash
aj code stats -t gpu --list-all  # full file listing
aj code stats -t gpu -n 20       # top-N largest files
```

## Setup

```bash
aj init                          # scaffold .azure_jobs/, register workspace
aj init amlt                     # additionally configure amlt integration
```

## Tool config

```bash
aj config show
aj config timezone Asia/Shanghai
aj config experiment <name>
```

`aj_config.json` lives at `.azure_jobs/aj_config.json`. Override `AJ_HOME` to relocate.

## Agent skill

`aj` ships a `SKILL.md` so coding agents (GitHub Copilot, Claude Code) can
discover and call `aj` correctly. Install it once per machine:

```bash
aj skill install                 # both copilot + claude, user scope
aj skill install -t copilot      # only one
aj skill install -s project      # ./.copilot/skills, ./.claude/skills
aj skill show                    # print the bundled SKILL.md
```

Targets the conventional layout: `~/.copilot/skills/aj/SKILL.md` and
`~/.claude/skills/aj/SKILL.md`.

**Run `aj init` first.** The skill assumes `.azure_jobs/aj_config.json`
and at least one template exist — `aj init` is interactive (picks a
workspace) so the agent does not run it for you.

Example prompt once the skill is installed:

> *"Submit a single-GPU smoke-test Azure ML job on `gpu-a100` that prints
> all `AJ_*` env vars."*

The agent will write a one-liner `smoke.sh` and run
`aj run -t gpu-a100 -n 1 -p 1 smoke.sh`.
