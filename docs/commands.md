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
aj dash                          # initially load 50 jobs, 50 per page
aj dash -n 500 --page-size 40   # larger browse/filter scope
```

Inside `aj dash`, press `d` to permanently delete the selected terminal job.
Running or queued jobs must be canceled first.
Press `→` on the final loaded page to fetch another page from Azure.

## Background daemon

Every command executes through a local daemon, which shares authentication,
watches jobs, and runs the submission queue. It starts on demand. There is no
in-process mode: if the daemon cannot be reached, the command fails with the
steps needed to recover.

```bash
aj daemon status                 # is one running, and what is it doing
aj daemon start / stop / restart # restart after upgrading aj

# It speaks ordinary HTTP over a Unix socket, so it needs no special tooling:
curl -s --unix-socket ~/.cache/../aj/daemon.sock http://d/v1/info
curl -s --unix-socket ... http://d/openapi.json  # generated API schema
aj daemon stop --force           # stop even if submissions are running
```

### Submission queue

```bash
aj run --queue -t <template> <command>   # returns a ticket immediately
aj queue list                            # queued / running / recent
aj queue show <ticket>
aj queue wait <ticket>                   # block until terminal; exit 1 on failure
aj queue cancel <ticket>                 # only while still pending
```

Submissions run one at a time and survive the shell that started them. Pending
work is journalled, so a daemon restart does not lose it.

### Job watching and notifications

```bash
aj watch add <job>       # daemon keeps polling after your shell exits
aj watch list
aj watch listen          # stream changes, with OS notifications
aj watch remove <job>
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
aj auth status                   # credential health, as the daemon sees it
```

Every command is a thin wrapper over the SDK — `aj ds list` is `d.ds.list()`:

```python
from azure_jobs import connect

with connect() as d:
    d.job.list(limit=20)
    d.ws("other").ds.list()
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
