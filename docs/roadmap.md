# Roadmap

## Completed

- **Pure REST architecture** — replaced `azure-ai-ml` SDK and `amlt` with direct REST API calls
- **Job submission** — code upload, environment registration, datastore creation, distributed training, all via REST
- **Job lifecycle** — `aj job list/show/cancel/logs` with server-side filtering
- **Template management** — `aj template list/show/validate/pull/push/diff`
- **Workspace management** — `aj ws list/show/set` with interactive picker
- **Environment browsing** — `aj env list/show` for registered environments
- **Datastore browsing** — `aj ds list/show` for workspace datastores
- **Experiment browsing** — `aj exp list/show` aggregated from jobs
- **Quota monitoring** — `aj quota list` for Singularity VCs and AML clusters
- **Image catalog** — `aj image list` for Singularity curated base images
- **Interactive dashboard** — `aj dash` TUI with job table, filtering, keyboard shortcuts
- **Authentication** — `aj auth status/login/logout` with credential health checks
- Rich terminal output — panels, tables, spinners, styled messages
- Unified `aj_config.json` — defaults, workspace, repo_id in one file
- YAML template inheritance with circular detection and merge rules
- Multi-node distributed training preamble (MPI rank mapping, NCCL tuning)
- Content-addressed artifact dedup (SHA-based code and environment versions)
- 335 tests

## Future

- Pre-submission validation (validate SKU names, check cluster existence)
- Resubmit support (`aj run --resubmit <id>`)
- Job metrics — queue time tracking, cost estimation
- Shell completions
- Log streaming for running jobs
- Job diff — compare configs of two submissions
