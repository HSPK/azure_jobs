# aj vs amlt

A quick map of where each tool fits. `aj` (this project) and `amlt` (Microsoft's Amulet) both submit Azure ML jobs but optimize for different workflows.

## Philosophy

| | aj | amlt |
|---|---|---|
| Approach | Pure REST, no Azure SDK | Full Azure ML Python SDK |
| Runtime deps | `click` + `pyyaml` + `rich` + `textual` + `azure-identity` + `requests` | Azure ML SDK + many extras |
| `--help` startup | ~160 ms | ~2 s |
| Backends | native (REST) / amlt / volcano | AzureML / Singularity |
| Config | Template inheritance via `base` chain | Single YAML with `imports` |

## What aj has, amlt doesn't

| Command | Notes |
|---------|-------|
| `aj job stats` | GPU-hours and success rate, grouped by experiment/compute/user/workspace |
| `aj quota list` / `--aml` | VC quotas and AML cluster availability in one table |
| `aj sku list` / `aj sku check` | Browse SKUs by VC; pre-flight validate against a template |
| `aj code stats` | Preview the next upload — count, size, hash, top-N largest files |
| `aj dash` | Interactive TUI dashboard |
| `aj image list` | Singularity curated base images |
| `aj env list/show`, `aj ds list/show` | Browse environments and datastores |
| Volcano backend | Kubernetes Volcano cluster submission via `kubectl` |
| Auto SKU adjustment | `aj run` will toggle `-NvLink` when the VC only has the opposite variant |
| Content-addressed upload dedup | Re-submits skip the upload entirely on identical inputs |

## What amlt has, aj doesn't

**High priority**

| Feature | amlt | Note |
|---------|------|------|
| SSH into job | `amlt ssh` | Essential for live debug |
| Download results | `amlt results download` | aj has logs only |
| Follow logs live | `amlt logs -f` | aj's `job logs` is one-shot |
| Rerun experiment | `amlt rerun` | aj re-runs from the template |

**Medium**

| Feature | amlt | Note |
|---------|------|------|
| Conda / pip declaration | `conda_file` / `pip` in YAML | aj uses setup commands |
| Dockerfile build | `docker.build` | aj uses pre-built images |
| YAML variable substitution | `$VAR` + `env_defaults` | aj uses Python format strings only |
| Hyperparameter search | `--search` | grid / random / bayesian |
| Storage CLI | `amlt storage upload/download/rm` | direct blob ops |
| Metrics extraction | `amlt metrics` | pull and tabulate |
| Pause / Resume | `amlt pause/resume` | aj only cancel |
| Interactive debug | `amlt debug` | tmux session |

**Low**

| Feature | amlt | Note |
|---------|------|------|
| Shell completion | `amlt completion` | bash/zsh/fish |
| Parallel multi-job submit | thread pool | aj submits one at a time |
| Move jobs between experiments | `amlt move` | |
| Node power management | `amlt power` | restart/drain |

## When to pick which

**Use `aj` when** you want fast startup, minimal deps, template inheritance, the volcano backend, quota/SKU browsing, GPU-hours stats, or the TUI dashboard.

**Use `amlt` when** you need SSH, live log following, results download, conda/pip/Dockerfile builds, hyperparameter search, or pause/resume — and don't mind the SDK weight. (`aj run --amlt` is a shim that hands off to amlt while keeping `aj`'s template layer.)
