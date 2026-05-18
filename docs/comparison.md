# aj vs amlt

Both submit Azure ML jobs; different trade-offs.

| | aj | amlt |
|---|---|---|
| Approach | Pure REST | Azure ML SDK |
| Backends | native / amlt / volcano | AzureML / Singularity |
| Config | template inheritance via `base` | YAML with `imports` |
| Python SDK | first-class (`from azure_jobs import …`) | CLI-first |
| Dashboard | built-in TUI (`aj dash`) | external (portal / `amlt status`) |

## aj-only

- `aj job stats` — GPU-hours, success rate, grouped views.
- `aj quota list` / `--aml` — VC and AML cluster availability.
- `aj sku list` / `aj sku check` — browse and pre-flight validate.
- `aj code stats` — preview the next upload (count, size, hash, top-N).
- `aj dash` — TUI dashboard.
- Volcano backend.
- Auto `-NvLink` toggle when the VC carries the opposite variant.
- Content-addressed upload dedup.

## amlt-only

| Feature | Why it matters |
|---------|----------------|
| `amlt ssh` | live debug |
| `amlt results download` | aj has logs only |
| `amlt logs -f` | live tail |
| `amlt rerun` | re-run from job ID |
| `conda_file` / `pip` / `docker.build` | environment build |
| `--search` | hyperparameter sweep |
| `amlt storage ...` | direct blob ops |
| `amlt metrics` | tabulate metrics |
| `amlt pause/resume` | lifecycle control |

## Pick

**aj** — fast, minimal deps, template inheritance, volcano, quota/SKU browsing.
**amlt** — SSH, log tailing, results download, conda/pip builds, hyperparameter search.

`aj run --amlt` keeps aj's template layer while delegating submission to amlt.
