# Roadmap

## Done

- Pure REST submission engine — no `azure-ai-ml`, no `amlt` runtime dependency.
- Three submission backends behind one contract: **native** (REST), **amlt** (compat), **volcano** (Kubernetes).
- Volcano: pod-local emptyDir workdir at `/mnt/aj-workdir`, PVC code upload via `kubectl exec` + tar, signal-safe pod cleanup.
- Content-addressed code upload: re-submits with identical inputs reuse the prior asset.
- Job lifecycle: `aj job list/show/cancel/logs/stats` (server-side filtering).
- Templates: inheritance, merge, validate, diff, pull, push.
- Workspace, auth, env, datastore, experiment, image, quota, SKU commands.
- `aj sku check` — pre-flight SKU/quota/compute validation, auto-toggles `-NvLink` when the VC only carries the opposite variant.
- `aj code stats` — preview the upload payload (count, size, content hash, top-N largest files).
- Interactive TUI dashboard (`aj dash`).
- Multi-node distributed preamble (MPI rank mapping, NCCL tuning).
- ~370 tests.

## Next

- Log streaming for running jobs (`aj job logs -f`).
- `aj run --resubmit <id>`.
- Job diff — compare two submissions.
- Shell completions.
- Cost / GPU-hour budgeting in `aj job stats`.
- Direct results download (`aj job results pull <id>`).
