# Smoke-test scripts

Three minimal scripts to validate an `aj` Singularity submission end-to-end.
Each is a single-file bash script meant to be invoked through `aj run`.

| Script | What it checks | Suggested submit |
| --- | --- | --- |
| [test_ssh_git.sh](test_ssh_git.sh) | `~/.ssh/` was copied into the job and the key is accepted by `git@github.com` | `aj run -t <tpl> -n 1 -p 1 bash scripts/test_ssh_git.sh` |
| [test_single_gpu.sh](test_single_gpu.sh) | 1× GPU is visible, NCCL works on a single rank, 5 fake training steps complete | `aj run -t <tpl> -n 1 -p 1 bash scripts/test_single_gpu.sh` |
| [test_multi_gpu.sh](test_multi_gpu.sh) | All `$AJ_PROCESSES × $AJ_NODES` ranks join, NCCL all-reduce sums correctly, 5 fake training steps complete | `aj run -t <tpl> -n 1 -p 8 bash scripts/test_multi_gpu.sh` |

`test_single_gpu.sh` and `test_multi_gpu.sh` share the tiny DDP workload in
[_ddp_smoke.py](_ddp_smoke.py); it has no external data and no checkpoints, so
it finishes in seconds. Both scripts source [_install_torch.sh](_install_torch.sh)
which:

* creates a venv at `$AJ_VENV` (default `~/.aj-venv`) using the `uv`
  binary already on the image,
* `uv pip install`s a CUDA torch wheel from `$TORCH_INDEX_URL`
  (default `https://download.pytorch.org/whl/cu124`),
* skips the install if `import torch` already works with CUDA.

Override with `TORCH_VERSION`, `TORCH_INDEX_URL`, or `AJ_VENV` env vars.

The scripts read these env vars (already set by `aj_runner.sh`):

* `AJ_NODES` — number of nodes (default `1`)
* `AJ_PROCESSES` — processes per node (default to `nvidia-smi` count)
* `MASTER_ADDR` / `MASTER_PORT` / `NODE_RANK` — set by Singularity for
  multi-node; the multi-GPU script falls back to `--standalone` when missing.
