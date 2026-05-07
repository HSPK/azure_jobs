#!/usr/bin/env bash
# Smoke test: single-GPU NCCL + DDP (world_size=1).
#
# Submit with:
#   aj run -t <template> -n 1 -p 1 bash scripts/test_single_gpu.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== nvidia-smi ==="
nvidia-smi || { echo "[FAIL] nvidia-smi missing"; exit 1; }
echo

echo "=== AJ env ==="
echo "AJ_NODES=${AJ_NODES:-<unset>}  AJ_PROCESSES=${AJ_PROCESSES:-<unset>}"
echo

echo "=== torchrun --standalone --nproc_per_node=1 ==="
torchrun \
    --standalone \
    --nnodes=1 \
    --nproc_per_node=1 \
    "$SCRIPT_DIR/_ddp_smoke.py"

echo
echo "=== SINGLE-GPU SMOKE TEST PASSED ==="
