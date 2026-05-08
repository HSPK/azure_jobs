#!/usr/bin/env bash
# Smoke test: multi-GPU NCCL + DDP via torchrun.
#
# Reads the standard torchrun env vars (all optional; sensible defaults
# for single-node):
#   NNODES           — node count                  (default: 1)
#   NPROC_PER_NODE   — GPUs per node               (default: nvidia-smi count)
#   NODE_RANK        — this node's index           (default: 0)
#   MASTER_ADDR      — rendezvous host             (default: 127.0.0.1)
#   MASTER_PORT      — rendezvous port             (default: 29500)
#
# Submit with:
#   aj run -t <template> -n 1 -p 8 bash scripts/test_multi_gpu.sh
#   aj run -t <template> -n 2 -p 8 bash scripts/test_multi_gpu.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== install torch ==="
# shellcheck disable=SC1091
source "$SCRIPT_DIR/_install_torch.sh"
echo

: "${NODE_RANK:=0}"
: "${MASTER_ADDR:=127.0.0.1}"
: "${MASTER_PORT:=29500}"

echo "=== nvidia-smi ==="
nvidia-smi || { echo "[FAIL] nvidia-smi missing"; exit 1; }
echo

echo "=== distributed env ==="
echo "NNODES=$AJ_NODES  NPROC_PER_NODE=$AJ_GPUS_PER_NODE  NODE_RANK=$NODE_RANK"
echo "MASTER_ADDR=$MASTER_ADDR  MASTER_PORT=$MASTER_PORT"
echo

echo "=== torchrun ==="
torchrun \
    --nnodes="$AJ_NODES" \
    --nproc_per_node="$AJ_GPUS_PER_NODE" \
    --node_rank="$NODE_RANK" \
    --master_addr="$MASTER_ADDR" \
    --master_port="$MASTER_PORT" \
    "$SCRIPT_DIR/_ddp_smoke.py"

echo
echo "=== MULTI-GPU SMOKE TEST PASSED ==="
