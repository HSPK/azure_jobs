#!/usr/bin/env bash
# Smoke test: multi-GPU NCCL + DDP via torchrun.
#
# Single-node:   torchrun --standalone --nproc_per_node=$AJ_PROCESSES ...
# Multi-node :   torchrun --nnodes=$AJ_NODES --node_rank=$NODE_RANK \
#                         --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT ...
#
# Submit with:
#   aj run -t <template> -n 1 -p 8 bash scripts/test_multi_gpu.sh
#   aj run -t <template> -n 2 -p 8 bash scripts/test_multi_gpu.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NODES="${AJ_NODES:-1}"
# Default to all visible GPUs if AJ_PROCESSES is unset / zero.
if [[ -z "${AJ_PROCESSES:-}" || "${AJ_PROCESSES}" == "0" ]]; then
    PROCS="$(nvidia-smi -L | wc -l)"
else
    PROCS="${AJ_PROCESSES}"
fi

echo "=== nvidia-smi ==="
nvidia-smi || { echo "[FAIL] nvidia-smi missing"; exit 1; }
echo

echo "=== AJ env ==="
echo "AJ_NODES=$NODES  AJ_PROCESSES=$PROCS"
echo "MASTER_ADDR=${MASTER_ADDR:-<unset>}  MASTER_PORT=${MASTER_PORT:-<unset>}"
echo "NODE_RANK=${NODE_RANK:-<unset>}      RANK=${RANK:-<unset>}"
echo

if [[ "$NODES" -gt 1 ]]; then
    : "${MASTER_ADDR:?MASTER_ADDR must be set for multi-node}"
    : "${MASTER_PORT:?MASTER_PORT must be set for multi-node}"
    : "${NODE_RANK:?NODE_RANK must be set for multi-node}"
    echo "=== torchrun multi-node (nnodes=$NODES, nproc_per_node=$PROCS) ==="
    torchrun \
        --nnodes="$NODES" \
        --nproc_per_node="$PROCS" \
        --node_rank="$NODE_RANK" \
        --master_addr="$MASTER_ADDR" \
        --master_port="$MASTER_PORT" \
        "$SCRIPT_DIR/_ddp_smoke.py"
else
    echo "=== torchrun --standalone --nproc_per_node=$PROCS ==="
    torchrun \
        --standalone \
        --nnodes=1 \
        --nproc_per_node="$PROCS" \
        "$SCRIPT_DIR/_ddp_smoke.py"
fi

echo
echo "=== MULTI-GPU SMOKE TEST PASSED ==="
