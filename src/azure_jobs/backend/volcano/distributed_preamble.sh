#!/bin/bash
# Volcano distributed env fallback — sourced when amlt-style RANK/WORLD_SIZE
# vars aren't already set by the platform.
#
# WORLD_SIZE defaults are substituted at submission time via {WORLD_SIZE_DEFAULT}.
# Other vars come from Volcano's pod naming convention:
#   {job}-master-0 / {job}-worker-N

JOB_NAME=$(echo "$HOSTNAME" | sed 's/-\(master\|worker\)-[0-9]*$//')
if echo "$HOSTNAME" | grep -q "master"; then
    export NODE_RANK=0
else
    export NODE_RANK=$((${VK_TASK_INDEX:-0} + 1))
fi
export RANK=${RANK:-$NODE_RANK}
export WORLD_SIZE=${WORLD_SIZE:-{WORLD_SIZE_DEFAULT}}
export MASTER_ADDR="${MASTER_ADDR:-${JOB_NAME}-master-0.${JOB_NAME}}"
export MASTER_PORT=${MASTER_PORT:-6105}
