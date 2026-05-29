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
