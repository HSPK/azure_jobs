"""Multi-node job wrapper for distributed training.

Generates the shell command preamble that handles:
- Rank detection (MPI → PyTorch env vars)
- NCCL environment configuration
- Rank-0 setup with file-based barrier synchronization

This replaces amlt's aml_code_runner.py with a lightweight inline
shell script — no Python wrapper needed inside the container.
"""

from __future__ import annotations


def build_distributed_preamble(
    setup_commands: list[str],
    *,
    master_port: int = 6105,
) -> list[str]:
    """Return shell commands that configure distributed training env vars.

    These are prepended to the user command so that multi-node jobs
    get proper RANK/WORLD_SIZE/MASTER_ADDR variables.

    The barrier ensures setup commands (pip install, etc.) run only on
    local rank 0, and other ranks wait until setup completes.
    """
    lines: list[str] = []

    # Detect MPI and translate to PyTorch-style env vars
    lines.append(
        'if [ -n "$OMPI_COMM_WORLD_RANK" ]; then'
        " export RANK=$OMPI_COMM_WORLD_RANK"
        " WORLD_SIZE=$OMPI_COMM_WORLD_SIZE"
        " LOCAL_RANK=$OMPI_COMM_WORLD_LOCAL_RANK"
        ' NODE_RANK=$((OMPI_COMM_WORLD_RANK / OMPI_COMM_WORLD_LOCAL_SIZE));'
        " fi"
    )

    # Set MASTER_ADDR / MASTER_PORT
    lines.append(
        'if [ -n "$AZ_BATCH_MASTER_NODE" ]; then'
        ' export MASTER_ADDR=$(echo "$AZ_BATCH_MASTER_NODE" | cut -d: -f1);'
        " fi"
    )
    lines.append(
        'if [ -n "$AZ_BATCHAI_MPI_MASTER_NODE" ]; then'
        " export MASTER_ADDR=$AZ_BATCHAI_MPI_MASTER_NODE;"
        " fi"
    )
    lines.append(f'export MASTER_PORT=${{MASTER_PORT:-{master_port}}}')

    # NCCL tuning
    lines.append('export NCCL_SOCKET_IFNAME=${NCCL_SOCKET_IFNAME:-"^docker0,lo"}')

    # Rank-0 setup with barrier
    if setup_commands:
        marker = "/tmp/.aj_setup_done"
        setup_str = " && ".join(setup_commands)
        lines.append(
            f'if [ "${{LOCAL_RANK:-0}}" = "0" ]; then'
            f" {setup_str} && touch {marker};"
            f" else"
            f" while [ ! -f {marker} ]; do sleep 1; done;"
            f" fi"
        )

    return lines
