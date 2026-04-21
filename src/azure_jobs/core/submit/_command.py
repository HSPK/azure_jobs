"""Runner script generation and command building."""

from __future__ import annotations

from typing import Any

from ._models import SubmitRequest

_RUNNER_FILENAME = "aj_runner.sh"


def _generate_runner_script(
    request: SubmitRequest,
    identity_client_id: str = "",
) -> str:
    """Generate the aj_runner.sh script that runs inside the container.

    The script handles:
    - Identity exports (Singularity UAI)
    - Distributed env detection (MPI → PyTorch vars)
    - NCCL configuration
    - Rank-0-only setup with barrier
    - User command execution
    """
    lines: list[str] = ["#!/bin/bash", "set -e", ""]

    # --- Identity exports ---
    if identity_client_id:
        lines.append("# Singularity managed identity")
        lines.append(f"export DEFAULT_IDENTITY_CLIENT_ID={identity_client_id}")
        lines.append(f"export AZURE_CLIENT_ID={identity_client_id}")
        lines.append("")

    # --- Distributed preamble ---
    is_distributed = request.nodes > 1 or request.processes_per_node > 1
    if is_distributed:
        lines.append("# Distributed training env detection")
        lines.append('if [ -n "$OMPI_COMM_WORLD_RANK" ]; then')
        lines.append("  export RANK=$OMPI_COMM_WORLD_RANK")
        lines.append("  export WORLD_SIZE=$OMPI_COMM_WORLD_SIZE")
        lines.append("  export LOCAL_RANK=$OMPI_COMM_WORLD_LOCAL_RANK")
        lines.append("  export NODE_RANK=$((OMPI_COMM_WORLD_RANK / OMPI_COMM_WORLD_LOCAL_SIZE))")
        lines.append("fi")
        lines.append("")

        lines.append("# Master address resolution")
        lines.append('if [ -n "$AZ_BATCH_MASTER_NODE" ]; then')
        lines.append('  export MASTER_ADDR=$(echo "$AZ_BATCH_MASTER_NODE" | cut -d: -f1)')
        lines.append("fi")
        lines.append('if [ -n "$AZ_BATCHAI_MPI_MASTER_NODE" ]; then')
        lines.append("  export MASTER_ADDR=$AZ_BATCHAI_MPI_MASTER_NODE")
        lines.append("fi")
        lines.append("export MASTER_PORT=${MASTER_PORT:-6105}")
        lines.append("")

        lines.append("# NCCL tuning")
        lines.append('export NCCL_SOCKET_IFNAME=${NCCL_SOCKET_IFNAME:-"^docker0,lo"}')
        lines.append("")

    # --- Setup commands (rank-0 only for distributed) ---
    if request.setup_commands:
        if is_distributed:
            lines.append("# Setup (rank-0 only with barrier)")
            lines.append('if [ "${LOCAL_RANK:-0}" = "0" ]; then')
            for cmd in request.setup_commands:
                lines.append(f"  {cmd}")
            lines.append("  touch /tmp/.aj_setup_done")
            lines.append("else")
            lines.append("  _aj_waited=0")
            lines.append("  while [ ! -f /tmp/.aj_setup_done ]; do")
            lines.append("    sleep 1")
            lines.append("    _aj_waited=$((_aj_waited + 1))")
            lines.append('    if [ "$_aj_waited" -ge 600 ]; then')
            lines.append('      echo "ERROR: setup barrier timed out after 600s" >&2')
            lines.append("      exit 1")
            lines.append("    fi")
            lines.append("  done")
            lines.append("fi")
        else:
            lines.append("# Setup")
            for cmd in request.setup_commands:
                lines.append(cmd)
        lines.append("")

    # --- User command ---
    lines.append("# Run")
    for cmd in request.command:
        lines.append(cmd)
    lines.append("")

    return "\n".join(lines)
