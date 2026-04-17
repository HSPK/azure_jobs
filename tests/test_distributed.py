"""Tests for distributed training command wrapper."""

from azure_jobs.core.distributed import build_distributed_preamble


class TestDistributedPreamble:
    def test_empty_setup(self):
        lines = build_distributed_preamble([])
        joined = " && ".join(lines)
        assert "OMPI_COMM_WORLD_RANK" in joined
        assert "MASTER_PORT" in joined
        assert "NCCL_SOCKET_IFNAME" in joined
        # No barrier when no setup commands
        assert ".aj_setup_done" not in joined

    def test_setup_barrier(self):
        lines = build_distributed_preamble(["pip install torch", "echo done"])
        joined = " && ".join(lines)
        assert "LOCAL_RANK" in joined
        assert ".aj_setup_done" in joined
        assert "pip install torch" in joined
        assert "sleep 1" in joined  # barrier polling

    def test_custom_port(self):
        lines = build_distributed_preamble([], master_port=29500)
        joined = " && ".join(lines)
        assert "29500" in joined

    def test_nccl_ifname(self):
        lines = build_distributed_preamble([])
        joined = " && ".join(lines)
        assert "^docker0,lo" in joined

    def test_mpi_to_pytorch_vars(self):
        lines = build_distributed_preamble([])
        joined = " && ".join(lines)
        assert "RANK=$OMPI_COMM_WORLD_RANK" in joined
        assert "WORLD_SIZE=$OMPI_COMM_WORLD_SIZE" in joined
        assert "LOCAL_RANK=$OMPI_COMM_WORLD_LOCAL_RANK" in joined
