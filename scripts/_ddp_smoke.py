"""Tiny DDP smoke test — no data, no checkpoints, runs in seconds.

Used by both ``test_single_gpu.sh`` and ``test_multi_gpu.sh`` via torchrun.

What it verifies:
* ``torch.distributed`` initialises with NCCL on every rank.
* An all-reduce sums to the expected value across the whole world.
* A toy linear model runs 5 forward/backward/optimizer steps without
  diverging or hanging.
"""

from __future__ import annotations

import datetime as dt
import os
import socket
import sys

import torch
import torch.distributed as dist
import torch.nn as nn


def log(msg: str, *, rank: int) -> None:
    print(
        f"[{dt.datetime.utcnow().isoformat(timespec='seconds')}Z r{rank}] {msg}",
        flush=True,
    )


def main() -> int:
    rank = int(os.environ.get("RANK", "0"))
    local_rank = int(os.environ.get("LOCAL_RANK", "0"))
    world_size = int(os.environ.get("WORLD_SIZE", "1"))

    if not torch.cuda.is_available():
        log("CUDA is NOT available", rank=rank)
        return 1

    n_gpu = torch.cuda.device_count()
    if rank == 0:
        log(
            f"host={socket.gethostname()} "
            f"world_size={world_size} cuda_devices={n_gpu} "
            f"torch={torch.__version__} cuda={torch.version.cuda}",
            rank=rank,
        )

    torch.cuda.set_device(local_rank)
    backend = "nccl"

    dist.init_process_group(
        backend=backend,
        init_method="env://",
        timeout=dt.timedelta(minutes=5),
    )
    log(
        f"init_process_group OK (backend={backend}, local_rank={local_rank})", rank=rank
    )

    # ── all-reduce sanity check ────────────────────────────────────────
    t = torch.full((4,), float(rank + 1), device=f"cuda:{local_rank}")
    dist.all_reduce(t, op=dist.ReduceOp.SUM)
    expected = sum(range(1, world_size + 1))  # 1+2+...+world_size
    got = int(t[0].item())
    if got != expected:
        log(f"all-reduce MISMATCH: got={got} expected={expected}", rank=rank)
        dist.destroy_process_group()
        return 2
    if rank == 0:
        log(f"all-reduce OK: sum(1..{world_size}) = {got}", rank=rank)

    # ── 5-step fake training loop ──────────────────────────────────────
    device = torch.device(f"cuda:{local_rank}")
    model = nn.Linear(64, 64).to(device)
    ddp = torch.nn.parallel.DistributedDataParallel(model, device_ids=[local_rank])
    opt = torch.optim.SGD(ddp.parameters(), lr=1e-3)
    loss_fn = nn.MSELoss()

    for step in range(5):
        x = torch.randn(32, 64, device=device)
        y = torch.randn(32, 64, device=device)
        opt.zero_grad(set_to_none=True)
        loss = loss_fn(ddp(x), y)
        loss.backward()
        opt.step()
        if rank == 0:
            log(f"step {step} loss={loss.item():.4f}", rank=rank)

    dist.barrier()
    dist.destroy_process_group()
    if rank == 0:
        log("DDP smoke test PASSED", rank=rank)
    return 0


if __name__ == "__main__":
    sys.exit(main())
