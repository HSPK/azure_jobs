"""Singularity instance-type vocabulary, shared by both sides."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class InstanceTypeInfo:
    """One Singularity instance type (e.g."""

    name: str
    series_id: str
    num_gpus: int = 0
    num_cores: int = 0
    memory_gib: int = 0
    scratch_gib: int = 0
    description: str = ""
    accelerator: str = ""
    gpu_memory_gb: int = 0
    nvlink: bool = False
    infiniband: bool = False

    @property
    def is_cpu(self) -> bool:
        return self.num_gpus == 0

    @property
    def short_name(self) -> str:
        """Name without the Singularity."""
        return self.name.removeprefix("Singularity.")

    @property
    def shorthand(self) -> str:
        """Render this row as the amlt SKU shorthand it satisfies."""
        if self.is_cpu:
            return f"C{self.num_cores}" if self.num_cores else "C"
        parts = []
        if self.gpu_memory_gb:
            parts.append(f"{self.gpu_memory_gb}G{self.num_gpus}")
        else:
            parts.append(f"G{self.num_gpus}")
        if self.accelerator and self.accelerator != "CPU":
            parts.append(self.accelerator)
        if self.nvlink:
            parts.append("NvLink")
        return "-".join(parts)
