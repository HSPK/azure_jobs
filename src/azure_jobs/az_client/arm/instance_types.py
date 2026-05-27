"""arm.instance_types — Singularity instance type catalog."""

from __future__ import annotations

import re
from dataclasses import dataclass

from azure_jobs.errors import NETWORK_LIKE_ERRORS

from ._base import ArmNamespace

API_VERSION = "2021-03-01-preview"

_DESC_ACCEL_RE = re.compile(
    r"\b(?:NVIDIA|AMD)\s+(?:NC\s+|ND\s+)?([A-Za-z0-9]+)(?:\s+(\d+)\s*GB)?",
    re.IGNORECASE,
)
_DESC_NVLINK_RE = re.compile(r"\bnvlink\b", re.IGNORECASE)
_DESC_IB_RE = re.compile(r"(?<![A-Za-z])IB(?![A-Za-z])")
_DESC_SCRATCH_RE = re.compile(
    r"Scratch\s+Storage[^:]*:\s*(\d+)",
    re.IGNORECASE,
)
_DEFAULT_GPU_MEMORY = {
    "MI300X": 192,
    "MI200": 64,
    "H200": 141,
    "H100": 80,
    "A100": 80,
    "A10": 24,
    "T4": 16,
    "V100": 16,
    "K80": 12,
}

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

def _parse_description(desc: str, num_gpus: int) -> tuple[str, int, bool, bool]:
    accel = ""
    gpu_mem = 0
    if num_gpus > 0:
        m = _DESC_ACCEL_RE.search(desc)
        if m:
            accel = m.group(1).upper()
            if m.group(2):
                gpu_mem = int(m.group(2))
        if not gpu_mem and accel:
            gpu_mem = _DEFAULT_GPU_MEMORY.get(accel, 0)
    else:
        accel = "CPU"
    return (
        accel,
        gpu_mem,
        bool(_DESC_NVLINK_RE.search(desc)),
        bool(_DESC_IB_RE.search(desc)),
    )

def _row_to_info(row: dict) -> InstanceTypeInfo:
    num_gpus = int(row.get("numberOfGPUs") or 0)
    desc = row.get("description") or ""
    accel, mem, nvlink, ib = _parse_description(desc, num_gpus)
    scratch_match = _DESC_SCRATCH_RE.search(desc)
    return InstanceTypeInfo(
        name=row.get("name") or "",
        series_id=row.get("instanceTypeSeriesId") or "",
        num_gpus=num_gpus,
        num_cores=int(row.get("numberOfCores") or 0),
        memory_gib=int(row.get("memoryGiB") or 0),
        scratch_gib=int(scratch_match.group(1)) if scratch_match else 0,
        description=desc,
        accelerator=accel,
        gpu_memory_gb=mem,
        nvlink=nvlink,
        infiniband=ib,
    )

_VARIANT_SUFFIX_RE = re.compile(r"-n\d+$")

class InstanceTypesAPI(ArmNamespace):
    """arm.instance_types — Singularity instance type catalog per region."""

    def list(
        self,
        location: str,
        *,
        subscription_id: str = "",
    ) -> list[InstanceTypeInfo]:
        """List every Singularity instance type available in *location*."""
        if not location:
            return []
        if not subscription_id:
            try:
                subs = self._client.subscriptions.list()
            except NETWORK_LIKE_ERRORS:
                return []
            if not subs:
                return []
            subscription_id = subs[0]

        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/locations/{location}/instanceTypeSeries"
            f"?api-version={API_VERSION}"
        )
        try:
            data = self._get(url)
        except NETWORK_LIKE_ERRORS:
            return []

        return [
            _row_to_info(row)
            for row in data.get("value", [])
            if not _VARIANT_SUFFIX_RE.search(row.get("name", ""))
        ]

__all__ = ["InstanceTypeInfo", "InstanceTypesAPI"]
