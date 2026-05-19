"""Azure ML VM size → GPU info catalog.

Loaded once from ``vm_gpu.yaml`` next to this module — edit the YAML
to add new VM sizes without touching Python.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_YAML = Path(__file__).parent / "vm_gpu.yaml"

# vm_size (lower) → (accelerator, gpu_count, gpu_memory_gb)
AML_VM_GPU: dict[str, tuple[str, int, int]] = {
    k.lower(): (v[0], int(v[1]), int(v[2]))
    for k, v in (yaml.safe_load(_YAML.read_text()) or {}).items()
}


def vm_sku_label(vm_size: str) -> str:
    """Derive a short SKU label (like amlt) from a VM size string."""
    info = AML_VM_GPU.get(vm_size.lower())
    if info:
        accel, count, mem = info
        return f"{mem}G{count}-{accel}" if accel != "CPU" else "CPU"
    low = vm_size.lower()
    if low.startswith(("standard_d", "standard_e", "standard_f")):
        return "CPU"
    return ""
