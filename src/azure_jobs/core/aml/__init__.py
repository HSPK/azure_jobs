"""Azure ML (AML) target-specific catalogs.

Mirrors :mod:`azure_jobs.core.submit.native.sku` for the Singularity target service:

* :mod:`.vm_gpu` — VM size → GPU info, loaded from ``vm_gpu.yaml``.

Compute discovery and fan-out now lives in
:mod:`azure_jobs.core.az_client.arm.compute` (``arm.compute.list_all``).
"""

from .vm_gpu import AML_VM_GPU, vm_sku_label

__all__ = [
    "AML_VM_GPU",
    "vm_sku_label",
]
