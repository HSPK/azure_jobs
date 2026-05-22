"""Azure ML (AML) target-specific catalogs."""

from .vm_gpu import AML_VM_GPU, vm_sku_label

__all__ = [
    "AML_VM_GPU",
    "vm_sku_label",
]
