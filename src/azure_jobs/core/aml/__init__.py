"""Azure ML (AML) target-specific catalogs and fetchers.

Mirrors :mod:`azure_jobs.core.sku` for the Singularity target service:

* :mod:`.vm_gpu`   — VM size → GPU info, loaded from ``vm_gpu.yaml``.
* :mod:`.computes` — parallel fan-out to list every workspace's
  ``AmlCompute`` cluster.

Functions here are UI-free; ``aj quota --aml`` is the only CLI caller.
"""

from .computes import fetch_aml_computes_all_workspaces
from .vm_gpu import AML_VM_GPU, vm_sku_label

__all__ = [
    "AML_VM_GPU",
    "vm_sku_label",
    "fetch_aml_computes_all_workspaces",
]
