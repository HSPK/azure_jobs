"""amlt SKU shorthand parser.

Parses strings like ``1x80G8-A100-NvLink`` into a structured spec the
family matcher can compare against.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class SkuSpec:
    """Parsed representation of an amlt SKU shorthand."""

    num_nodes: int = 1
    num_units: int = 1
    unit_memory: int | None = None
    is_cpu: bool = False
    accelerators: list[str] = field(default_factory=list)
    nvlink: bool = False

    @classmethod
    def parse(cls, raw: str) -> SkuSpec:
        """Parse amlt-style SKU string.

        Examples::

            1xC1              → 1 CPU
            1x80G8-A100-NvLink → 8 × A100 80GB w/ NvLink
            2x40G4-A100       → 4 × A100 40GB, 2 nodes
            G1                → 1 generic GPU
        """
        m = re.fullmatch(
            r"""
            (?:(\d+)\s*x)?\s*          # optional {nodes}x
            (\d+)?\s*                   # optional unit_memory
            ([CG])                      # C=CPU, G=GPU
            (\d+)?\s*                   # num_units
            (?:-(.+))?                  # optional accelerator/flags
            """,
            raw.strip(),
            re.X,
        )
        if not m:
            return cls()

        spec = cls()
        spec.num_nodes = int(m.group(1)) if m.group(1) else 1
        spec.unit_memory = int(m.group(2)) if m.group(2) else None
        spec.is_cpu = m.group(3) == "C"
        spec.num_units = int(m.group(4)) if m.group(4) else 1

        if m.group(5):
            parts = [
                p.strip().upper() for p in re.split(r"[-]", m.group(5)) if p.strip()
            ]
            for p in parts:
                if p == "NVLINK":
                    spec.nvlink = True
                elif p == "IB":
                    pass  # ignore IB flag for matching
                else:
                    spec.accelerators.append(p)

        if spec.is_cpu:
            spec.accelerators = ["CPU"]

        return spec
