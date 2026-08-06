from __future__ import annotations

from azure_jobs.server.submit import get_backend
import azure_jobs.server.submit.volcano as volcano_mod
from azure_jobs.shared.template.models import Template


def test_volcano_private_helpers_and_backend_registration() -> None:
    template = Template.from_dict(
        {
            "target": {
                "service": "volcano",
                "namespace": "ns",
                "queue": "q",
                "context": "ctx",
                "gpus_per_node": 4,
            },
            "jobs": [{"sku": "x"}],
        }
    )

    spec = volcano_mod._build_volcano_spec(template)
    loaded = volcano_mod._load_volcano_spec(
        {"namespace": "ns-2", "queue": "q-2", "unknown": "ignored"}
    )
    normalized = volcano_mod._normalize_volcano_name("9.BAD_name")
    backend = get_backend("volcano")

    assert spec.namespace == "ns"
    assert spec.queue == "q"
    assert spec.context == "ctx"
    assert spec.gpus_per_node == 4
    assert loaded.namespace == "ns-2"
    assert loaded.queue == "q-2"
    assert not hasattr(loaded, "unknown")
    assert normalized.startswith("j-")
    assert "_" not in normalized and "." not in normalized
    assert backend.fn is volcano_mod.submit_via_volcano
    assert backend.label == "Volcano"
