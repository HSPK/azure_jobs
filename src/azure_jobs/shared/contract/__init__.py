"""HTTP contract: wire models, typed errors, routes, and payload codecs.

The concrete resource surface is described by FastAPI's OpenAPI document.
There is deliberately no parallel Python ``Protocol`` hierarchy to keep in
sync with it.
"""

# Registers the Azure value types so tagged payloads rebuild with their
# behaviour intact on whichever side of the socket decodes them.
from azure_jobs.shared.contract.typed import install_azure_types as _install_azure_types

_install_azure_types()

__all__: list[str] = []
