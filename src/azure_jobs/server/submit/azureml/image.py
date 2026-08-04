"""Docker image environment registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from azure_jobs.shared.job.spec import JobSpec

if TYPE_CHECKING:
    from azure_jobs.server.az_client import AzureMLClient

log = logging.getLogger(__name__)

_SING_IMAGE_PREFIX = "amlt-sing/"
_SING_DUMMY_IMAGE = "mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest"

def _build_environment(request: JobSpec, client: AzureMLClient) -> str:
    if request.image_registry:
        image = f"{request.image_registry}/{request.image}"
    else:
        image = request.image

    if image.startswith(_SING_IMAGE_PREFIX) and request.service == "sing":
        image = _SING_DUMMY_IMAGE

    import hashlib

    version = hashlib.sha256(image.encode()).hexdigest()[:16]
    env_name = request.expr_name or "aj"

    try:
        cached = client.environments.get(env_name, version)
        if cached:
            return cached.id
    except Exception:
        log.debug("Environment %s:%s not cached, creating new", env_name, version)

    try:
        registered = client.environments.create_or_update(env_name, version, image)
        return registered.id
    except Exception:
        log.debug("Failed to register environment, using inline", exc_info=True)
    return ""
