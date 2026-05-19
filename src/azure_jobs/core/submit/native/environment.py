"""Docker image environment registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..models import SubmitRequest

if TYPE_CHECKING:
    from azure_jobs.core.rest_client import AzureMLClient

log = logging.getLogger(__name__)

_SING_IMAGE_PREFIX = "amlt-sing/"
# Dummy environment image for Singularity — the actual image is specified
# via imageVersion in the AISuperComputer resources dict.
_SING_DUMMY_IMAGE = "mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest"


def _build_environment(request: SubmitRequest, client: AzureMLClient) -> str:
    """Register a Docker image as an environment and return its ARM ID.

    For Singularity curated images (``amlt-sing/...``), uses a dummy MCR image
    that passes Azure ML validation.  The real image is selected at runtime
    by the Singularity platform via the ``imageVersion`` resource property.
    """
    if request.image_registry:
        image = f"{request.image_registry}/{request.image}"
    else:
        image = request.image

    # Singularity curated images: use dummy MCR image for Azure ML
    if image.startswith(_SING_IMAGE_PREFIX) and request.service == "sing":
        image = _SING_DUMMY_IMAGE

    # Deterministic version from image string for caching
    import hashlib

    version = hashlib.sha256(image.encode()).hexdigest()[:16]
    env_name = request.expr_name or "aj"

    # Reuse existing environment if available
    try:
        cached = client.resources.get_environment_version(env_name, version)
        if cached:
            return cached.get("id", "")
    except Exception:
        log.debug("Environment %s:%s not cached, creating new", env_name, version)

    try:
        registered = client.resources.create_or_update_environment(
            env_name, version, image
        )
        return registered.get("id", "")
    except Exception:
        log.debug("Failed to register environment, using inline", exc_info=True)
    return ""
