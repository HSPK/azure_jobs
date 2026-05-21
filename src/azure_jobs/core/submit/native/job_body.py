"""Pure assembly of the Azure ML REST job-create payload.

Everything in this module is deterministic and side-effect free: given
a :class:`SubmitRequest` plus the dynamic ids resolved by the
orchestrator (env id, code id, compute id, identity, resources, …), it
returns the JSON body that ``PUT /jobs/{name}`` expects. No ARM calls,
no logging, no UI — easy to unit-test.
"""

from __future__ import annotations

from typing import Any

from ..models import SubmitRequest
from .runner import RUNNER_FILENAME

# Singularity workers expect a few env vars to be set; the user's own
# template entries take precedence (we only ``setdefault``).
_SING_DEFAULT_ENV = {
    "SUDO": "sudo",
    "AZCOPY_AUTO_LOGIN_TYPE": "MSI",
    "JOB_EXECUTION_MODE": "Basic",
    "AZUREML_COMPUTE_USE_COMMON_RUNTIME": "false",
}


def _build_env_vars(
    request: SubmitRequest, dataref_env: dict[str, str]
) -> dict[str, str]:
    """Build the environment variables dict for the job."""
    env_vars = dict(request.env_vars)
    if request.shm_size:
        env_vars.setdefault("SHM_SIZE", request.shm_size)
    if request.service == "sing":
        for k, v in _SING_DEFAULT_ENV.items():
            env_vars.setdefault(k, v)
        if request.sing.group_policy:
            env_vars.setdefault("AML_JOB_GROUP_POLICY", request.sing.group_policy)
    env_vars.update(dataref_env)
    return env_vars


def _build_tags(tag_strings: list[str]) -> dict[str, str | None]:
    """Parse ``key:value`` tag strings into a dict."""
    tags: dict[str, str | None] = {}
    for tag_str in tag_strings:
        key, _, value = tag_str.partition(":")
        tags[key.strip()] = value.strip() or None
    return tags


def _build_job_body(
    request: SubmitRequest,
    *,
    env_id: str,
    code_id: str,
    compute_id: str,
    env_vars: dict[str, str],
    distribution: dict[str, Any] | None,
    identity: dict[str, Any] | None,
    resources: dict[str, Any] | None,
    outputs: dict[str, Any] | None,
    custom_props: dict[str, Any] | None,
    tags: dict[str, str | None],
) -> dict[str, Any]:
    """Assemble the Azure ML REST job-create payload.

    The shape matches ``PUT /jobs/{name}``'s body — top-level
    ``properties`` carries the job spec; ``properties.properties`` is
    Azure ML's user-defined custom-properties dict (named confusingly
    by Azure).
    """
    job_payload: dict[str, Any] = {
        "jobType": "Command",
        "displayName": request.name,
        "description": request.description,
        "experimentName": request.expr_name,
        "command": f"bash {RUNNER_FILENAME}",
        "computeId": compute_id,
        "environmentVariables": env_vars,
    }

    if code_id:
        job_payload["codeId"] = code_id

    # environmentId is either a registered asset id or an inline image ref.
    if env_id:
        job_payload["environmentId"] = env_id
    else:
        image = request.image
        if request.image_registry:
            image = f"{request.image_registry}/{image}"
        job_payload["environmentId"] = image

    if distribution:
        job_payload["distribution"] = distribution
    if identity:
        job_payload["identity"] = identity

    res: dict[str, Any] = {"instanceCount": request.nodes}
    if resources:
        res["properties"] = resources.get("properties", {})
    if request.shm_size:
        res["shmSize"] = request.shm_size
    job_payload["resources"] = res

    if outputs:
        job_payload["outputs"] = outputs
    if tags:
        job_payload["tags"] = tags
    if custom_props:
        # Azure ML's user-defined properties live at properties.properties.
        job_payload["properties"] = custom_props

    return {"properties": job_payload}
