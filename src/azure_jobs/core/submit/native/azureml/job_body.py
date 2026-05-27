"""Pure assembly of the Azure ML REST job-create payload."""

from __future__ import annotations

from typing import Any

from ...models import SubmitRequest
from .runner import RUNNER_FILENAME

_SING_DEFAULT_ENV = {
    "SUDO": "sudo",
    "AZCOPY_AUTO_LOGIN_TYPE": "MSI",
    "JOB_EXECUTION_MODE": "Basic",
    "AZUREML_COMPUTE_USE_COMMON_RUNTIME": "false",
}

def _build_env_vars(
    request: SubmitRequest, dataref_env: dict[str, str]
) -> dict[str, str]:
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
        job_payload["properties"] = custom_props

    return {"properties": job_payload}
