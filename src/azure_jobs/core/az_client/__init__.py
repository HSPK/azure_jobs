"""Azure ML REST client package — pure HTTP, no ``azure-ai-ml`` SDK.

Two top-level clients:

- :class:`AzureARMClient`  — workspace-agnostic ARM operations
  (subscriptions, Resource Graph, workspace discovery, VC quotas)
- :class:`AzureMLClient`   — workspace-scoped operations
  (jobs, environments, datastores, code upload)

Domain APIs live under :mod:`azure_jobs.core.az_client.api` and are
composed onto :class:`AzureMLClient` as ``client.jobs``,
``client.resources``, and ``client.blob``.

Usage::

    from azure_jobs.core.az_client import AzureMLClient, create_rest_client

    with create_rest_client() as client:
        page, next_link = client.jobs.list_page(top=50)
        envs = client.resources.list_environments()
        code_id = client.blob.upload_code("./src")
"""

from .api import JobInfo
from .arm import AzureARMClient
from .client import AzureMLClient
from .factory import create_rest_client

__all__ = [
    "AzureARMClient",
    "AzureMLClient",
    "JobInfo",
    "create_rest_client",
]
