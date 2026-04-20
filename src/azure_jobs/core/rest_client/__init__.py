"""Azure ML REST client package — pure HTTP, no ``azure-ai-ml`` SDK.

Provides ``AzureARMClient`` (generic ARM operations) and
``AzureMLJobsClient`` (workspace-scoped: jobs, environments, datastores,
code upload, and job submission).

All public names are re-exported here so callers can simply do::

    from azure_jobs.core.rest_client import AzureARMClient
    from azure_jobs.core.rest_client import AzureMLJobsClient
    from azure_jobs.core.rest_client import create_rest_client
"""

from ._arm import AzureARMClient
from ._factory import create_rest_client
from ._ml_client import AzureMLJobsClient

__all__ = [
    "AzureARMClient",
    "AzureMLJobsClient",
    "create_rest_client",
]
