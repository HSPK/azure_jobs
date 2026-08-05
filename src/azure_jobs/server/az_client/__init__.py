"""Server-side Azure HTTP clients.

``AzureClient`` owns account-scoped namespaces. ``AzureWorkspaceClient`` owns
workspace-scoped namespaces. Domain row types live in ``shared.types`` rather
than being re-exported through this package.
"""

from .arm import AzureClient
from .auth import WorkspaceCoords
from .ml import (
    AzureWorkspaceClient,
    fetch_jobs_all_workspaces,
)

__all__ = [
    "AzureClient",
    "AzureWorkspaceClient",
    "WorkspaceCoords",
    "fetch_jobs_all_workspaces",
]
