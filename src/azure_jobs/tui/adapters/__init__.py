"""Concrete dashboard service adapters."""

from azure_jobs.tui.adapters.azureml import (
    AzureSessionFactory,
    ConfigTargetCatalog,
    ConfigWorkspaceCatalog,
)

__all__ = [
    "AzureSessionFactory",
    "ConfigTargetCatalog",
    "ConfigWorkspaceCatalog",
]
