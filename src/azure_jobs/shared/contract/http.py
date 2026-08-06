"""Small HTTP constants shared by the daemon and SDK transport."""

API_PREFIX = "/v2"
API_VERSION = 2
MIN_API_VERSION = 2

ROOT_HEADER = "X-AJ-Root"
CLIENT_VERSION_HEADER = "X-AJ-Client"

# Azure ML workspace names are 3-33 characters, so "_" cannot collide.
DEFAULT_WORKSPACE = "_"

__all__ = [
    "API_PREFIX",
    "API_VERSION",
    "CLIENT_VERSION_HEADER",
    "DEFAULT_WORKSPACE",
    "MIN_API_VERSION",
    "ROOT_HEADER",
]
