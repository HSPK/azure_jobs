"""The build identity both sides report in the handshake."""

from __future__ import annotations


def aj_version() -> str:
    from azure_jobs._version import __version__

    return __version__


__all__ = ["aj_version"]
