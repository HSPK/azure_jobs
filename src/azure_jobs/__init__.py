"""Azure Jobs — fast CLI for Azure ML job submission and management."""

try:
    from azure_jobs._version import __version__
except ImportError:  # editable install without build
    __version__ = "0.0.0.dev0"