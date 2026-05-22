"""Behaviour controllers for the TUI dashboard."""

from __future__ import annotations

from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.jobs import JobsController
from azure_jobs.tui.controllers.logs import LogsController
from azure_jobs.tui.controllers.workspace import WorkspaceController

__all__ = [
    "Controller",
    "JobsController",
    "LogsController",
    "WorkspaceController",
]
