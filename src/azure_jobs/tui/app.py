"""Interactive TUI dashboard composition root."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import Input, OptionList

from azure_jobs.tui.adapters import AzureSessionFactory, ConfigTargetCatalog
from azure_jobs.tui.bindings import (
    COMMAND_BINDINGS,
    CommandHandler,
    CommandRegistry,
    textual_bindings,
)
from azure_jobs.tui.components import DashboardShell, LogViewer, PickerItem
from azure_jobs.tui.controllers import (
    JobsController,
    LogsController,
    WorkspaceController,
)
from azure_jobs.tui.events import (
    EventBus,
    JobSelectionChanged,
    TargetChanging,
    TargetMissing,
    TargetReady,
)
from azure_jobs.tui.features import DashboardFeature, Feature, FeatureRegistry
from azure_jobs.tui.helpers import get_page_size
from azure_jobs.tui.log_store import LogsStore
from azure_jobs.tui.models import Job, ViewMode
from azure_jobs.tui.ports import SessionFactory, TargetCatalog
from azure_jobs.tui.runtime import TaskRunner
from azure_jobs.tui.settings import validate_last, validate_page_size
from azure_jobs.tui.stores import JobsStore, TargetStore
from azure_jobs.tui.ui import DashboardUI


class AjDashboard(App):
    """Textual shell and cross-feature composition root."""

    TITLE = "aj dashboard"
    CSS_PATH = "dashboard.tcss"
    BINDINGS = textual_bindings()
    ENABLE_COMMAND_PALETTE = True

    def __init__(
        self,
        last: int = 100,
        page_size: int | None = None,
        *,
        mouse: bool = False,
        workspace_catalog: TargetCatalog | None = None,
        session_factory: SessionFactory | None = None,
        features: Sequence[DashboardFeature] = (),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._mouse = mouse
        self._shutting_down = False
        limit = validate_last(last)
        size = validate_page_size(
            page_size if page_size is not None else get_page_size()
        )

        self.ui = DashboardUI(self)
        self.tasks = TaskRunner(self)
        self.events = EventBus()
        self.target_store = TargetStore(self.events)
        self.jobs_store = JobsStore(
            self.events,
            page_size=size,
            fetch_limit=limit,
        )
        self.logs_store = LogsStore(self.events)

        self.workspace = WorkspaceController(
            self.ui.target,
            self.tasks,
            self.target_store,
            catalog=workspace_catalog or ConfigTargetCatalog(),
            session_factory=session_factory or AzureSessionFactory(),
        )
        self.jobs = JobsController(
            self.ui.jobs,
            self.tasks,
            self.jobs_store,
            self.events,
            session_provider=lambda: self.workspace.session,
            can_actions=lambda: self.workspace.can_actions,
            can_delete=lambda: self.workspace.can_delete,
            target_id=self._target_id,
        )
        self.logs = LogsController(
            self.ui.logs,
            self.tasks,
            self.logs_store,
            self.events,
            session_provider=lambda: self.workspace.session,
            can_logs=lambda: self.workspace.can_logs,
            selected_job=lambda: self.jobs.state.selected_job,
            target_id=self._target_id,
            render_selected_info=self._render_selected_info,
        )
        self.events.subscribe(TargetChanging, self._on_target_changing)
        self.events.subscribe(TargetReady, self._on_target_ready)
        self.events.subscribe(TargetMissing, self._on_target_missing)
        self.events.subscribe(JobSelectionChanged, self._on_job_changed)
        self._feature_registry = FeatureRegistry(
            (
                Feature(
                    "target",
                    self.workspace.commands,
                    finalizer=self.workspace.shutdown,
                ),
                Feature("jobs", self.jobs.commands),
                Feature(
                    "logs",
                    self.logs.commands,
                    finalizer=self.logs.stop_streaming,
                ),
                *features,
            )
        )
        self._feature_registry.install(self)
        command_specs = (
            *COMMAND_BINDINGS,
            *self._feature_registry.command_specs(),
        )
        self._command_specs = command_specs
        self.ui.shell.set_command_specs(command_specs)
        self.commands = CommandRegistry(command_specs)
        for handlers in self._feature_registry.commands():
            if handlers:
                self.commands.register(handlers)
        self.commands.register(
            {
                "app.quit": CommandHandler(self._request_quit),
                "app.escape": CommandHandler(self._escape),
                "app.features": CommandHandler(
                    self._pick_feature,
                    lambda: bool(self._feature_registry.screen_entries()),
                ),
            }
        )
        self.commands.validate()

    def run(self, *args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("mouse", self._mouse)
        return super().run(*args, **kwargs)

    def compose(self) -> ComposeResult:
        yield DashboardShell()

    def on_mount(self) -> None:
        self.events.bind_thread()
        self.target_store.bind_thread()
        self.jobs_store.bind_thread()
        self.logs_store.bind_thread()
        for spec in self._feature_registry.command_specs():
            self.bind(
                spec.key,
                f"command({spec.command!r})",
                description=spec.description,
                show=spec.show,
            )
        self.ui.mount()
        self.ui.logs.show_info()
        self.jobs.view.render()
        self.logs.update_tab_title()
        self.workspace.start()

    def action_command(self, name: str) -> None:
        if not self.commands.contains(name):
            self.notify(f"Unknown dashboard command: {name}", severity="error")
            return
        self.commands.execute(name)

    def check_action(
        self,
        action: str,
        parameters: tuple[object, ...],
    ) -> bool | None:
        if action == "command" and parameters:
            return self.commands.enabled(str(parameters[0]))
        return True

    def _escape(self) -> None:
        if self.jobs.filters.close_search_bar(clear=True):
            return
        self.ui.shell.show_help()

    def _pick_feature(self) -> None:
        entries = self._feature_registry.screen_entries()
        self.ui.shell.pick(
            "Features",
            [
                PickerItem(screen_name, Text(label))
                for label, screen_name in entries
            ],
            "",
            self._open_feature,
        )

    def _open_feature(self, screen_name: str | None) -> None:
        if screen_name:
            self.push_screen(screen_name)

    def _request_quit(self) -> None:
        self._shutdown_dashboard()
        self.exit()

    def _shutdown_dashboard(self) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        try:
            self._feature_registry.shutdown()
        finally:
            self.tasks.shutdown()

    def on_unmount(self) -> None:
        self._shutdown_dashboard()

    def _target_id(self) -> str:
        target = self.workspace.state.current
        return target.id if target is not None else ""

    def _on_target_changing(self, event: TargetChanging) -> None:
        self.tasks.cancel_prefix("workspace.session")
        self.tasks.cancel_prefix("jobs.")
        self.tasks.cancel_prefix("logs.")
        self.logs.on_target_changing()
        self.jobs.reset()

    def _on_target_ready(self, event: TargetReady) -> None:
        self.jobs.fetcher.init_fetch()

    def _on_target_missing(self, event: TargetMissing) -> None:
        if event.target is None:
            self.ui.target.hide_info_loading()
            self.ui.target.set_info(
                "No workspace configured. Press [bold]w[/bold] to select."
            )

    def _on_job_changed(self, event: JobSelectionChanged) -> None:
        self.logs.on_job_changed(event.job)
        self.logs.update_tab_title()

    def _render_selected_info(self) -> None:
        if self.logs.state.view_mode is not ViewMode.INFO:
            return
        job = self.jobs.state.selected_job
        if job is not None:
            self.jobs.view.show_info(job)
        self.logs.update_tab_title()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "search-input":
            self.jobs.filters.on_input_changed(event.value)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search-input":
            self.jobs.filters.on_input_submitted()

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected
    ) -> None:
        if event.option_list.id == "job-list":
            self.jobs.view.on_option_selected(event.option_index)

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted
    ) -> None:
        if event.option_list.id == "job-list":
            self.jobs.view.on_option_highlighted(event.option_index)

    def on_log_viewer_backfill_requested(
        self,
        event: LogViewer.BackfillRequested,
    ) -> None:
        if self.logs.state.head_offset > 0:
            self.logs.backfill(all_remaining=event.all_remaining)
        elif self.ui.logs.log is not None:
            self.ui.logs.log.scroll_home(animate=False)
