"""Single source of truth for dashboard commands and help."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from textual.binding import Binding


@dataclass(frozen=True)
class CommandBinding:
    section: str
    key: str
    command: str
    description: str
    show: bool = False


@dataclass(frozen=True)
class CommandHandler:
    run: Callable[[], None]
    enabled: Callable[[], bool] = lambda: True


COMMAND_BINDINGS = (
    CommandBinding("Jobs", "r", "jobs.refresh", "Refresh"),
    CommandBinding("Jobs", "c", "jobs.cancel", "Cancel"),
    CommandBinding("Jobs", "d", "jobs.delete", "Delete"),
    CommandBinding("Jobs", "left", "jobs.prev", "Previous page"),
    CommandBinding("Jobs", "right", "jobs.next", "Next page"),
    CommandBinding("Jobs", "up", "jobs.selection_prev", "Previous job"),
    CommandBinding("Jobs", "down", "jobs.selection_next", "Next job"),
    CommandBinding("Filters", "f", "jobs.status", "Status"),
    CommandBinding("Filters", "e", "jobs.experiment", "Experiment"),
    CommandBinding("Filters", "slash", "jobs.search", "Search"),
    CommandBinding("Filters", "F", "jobs.clear", "Clear filters"),
    CommandBinding("Workspace", "w", "workspace.pick", "Workspace", True),
    CommandBinding("Views", "l", "logs.show", "Logs", True),
    CommandBinding("Views", "i", "logs.info", "Info", True),
    CommandBinding("Logs", "L", "logs.toggle", "Stop/resume live tail"),
    CommandBinding("Logs", "s", "logs.scroll", "Toggle auto-scroll"),
    CommandBinding("Logs", "ctrl+s", "logs.save", "Save buffer"),
    CommandBinding("Logs", "o", "logs.file", "Pick log file"),
    CommandBinding("Info navigation", "h", "info.left", "Scroll left"),
    CommandBinding("Info navigation", "j", "info.down", "Scroll down"),
    CommandBinding("Info navigation", "k", "info.up", "Scroll up"),
    CommandBinding("Info navigation", "g", "info.home", "Jump to top"),
    CommandBinding("Info navigation", "G", "info.end", "Jump to bottom"),
    CommandBinding("Info navigation", "ctrl+d", "info.page_down", "Page down"),
    CommandBinding("Info navigation", "ctrl+u", "info.page_up", "Page up"),
    CommandBinding("Info navigation", "ctrl+f", "info.page_down", "Page down"),
    CommandBinding("Info navigation", "ctrl+b", "info.page_up", "Page up"),
    CommandBinding("General", "escape", "app.escape", "Manual"),
    CommandBinding("General", "p", "app.features", "Feature screens"),
    CommandBinding("General", "q", "app.quit", "Quit"),
)


class CommandRegistry:
    """Validated command metadata, handlers, context, and help."""

    def __init__(self, specs: tuple[CommandBinding, ...]) -> None:
        keys = [spec.key for spec in specs]
        if len(keys) != len(set(keys)):
            raise ValueError("Duplicate dashboard key binding")
        self.specs = specs
        self._handlers: dict[str, CommandHandler] = {}

    def register(
        self,
        handlers: Mapping[str, CommandHandler | Callable[[], None]],
    ) -> None:
        known = {spec.command for spec in self.specs}
        for name, value in handlers.items():
            if name not in known:
                raise KeyError(f"No command metadata registered for {name!r}")
            if name in self._handlers:
                raise ValueError(f"Duplicate dashboard command handler {name!r}")
            self._handlers[name] = (
                value if isinstance(value, CommandHandler) else CommandHandler(value)
            )

    def validate(self) -> None:
        missing = {
            spec.command for spec in self.specs
        } - self._handlers.keys()
        if missing:
            raise RuntimeError(
                "Missing dashboard command handlers: "
                + ", ".join(sorted(missing))
            )

    def execute(self, name: str) -> bool:
        handler = self._handlers.get(name)
        if handler is None or not handler.enabled():
            return False
        handler.run()
        return True

    def contains(self, name: str) -> bool:
        return name in {spec.command for spec in self.specs}

    def enabled(self, name: str) -> bool:
        handler = self._handlers.get(name)
        return bool(handler and handler.enabled())


def textual_bindings(
    specs: tuple[CommandBinding, ...] = COMMAND_BINDINGS,
) -> list[Binding]:
    return [
        Binding(
            item.key,
            f"command({item.command!r})",
            item.description,
            show=item.show,
        )
        for item in specs
    ]


def help_text(
    specs: tuple[CommandBinding, ...] = COMMAND_BINDINGS,
) -> str:
    lines: list[str] = []
    sections = dict.fromkeys(item.section for item in specs)
    for section in sections:
        lines.append(f"  [bold cyan]{section}[/bold cyan]")
        for item in specs:
            if item.section != section:
                continue
            key = item.key.replace("slash", "/").replace("ctrl+", "^")
            lines.append(f"    [bold]{key:<10}[/bold] {item.description}")
        lines.append("")
    lines.append("  [dim]Press Esc or q to close[/dim]")
    return "\n".join(lines)
