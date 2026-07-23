"""Feature lifecycle and optional Screen installation."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
import logging
from typing import Protocol

from textual.app import App
from textual.screen import Screen

from azure_jobs.tui.bindings import CommandBinding, CommandHandler

log = logging.getLogger(__name__)


class DashboardFeature(Protocol):
    name: str

    def commands(self) -> Mapping[str, CommandHandler]: ...

    def command_specs(self) -> tuple[CommandBinding, ...]: ...

    def install(self, app: App) -> None: ...

    def shutdown(self) -> None: ...

    def screen_entry(self) -> tuple[str, str] | None: ...


@dataclass
class Feature:
    name: str
    command_provider: Callable[[], Mapping[str, CommandHandler]]
    installer: Callable[[App], None] = lambda _app: None
    finalizer: Callable[[], None] = lambda: None
    specs: tuple[CommandBinding, ...] = ()

    def commands(self) -> Mapping[str, CommandHandler]:
        return self.command_provider()

    def install(self, app: App) -> None:
        self.installer(app)

    def command_specs(self) -> tuple[CommandBinding, ...]:
        return self.specs

    def shutdown(self) -> None:
        self.finalizer()

    def screen_entry(self) -> tuple[str, str] | None:
        return None


@dataclass
class ScreenFeature:
    """Simple extension unit for a lazily installed Textual Screen."""

    name: str
    screen_name: str
    screen_factory: Callable[[], Screen]
    command_provider: Callable[[], Mapping[str, CommandHandler]] = field(
        default=lambda: {}
    )
    specs: tuple[CommandBinding, ...] = ()

    def commands(self) -> Mapping[str, CommandHandler]:
        return self.command_provider()

    def install(self, app: App) -> None:
        app.install_screen(self.screen_factory(), self.screen_name)

    def command_specs(self) -> tuple[CommandBinding, ...]:
        return self.specs

    def shutdown(self) -> None:
        return None

    def screen_entry(self) -> tuple[str, str] | None:
        return self.name, self.screen_name


class FeatureRegistry:
    def __init__(
        self,
        features: Sequence[DashboardFeature] = (),
    ) -> None:
        self._features: list[DashboardFeature] = []
        for feature in features:
            self.add(feature)

    def add(self, feature: DashboardFeature) -> None:
        if any(existing.name == feature.name for existing in self._features):
            raise ValueError(f"Duplicate dashboard feature {feature.name!r}")
        self._features.append(feature)

    def install(self, app: App) -> None:
        for feature in self._features:
            feature.install(app)

    def commands(self) -> tuple[Mapping[str, CommandHandler], ...]:
        return tuple(feature.commands() for feature in self._features)

    def command_specs(self) -> tuple[CommandBinding, ...]:
        return tuple(
            spec
            for feature in self._features
            for spec in feature.command_specs()
        )

    def shutdown(self) -> tuple[Exception, ...]:
        errors: list[Exception] = []
        for feature in reversed(self._features):
            try:
                feature.shutdown()
            except Exception as exc:
                errors.append(exc)
                log.debug(
                    "Dashboard feature %s failed to shut down",
                    feature.name,
                    exc_info=True,
                )
        return tuple(errors)

    def screen_entries(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            entry
            for feature in self._features
            if (entry := feature.screen_entry()) is not None
        )
