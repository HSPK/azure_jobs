"""Base class for behavioural controllers.

Pattern: *State + Controller*. Each controller is a plain class that
owns a state dataclass (``self.state``) and holds a back-reference to
the :class:`~azure_jobs.tui.app.AjDashboard` App as the shared context
(``self.app``) for cross-domain access.

Background workers are scheduled via ``self.app.run_worker(...)`` rather
than the ``@work`` decorator (which requires the host class to be a
Textual ``App`` / ``MessageTarget``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Generic, TypeVar

if TYPE_CHECKING:
    from azure_jobs.tui.app import AjDashboard

S = TypeVar("S")
W = TypeVar("W")


class Controller(Generic[S]):
    """Holds (a) the App context and (b) its own state slice.

    Provides four small helpers used by every concrete controller so the
    same fault-tolerant patterns aren't re-implemented per file:

    * :meth:`safe_query` — ``query_one`` that returns ``None`` instead of
      raising during teardown / modal stacking.
    * :meth:`render_info` — markup-safe write to the right-pane info area.
    * :meth:`spawn` — uniform thread-worker scheduling.
    * :meth:`notify` — markup-safe notification.
    """

    def __init__(self, app: "AjDashboard", state: S) -> None:
        self.app: AjDashboard = app
        self.state: S = state

    # ---- widget helpers -----------------------------------------------------

    def safe_query(self, selector: str, widget_type: type["W"]) -> "W | None":
        """Type-checked ``query_one`` returning ``None`` on NoMatches."""
        try:
            return self.app.query_one(selector, widget_type)
        except Exception:
            return None

    def render_info(self, hint: str) -> None:
        """Render *hint* into the info panel via the markup-safe boundary."""
        from azure_jobs.tui.helpers import kv, safe_set

        safe_set(self.app.widgets.info, kv([("", "")], hint=hint))

    def notify(
        self, msg: str, *, severity: str = "information", timeout: float = 5
    ) -> None:
        """Emit a notification; falls back to plain text on MarkupError."""
        from azure_jobs.tui.helpers import safe_notify

        safe_notify(self.app, msg, severity=severity, timeout=timeout)

    # ---- worker helpers -----------------------------------------------------

    def spawn(
        self,
        fn: Callable[[], None],
        *,
        group: str,
        exclusive: bool = True,
    ) -> None:
        """Schedule *fn* on a thread worker (uniform call site)."""
        self.app.run_worker(fn, thread=True, exclusive=exclusive, group=group)
