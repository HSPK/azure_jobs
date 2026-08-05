"""Every CLI command actually runs.

These exist because the SDK migration shipped two commands that raised
``NameError``/``ImportError`` on their first line while 893 other tests passed:
nothing invoked them. A command body is only covered by *running* it, so this
walks the whole command tree and does exactly that.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from azure_jobs.client.cli import main

#: Commands that would do something irreversible, prompt, or take over the
#: terminal. Their module-level wiring is still checked by ``--help``.
#:
#: Aliases are separate Click commands that delegate, so an alias of an
#: excluded command has to be named here too — ``aj d`` really does start the
#: dashboard.
NOT_INVOKED = {
    "aj d",
    "aj dash",
    "aj daemon restart",
    "aj daemon start",
    "aj daemon stop",
    "aj init amlt",
    "aj pull",
    "aj push",
    "aj run",
    "aj template init",
    "aj template pull",
    "aj template push",
    "aj watch listen",
    "aj ws set",
}


def _walk(command, prefix="aj"):
    """Yield every leaf command with the path a user would type."""
    import click

    if isinstance(command, click.Group):
        ctx = click.Context(command)
        for name in command.list_commands(ctx):
            child = command.get_command(ctx, name)
            if child is not None:
                yield from _walk(child, f"{prefix} {name}")
    else:
        yield prefix, command


ALL_COMMANDS = sorted(_walk(main), key=lambda pair: pair[0])
assert ALL_COMMANDS, "the command tree failed to load"

INVOKED = [path for path, _ in ALL_COMMANDS if path not in NOT_INVOKED]
assert len(INVOKED) < len(ALL_COMMANDS), "nothing was excluded; check the names"
_UNKNOWN = NOT_INVOKED - {path for path, _ in ALL_COMMANDS}
assert not _UNKNOWN, f"exclusions that match no command: {sorted(_UNKNOWN)}"


@pytest.mark.parametrize("path", [p for p, _ in ALL_COMMANDS])
def test_help_renders(path: str) -> None:
    """Catches an import or decorator error in the command's module."""
    result = CliRunner().invoke(main, path.split()[1:] + ["--help"])
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize("path", INVOKED)
def test_the_body_runs(path: str, aj_env) -> None:
    """Invoke the command with the SDK stubbed out.

    The daemon is replaced wholesale rather than stubbed at the Azure layer:
    the point is to execute the command's *own* code, and a real backend would
    make this a network test. Whatever the command renders is not asserted —
    only that it did not fall over on a name that does not exist, which is the
    failure a passing test suite hid twice during the SDK migration.
    """
    from unittest.mock import MagicMock, patch

    with patch(
        "azure_jobs.connect", return_value=MagicMock()
    ):
        result = CliRunner().invoke(main, path.split()[1:])

    if isinstance(result.exception, (NameError, ImportError)):
        raise AssertionError(
            f"{path} is broken: "
            f"{type(result.exception).__name__}: {result.exception}"
        ) from result.exception


class TestInteractiveCommandsAreWiredUp:
    """The excluded commands still need their imports checked.

    ``aj dash`` is skipped above because it takes over the terminal, and that
    exclusion is exactly how a broken import reached a release: `--help` never
    runs the body. So run the body with the UI stubbed.
    """

    def test_the_dashboard_uses_its_default_sdk(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        from azure_jobs.client.cli.dashboard import dashboard

        with patch("azure_jobs.client.tui.app.AjDashboard") as app:
            app.return_value = MagicMock()
            result = CliRunner().invoke(dashboard, [])
        assert result.exception is None, result.exception
        assert result.exception is None, result.exception
        assert "session_factory" not in app.call_args.kwargs

    def test_the_dashboard_alias_reaches_the_same_command(self) -> None:
        from unittest.mock import MagicMock, patch

        with patch("azure_jobs.client.tui.app.AjDashboard") as app:
            app.return_value = MagicMock()
            result = CliRunner().invoke(main, ["d"])

        assert result.exception is None, result.exception
        assert app.called
