"""End-to-end JSON-mode audit for ``aj`` commands.

Verifies that representative commands emit a single, parseable JSON
envelope on stdout with no Rich markup leakage.
"""

from __future__ import annotations

import json

from click.testing import CliRunner

from azure_jobs.cli import main
from azure_jobs.utils.ui import set_output_mode


def _capture_aj(args: list[str]) -> str:
    """Invoke the aj CLI and return stdout."""
    runner = CliRunner()
    result = runner.invoke(main, args, catch_exceptions=False)
    return result.output


def _invoke_aj(args: list[str]):
    runner = CliRunner()
    return runner.invoke(main, args, catch_exceptions=False)


def _assert_clean_json(out: str) -> dict:
    """Parse *out* as JSON; assert no Rich markup leaks."""
    parsed = json.loads(out)
    flat = json.dumps(parsed)
    assert "[bold" not in flat, f"Rich markup leaked: {flat!r}"
    assert "[/bold" not in flat
    assert "[dim" not in flat
    assert "[link=" not in flat
    return parsed


class TestConfigJson:
    def teardown_method(self):
        set_output_mode("rich")

    def test_config_show_emits_envelope(self, aj_home):
        out = _capture_aj(["--json", "config", "show"])
        parsed = _assert_clean_json(out)
        assert parsed["kind"] == "config"
        assert isinstance(parsed["config"], dict)

    def test_config_timezone_get(self, aj_home):
        out = _capture_aj(["--json", "config", "timezone"])
        parsed = _assert_clean_json(out)
        assert parsed["kind"] == "config_value"
        assert parsed["key"] == "timezone"

    def test_config_experiment_set(self, aj_home):
        out = _capture_aj(["--json", "config", "experiment", "my_exp"])
        parsed = _assert_clean_json(out)
        assert parsed["kind"] == "command_result"
        assert parsed["action"] == "config.experiment"
        assert parsed["status"] == "ok"
        assert parsed["value"] == "my_exp"


class TestTemplateJson:
    def teardown_method(self):
        set_output_mode("rich")

    def _write_template(self, aj_home, name="demo"):
        (aj_home / "template" / f"{name}.yaml").write_text(
            "base: null\n"
            "config:\n"
            "  jobs:\n"
            "    - name: demo\n"
            "      sku: G1\n"
            "      command: ['echo']\n"
            "  target:\n"
            "    service: aml\n"
            "    name: aml-compute\n"
        )

    def test_template_show_emits_envelope(self, aj_home):
        self._write_template(aj_home)
        out = _capture_aj(["--json", "template", "show", "demo"])
        parsed = _assert_clean_json(out)
        assert parsed["kind"] == "template_detail"
        assert parsed["name"] == "demo"
        assert parsed["config"]["jobs"][0]["sku"] == "G1"

    def test_template_validate_emits_envelope(self, aj_home):
        self._write_template(aj_home)
        out = _capture_aj(["--json", "template", "validate"])
        parsed = _assert_clean_json(out)
        assert parsed["kind"] == "template_validate"
        assert parsed["valid_count"] == 1
        assert parsed["invalid_count"] == 0
        assert parsed["results"][0]["name"] == "demo"
        assert parsed["results"][0]["ok"] is True


class TestInitJsonRefuses:
    def teardown_method(self):
        set_output_mode("rich")

    def test_init_in_json_mode_emits_failure(self, aj_home):
        result = _invoke_aj(["--json", "init"])
        assert result.exit_code == 1
        parsed = _assert_clean_json(result.output)
        assert parsed["kind"] == "command_result"
        assert parsed["action"] == "init"
        assert parsed["status"] == "failed"


class TestWorkspaceShowJsonNoConfig:
    def teardown_method(self):
        set_output_mode("rich")

    def test_no_workspace_emits_envelope(self, aj_home):
        out = _capture_aj(["--json", "ws", "show"])
        parsed = _assert_clean_json(out)
        assert parsed["kind"] == "workspace_detail"
        assert parsed["configured"] is False
        assert parsed["workspace"] is None


class TestJsonContract:
    """Generic invariants every JSON-mode envelope must satisfy."""

    def teardown_method(self):
        set_output_mode("rich")

    def test_envelopes_have_kind_discriminator(self, aj_home):
        for args in [
            ["--json", "config", "show"],
            ["--json", "config", "timezone"],
            ["--json", "ws", "show"],
        ]:
            out = _capture_aj(args)
            parsed = _assert_clean_json(out)
            assert "kind" in parsed, f"{args} missing 'kind' discriminator"
