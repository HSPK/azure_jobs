"""Tests for the table display middleware (utils/ui/render.py)."""

from __future__ import annotations

import io
import json
import sys

from azure_jobs.client.ui import (
    Column,
    TableView,
    get_output_mode,
    render_table,
    set_output_mode,
)
from azure_jobs.client.ui.tables import show_sing_images_table


def _capture_stdout(fn) -> str:
    buf = io.StringIO()
    saved = sys.stdout
    sys.stdout = buf
    try:
        fn()
    finally:
        sys.stdout = saved
    return buf.getvalue()


class TestOutputMode:
    def setup_method(self):
        set_output_mode("rich")

    def teardown_method(self):
        set_output_mode("rich")

    def test_env_var_overrides(self, monkeypatch):
        monkeypatch.setenv("AJ_OUTPUT", "json")
        assert get_output_mode() == "json"

    def test_set_output_mode(self):
        set_output_mode("json")
        assert get_output_mode() == "json"
        set_output_mode("rich")
        assert get_output_mode() == "rich"


class TestRenderTableJson:
    def setup_method(self):
        set_output_mode("json")

    def teardown_method(self):
        set_output_mode("rich")

    def test_emits_json_with_rows_and_columns(self):
        view = TableView(
            title="Workspaces",
            rows=[
                {"name": "ws1", "location": "eastus"},
                {"name": "ws2", "location": "westus2"},
            ],
            columns=[
                Column(key="name"),
                Column(key="location", style="dim"),
            ],
            metadata={"subscription_id": "abc-123"},
        )
        out = _capture_stdout(lambda: render_table(view))
        parsed = json.loads(out)
        assert parsed["title"] == "Workspaces"
        assert parsed["columns"] == ["name", "location"]
        assert parsed["rows"][0]["name"] == "ws1"
        assert parsed["metadata"]["subscription_id"] == "abc-123"

    def test_json_carries_raw_status_not_markup(self):
        view = TableView(
            rows=[{"status": "Running"}],
            columns=[Column(key="status", type="status")],
        )
        out = _capture_stdout(lambda: render_table(view))
        parsed = json.loads(out)
        assert parsed["rows"][0]["status"] == "Running"
        assert "[" not in parsed["rows"][0]["status"]

    def test_empty_rows_still_emit_valid_json(self):
        view = TableView(rows=[], columns=[Column(key="x")])
        out = _capture_stdout(lambda: render_table(view))
        parsed = json.loads(out)
        assert parsed["rows"] == []


class TestRenderTableRich:
    def setup_method(self):
        set_output_mode("rich")

    def test_renders_table_with_rich_output(self, capsys):
        view = TableView(
            title="Test",
            rows=[{"a": "1", "b": "x"}],
            columns=[Column(key="a"), Column(key="b")],
        )
        render_table(view)
        captured = capsys.readouterr()
        assert "Test" in captured.out
        assert "1" in captured.out
        assert "x" in captured.out

    def test_empty_rows_warn(self, capsys):
        view = TableView(
            rows=[],
            columns=[Column(key="x")],
            empty_message="Nothing here",
        )
        render_table(view)
        captured = capsys.readouterr()
        assert "Nothing here" in captured.out


class TestColumnHeader:
    def test_explicit_header_used(self):
        assert Column(key="foo_bar", header="Custom").display_header() == "Custom"

    def test_default_header_titlecases_underscore(self):
        assert Column(key="resource_group").display_header() == "Resource Group"


class TestColumnFormatAndLink:
    def setup_method(self):
        set_output_mode("json")

    def teardown_method(self):
        set_output_mode("rich")

    def test_format_callable_used_only_for_rich(self):
        view = TableView(
            rows=[{"name": "demo", "is_default": True}],
            columns=[
                Column(
                    key="name",
                    format=lambda v, r: f"{v} (DEFAULT)" if r["is_default"] else str(v),
                ),
            ],
        )
        out = _capture_stdout(lambda: render_table(view))
        parsed = json.loads(out)
        # JSON ships raw value, NOT the formatted one
        assert parsed["rows"][0]["name"] == "demo"
        assert "DEFAULT" not in out

    def test_link_key_ignored_in_json(self):
        view = TableView(
            rows=[{"display_name": "MyJob", "portal_url": "https://portal/x"}],
            columns=[
                Column(key="display_name", link_key="portal_url"),
                Column(key="portal_url"),
            ],
        )
        out = _capture_stdout(lambda: render_table(view))
        parsed = json.loads(out)
        assert parsed["rows"][0]["display_name"] == "MyJob"
        assert "[link=" not in out


class TestStatusColumn:
    def setup_method(self):
        set_output_mode("rich")

    def test_custom_icon_and_style_maps(self, capsys):
        view = TableView(
            rows=[{"status": "submitted"}],
            columns=[
                Column(
                    key="status",
                    type="status",
                    icon_map={"submitted": "↑"},
                    style_map={"submitted": "cyan"},
                ),
            ],
        )
        render_table(view)
        out = capsys.readouterr().out
        assert "submitted" in out
        # ↑ icon should appear in Rich rendering
        assert "↑" in out


class TestSingImagesTable:
    def setup_method(self):
        set_output_mode("json")

    def teardown_method(self):
        set_output_mode("rich")

    def test_json_has_only_image_and_aliases_columns(self):
        out = _capture_stdout(
            lambda: show_sing_images_table(
                [
                    {
                        "id": "/subscriptions/sub/providers/Microsoft.Singularity/images/x",
                        "name": "torch:cuda12",
                        "aliases": ["latest", "torch:cuda12", "torch2"],
                    }
                ]
            )
        )
        parsed = json.loads(out)
        assert parsed["columns"] == ["image", "aliases"]
        assert parsed["rows"] == [
            {"image": "amlt-sing/torch:cuda12", "aliases": ["latest", "torch2"]}
        ]
