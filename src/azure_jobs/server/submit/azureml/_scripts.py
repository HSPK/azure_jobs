"""Tiny loader for Azure ML bootstrap resources."""

from __future__ import annotations

from pathlib import Path

_SCRIPTS_DIR = Path(__file__).parent / "scripts"


def script_path(name: str) -> Path:
    return _SCRIPTS_DIR / name


def load_script(name: str, /, **subs: object) -> list[str]:
    text = script_path(name).read_text(encoding="utf-8")
    for key, value in subs.items():
        text = text.replace("{" + key + "}", str(value))
    return text.splitlines()


__all__ = ["load_script", "script_path"]
