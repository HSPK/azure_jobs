"""Load Volcano runtime scripts with literal placeholder substitution."""

from __future__ import annotations

from pathlib import Path

_SCRIPTS_DIR = Path(__file__).parent / "scripts"


def load_script(name: str, /, **subs: object) -> list[str]:
    text = (_SCRIPTS_DIR / name).read_text(encoding="utf-8")
    for key, value in subs.items():
        text = text.replace("{" + key + "}", str(value))
    return text.splitlines()


__all__ = ["load_script"]
