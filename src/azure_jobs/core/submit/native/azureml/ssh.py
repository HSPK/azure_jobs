"""SSH key shipping for native AML / Singularity submissions."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable

from ...models import SubmitEvent

log = logging.getLogger(__name__)

_SSH_OPT_OUT_ENV = "AJ_SHIP_SSH"
_SSH_WHITELIST = frozenset(
    {"id_rsa", "id_ed25519", "id_ecdsa", "config", "known_hosts"}
)
_FALSY = frozenset({"0", "false", "no", "off"})

def _ssh_disabled() -> bool:
    return os.getenv(_SSH_OPT_OUT_ENV, "").strip().lower() in _FALSY

def _collect_ssh_files(
    code_dir: str,
    on_event: Callable[[SubmitEvent], None],
) -> dict[str, bytes]:
    code_path = Path(code_dir).resolve()
    if (code_path / ".ssh").is_dir():
        return {}

    if _ssh_disabled():
        return {".ssh/.keep": b""}

    home_ssh = Path.home() / ".ssh"
    if not home_ssh.is_dir():
        return {".ssh/.keep": b""}

    result: dict[str, bytes] = {}
    shipped: list[str] = []
    for fp in sorted(home_ssh.iterdir()):
        if fp.is_file() and fp.name in _SSH_WHITELIST:
            try:
                result[f".ssh/{fp.name}"] = fp.read_bytes()
                shipped.append(fp.name)
            except OSError:
                log.debug("Failed to read %s", fp, exc_info=True)
    if not result:
        return {".ssh/.keep": b""}

    on_event(
        SubmitEvent(
            kind="log",
            detail=f"Shipping ~/.ssh files to remote: {', '.join(shipped)} "
            f"(disable with {_SSH_OPT_OUT_ENV}=0)",
        )
    )
    return result
