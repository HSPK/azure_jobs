"""Generic filesystem helpers shared by submit / upload paths."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from azure_jobs.utils.ignore import IgnoreMatcher

DEFAULT_IGNORE_DIRS: frozenset[str] = frozenset(
    {"__pycache__", ".git", ".venv", "node_modules"}
)

IGNORE_FILES: tuple[str, ...] = (".codeignore", ".amltignore")

_AJ_META = ".azure_jobs"
_AJ_META_KEEP = "scripts"

def read_ignore_file(code_dir: str | Path) -> list[str]:
    """Load ignore patterns from .codeignore / .amltignore."""
    base = Path(code_dir).resolve() if code_dir else Path.cwd()
    if not base.is_dir():
        return []
    for fname in IGNORE_FILES:
        fp = base / fname
        if not fp.is_file():
            continue
        try:
            lines = fp.read_text(encoding="utf-8").splitlines()
        except OSError:
            return []
        out: list[str] = []
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            out.append(line)
        return out
    return []

def _is_default_excluded(rel: str) -> bool:
    parts = rel.split("/")
    if any(c in DEFAULT_IGNORE_DIRS for c in parts):
        return True
    if len(parts) >= 2 and parts[0] == _AJ_META and parts[1] != _AJ_META_KEEP:
        return True
    return False

def should_ignore(
    fp: Path,
    root: Path,
    patterns: list[str] | None,
) -> bool:
    """Return True if *fp* should be excluded from a code upload."""
    rel = fp.relative_to(root).as_posix()
    if _is_default_excluded(rel):
        return True
    if not patterns:
        return False
    return IgnoreMatcher(patterns).match(rel, is_dir=fp.is_dir())

@dataclass(frozen=True)
class CodeFile:
    """One file selected for upload."""

    rel: str
    path: Path
    size: int

def walk_code(
    code_dir: str | Path,
    patterns: Iterable[str] | None = None,
) -> list[CodeFile]:
    """Walk *code_dir* and return the files selected for upload."""
    base = Path(code_dir).resolve()
    if not base.is_dir():
        return []
    matcher = IgnoreMatcher(list(patterns) if patterns else [])

    out: list[CodeFile] = []
    for root, dirs, files in os.walk(base):
        rel_root = os.path.relpath(root, base)
        rel_root = "" if rel_root == "." else rel_root.replace(os.sep, "/")

        kept: list[str] = []
        for d in sorted(dirs):
            if d in DEFAULT_IGNORE_DIRS:
                continue
            rel_d = f"{rel_root}/{d}" if rel_root else d
            if _is_default_excluded(rel_d):
                continue
            if (
                matcher
                and matcher.match(rel_d, is_dir=True)
                and matcher.prune_safe_for(rel_d)
            ):
                continue
            kept.append(d)
        dirs[:] = kept

        for f in sorted(files):
            rel = f"{rel_root}/{f}" if rel_root else f
            if _is_default_excluded(rel):
                continue
            if matcher and matcher.match(rel, is_dir=False):
                continue
            abs_path = Path(root) / f
            try:
                size = abs_path.stat().st_size
            except OSError:
                size = 0
            out.append(CodeFile(rel=rel, path=abs_path, size=size))
    return out

def compute_code_hash(
    files: Iterable[CodeFile],
    extras: dict[str, bytes] | None = None,
    *,
    length: int = 16,
) -> str:
    """Deterministic content hash for a code upload."""
    on_disk = {cf.rel: cf.path for cf in files}
    extras = extras or {}
    hasher = hashlib.sha256()
    for rel in sorted({*on_disk, *extras}):
        hasher.update(rel.encode())
        if rel in on_disk:
            file_hash = hashlib.sha256()
            with on_disk[rel].open("rb") as fh:
                for chunk in iter(lambda: fh.read(65536), b""):
                    file_hash.update(chunk)
            hasher.update(file_hash.digest())
        else:
            hasher.update(hashlib.sha256(extras[rel]).digest())
    return hasher.hexdigest()[:length]
