"""Gitignore-style pattern matching."""

from __future__ import annotations

import re
from typing import Iterable

__all__ = ["IgnoreMatcher", "compile_ignore"]

_GLOB_META = re.compile(r"[*?\[]")

def _translate(pat: str) -> str:
    out: list[str] = []
    i = 0
    n = len(pat)
    while i < n:
        c = pat[i]
        if c == "*":
            if i + 1 < n and pat[i + 1] == "*":
                if i + 2 < n and pat[i + 2] == "/":
                    out.append("(?:.*/)?")
                    i += 3
                    continue
                out.append(".*")
                i += 2
                continue
            out.append("[^/]*")
            i += 1
            continue
        if c == "?":
            out.append("[^/]")
            i += 1
            continue
        if c == "[":
            j = i + 1
            if j < n and pat[j] == "!":
                j += 1
            if j < n and pat[j] == "]":
                j += 1
            while j < n and pat[j] != "]":
                j += 1
            if j >= n:
                out.append(re.escape(c))
                i += 1
                continue
            cls = pat[i + 1 : j]
            if cls.startswith("!"):
                cls = "^" + cls[1:]
            out.append("[" + cls + "]")
            i = j + 1
            continue
        if c == "/":
            out.append("/")
        else:
            out.append(re.escape(c))
        i += 1
    return "".join(out)

class _Rule:

    __slots__ = ("regex", "negate", "dir_only")

    def __init__(self, regex: re.Pattern[str], negate: bool, dir_only: bool):
        self.regex = regex
        self.negate = negate
        self.dir_only = dir_only

def _parse(line: str) -> tuple[str, bool, bool, bool] | None:
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    negate = False
    if s.startswith("!"):
        negate = True
        s = s[1:]
    elif s.startswith("\\!") or s.startswith("\\#"):
        s = s[1:]
    dir_only = s.endswith("/")
    if dir_only:
        s = s[:-1]
    if not s:
        return None
    if s.startswith("/"):
        s = s[1:]
        anchored = True
    else:
        anchored = "/" in s
    return s, anchored, dir_only, negate

def _build_regex(body: str, anchored: bool) -> re.Pattern[str]:
    rx = _translate(body)
    if anchored:
        full = r"\A" + rx + r"\Z"
    else:
        full = r"(?:\A|/)" + rx + r"\Z"
    return re.compile(full)

class IgnoreMatcher:
    """Compiled gitignore-style matcher."""

    __slots__ = (
        "_rules",
        "_has_negation",
        "_negate_prefixes",
        "_negate_anywhere",
        "_literal_files",
        "_literal_dirs",
        "_basename_re",
        "_basename_dir_re",
        "_path_re",
        "_path_dir_re",
        "_empty",
    )

    def __init__(self, patterns: Iterable[str]) -> None:
        parsed: list[tuple[str, bool, bool, bool]] = []
        has_negation = False
        for raw in patterns:
            p = _parse(raw)
            if p is None:
                continue
            if p[3]:
                has_negation = True
            parsed.append(p)

        self._has_negation = has_negation
        self._empty = not parsed

        negate_prefixes: list[str] = []
        negate_anywhere = False
        for body, anchored, _dir_only, negate in parsed:
            if not negate:
                continue
            if not anchored:
                negate_anywhere = True
                continue
            m = _GLOB_META.search(body)
            literal = body[: m.start()] if m else body
            negate_prefixes.append(literal)
        self._negate_prefixes = tuple(negate_prefixes)
        self._negate_anywhere = negate_anywhere

        if has_negation:
            self._rules = [
                _Rule(_build_regex(body, anchored), negate, dir_only)
                for body, anchored, dir_only, negate in parsed
            ]
            self._literal_files = frozenset()
            self._literal_dirs = frozenset()
            self._basename_re = None
            self._basename_dir_re = None
            self._path_re = None
            self._path_dir_re = None
            return

        lit_files: set[str] = set()
        lit_dirs: set[str] = set()
        bn_any: list[str] = []
        bn_dir: list[str] = []
        path_any: list[str] = []
        path_dir: list[str] = []

        for body, anchored, dir_only, _ in parsed:
            if not anchored and not _GLOB_META.search(body):
                if dir_only:
                    lit_dirs.add(body)
                else:
                    lit_files.add(body)
                    lit_dirs.add(body)
                continue
            rx = _translate(body)
            if anchored:
                full = rx
                bucket = path_dir if dir_only else path_any
            else:
                full = rx
                bucket = bn_dir if dir_only else bn_any
            bucket.append(full)

        def _combine(parts: list[str], anchored: bool) -> re.Pattern[str] | None:
            if not parts:
                return None
            joined = "|".join(f"(?:{p})" for p in parts)
            prefix = r"\A" if anchored else r"(?:\A|/)"
            return re.compile(prefix + "(?:" + joined + r")\Z")

        self._rules = []
        self._literal_files = frozenset(lit_files)
        self._literal_dirs = frozenset(lit_dirs)
        self._basename_re = _combine(bn_any, anchored=False)
        self._basename_dir_re = _combine(bn_dir, anchored=False)
        self._path_re = _combine(path_any, anchored=True)
        self._path_dir_re = _combine(path_dir, anchored=True)

    def __bool__(self) -> bool:
        return not self._empty

    def match(self, rel: str, is_dir: bool = False) -> bool:
        """Return True if *rel* (forward-slash, no leading /) is ignored."""
        if self._empty:
            return False
        if self._has_negation:
            return self._match_slow(rel, is_dir)

        lit_dirs = self._literal_dirs
        lit_files = self._literal_files
        if lit_dirs or lit_files:
            parts = rel.split("/")
            for comp in parts[:-1]:
                if comp in lit_dirs:
                    return True
            leaf = parts[-1]
            if is_dir:
                if leaf in lit_dirs:
                    return True
            else:
                if leaf in lit_files:
                    return True
        if is_dir:
            if self._basename_dir_re and self._basename_dir_re.search(rel):
                return True
            if self._path_dir_re and self._path_dir_re.search(rel):
                return True
        if self._basename_re and self._basename_re.search(rel):
            return True
        if self._path_re and self._path_re.search(rel):
            return True
        return False

    def _match_slow(self, rel: str, is_dir: bool) -> bool:
        ignored = False
        for rule in self._rules:
            if rule.dir_only and not is_dir:
                continue
            if rule.regex.search(rel) is not None:
                ignored = not rule.negate
        return ignored

    @property
    def has_negation(self) -> bool:
        """True if any pattern is a !"""
        return self._has_negation

    def prune_safe_for(self, dir_rel: str) -> bool:
        """Return True if pruning *dir_rel*'s subtree won't drop a file that a…."""
        if not self._has_negation:
            return True
        if self._negate_anywhere:
            return False
        prefix = dir_rel + "/" if dir_rel else ""
        for p in self._negate_prefixes:
            if p == dir_rel or (prefix and p.startswith(prefix)):
                return False
        return True

def compile_ignore(patterns: Iterable[str]) -> IgnoreMatcher:
    """Convenience: build an :class:IgnoreMatcher from a pattern list."""
    return IgnoreMatcher(patterns)
