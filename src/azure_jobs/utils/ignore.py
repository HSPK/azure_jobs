"""Gitignore-style pattern matching.

Implements a fast subset of git's ignore semantics:

* ``#`` introduces a comment (escape with ``\\#``).
* Trailing ``/`` constrains a pattern to directories only.
* Leading ``/`` anchors to the root of the walk; an internal ``/`` likewise
  anchors. Patterns without any ``/`` match a basename anywhere.
* ``*`` matches zero or more characters, but never ``/``.
* ``?`` matches a single character, never ``/``.
* ``[abc]`` / ``[!abc]`` character classes.
* ``**`` matches zero or more path components. Recognized forms are
  ``**/foo``, ``foo/**``, ``foo/**/bar`` (the most common shapes used in
  ``.gitignore``).
* ``!`` prefix negates a previous match (last-match-wins).

Each user pattern is translated to a Python ``re.Pattern`` exactly once at
construction time. The match path uses two pre-compiled alternation
regexes (basename rules + anchored path rules) plus an ``frozenset`` of
literal basenames, giving O(1) (in pattern count) checks per file/dir.
The slow per-rule path is only used when a pattern set contains
negations.
"""

from __future__ import annotations

import re
from typing import Iterable

__all__ = ["IgnoreMatcher", "compile_ignore"]


_GLOB_META = re.compile(r"[*?\[]")


def _translate(pat: str) -> str:
    """Translate a gitignore glob to a regex body (no anchors)."""
    out: list[str] = []
    i = 0
    n = len(pat)
    while i < n:
        c = pat[i]
        if c == "*":
            # Detect ``**`` and its surrounding slashes.
            if i + 1 < n and pat[i + 1] == "*":
                # ``/**/`` -> zero-or-more components (the slashes are
                # consumed by the regex translation directly).
                if i + 2 < n and pat[i + 2] == "/":
                    out.append("(?:.*/)?")
                    i += 3
                    continue
                # Trailing ``**`` matches anything (including slashes).
                out.append(".*")
                i += 2
                continue
            # Single ``*`` — no slash crossing.
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
    """A single compiled ignore rule (used only on the negation slow path)."""

    __slots__ = ("regex", "negate", "dir_only")

    def __init__(self, regex: re.Pattern[str], negate: bool, dir_only: bool):
        self.regex = regex
        self.negate = negate
        self.dir_only = dir_only


def _parse(line: str) -> tuple[str, bool, bool, bool] | None:
    """Parse a raw line. Return ``(body, anchored, dir_only, negate)`` or
    ``None`` if the line is empty / a comment."""
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
    """Build the final compiled regex for a parsed pattern."""
    rx = _translate(body)
    if anchored:
        full = r"\A" + rx + r"\Z"
    else:
        # Match either at the start of the rel-path or right after a ``/``.
        full = r"(?:\A|/)" + rx + r"\Z"
    return re.compile(full)


class IgnoreMatcher:
    """Compiled gitignore-style matcher.

    Construction is ``O(P)`` in the number of patterns; ``match()`` is
    ``O(1)`` per call when no negation patterns are present (one set
    membership test plus two combined-regex searches). With negations the
    matcher falls back to a per-rule scan that is still ``O(P)`` but
    rarely a bottleneck because typical pattern sets are small.
    """

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

        # ---- compute negation "scope" prefixes for safe directory pruning.
        # A negation rule can only re-include paths under its literal
        # prefix (the substring up to the first glob metacharacter). For
        # basename-only negations (no anchoring slash) the prefix could
        # match anywhere, so pruning is universally unsafe.
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
            # Slow path: keep individual rules so last-match-wins works.
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

        # Fast path: bucket by (anchored?, dir_only?) and merge into one
        # alternation regex per bucket. Literal basename patterns become
        # a frozenset for hash lookup.
        lit_files: set[str] = set()
        lit_dirs: set[str] = set()
        bn_any: list[str] = []
        bn_dir: list[str] = []
        path_any: list[str] = []
        path_dir: list[str] = []

        for body, anchored, dir_only, _ in parsed:
            if not anchored and not _GLOB_META.search(body):
                # Literal basename pattern (e.g. ``__pycache__``).
                if dir_only:
                    lit_dirs.add(body)
                else:
                    # Files: the same name can also match a dir of that name
                    # (rare but legal). Track both so dir pruning works.
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
        """Return ``True`` if *rel* (forward-slash, no leading ``/``) is
        ignored. *is_dir* is required for correct ``dir/`` handling."""
        if self._empty:
            return False
        if self._has_negation:
            return self._match_slow(rel, is_dir)

        # ---- fast path -----------------------------------------------------
        # Basename literal check: scan path components in O(depth).
        lit_dirs = self._literal_dirs
        lit_files = self._literal_files
        if lit_dirs or lit_files:
            parts = rel.split("/")
            # Any ancestor-dir name in lit_dirs => ignored.
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
        """True if any pattern is a ``!`` negation. Disables dir pruning."""
        return self._has_negation

    def prune_safe_for(self, dir_rel: str) -> bool:
        """Return ``True`` if pruning *dir_rel*'s subtree won't drop a
        file that a negation rule would re-include.

        Used by directory walkers: when this returns ``False``, the dir
        must be descended into even if :meth:`match` reports it as
        ignored, because a deeper path may match a ``!`` rule.
        """
        if not self._has_negation:
            return True
        if self._negate_anywhere:
            # A basename-only negation can re-include something anywhere.
            return False
        prefix = dir_rel + "/" if dir_rel else ""
        for p in self._negate_prefixes:
            # Negation prefix lands inside (or equal to) dir_rel's subtree.
            if p == dir_rel or (prefix and p.startswith(prefix)):
                return False
        return True


def compile_ignore(patterns: Iterable[str]) -> IgnoreMatcher:
    """Convenience: build an :class:`IgnoreMatcher` from a pattern list."""
    return IgnoreMatcher(patterns)
