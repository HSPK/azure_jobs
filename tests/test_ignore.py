"""Tests for the gitignore-style matcher and code-tree walker."""

from __future__ import annotations

from pathlib import Path

import pytest

from azure_jobs.utils.fs import walk_code
from azure_jobs.utils.ignore import IgnoreMatcher

# ---------------------------------------------------------------------------
# IgnoreMatcher: basic gitignore semantics
# ---------------------------------------------------------------------------


class TestMatcherBasic:
    def test_empty_matcher_matches_nothing(self):
        m = IgnoreMatcher([])
        assert not m
        assert m.match("anything") is False
        assert m.match("a/b/c", is_dir=True) is False

    def test_blank_lines_and_comments_skipped(self):
        m = IgnoreMatcher(["", "  ", "# comment", "*.pyc"])
        assert m.match("foo.pyc") is True
        assert m.match("foo.py") is False

    def test_basename_no_slash_matches_anywhere(self):
        m = IgnoreMatcher(["__pycache__"])
        assert m.match("__pycache__", is_dir=True) is True
        assert m.match("a/__pycache__", is_dir=True) is True
        assert m.match("a/b/__pycache__/x.pyc") is True
        assert m.match("foo") is False

    def test_anchored_leading_slash(self):
        m = IgnoreMatcher(["/build"])
        assert m.match("build") is True
        assert m.match("a/build") is False

    def test_internal_slash_is_anchored(self):
        m = IgnoreMatcher(["src/build"])
        assert m.match("src/build") is True
        assert m.match("x/src/build") is False

    def test_dir_only_pattern(self):
        m = IgnoreMatcher(["logs/"])
        assert m.match("logs", is_dir=True) is True
        assert m.match("logs", is_dir=False) is False
        assert m.match("a/logs", is_dir=True) is True

    def test_star_does_not_cross_slash(self):
        m = IgnoreMatcher(["*.pyc"])
        assert m.match("foo.pyc") is True
        assert m.match("a/foo.pyc") is True
        # Still matches via basename anchoring after a slash.
        assert m.match("a/b/foo.pyc") is True
        assert m.match("foo.py") is False

    def test_question_mark(self):
        m = IgnoreMatcher(["?.txt"])
        assert m.match("a.txt") is True
        assert m.match("ab.txt") is False

    def test_character_class(self):
        m = IgnoreMatcher(["[abc].txt"])
        assert m.match("a.txt") is True
        assert m.match("d.txt") is False

    def test_negated_class(self):
        m = IgnoreMatcher(["[!abc].txt"])
        assert m.match("d.txt") is True
        assert m.match("a.txt") is False


class TestMatcherDoubleStar:
    def test_leading_doublestar(self):
        m = IgnoreMatcher(["**/build"])
        assert m.match("build", is_dir=True) is True
        assert m.match("a/build", is_dir=True) is True
        assert m.match("a/b/c/build", is_dir=True) is True

    def test_trailing_doublestar(self):
        m = IgnoreMatcher(["logs/**"])
        assert m.match("logs/a") is True
        assert m.match("logs/a/b/c") is True
        assert m.match("logs") is False

    def test_middle_doublestar(self):
        m = IgnoreMatcher(["a/**/b"])
        assert m.match("a/b") is True
        assert m.match("a/x/b") is True
        assert m.match("a/x/y/b") is True
        assert m.match("a/b/c") is False


class TestMatcherNegation:
    def test_negation_reincludes(self):
        m = IgnoreMatcher(["*.log", "!keep.log"])
        assert m.has_negation is True
        assert m.match("foo.log") is True
        assert m.match("keep.log") is False

    def test_last_match_wins(self):
        m = IgnoreMatcher(["*.log", "!keep.log", "keep.log"])
        assert m.match("keep.log") is True

    def test_prune_safe_without_negation(self):
        m = IgnoreMatcher(["*.pyc"])
        assert m.prune_safe_for("any/dir") is True

    def test_prune_safe_under_negation_scope(self):
        # Negation only re-includes things under .azure_jobs/scripts/
        m = IgnoreMatcher([".azure_jobs/**", "!.azure_jobs/scripts/**"])
        assert m.prune_safe_for("__pycache__") is True
        assert m.prune_safe_for("logs") is True
        # Not safe to prune .azure_jobs because scripts/ is under it.
        assert m.prune_safe_for(".azure_jobs") is False
        assert m.prune_safe_for(".azure_jobs/scripts") is False
        assert m.prune_safe_for(".azure_jobs/other") is True

    def test_prune_unsafe_with_basename_negation(self):
        # Basename-only negation can re-include anywhere → never safe.
        m = IgnoreMatcher(["data/", "!keep.txt"])
        assert m.prune_safe_for("data") is False
        assert m.prune_safe_for("anywhere") is False


# ---------------------------------------------------------------------------
# walk_code: built-in defaults + user patterns
# ---------------------------------------------------------------------------


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """Build a representative project tree."""
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text("x")
    (tmp_path / "src" / "main.pyc").write_text("c")
    (tmp_path / "src" / "__pycache__").mkdir()
    (tmp_path / "src" / "__pycache__" / "main.cpython.pyc").write_text("c")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "HEAD").write_text("ref")
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv" / "bin").mkdir()
    (tmp_path / ".venv" / "bin" / "python").write_text("")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "pkg.json").write_text("{}")
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "big.bin").write_text("b")
    (tmp_path / "data" / "keep.txt").write_text("k")
    (tmp_path / ".azure_jobs").mkdir()
    (tmp_path / ".azure_jobs" / "meta.json").write_text("{}")
    (tmp_path / ".azure_jobs" / "scripts").mkdir()
    (tmp_path / ".azure_jobs" / "scripts" / "run.sh").write_text("echo")
    (tmp_path / "README.md").write_text("hi")
    return tmp_path


class TestWalkCode:
    def test_defaults_excluded(self, tree: Path):
        rels = {cf.rel for cf in walk_code(tree)}
        assert "README.md" in rels
        assert "src/main.py" in rels
        assert "src/main.pyc" in rels  # only excluded by user pattern
        # Built-in junk dirs gone.
        assert not any(r.startswith("__pycache__") for r in rels)
        assert not any(r.startswith("src/__pycache__") for r in rels)
        assert not any(r.startswith(".git") for r in rels)
        assert not any(r.startswith(".venv") for r in rels)
        assert not any(r.startswith("node_modules") for r in rels)
        # .azure_jobs metadata dropped except scripts/.
        assert ".azure_jobs/meta.json" not in rels
        assert ".azure_jobs/scripts/run.sh" in rels

    def test_user_pattern_filters_files(self, tree: Path):
        rels = {cf.rel for cf in walk_code(tree, ["*.pyc"])}
        assert "src/main.py" in rels
        assert "src/main.pyc" not in rels

    def test_user_dir_only_pattern(self, tree: Path):
        rels = {cf.rel for cf in walk_code(tree, ["data/"])}
        assert all(not r.startswith("data/") for r in rels)
        assert "README.md" in rels

    def test_user_negation_reincludes(self, tree: Path):
        rels = {cf.rel for cf in walk_code(tree, ["data/**", "!data/keep.txt"])}
        assert "data/keep.txt" in rels
        assert "data/big.bin" not in rels

    def test_results_deterministic(self, tree: Path):
        # Two walks return the same order (sorted within each dir).
        rels1 = [cf.rel for cf in walk_code(tree)]
        rels2 = [cf.rel for cf in walk_code(tree)]
        assert rels1 == rels2

    def test_size_populated(self, tree: Path):
        results = walk_code(tree)
        readme = next(cf for cf in results if cf.rel == "README.md")
        assert readme.size == 2  # "hi"

    def test_missing_dir_returns_empty(self, tmp_path: Path):
        assert walk_code(tmp_path / "nope") == []
