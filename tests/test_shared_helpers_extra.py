from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from azure_jobs.shared.config.defaults import ensure_experiment, get_experiment
from azure_jobs.shared.types.instance import InstanceTypeInfo
from azure_jobs.shared.utils.format import format_size
from azure_jobs.shared.utils.fs import (
    CodeFile,
    compute_code_hash,
    find_az,
    read_ignore_file,
    should_ignore,
    walk_code,
)


class TestDefaultsHelpers:
    def test_get_experiment_returns_empty_string_when_unset(self, aj_config):
        assert get_experiment() == ""

    def test_ensure_experiment_returns_existing_value_without_prompt(self, aj_config, monkeypatch):
        aj_config.write_text('{"experiment": "saved-exp"}')
        monkeypatch.setattr(
            "azure_jobs.shared.config.prompts._prompt",
            lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not prompt")),
        )

        assert ensure_experiment() == "saved-exp"

    def test_ensure_experiment_uses_suggestion_when_user_submits_blank(self, aj_config, monkeypatch):
        echoes: list[str] = []
        monkeypatch.setattr("azure_jobs.shared.config.defaults.secrets.token_hex", lambda n: "deadbeef")
        monkeypatch.setattr("azure_jobs.shared.config.prompts._echo", lambda text="": echoes.append(text))
        monkeypatch.setattr("azure_jobs.shared.config.prompts._prompt", lambda *a, **kw: "   ")

        result = ensure_experiment()

        assert result == "experiment-deadbeef"
        assert get_experiment() == "experiment-deadbeef"
        assert any("Experiment set to" in line for line in echoes)

    def test_ensure_experiment_strips_user_input_before_saving(self, aj_config, monkeypatch):
        monkeypatch.setattr("azure_jobs.shared.config.prompts._echo", lambda text="": None)
        monkeypatch.setattr("azure_jobs.shared.config.prompts._prompt", lambda *a, **kw: "  team-exp  ")

        assert ensure_experiment() == "team-exp"
        assert get_experiment() == "team-exp"


class TestInstanceTypeInfo:
    def test_short_name_and_cpu_shorthand(self):
        info = InstanceTypeInfo(
            name="Singularity.Standard",
            series_id="CPU",
            num_gpus=0,
            num_cores=96,
        )

        assert info.is_cpu is True
        assert info.short_name == "Standard"
        assert info.shorthand == "C96"

    def test_cpu_without_core_count_uses_generic_c(self):
        assert InstanceTypeInfo(name="cpu", series_id="CPU").shorthand == "C"

    def test_gpu_shorthand_includes_memory_accelerator_and_nvlink(self):
        info = InstanceTypeInfo(
            name="gpu",
            series_id="GPU",
            num_gpus=8,
            gpu_memory_gb=80,
            accelerator="H100",
            nvlink=True,
        )

        assert info.is_cpu is False
        assert info.shorthand == "80G8-H100-NvLink"

    def test_gpu_without_memory_or_accelerator_uses_plain_gpu_count(self):
        info = InstanceTypeInfo(
            name="gpu",
            series_id="GPU",
            num_gpus=2,
            accelerator="CPU",
        )

        assert info.shorthand == "G2"


class TestFilesystemHelpers:
    def test_read_ignore_file_prefers_codeignore_and_strips_comments(self, tmp_path):
        (tmp_path / ".codeignore").write_text("\n# comment\nbuild/\n*.pyc\n")
        (tmp_path / ".amltignore").write_text("ignored\n")

        assert read_ignore_file(tmp_path) == ["build/", "*.pyc"]

    def test_read_ignore_file_returns_empty_on_missing_dir_or_read_error(self, tmp_path, monkeypatch):
        missing = tmp_path / "missing"
        assert read_ignore_file(missing) == []

        ignore_file = tmp_path / ".codeignore"
        ignore_file.write_text("*.pyc\n")
        original = Path.read_text

        def broken_read_text(self: Path, *args, **kwargs):
            if self == ignore_file.resolve():
                raise OSError("denied")
            return original(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", broken_read_text)
        assert read_ignore_file(tmp_path) == []

    def test_should_ignore_applies_defaults_and_preserves_scripts_under_azure_jobs(self, tmp_path):
        root = tmp_path
        kept = root / ".azure_jobs" / "scripts" / "run.sh"
        kept.parent.mkdir(parents=True)
        kept.write_text("echo hi")
        ignored = root / ".azure_jobs" / "state.json"
        ignored.write_text("{}")

        assert should_ignore(ignored, root, None) is True
        assert should_ignore(kept, root, None) is False

    def test_walk_code_sets_size_to_zero_when_stat_fails(self, tmp_path, monkeypatch):
        target = tmp_path / "main.py"
        target.write_text("print('x')\n")
        original = Path.stat

        def broken_stat(self: Path, *args, **kwargs):
            if self == target:
                raise OSError("gone")
            return original(self, *args, **kwargs)

        monkeypatch.setattr(Path, "stat", broken_stat)

        files = walk_code(tmp_path)

        assert files == [CodeFile(rel="main.py", path=target, size=0)]

    def test_compute_code_hash_is_stable_across_file_and_extra_ordering(self, tmp_path):
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_text("alpha")
        b.write_text("beta")
        files = [CodeFile("b.txt", b, 4), CodeFile("a.txt", a, 5)]

        first = compute_code_hash(files, {"z.txt": b"z", "c.txt": b"c"}, length=12)
        second = compute_code_hash(
            list(reversed(files)),
            {"c.txt": b"c", "z.txt": b"z"},
            length=12,
        )

        assert first == second
        assert len(first) == 12

    def test_compute_code_hash_propagates_file_read_errors(self, tmp_path):
        missing = tmp_path / "missing.txt"
        files = [CodeFile("missing.txt", missing, 0)]

        with pytest.raises(FileNotFoundError):
            compute_code_hash(files)

    def test_find_az_returns_path_and_raises_when_missing(self, monkeypatch):
        monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/az")
        assert find_az() == "/usr/bin/az"

        monkeypatch.setattr(shutil, "which", lambda name: None)
        with pytest.raises(FileNotFoundError, match="Azure CLI not found"):
            find_az()


@pytest.mark.parametrize(
    ("size", "expected"),
    [
        (0, "0 B"),
        (1023, "1023 B"),
        (1024, "1.0 KB"),
        (1536, "1.5 KB"),
        (1024**2, "1.0 MB"),
        (1024**4, "1.0 TB"),
    ],
)
def test_format_size_boundaries(size: int, expected: str) -> None:
    assert format_size(size) == expected
