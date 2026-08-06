from __future__ import annotations

import hashlib
import os
import tarfile

import pytest

from azure_jobs.server.submit import archive as archive_mod
from azure_jobs.server.submit.archive import ArchiveError, create_code_archive
from azure_jobs.shared.utils.fs import CodeFile


def test_archive_contents_ignore_override_modes_and_progress(tmp_path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    (code / "keep.txt").write_text("keep", encoding="utf-8")
    (code / "ignored.tmp").write_text("ignored", encoding="utf-8")
    (code / "aj_runner.sh").write_text("shadowed", encoding="utf-8")
    executable = code / "tool.sh"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    progress = []
    archive_path = tmp_path / "bundle.tar.gz"

    metadata = create_code_archive(
        code,
        archive_path,
        ignore_patterns=["*.tmp"],
        extra_files={
            "aj_runner.sh": "#!/bin/bash\necho generated\n",
            ".ssh/id_ed25519": b"private-key",
        },
        on_progress=lambda completed, total, current: progress.append(
            (completed, total, current)
        ),
    )

    assert metadata.file_count == 4
    assert metadata.size_bytes == archive_path.stat().st_size
    assert metadata.code_hash == hashlib.sha256(archive_path.read_bytes()).hexdigest()
    assert progress[0] == (0, 4, "code.tar.gz")
    assert progress[-1] == (4, 4, "tool.sh")

    with tarfile.open(archive_path, "r:gz") as archive:
        assert archive.getnames() == [
            ".ssh/id_ed25519",
            "aj_runner.sh",
            "keep.txt",
            "tool.sh",
        ]
        assert archive.extractfile("aj_runner.sh").read() == (
            b"#!/bin/bash\necho generated\n"
        )
        assert archive.getmember("aj_runner.sh").mode == 0o755
        assert archive.getmember(".ssh/id_ed25519").mode == 0o600
        assert archive.getmember("keep.txt").mode == 0o644
        assert archive.getmember("tool.sh").mode == 0o755
        assert all(member.mtime == 0 for member in archive.getmembers())


def test_archive_hash_and_bytes_are_stable_across_mtime_and_destination(tmp_path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    source = code / "train.py"
    source.write_text("print('stable')\n", encoding="utf-8")
    first = tmp_path / "first.tar.gz"
    second = tmp_path / "second.tar.gz"

    first_meta = create_code_archive(code, first)
    os.utime(source, (2_000_000_000, 2_000_000_000))
    second_meta = create_code_archive(code, second)

    assert first_meta.code_hash == second_meta.code_hash
    assert first.read_bytes() == second.read_bytes()


def test_archive_rejects_empty_and_unsafe_inputs(tmp_path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    (code / "ignored.txt").write_text("ignored", encoding="utf-8")
    empty_archive = tmp_path / "empty.tar.gz"

    with pytest.raises(ArchiveError, match="No files remain"):
        create_code_archive(
            code,
            empty_archive,
            ignore_patterns=["*.txt"],
        )
    assert not empty_archive.exists()

    with pytest.raises(ArchiveError, match="Unsafe archive path"):
        create_code_archive(
            code,
            tmp_path / "unsafe.tar.gz",
            extra_files={"../escape": b"no"},
        )


def test_archive_rejects_symlinks_even_when_the_target_is_readable(tmp_path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    (code / "linked.txt").symlink_to(outside)

    with pytest.raises(ArchiveError, match="non-regular code file"):
        create_code_archive(code, tmp_path / "symlink.tar.gz")

    (code / "linked.txt").unlink()
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()
    (code / "linked-dir").symlink_to(outside_dir, target_is_directory=True)
    with pytest.raises(ArchiveError, match="symlinked code directory"):
        create_code_archive(code, tmp_path / "symlink-dir.tar.gz")


def test_archive_excludes_existing_destination_inside_source(tmp_path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    (code / "train.py").write_text("print('train')\n", encoding="utf-8")
    destination = code / "code.tar.gz"
    destination.write_bytes(b"old archive")

    metadata = create_code_archive(code, destination)

    assert metadata.file_count == 1
    with tarfile.open(destination, "r:gz") as archive:
        assert archive.getnames() == ["train.py"]


def test_archive_wraps_inspection_and_write_errors(tmp_path, monkeypatch) -> None:
    code = tmp_path / "code"
    code.mkdir()
    missing = code / "vanished.py"
    monkeypatch.setattr(
        archive_mod,
        "walk_code",
        lambda code_dir, patterns: [CodeFile("vanished.py", missing, 0)],
    )

    with pytest.raises(ArchiveError, match="Could not inspect code file"):
        create_code_archive(code, tmp_path / "inspect.tar.gz")

    monkeypatch.setattr(archive_mod, "walk_code", lambda code_dir, patterns: [])
    destination = tmp_path / "write.tar.gz"

    def _fail_write(path, entries, on_progress):
        path.write_bytes(b"partial")
        raise OSError("disk full")

    monkeypatch.setattr(archive_mod, "_write_tar_gz", _fail_write)
    with pytest.raises(ArchiveError, match="Failed to create code archive"):
        create_code_archive(
            code,
            destination,
            extra_files={"aj_runner.sh": "runner"},
        )
    assert not destination.exists()
