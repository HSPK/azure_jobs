"""Deterministic, backend-neutral code archive creation."""

from __future__ import annotations

import gzip
import hashlib
import io
import os
import stat
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Callable, Iterable

from azure_jobs.shared.errors import AJError
from azure_jobs.shared.utils.fs import should_ignore, walk_code

ArchiveProgress = Callable[[int, int, str], None]

_RUNNER_PATH = "aj_runner.sh"


class ArchiveError(AJError):
    """A code archive could not be created safely."""


@dataclass(frozen=True, slots=True)
class ArchiveMetadata:
    """Content-addressed metadata for one completed archive."""

    code_hash: str
    size_bytes: int
    file_count: int


@dataclass(frozen=True, slots=True)
class _ArchiveEntry:
    arcname: str
    size: int
    mode: int
    path: Path | None = None
    content: bytes | None = None

    def open(self) -> BinaryIO:
        if self.path is not None:
            return self.path.open("rb")
        return io.BytesIO(self.content or b"")


def _safe_arcname(name: str) -> str:
    raw = name.replace("\\", "/")
    path = PurePosixPath(raw)
    if (
        not raw
        or "\0" in raw
        or raw.startswith("/")
        or path.is_absolute()
        or not path.parts
        or any(part == ".." for part in path.parts)
        or path.parts[0].endswith(":")
    ):
        raise ArchiveError(
            f"Unsafe archive path {name!r}; paths must be relative and may not "
            "contain '..'."
        )
    return path.as_posix()


def _entry_mode(arcname: str, *, source_mode: int = 0o644) -> int:
    if arcname == _RUNNER_PATH:
        return 0o755
    if arcname == ".ssh" or arcname.startswith(".ssh/"):
        return 0o600
    return 0o755 if source_mode & 0o111 else 0o644


def _collect_entries(
    code_dir: str | Path,
    ignore_patterns: Iterable[str] | None,
    extra_files: dict[str, str | bytes] | None,
    archive_path: Path,
) -> list[_ArchiveEntry]:
    entries: dict[str, _ArchiveEntry] = {}
    source_root = Path(code_dir).resolve()
    destination = archive_path.resolve()
    patterns = list(ignore_patterns or ())

    for root, dirs, _files in os.walk(source_root, followlinks=False):
        for name in dirs:
            path = Path(root) / name
            if path.is_symlink() and not should_ignore(
                path,
                source_root,
                patterns,
            ):
                raise ArchiveError(
                    f"Refusing to archive symlinked code directory {path}; "
                    "directory symlinks are not supported."
                )

    for code_file in walk_code(code_dir, patterns):
        try:
            if code_file.path.resolve() == destination:
                continue
            source_stat = code_file.path.lstat()
        except OSError as exc:
            raise ArchiveError(
                f"Could not inspect code file {code_file.path}: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if not stat.S_ISREG(source_stat.st_mode):
            raise ArchiveError(
                f"Refusing to archive non-regular code file {code_file.path}; "
                "symlinks, devices, sockets, and pipes are not supported."
            )
        try:
            code_file.path.resolve(strict=True).relative_to(source_root)
        except (OSError, ValueError) as exc:
            raise ArchiveError(
                f"Code file resolves outside the code directory: {code_file.path}"
            ) from exc
        arcname = _safe_arcname(code_file.rel)
        entries[arcname] = _ArchiveEntry(
            arcname=arcname,
            path=code_file.path,
            size=source_stat.st_size,
            mode=_entry_mode(arcname, source_mode=source_stat.st_mode),
        )

    for name, value in (extra_files or {}).items():
        arcname = _safe_arcname(name)
        content = value.encode("utf-8") if isinstance(value, str) else value
        entries[arcname] = _ArchiveEntry(
            arcname=arcname,
            content=content,
            size=len(content),
            mode=_entry_mode(arcname),
        )

    if not entries:
        raise ArchiveError(
            f"No files remain under {Path(code_dir).resolve()} after applying "
            "the configured ignore patterns; there is nothing to upload or submit."
        )
    return [entries[name] for name in sorted(entries)]


def _write_tar_gz(
    archive_path: Path,
    entries: list[_ArchiveEntry],
    on_progress: ArchiveProgress | None,
) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    if on_progress is not None:
        on_progress(0, len(entries), "code.tar.gz")

    with archive_path.open("wb") as raw:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            compresslevel=6,
            fileobj=raw,
            mtime=0,
        ) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.GNU_FORMAT,
            ) as archive:
                for index, entry in enumerate(entries, 1):
                    info = tarfile.TarInfo(entry.arcname)
                    info.size = entry.size
                    info.mode = entry.mode
                    info.mtime = 0
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    with entry.open() as source:
                        archive.addfile(info, source)
                    if on_progress is not None:
                        on_progress(index, len(entries), entry.arcname)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def create_code_archive(
    code_dir: str | Path,
    archive_path: str | Path,
    *,
    ignore_patterns: Iterable[str] | None = None,
    extra_files: dict[str, str | bytes] | None = None,
    on_progress: ArchiveProgress | None = None,
) -> ArchiveMetadata:
    """Write a deterministic tar.gz and return its content-addressed metadata."""
    destination = Path(archive_path)
    entries = _collect_entries(
        code_dir,
        ignore_patterns,
        extra_files,
        destination,
    )
    try:
        _write_tar_gz(destination, entries, on_progress)
        return ArchiveMetadata(
            code_hash=_sha256(destination),
            size_bytes=destination.stat().st_size,
            file_count=len(entries),
        )
    except (OSError, tarfile.TarError) as exc:
        destination.unlink(missing_ok=True)
        raise ArchiveError(
            f"Failed to create code archive {destination}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


__all__ = [
    "ArchiveError",
    "ArchiveMetadata",
    "ArchiveProgress",
    "create_code_archive",
]
