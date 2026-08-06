#!/usr/bin/env python3
"""Run one `aj --json` command without exposing its raw output."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import BinaryIO

from redaction import bounded_tail, redact_text
from summary import summarize


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log-tail", type=int, default=0)
    parser.add_argument("--diagnostic-tail", type=int, default=50)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if len(args.command) < 2:
        parser.error("an `aj --json ...` command is required after --")
    if Path(args.command[0]).name != "aj" or args.command[1] != "--json":
        parser.error("the command must start with `aj --json`")
    if args.log_tail < 0:
        parser.error("--log-tail cannot be negative")
    if args.diagnostic_tail < 1:
        parser.error("--diagnostic-tail must be positive")
    return args


def _read_text(stream: BinaryIO) -> str:
    stream.seek(0)
    return stream.read().decode("utf-8", errors="replace")


def _emit_diagnostic(label: str, value: str, lines: int) -> None:
    if not value:
        return
    print(f"{label} (redacted):", file=sys.stderr)
    print(bounded_tail(value, lines), file=sys.stderr)


def main() -> int:
    args = parse_args()
    try:
        with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
            result = subprocess.run(
                args.command,
                stdout=stdout,
                stderr=stderr,
                check=False,
            )
            stdout_text = _read_text(stdout)
            stderr_text = _read_text(stderr)
    except OSError as exc:
        print(
            "cannot run aj: "
            f"{type(exc).__name__}: {redact_text(str(exc))}",
            file=sys.stderr,
        )
        return 1

    try:
        value = json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        print(
            f"aj did not emit valid JSON: {type(exc).__name__}",
            file=sys.stderr,
        )
        _emit_diagnostic("aj stdout", stdout_text, args.diagnostic_tail)
        _emit_diagnostic("aj stderr", stderr_text, args.diagnostic_tail)
        return result.returncode or 1

    if not isinstance(value, dict):
        print("aj JSON root must be an object", file=sys.stderr)
        _emit_diagnostic("aj stderr", stderr_text, args.diagnostic_tail)
        return result.returncode or 1

    print(
        json.dumps(
            summarize(value, log_tail=args.log_tail),
            indent=2,
            sort_keys=True,
        )
    )
    _emit_diagnostic("aj stderr", stderr_text, args.diagnostic_tail)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
