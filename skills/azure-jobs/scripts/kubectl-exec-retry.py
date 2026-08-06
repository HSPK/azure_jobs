#!/usr/bin/env python3
"""Retry short idempotent kubectl exec reads on transient proxy failures."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

TRANSIENT = (
    "<!doctype html",
    "<html",
    "cloudflare",
    "unable to upgrade connection",
    "error dialing backend",
    "bad gateway",
    "gateway timeout",
    "service unavailable",
    "tls handshake timeout",
    "i/o timeout",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context")
    parser.add_argument("--namespace", "-n")
    parser.add_argument("--container", "-c")
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--backoff", type=float, default=3)
    parser.add_argument("pod")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a command is required after POD")
    if args.retries < 1:
        parser.error("--retries must be at least 1")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.backoff < 0:
        parser.error("--backoff cannot be negative")
    return args


def command(args: argparse.Namespace) -> list[str]:
    result = ["kubectl"]
    if args.context:
        result.extend(["--context", args.context])
    if args.namespace:
        result.extend(["--namespace", args.namespace])
    result.extend(["exec", args.pod])
    if args.container:
        result.extend(["--container", args.container])
    result.extend(["--", *args.command])
    return result


def transient(stdout: bytes, stderr: bytes) -> bool:
    text = (stdout + b"\n" + stderr).decode(
        "utf-8",
        errors="replace",
    ).lower()
    return any(marker in text for marker in TRANSIENT)


def emit(stream: object, data: bytes) -> None:
    if not data:
        return
    output = getattr(stream, "buffer", stream)
    output.write(data)
    output.flush()


def main() -> int:
    args = parse_args()
    cmd = command(args)
    for attempt in range(1, args.retries + 1):
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=args.timeout,
                check=False,
            )
            retry = (
                result.returncode != 0
                and transient(result.stdout, result.stderr)
            )
        except subprocess.TimeoutExpired as exc:
            result = None
            retry = True
            stdout = exc.stdout or b""
            stderr = exc.stderr or b""

        if result is not None and not retry:
            emit(sys.stdout, result.stdout)
            emit(sys.stderr, result.stderr)
            return result.returncode
        if attempt == args.retries:
            if result is not None:
                emit(sys.stdout, result.stdout)
                emit(sys.stderr, result.stderr)
                return result.returncode or 1
            emit(sys.stdout, stdout)
            emit(sys.stderr, stderr)
            return 124
        time.sleep(args.backoff * attempt)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
