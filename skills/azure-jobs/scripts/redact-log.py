#!/usr/bin/env python3
"""Print a bounded, secret-redacted tail from a private log file."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from redaction import bounded_tail


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--tail", type=int, default=200)
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete the private input after reading it",
    )
    args = parser.parse_args()
    if args.tail < 1:
        parser.error("--tail must be positive")
    try:
        value = args.path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        print(
            f"cannot read private log: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        value = None
    if args.delete:
        try:
            args.path.unlink(missing_ok=True)
        except OSError as exc:
            print(
                f"cannot delete private log: {type(exc).__name__}: {exc}",
                file=sys.stderr,
            )
            return 1
    if value is None:
        return 1
    print(bounded_tail(value, args.tail))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
