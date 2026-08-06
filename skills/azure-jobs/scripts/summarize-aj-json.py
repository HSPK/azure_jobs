#!/usr/bin/env python3
"""Print a secret-minimized summary of one `aj --json` document."""

from __future__ import annotations

import json
import argparse
import sys
from pathlib import Path

from summary import summarize


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--log-tail", type=int, default=0)
    args = parser.parse_args()
    if args.log_tail < 0:
        parser.error("--log-tail cannot be negative")
    source = args.path
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"cannot read aj JSON: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if not isinstance(value, dict):
        print("aj JSON root must be an object", file=sys.stderr)
        return 1
    print(
        json.dumps(
            summarize(value, log_tail=args.log_tail),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
