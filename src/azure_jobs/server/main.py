"""``python -m azure_jobs.server.main`` — the daemon process entry point."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from pathlib import Path

from azure_jobs.server.runner import DAEMON_IDLE_SHUTDOWN, Daemon

#: Distinct from a bind failure so a caller can tell "sign in" from "in use".
AUTH_EXIT_CODE = 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ajd", description="aj background daemon")
    parser.add_argument("--socket", required=True, help="Unix socket path to bind")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=1800.0,
        help="Close a target context after this many idle seconds",
    )
    parser.add_argument(
        "--watch-interval",
        type=float,
        default=20.0,
        help="Seconds between background job-status polls",
    )
    parser.add_argument(
        "--shutdown-when-idle",
        type=float,
        default=DAEMON_IDLE_SHUTDOWN,
        help="Exit after this many seconds with nothing to do (0 = never)",
    )
    parser.add_argument("--log-level", default=os.getenv("AJ_LOG_LEVEL", "WARNING"))
    parser.add_argument(
        "--skip-login-check",
        action="store_true",
        help="Start without verifying Azure sign-in (for tests)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if not args.skip_login_check:
        from azure_jobs.server.discovery import require_login
        from azure_jobs.shared.errors import AJError

        try:
            require_login()
        except AJError as exc:
            # Refuse to start rather than let every command fail separately:
            # the daemon is the only execution path, so a daemon that cannot
            # authenticate has nothing useful to serve.
            print(f"ajd: {exc}", file=sys.stderr)
            return AUTH_EXIT_CODE

    daemon = Daemon(
        Path(args.socket),
        idle_timeout=args.idle_timeout,
        watch_interval=args.watch_interval,
        shutdown_when_idle=args.shutdown_when_idle,
    )

    def stop(_signum: int, _frame: object) -> None:
        daemon.shutdown()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, stop)
        except ValueError:
            pass

    try:
        daemon.bind()
    except OSError as exc:
        print(
            f"ajd: failed to bind {args.socket} ({type(exc).__name__}: {exc})",
            file=sys.stderr,
        )
        return 1
    daemon.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
