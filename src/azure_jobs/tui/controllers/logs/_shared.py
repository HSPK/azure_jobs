"""Constants shared by the logs sub-controllers."""

from __future__ import annotations

LIVE_TAIL_BYTES = 65536

BACKFILL_BYTES = 1024 * 1024

JUMP_HOME_MAX_BYTES = 32 * 1024 * 1024

BACKFILL_TRIGGER_LINES = 3

MAX_BUFFER_LINES = 50_000

MAX_SNAPSHOTS = 32

NO_LOG_STATUSES = ("Queued", "NotStarted", "Provisioning", "Preparing")
