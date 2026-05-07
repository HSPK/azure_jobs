"""Constants shared by the logs sub-controllers."""

from __future__ import annotations

# Bytes of trailing content to show on initial open. Older content is
# fetched lazily via :meth:`LogsController.backfill` when the user
# scrolls close to the top of the buffer.
LIVE_TAIL_BYTES = 65536

# Chunk size pulled per backfill request (one Range GET). Sized so the
# common "scroll up a screen" interaction returns hundreds of lines per
# round-trip without blowing up RichLog's render budget.
BACKFILL_BYTES = 1024 * 1024

# Upper bound on a single ``g`` (jump-to-start) fetch. Beyond this we
# just give up and show what we have.
JUMP_HOME_MAX_BYTES = 32 * 1024 * 1024

# When ``scroll_y`` falls at or below this many lines from the top of
# the viewport we trigger an asynchronous backfill.
BACKFILL_TRIGGER_LINES = 3

# Hard cap on per-job in-memory buffer. Larger than before so backfill
# can grow the window meaningfully before eviction kicks in.
MAX_BUFFER_LINES = 50_000

# LRU cap on the per-job snapshot dict. Long sessions that browse many
# jobs would otherwise grow ``LogsState.snapshots`` unbounded (each
# snapshot can hold up to ``MAX_BUFFER_LINES`` lines).
MAX_SNAPSHOTS = 32

# Job statuses for which we shouldn't try to fetch a log file.
NO_LOG_STATUSES = ("Queued", "NotStarted", "Provisioning", "Preparing")
