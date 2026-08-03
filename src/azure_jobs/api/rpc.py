"""Line-delimited JSON-RPC 2.0 framing over a stream socket.

Stdlib only: the repo keeps its dependency set minimal, so this deliberately
avoids an HTTP or gRPC stack. One JSON object per line, UTF-8, ``\\n``
terminated. Server-to-client pushes are JSON-RPC notifications (no ``id``).
"""

from __future__ import annotations

import json
import logging
import socket
import threading
from typing import Any, Callable, Iterator, Mapping

from azure_jobs.api.errors import TransportError, error_from_json, error_to_json

# A log window is capped at 16 MiB and base64 inflates by ~4/3, so a frame has
# to clear ~22 MB. The ceiling exists to stop a peer from exhausting memory.
log = logging.getLogger(__name__)

MAX_FRAME_BYTES = 64 * 1024 * 1024

ERROR_CODE = -32000
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601


class FrameTooLarge(TransportError):
    """A peer sent a frame beyond ``MAX_FRAME_BYTES``."""


def encode(message: Mapping[str, Any]) -> bytes:
    """Serialise one message to a single newline-terminated frame."""
    payload = json.dumps(message, separators=(",", ":"), ensure_ascii=False)
    data = payload.encode("utf-8")
    if len(data) + 1 > MAX_FRAME_BYTES:
        raise FrameTooLarge(
            f"Refusing to send a {len(data)} byte frame; "
            f"the limit is {MAX_FRAME_BYTES} bytes"
        )
    return data + b"\n"


def request(req_id: int, method: str, params: Mapping[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "method": method, "params": dict(params)}


def notification(method: str, params: Mapping[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "method": method, "params": dict(params)}


def result(req_id: Any, value: Any) -> dict[str, Any]:
    """Build a response, validating now that the payload can be encoded."""
    frame = {"jsonrpc": "2.0", "id": req_id, "result": value}
    json.dumps(frame, separators=(",", ":"), ensure_ascii=False)
    return frame


def error(req_id: Any, exc: BaseException, *, code: int = ERROR_CODE) -> dict[str, Any]:
    data = error_to_json(exc)
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": code, "message": data["message"], "data": data},
    }


def raise_for_error(message: Mapping[str, Any]) -> None:
    """Re-raise a peer's error with its original type where possible."""
    err = message.get("error")
    if err is None:
        return
    raise error_from_json(err.get("data") or {"message": err.get("message")})


class FrameReader:
    """Incremental newline framing over a blocking socket."""

    def __init__(self, sock: socket.socket, *, limit: int = MAX_FRAME_BYTES) -> None:
        self._sock = sock
        self._limit = limit
        self._buf = bytearray()

    def __iter__(self) -> Iterator[dict[str, Any]]:
        while True:
            frame = self.read()
            if frame is None:
                return
            yield frame

    def read(self) -> dict[str, Any] | None:
        """Return the next message, or ``None`` once the peer closes."""
        while True:
            index = self._buf.find(b"\n")
            if index >= 0:
                line = bytes(self._buf[:index])
                del self._buf[: index + 1]
                if not line.strip():
                    continue
                try:
                    return json.loads(line.decode("utf-8"))
                except (ValueError, UnicodeDecodeError) as exc:
                    raise TransportError(
                        f"Malformed frame from peer ({type(exc).__name__}: {exc})"
                    ) from exc
            if len(self._buf) > self._limit:
                raise FrameTooLarge(
                    f"Peer frame exceeded {self._limit} bytes without a newline"
                )
            try:
                chunk = self._sock.recv(65536)
            except (TimeoutError, socket.timeout) as exc:
                raise TransportError(f"Timed out reading from peer: {exc}") from exc
            except OSError as exc:
                raise TransportError(
                    f"Connection lost ({type(exc).__name__}: {exc})"
                ) from exc
            if not chunk:
                return None
            self._buf.extend(chunk)


class FrameWriter:
    """Serialised writes so concurrent senders cannot interleave frames."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._lock = threading.Lock()

    def send(self, message: Mapping[str, Any]) -> None:
        data = encode(message)
        with self._lock:
            try:
                self._sock.sendall(data)
            except OSError as exc:
                raise TransportError(
                    f"Failed to send frame ({type(exc).__name__}: {exc})"
                ) from exc


def serve_connection(
    sock: socket.socket,
    handler: Callable[[str, Mapping[str, Any]], Any],
    *,
    on_ready: Callable[[FrameWriter], None] | None = None,
    on_close: Callable[[], None] | None = None,
) -> None:
    """Read requests from *sock*, dispatch to *handler*, write responses.

    ``handler`` runs on this connection's thread. Errors are converted to
    JSON-RPC error responses rather than killing the connection, so one bad
    call cannot take a client's whole session down.
    """
    writer = FrameWriter(sock)
    reader = FrameReader(sock)
    if on_ready is not None:
        on_ready(writer)
    try:
        while True:
            try:
                message = reader.read()
            except TransportError:
                return
            if message is None:
                return
            req_id = message.get("id")
            method = str(message.get("method") or "")
            params = message.get("params") or {}
            if not method:
                if req_id is not None:
                    writer.send(
                        error(
                            req_id,
                            TransportError("Request is missing a method"),
                            code=INVALID_REQUEST,
                        )
                    )
                continue
            try:
                value = handler(method, params)
            except BaseException as exc:  # noqa: BLE001 - reported to the peer
                if req_id is not None:
                    try:
                        writer.send(error(req_id, exc))
                    except TransportError:
                        return
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                continue
            if req_id is None:
                continue
            try:
                frame = result(req_id, value)
            except Exception as exc:  # noqa: BLE001 - a payload bug, not an outage
                # Encoding happens here rather than inside writer.send so an
                # unserialisable result fails one call instead of unwinding
                # serve_connection and killing every session on this socket.
                log.exception("Could not encode the result of %r", method)
                try:
                    writer.send(
                        error(
                            req_id,
                            TypeError(
                                f"Daemon could not encode the result of "
                                f"{method!r} ({type(exc).__name__}: {exc})"
                            ),
                        )
                    )
                except TransportError:
                    return
                continue
            try:
                writer.send(frame)
            except TransportError:
                return
    finally:
        if on_close is not None:
            on_close()


__all__ = [
    "ERROR_CODE",
    "FrameReader",
    "FrameTooLarge",
    "FrameWriter",
    "INVALID_REQUEST",
    "MAX_FRAME_BYTES",
    "METHOD_NOT_FOUND",
    "PARSE_ERROR",
    "encode",
    "error",
    "notification",
    "raise_for_error",
    "request",
    "result",
    "serve_connection",
]
