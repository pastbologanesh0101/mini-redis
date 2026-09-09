"""RESP (REdis Serialization Protocol), version 2.

Just enough of the wire format to talk to ``redis-cli`` and real client
libraries:

===============  =====  ==============================================
type             byte   example
===============  =====  ==============================================
simple string    ``+``  ``+OK\\r\\n``
error            ``-``  ``-ERR unknown command\\r\\n``
integer          ``:``  ``:42\\r\\n``
bulk string      ``$``  ``$5\\r\\nhello\\r\\n``  (``$-1\\r\\n`` = nil)
array            ``*``  ``*2\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n``
===============  =====  ==============================================

:class:`Reader` is a incremental parser fed raw socket bytes; :func:`encode`
renders Python values back to RESP.
"""

from __future__ import annotations

CRLF = b"\r\n"


class ProtocolError(Exception):
    """Malformed input — the server should close the connection."""


class NeedMoreData(Exception):
    """The buffer holds only a partial reply/command; feed more bytes."""


class Reader:
    """Incremental RESP parser.

    >>> r = Reader()
    >>> r.feed(b"*1\\r\\n$4\\r\\nPING\\r\\n")
    >>> r.read()
    [b'PING']
    """

    def __init__(self) -> None:
        self._buf = b""

    def feed(self, data: bytes) -> None:
        self._buf += bytes(data)

    def read(self):
        """Return the next complete value, or raise :class:`NeedMoreData`."""
        value, consumed = self._parse(self._buf, 0)
        self._buf = self._buf[consumed:]
        return value

    def __iter__(self):
        while True:
            try:
                yield self.read()
            except NeedMoreData:
                return

    # -- internals -------------------------------------------------
    # Parsing works on an immutable ``bytes`` snapshot with an integer cursor.
    # (An earlier version used ``memoryview`` over a ``bytearray``; a leaked
    # view keeps the buffer un-resizable if ``read`` raises ``NeedMoreData``
    # mid-parse, so plain ``bytes`` slicing is used instead.)
    def _parse(self, buf: bytes, start: int):
        """Parse one value starting at ``buf[start]``.

        Returns ``(value, end)`` where ``end`` is the absolute offset just past
        the value. Raises :class:`NeedMoreData` if the buffer is incomplete.
        """
        if start >= len(buf):
            raise NeedMoreData
        line_end = buf.find(b"\r\n", start)
        if line_end == -1:
            raise NeedMoreData
        prefix = buf[start:start + 1]
        payload = buf[start + 1:line_end]
        after = line_end + 2

        if prefix == b"+":
            return payload, after
        if prefix == b"-":
            return RespError(payload.decode()), after
        if prefix == b":":
            return int(payload), after
        if prefix == b"$":
            n = int(payload)
            if n == -1:
                return None, after
            end = after + n + 2
            if len(buf) < end:
                raise NeedMoreData
            if buf[end - 2:end] != CRLF:
                raise ProtocolError("bulk string not CRLF-terminated")
            return buf[after:after + n], end
        if prefix == b"*":
            n = int(payload)
            if n == -1:
                return None, after
            out = []
            off = after
            for _ in range(n):
                item, off = self._parse(buf, off)
                out.append(item)
            return out, off
        # Inline command: a human typing "PING\r\n" over netcat/telnet.
        return buf[start:line_end].split(), after


class RespError(str):
    """An error reply. Subclasses ``str`` so handlers can just return one."""

    __slots__ = ()


def encode(value) -> bytes:
    """Render a Python value as RESP bytes."""
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, RespError):
        return b"-" + value.encode() + CRLF
    if isinstance(value, SimpleString):
        return b"+" + value.encode() + CRLF
    if isinstance(value, bool):
        return b":" + (b"1" if value else b"0") + CRLF
    if isinstance(value, int):
        return b":" + str(value).encode() + CRLF
    if isinstance(value, (bytes, bytearray)):
        return b"$" + str(len(value)).encode() + CRLF + bytes(value) + CRLF
    if isinstance(value, str):
        b = value.encode()
        return b"$" + str(len(b)).encode() + CRLF + b + CRLF
    if isinstance(value, (list, tuple)):
        out = b"*" + str(len(value)).encode() + CRLF
        return out + b"".join(encode(v) for v in value)
    raise TypeError(f"cannot encode {type(value).__name__}")


class SimpleString(str):
    """Return one of these for ``+OK``-style replies instead of a bulk string."""

    __slots__ = ()


OK = SimpleString("OK")
PONG = SimpleString("PONG")
