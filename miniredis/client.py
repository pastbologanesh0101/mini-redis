"""A minimal blocking client — enough for tests and a REPL.

Not a full client library, but it speaks real RESP so it also works against a
genuine ``redis-server``.
"""

from __future__ import annotations

import socket

from .protocol import NeedMoreData, Reader, RespError


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380, timeout: float = 5.0):
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._reader = Reader()

    def execute(self, *args):
        """Send one command (args are str/bytes/int) and return the reply."""
        payload = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        frame = b"*%d\r\n" % len(payload)
        for p in payload:
            frame += b"$%d\r\n%s\r\n" % (len(p), p)
        self._sock.sendall(frame)
        return self._read_reply()

    # convenient alias so client("GET", "k") reads naturally
    __call__ = execute

    def _read_reply(self):
        while True:
            try:
                reply = self._reader.read()
            except NeedMoreData:
                chunk = self._sock.recv(65536)
                if not chunk:
                    raise ConnectionError("server closed the connection")
                self._reader.feed(chunk)
                continue
            if isinstance(reply, RespError):
                raise RedisError(str(reply))
            return reply

    def close(self):
        self._sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class RedisError(Exception):
    pass


def repl(host: str, port: int) -> None:  # pragma: no cover - interactive
    import shlex

    c = Client(host, port)
    print(f"connected to {host}:{port} — Ctrl-D to quit")
    try:
        while True:
            try:
                line = input(f"{host}:{port}> ").strip()
            except EOFError:
                print()
                return
            if not line:
                continue
            try:
                reply = c.execute(*shlex.split(line))
                print(_fmt(reply))
            except RedisError as e:
                print(f"(error) {e}")
    finally:
        c.close()


def _fmt(v, indent=0):  # pragma: no cover - display only
    pad = "  " * indent
    if v is None:
        return "(nil)"
    if isinstance(v, bytes):
        return v.decode("latin-1")
    if isinstance(v, int):
        return f"(integer) {v}"
    if isinstance(v, list):
        if not v:
            return "(empty array)"
        return "\n".join(f"{pad}{i+1}) {_fmt(x, indent)}" for i, x in enumerate(v))
    return str(v)
