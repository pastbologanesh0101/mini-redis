"""A threaded TCP server that speaks RESP.

One OS thread per connection (a bounded pool). A background thread actively
sweeps expired keys so memory does not depend on keys being read again. All
command execution is serialized by the datastore lock, so from a client's
point of view commands still apply one at a time — the same guarantee real
(single-threaded) Redis gives.
"""

from __future__ import annotations

import logging
import socket
import threading
from concurrent.futures import ThreadPoolExecutor

from . import commands  # noqa: F401  -- registers handlers on import
from .commands import dispatch
from .datastore import DataStore
from .protocol import NeedMoreData, ProtocolError, Reader, encode

log = logging.getLogger("miniredis")


class Server:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380,
                 max_clients: int = 128, sweep_interval: float = 0.1):
        self.host = host
        self.port = port
        self.store = DataStore()
        self._sweep_interval = sweep_interval
        self._pool = ThreadPoolExecutor(max_workers=max_clients,
                                        thread_name_prefix="client")
        self._sock: socket.socket | None = None
        self._stop = threading.Event()
        self._sweeper: threading.Thread | None = None
        self.clients_served = 0

    # -- lifecycle ---------------------------------------------
    def start(self) -> tuple[str, int]:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.listen(128)
        self._sock.settimeout(0.5)
        self.host, self.port = self._sock.getsockname()
        self._sweeper = threading.Thread(target=self._sweep_loop, daemon=True)
        self._sweeper.start()
        log.info("listening on %s:%d", self.host, self.port)
        return self.host, self.port

    def serve_forever(self) -> None:
        assert self._sock is not None, "call start() first"
        try:
            while not self._stop.is_set():
                try:
                    conn, addr = self._sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                self.clients_served += 1
                self._pool.submit(self._handle, conn, addr)
        finally:
            self._teardown()

    def run_in_thread(self) -> threading.Thread:
        self.start()
        t = threading.Thread(target=self.serve_forever, daemon=True)
        t.start()
        return t

    def stop(self) -> None:
        self._stop.set()
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass

    def _teardown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    # -- background expiry ----------------------------------
    def _sweep_loop(self) -> None:
        while not self._stop.wait(self._sweep_interval):
            try:
                self.store.sweep(limit=200)
            except Exception:  # pragma: no cover - defensive
                log.exception("sweep failed")

    # -- per-connection ------------------------------------
    def _handle(self, conn: socket.socket, addr) -> None:
        conn.settimeout(1.0)
        reader = Reader()
        try:
            with conn:
                while not self._stop.is_set():
                    try:
                        chunk = conn.recv(65536)
                    except socket.timeout:
                        continue
                    except OSError:
                        break
                    if not chunk:
                        break
                    reader.feed(chunk)
                    out = bytearray()
                    while True:
                        try:
                            msg = reader.read()
                        except NeedMoreData:
                            break
                        except ProtocolError as e:
                            out += encode(_err(f"ERR Protocol error: {e}"))
                            conn.sendall(out)
                            return
                        out += self._run(msg)
                    if out:
                        conn.sendall(out)
        except Exception:  # pragma: no cover - defensive
            log.exception("client %s crashed", addr)

    def _run(self, msg) -> bytes:
        if not isinstance(msg, list) or not msg:
            return encode(_err("ERR expected array of bulk strings"))
        parts = [p if isinstance(p, bytes) else str(p).encode() for p in msg]
        name, args = parts[0], parts[1:]
        return encode(dispatch(self.store, name, args))


def _err(text: str):
    from .protocol import RespError
    return RespError(text)


def main(argv=None) -> int:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(prog="miniredis")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6380)
    ns = ap.parse_args(argv)
    srv = Server(ns.host, ns.port)
    srv.start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.stop()
    return 0
