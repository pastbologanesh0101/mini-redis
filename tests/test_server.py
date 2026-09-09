"""End-to-end: a real server thread + real TCP clients speaking RESP."""

import threading
import time

import pytest

from miniredis import Client, RedisError, Server


@pytest.fixture
def server():
    srv = Server(port=0, sweep_interval=0.02)  # port 0 -> OS picks a free port
    srv.run_in_thread()
    yield srv
    srv.stop()


@pytest.fixture
def client(server):
    c = Client(port=server.port)
    yield c
    c.close()


def test_roundtrip_over_tcp(client):
    assert client("PING") == b"PONG"
    assert client("SET", "k", "hello") == b"OK"   # simple strings arrive as bytes
    assert client("GET", "k") == b"hello"
    assert client("GET", "missing") is None


def test_pipelined_writes(server):
    """Two commands sent before reading either reply."""
    import socket

    s = socket.create_connection(("127.0.0.1", server.port))
    s.sendall(b"*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n")
    time.sleep(0.1)
    data = s.recv(4096)
    assert data == b"+PONG\r\n+OK\r\n"
    s.close()


def test_error_reply_raises(client):
    client("SET", "k", "v")
    with pytest.raises(RedisError):
        client("LPUSH", "k", "x")
    # connection still usable afterwards
    assert client("GET", "k") == b"v"


def test_active_expiry_sweeper(server):
    """A key with a short TTL is dropped even if never read again."""
    import miniredis.datastore as ds

    real = time.monotonic
    # Use a store whose clock we can jump forward.
    now = [real()]
    server.store._clock = lambda: now[0]
    c = Client(port=server.port)
    c("SET", "k", "v", "PX", "50")
    assert server.store.exists(b"k")
    now[0] += 10  # 10s later
    time.sleep(0.1)  # let the sweeper run
    assert not server.store.exists(b"k")
    assert b"k" not in server.store._data
    c.close()


def test_concurrent_clients_incr(server):
    """20 threads * 500 INCRs must total exactly 10000 — no lost updates."""
    N_THREADS, N_OPS = 20, 500

    def worker():
        c = Client(port=server.port)
        for _ in range(N_OPS):
            c("INCR", "counter")
        c.close()

    threads = [threading.Thread(target=worker) for _ in range(N_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    c = Client(port=server.port)
    assert c("GET", "counter") == str(N_THREADS * N_OPS).encode()
    c.close()


def test_many_connections(server):
    for _ in range(50):
        c = Client(port=server.port)
        assert c("PING") == b"PONG"
        c.close()
    assert server.clients_served >= 50
