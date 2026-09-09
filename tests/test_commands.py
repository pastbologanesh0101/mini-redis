"""Command behaviour, driven straight through ``dispatch`` with a fake clock
so expiry is deterministic."""

import pytest

from miniredis.commands import dispatch
from miniredis.datastore import DataStore
from miniredis.protocol import RespError, SimpleString


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


@pytest.fixture
def store():
    return DataStore(clock=Clock())


def run(store, *args):
    parts = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return dispatch(store, parts[0], parts[1:])


def test_ping_echo(store):
    assert run(store, "PING") == SimpleString("PONG")
    assert run(store, "PING", "hi") == b"hi"
    assert run(store, "ECHO", "yo") == b"yo"


def test_set_get_del(store):
    assert run(store, "SET", "k", "v") == SimpleString("OK")
    assert run(store, "GET", "k") == b"v"
    assert run(store, "EXISTS", "k", "k", "nope") == 2
    assert run(store, "DEL", "k") == 1
    assert run(store, "GET", "k") is None


def test_set_nx_xx(store):
    assert run(store, "SET", "k", "1", "NX") == SimpleString("OK")
    assert run(store, "SET", "k", "2", "NX") is None
    assert run(store, "GET", "k") == b"1"
    assert run(store, "SET", "k", "3", "XX") == SimpleString("OK")
    assert run(store, "SET", "new", "x", "XX") is None


def test_incr_family(store):
    assert run(store, "INCR", "n") == 1
    assert run(store, "INCRBY", "n", "9") == 10
    assert run(store, "DECR", "n") == 9
    assert run(store, "DECRBY", "n", "4") == 5
    run(store, "SET", "s", "abc")
    assert isinstance(run(store, "INCR", "s"), RespError)


def test_append_strlen(store):
    assert run(store, "APPEND", "k", "foo") == 3
    assert run(store, "APPEND", "k", "bar") == 6
    assert run(store, "GET", "k") == b"foobar"
    assert run(store, "STRLEN", "k") == 6


def test_expiry_passive_and_ttl(store, request):
    clock = store._clock
    run(store, "SET", "k", "v")
    assert run(store, "EXPIRE", "k", "10") == 1
    assert run(store, "TTL", "k") == 10
    clock.advance(4)
    assert run(store, "TTL", "k") == 6
    clock.advance(7)
    assert run(store, "GET", "k") is None          # passively expired
    assert run(store, "TTL", "k") == -2
    assert run(store, "EXPIRE", "missing", "5") == 0


def test_persist_and_no_ttl(store):
    run(store, "SET", "k", "v")
    assert run(store, "TTL", "k") == -1
    run(store, "EXPIRE", "k", "100")
    assert run(store, "PERSIST", "k") == 1
    assert run(store, "TTL", "k") == -1


def test_set_clears_ttl_unless_keepttl(store):
    clock = store._clock
    run(store, "SET", "k", "v", "EX", "100")
    run(store, "SET", "k", "v2")
    assert run(store, "TTL", "k") == -1
    run(store, "SET", "k", "v3", "EX", "100")
    run(store, "APPEND", "k", "x")  # APPEND keeps ttl
    assert run(store, "TTL", "k") == 100


def test_keys_glob_and_type(store):
    run(store, "SET", "user:1", "a")
    run(store, "SET", "user:2", "b")
    run(store, "RPUSH", "list:1", "x")
    assert sorted(run(store, "KEYS", "user:*")) == [b"user:1", b"user:2"]
    assert run(store, "TYPE", "user:1") == SimpleString("string")
    assert run(store, "TYPE", "list:1") == SimpleString("list")
    assert run(store, "TYPE", "nope") == SimpleString("none")


def test_lists(store):
    assert run(store, "RPUSH", "l", "a", "b", "c") == 3
    assert run(store, "LPUSH", "l", "z") == 4
    assert run(store, "LRANGE", "l", "0", "-1") == [b"z", b"a", b"b", b"c"]
    assert run(store, "LRANGE", "l", "1", "2") == [b"a", b"b"]
    assert run(store, "LPOP", "l") == b"z"
    assert run(store, "RPOP", "l") == b"c"
    assert run(store, "LLEN", "l") == 2
    assert run(store, "LINDEX", "l", "-1") == b"b"
    run(store, "LPOP", "l"); run(store, "LPOP", "l")
    assert run(store, "EXISTS", "l") == 0     # emptied list is removed


def test_hashes(store):
    assert run(store, "HSET", "h", "f1", "v1", "f2", "v2") == 2
    assert run(store, "HSET", "h", "f1", "V1") == 0   # update, not add
    assert run(store, "HGET", "h", "f1") == b"V1"
    assert run(store, "HLEN", "h") == 2
    assert sorted(run(store, "HKEYS", "h")) == [b"f1", b"f2"]
    assert run(store, "HDEL", "h", "f1", "nope") == 1
    assert run(store, "HEXISTS", "h", "f2") == 1
    got = run(store, "HGETALL", "h")
    assert got == [b"f2", b"v2"]


def test_wrongtype_error(store):
    run(store, "SET", "k", "v")
    assert isinstance(run(store, "LPUSH", "k", "x"), RespError)
    assert isinstance(run(store, "HSET", "k", "f", "v"), RespError)


def test_unknown_command_and_arity(store):
    assert isinstance(run(store, "NOPE"), RespError)
    err = run(store, "GET")
    assert isinstance(err, RespError) and "wrong number of arguments" in str(err)


def test_dbsize_flushdb(store):
    run(store, "MSET", "a", "1", "b", "2", "c", "3")
    assert run(store, "DBSIZE") == 3
    assert run(store, "MGET", "a", "b", "x") == [b"1", b"2", None]
    run(store, "FLUSHDB")
    assert run(store, "DBSIZE") == 0
