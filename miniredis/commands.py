"""Command handlers.

Each handler takes ``(store, args)`` where ``args`` is the list of ``bytes``
arguments *after* the command name, and returns a value that
:func:`miniredis.protocol.encode` understands (``bytes``, ``int``, ``None``,
list, :class:`SimpleString`, :class:`RespError`).

``@command("NAME", arity=...)`` registers a handler. ``arity`` mirrors Redis:
a positive number means exactly N args, a negative number means "at least |N|".
"""

from __future__ import annotations

import time
from typing import Callable

from .datastore import DataStore, WrongType
from .protocol import OK, PONG, RespError, SimpleString

REGISTRY: dict[bytes, "Cmd"] = {}


class Cmd:
    def __init__(self, name: str, fn: Callable, arity: int):
        self.name = name
        self.fn = fn
        self.arity = arity

    def check_arity(self, args: list) -> bool:
        return len(args) == self.arity - 1 if self.arity >= 0 else len(args) >= -self.arity - 1


def command(name: str, arity: int):
    def deco(fn):
        REGISTRY[name.encode().upper()] = Cmd(name, fn, arity)
        return fn
    return deco


def dispatch(store: DataStore, name: bytes, args: list):
    cmd = REGISTRY.get(name.upper())
    if cmd is None:
        pretty = name.decode("latin-1")
        return RespError(f"ERR unknown command '{pretty}'")
    if not cmd.check_arity(args):
        return RespError(f"ERR wrong number of arguments for '{cmd.name}' command")
    try:
        with store.transaction():
            return cmd.fn(store, args)
    except WrongType:
        return RespError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
    except _CmdError as e:
        return RespError(str(e))


class _CmdError(Exception):
    pass


def _int(b: bytes) -> int:
    try:
        return int(b)
    except ValueError:
        raise _CmdError("ERR value is not an integer or out of range")


# ============================ connection / server ========================
@command("PING", arity=-1)
def _ping(store, args):
    return args[0] if args else PONG


@command("ECHO", arity=2)
def _echo(store, args):
    return args[0]


@command("COMMAND", arity=-1)
def _command(store, args):
    return [c.name for c in REGISTRY.values()]


# ============================ generic keys ==============================
@command("DEL", arity=-2)
def _del(store, args):
    return store.delete(*args)


@command("EXISTS", arity=-2)
def _exists(store, args):
    return sum(store.exists(k) for k in args)


@command("KEYS", arity=2)
def _keys(store, args):
    return store.keys(args[0])


@command("TYPE", arity=2)
def _type(store, args):
    return SimpleString(store.type_of(args[0]))


@command("EXPIRE", arity=3)
def _expire(store, args):
    return int(store.set_expiry(args[0], _int(args[1])))


@command("PEXPIRE", arity=3)
def _pexpire(store, args):
    return int(store.set_expiry(args[0], _int(args[1]) / 1000))


@command("TTL", arity=2)
def _ttl(store, args):
    t = store.ttl(args[0])
    return t if t < 0 else int(round(t))


@command("PTTL", arity=2)
def _pttl(store, args):
    t = store.ttl(args[0])
    return t if t < 0 else int(round(t * 1000))


@command("PERSIST", arity=2)
def _persist(store, args):
    return int(store.persist(args[0]))


@command("DBSIZE", arity=1)
def _dbsize(store, args):
    return store.dbsize()


@command("FLUSHDB", arity=-1)
def _flushdb(store, args):
    store.flush()
    return OK


@command("FLUSHALL", arity=-1)
def _flushall(store, args):
    store.flush()
    return OK


# ============================ strings ==================================
@command("SET", arity=-3)
def _set(store, args):
    key, value = args[0], args[1]
    ex = px = None
    nx = xx = keepttl = False
    i = 2
    while i < len(args):
        opt = args[i].upper()
        if opt == b"EX":
            ex = _int(args[i + 1]); i += 2
        elif opt == b"PX":
            px = _int(args[i + 1]); i += 2
        elif opt == b"NX":
            nx = True; i += 1
        elif opt == b"XX":
            xx = True; i += 1
        elif opt == b"KEEPTTL":
            keepttl = True; i += 1
        else:
            raise _CmdError("ERR syntax error")
    exists = store.exists(key)
    if (nx and exists) or (xx and not exists):
        return None
    store.put_string(key, value, keep_ttl=keepttl)
    if ex is not None:
        store.set_expiry(key, ex)
    elif px is not None:
        store.set_expiry(key, px / 1000)
    return OK


@command("GET", arity=2)
def _get(store, args):
    return store.get_string(args[0])


@command("GETSET", arity=3)
def _getset(store, args):
    old = store.get_string(args[0])
    store.put_string(args[0], args[1])
    return old


@command("SETNX", arity=3)
def _setnx(store, args):
    if store.exists(args[0]):
        return 0
    store.put_string(args[0], args[1])
    return 1


@command("STRLEN", arity=2)
def _strlen(store, args):
    v = store.get_string(args[0])
    return len(v) if v is not None else 0


@command("APPEND", arity=3)
def _append(store, args):
    cur = store.get_string(args[0]) or b""
    new = cur + args[1]
    store.put_string(args[0], new, keep_ttl=True)
    return len(new)


def _incr_by(store, key, delta):
    cur = store.get_string(key)
    n = _int(cur) if cur is not None else 0
    n += delta
    store.put_string(key, str(n).encode(), keep_ttl=True)
    return n


@command("INCR", arity=2)
def _incr(store, args):
    return _incr_by(store, args[0], 1)


@command("DECR", arity=2)
def _decr(store, args):
    return _incr_by(store, args[0], -1)


@command("INCRBY", arity=3)
def _incrby(store, args):
    return _incr_by(store, args[0], _int(args[1]))


@command("DECRBY", arity=3)
def _decrby(store, args):
    return _incr_by(store, args[0], -_int(args[1]))


@command("MSET", arity=-3)
def _mset(store, args):
    if len(args) % 2:
        raise _CmdError("ERR wrong number of arguments for 'mset' command")
    for i in range(0, len(args), 2):
        store.put_string(args[i], args[i + 1])
    return OK


@command("MGET", arity=-2)
def _mget(store, args):
    out = []
    for k in args:
        try:
            out.append(store.get_string(k))
        except WrongType:
            out.append(None)
    return out


# ============================ lists ===================================
@command("LPUSH", arity=-3)
def _lpush(store, args):
    lst = store.get_list(args[0], create=True)
    for v in args[1:]:
        lst.appendleft(v)
    return len(lst)


@command("RPUSH", arity=-3)
def _rpush(store, args):
    lst = store.get_list(args[0], create=True)
    for v in args[1:]:
        lst.append(v)
    return len(lst)


@command("LPOP", arity=2)
def _lpop(store, args):
    lst = store.get_list(args[0])
    if not lst:
        return None
    v = lst.popleft()
    store.drop_if_empty(args[0])
    return v


@command("RPOP", arity=2)
def _rpop(store, args):
    lst = store.get_list(args[0])
    if not lst:
        return None
    v = lst.pop()
    store.drop_if_empty(args[0])
    return v


@command("LLEN", arity=2)
def _llen(store, args):
    lst = store.get_list(args[0])
    return len(lst) if lst is not None else 0


@command("LRANGE", arity=4)
def _lrange(store, args):
    lst = store.get_list(args[0])
    if not lst:
        return []
    items = list(lst)
    start, stop = _int(args[1]), _int(args[2])
    n = len(items)
    if start < 0:
        start = max(0, n + start)
    if stop < 0:
        stop = n + stop
    stop = min(stop, n - 1)
    if start > stop:
        return []
    return items[start:stop + 1]


@command("LINDEX", arity=3)
def _lindex(store, args):
    lst = store.get_list(args[0])
    if not lst:
        return None
    items = list(lst)
    i = _int(args[1])
    try:
        return items[i]
    except IndexError:
        return None


# ============================ hashes ==================================
@command("HSET", arity=-4)
def _hset(store, args):
    if len(args) % 2 == 0:
        raise _CmdError("ERR wrong number of arguments for 'hset' command")
    h = store.get_hash(args[0], create=True)
    added = 0
    for i in range(1, len(args), 2):
        if args[i] not in h:
            added += 1
        h[args[i]] = args[i + 1]
    return added


@command("HGET", arity=3)
def _hget(store, args):
    h = store.get_hash(args[0])
    return h.get(args[1]) if h else None


@command("HMGET", arity=-3)
def _hmget(store, args):
    h = store.get_hash(args[0]) or {}
    return [h.get(f) for f in args[1:]]


@command("HDEL", arity=-3)
def _hdel(store, args):
    h = store.get_hash(args[0])
    if not h:
        return 0
    n = sum(h.pop(f, None) is not None for f in args[1:])
    store.drop_if_empty(args[0])
    return n


@command("HGETALL", arity=2)
def _hgetall(store, args):
    h = store.get_hash(args[0]) or {}
    out = []
    for k, v in h.items():
        out.append(k)
        out.append(v)
    return out


@command("HKEYS", arity=2)
def _hkeys(store, args):
    h = store.get_hash(args[0]) or {}
    return list(h.keys())


@command("HVALS", arity=2)
def _hvals(store, args):
    h = store.get_hash(args[0]) or {}
    return list(h.values())


@command("HLEN", arity=2)
def _hlen(store, args):
    h = store.get_hash(args[0])
    return len(h) if h else 0


@command("HEXISTS", arity=3)
def _hexists(store, args):
    h = store.get_hash(args[0])
    return int(bool(h) and args[1] in h)
