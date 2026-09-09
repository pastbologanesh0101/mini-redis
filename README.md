# Mini Redis

An in-memory data store that speaks the real **Redis wire protocol (RESP2)**,
written from scratch in pure Python with no third-party dependencies. Real
`redis-cli` and real client libraries connect to it unmodified.

Built to make three things concrete: how a binary protocol is framed on a TCP
stream, how a server stays correct while handling many connections with
**threads**, and how a **cache** implements key expiry.

```
        redis-cli / redis-py / nc
                  │  RESP2 over TCP
        ┌─────────▼──────────┐
        │   Server           │  accept loop + ThreadPoolExecutor
        │   thread/conn      │  + background expiry sweeper
        ├────────────────────┤
        │   Reader / encode  │  incremental RESP parser + serializer
        ├────────────────────┤
        │   dispatch()       │  command registry, arity + type checks
        ├────────────────────┤
        │   DataStore        │  dict + per-key TTL, one RLock = atomicity
        └────────────────────┘
```

## Quick start

```bash
git clone https://github.com/pastbologanesh0101/mini-redis
cd mini-redis
pip install -e ".[dev]"
pytest -q                              # 35 tests

python -m miniredis --port 6380        # start the server
```

Talk to it with the **real** Redis CLI:

```console
$ redis-cli -p 6380 PING
PONG
$ redis-cli -p 6380 SET greeting "hello world"
OK
$ redis-cli -p 6380 RPUSH fruits apple banana cherry
(integer) 3
$ redis-cli -p 6380 LRANGE fruits 0 -1
1) "apple"
2) "banana"
3) "cherry"
$ redis-cli -p 6380 SET tmp x EX 100
OK
$ redis-cli -p 6380 TTL tmp
(integer) 100
```

Or the bundled client / REPL:

```python
from miniredis import Server, Client

srv = Server(port=6380); srv.run_in_thread()
c = Client(port=6380)
c("SET", "user:1", "alice")
c("GET", "user:1")            # -> b"alice"
```

```bash
python -m miniredis --client 127.0.0.1 6380
```

## Concepts demonstrated

| Area | Where | What it shows |
|---|---|---|
| **Protocol / networking** | `miniredis/protocol.py` | An **incremental** RESP2 parser: `feed()` raw socket chunks, `read()` pulls complete values, `NeedMoreData` when a command straddles two `recv()`s. Handles pipelining (many commands per packet) and inline commands. `encode()` renders replies. |
| **Multithreading** | `miniredis/server.py` | One thread per connection via a bounded `ThreadPoolExecutor`; a separate daemon thread actively expires keys. A single `RLock` around the keyspace serializes command execution, so clients still see "one command at a time" — real Redis's guarantee, reached a different way. Proven by a test running 20 threads × 500 `INCR` and asserting the counter is exactly 10000 (no lost updates). |
| **Caching** | `miniredis/datastore.py` | Per-key TTL with **passive** expiry (checked on access) *and* **active** expiry (background sweep), matching Redis's dual strategy. `SET ... EX/PX/NX/XX/KEEPTTL`, `EXPIRE`, `TTL`, `PERSIST`. |
| **Wire compatibility** | CI job `redis-cli-compat` | Every push runs actual `redis-cli` against the server. |
| **Serialization** | `protocol.py` | Length-prefixed binary framing by hand; `bytes` in, `bytes` out, no `str` assumptions. |

## Supported commands (~45)

```
connection   PING ECHO COMMAND
keys         DEL EXISTS KEYS TYPE EXPIRE PEXPIRE TTL PTTL PERSIST
             DBSIZE FLUSHDB FLUSHALL
strings      SET GET GETSET SETNX STRLEN APPEND INCR DECR INCRBY
             DECRBY MSET MGET
lists        LPUSH RPUSH LPOP RPOP LLEN LRANGE LINDEX
hashes       HSET HGET HMGET HDEL HGETALL HKEYS HVALS HLEN HEXISTS
```

`SET` supports `EX`, `PX`, `NX`, `XX`, `KEEPTTL`. `KEYS` supports glob
patterns. Wrong-type operations return `WRONGTYPE`, unknown commands and bad
arity return proper `-ERR` replies — and the connection stays usable.

## How the parser stays correct across packet boundaries

TCP is a byte stream: one `recv()` can return half a command, or three and a
half. `Reader` keeps an internal buffer:

```python
reader.feed(chunk)              # append whatever arrived
while True:
    try:
        cmd = reader.read()    # one complete command, or...
    except NeedMoreData:
        break                  # ...wait for the next recv()
    replies += handle(cmd)
```

Parsing runs over an immutable `bytes` snapshot with an integer cursor, so a
half-parsed command that raises `NeedMoreData` never corrupts the buffer or
pins it against the next `feed()`.

## Concurrency model

Real Redis is single-threaded: commands can't interleave. Mini Redis uses many
threads but wraps every keyspace access in one `RLock` (`DataStore.transaction()`),
so a read-modify-write like `INCR` or `APPEND` is atomic. The trade-off is the
classic one — throughput is capped by the lock, exactly as Redis's throughput
is capped by its single core — and it keeps the semantics dead simple.

## Limitations

- **No persistence** (no RDB/AOF), **no replication**, **no pub/sub**, **no
  transactions/`MULTI`**, **no Lua**.
- One logical database (`SELECT` is a no-op-ish); `KEYS` scans everything.
- RESP2 only, not RESP3.
- The lock makes it a teaching server, not a benchmark contender.

## Project layout

```
miniredis/
  protocol.py    incremental RESP2 Reader + encode()
  datastore.py   keyspace: typed values, per-key TTL, locking
  commands.py    ~45 handlers in a registry, arity/type checks
  server.py      threaded TCP accept loop + expiry sweeper
  client.py      minimal blocking client + REPL
tests/           protocol / commands (fake clock) / end-to-end over TCP
```

## License

MIT
