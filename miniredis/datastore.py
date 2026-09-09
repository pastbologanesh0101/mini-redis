"""The keyspace: a dict of typed values with per-key expiry.

Redis is single-threaded, so command execution is naturally atomic. Mini Redis
uses a thread pool instead (one thread per connection), so every access to the
store goes through a single ``threading.RLock`` — the :meth:`transaction`
context manager — which gives the same "one command at a time" guarantee.

Values are plain Python objects tagged by type:

* string  -> ``bytes``
* list    -> ``collections.deque``
* hash    -> ``dict[bytes, bytes]``
"""

from __future__ import annotations

import fnmatch
import threading
import time
from collections import deque
from typing import Optional


class WrongType(Exception):
    """Raised when a command is used on a key holding a different type."""


class DataStore:
    def __init__(self, clock=time.monotonic):
        self._data: dict[bytes, object] = {}
        self._expires: dict[bytes, float] = {}  # key -> monotonic deadline
        self._lock = threading.RLock()
        self._clock = clock

    # -- locking --------------------------------------------------
    def transaction(self):
        return self._lock

    # -- expiry --------------------------------------------------
    def _expired(self, key: bytes) -> bool:
        dl = self._expires.get(key)
        return dl is not None and dl <= self._clock()

    def _check_expiry(self, key: bytes) -> None:
        if self._expired(key):
            self._data.pop(key, None)
            self._expires.pop(key, None)

    def set_expiry(self, key: bytes, seconds: float) -> bool:
        with self._lock:
            self._check_expiry(key)
            if key not in self._data:
                return False
            self._expires[key] = self._clock() + seconds
            return True

    def persist(self, key: bytes) -> bool:
        with self._lock:
            self._check_expiry(key)
            return self._expires.pop(key, None) is not None

    def ttl(self, key: bytes) -> float:
        """Seconds left; -2 if missing, -1 if no expiry."""
        with self._lock:
            self._check_expiry(key)
            if key not in self._data:
                return -2
            if key not in self._expires:
                return -1
            return max(0.0, self._expires[key] - self._clock())

    def sweep(self, limit: int = 100) -> int:
        """Actively drop up to ``limit`` expired keys. Returns how many."""
        with self._lock:
            now = self._clock()
            dead = [k for k, dl in self._expires.items() if dl <= now][:limit]
            for k in dead:
                self._data.pop(k, None)
                self._expires.pop(k, None)
            return len(dead)

    # -- generic key ops --------------------------------------
    def exists(self, key: bytes) -> bool:
        with self._lock:
            self._check_expiry(key)
            return key in self._data

    def delete(self, *keys: bytes) -> int:
        with self._lock:
            n = 0
            for k in keys:
                self._check_expiry(k)
                if self._data.pop(k, None) is not None:
                    self._expires.pop(k, None)
                    n += 1
            return n

    def keys(self, pattern: bytes) -> list[bytes]:
        with self._lock:
            self.sweep(limit=10_000)
            pat = pattern.decode("latin-1")
            return [k for k in self._data if fnmatch.fnmatchcase(k.decode("latin-1"), pat)]

    def type_of(self, key: bytes) -> str:
        with self._lock:
            self._check_expiry(key)
            v = self._data.get(key)
            if v is None:
                return "none"
            if isinstance(v, bytes):
                return "string"
            if isinstance(v, deque):
                return "list"
            if isinstance(v, dict):
                return "hash"
            return "unknown"

    def dbsize(self) -> int:
        with self._lock:
            self.sweep(limit=10_000)
            return len(self._data)

    def flush(self) -> None:
        with self._lock:
            self._data.clear()
            self._expires.clear()

    # -- typed accessors (call while holding the lock) -------
    def get_string(self, key: bytes) -> Optional[bytes]:
        self._check_expiry(key)
        v = self._data.get(key)
        if v is None:
            return None
        if not isinstance(v, bytes):
            raise WrongType()
        return v

    def put_string(self, key: bytes, value: bytes, keep_ttl: bool = False) -> None:
        self._check_expiry(key)
        self._data[key] = value
        if not keep_ttl:
            self._expires.pop(key, None)

    def get_list(self, key: bytes, create: bool = False) -> Optional[deque]:
        self._check_expiry(key)
        v = self._data.get(key)
        if v is None:
            if not create:
                return None
            v = deque()
            self._data[key] = v
        if not isinstance(v, deque):
            raise WrongType()
        return v

    def get_hash(self, key: bytes, create: bool = False) -> Optional[dict]:
        self._check_expiry(key)
        v = self._data.get(key)
        if v is None:
            if not create:
                return None
            v = {}
            self._data[key] = v
        if not isinstance(v, dict):
            raise WrongType()
        return v

    def drop_if_empty(self, key: bytes) -> None:
        v = self._data.get(key)
        if v is not None and hasattr(v, "__len__") and len(v) == 0:
            self._data.pop(key, None)
            self._expires.pop(key, None)
