"""Mini Redis — a RESP-speaking in-memory data store built from scratch."""

from .client import Client, RedisError
from .datastore import DataStore
from .server import Server

__version__ = "0.1.0"
__all__ = ["Server", "Client", "DataStore", "RedisError"]
