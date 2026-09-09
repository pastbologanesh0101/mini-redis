"""``python -m miniredis`` -> start a server.
``python -m miniredis --client [host] [port]`` -> open a REPL.
"""

import sys

if "--client" in sys.argv:
    rest = [a for a in sys.argv[1:] if a != "--client"]
    host = rest[0] if rest else "127.0.0.1"
    port = int(rest[1]) if len(rest) > 1 else 6380
    from .client import repl

    repl(host, port)
else:
    from .server import main

    raise SystemExit(main())
