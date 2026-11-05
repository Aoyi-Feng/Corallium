"""`python3 -m corallium` — the one entry point (PROTOCOL §13.11).

There is deliberately no `server.py` shim: a second entry point is a second
thing to keep in step, and the copy that rots is always the one nobody runs.
systemd uses `-m corallium`, `coral serve` execs `-m corallium`, and the tests
spawn the same module, so every invocation exercises the same code path.
"""
import sys

from .main import main

if __name__ == "__main__":
    sys.exit(main())
