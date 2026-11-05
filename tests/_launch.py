"""How a test starts the server: as a module, not a file (PROTOCOL §13.11).

There is one entry point — `python3 -m corallium`. Tests used to spawn
`server.py`, which meant the documented start path and the tested start path
were two different things. `spec` is a module name ("corallium") or, kept for
pointing a test at an older checkout, a path to a `.py` file.
"""
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def argv(spec: str, *flags) -> list:
    head = [sys.executable, spec] if str(spec).endswith(".py") else [sys.executable, "-m", str(spec)]
    return head + [str(f) for f in flags]


def env(extra=None) -> dict:
    """Child env with the repo root importable, so `-m corallium` works from any cwd."""
    e = dict(os.environ)
    prev = e.get("PYTHONPATH", "")
    e["PYTHONPATH"] = str(REPO) + (os.pathsep + prev if prev else "")
    e.update(extra or {})
    return e
