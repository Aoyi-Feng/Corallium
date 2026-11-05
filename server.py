#!/usr/bin/env python3
"""Corallium server entry point. The code lives in `corallium/` (PROTOCOL §13).

Kept as a file because systemd, `coral serve` and the smoke test all invoke
`server.py`; the split is internal.
"""
import sys

from corallium.main import main

if __name__ == "__main__":
    sys.exit(main())
