"""Entry point: flags, registry, one ThreadingHTTPServer on one port."""
import argparse
import json
import os
import sys
from http.server import ThreadingHTTPServer
from pathlib import Path

from .http import make_handler
from .registry import init_coral, load_registry
from .store import Store

DEFAULT_PORT = 8731

def main() -> int:
    ap = argparse.ArgumentParser(prog="coral-serve")
    ap.add_argument("--root", default="", help="single-coral mode: one data directory")
    ap.add_argument("--coral", action="append", default=[],
                    metavar="NAME=PATH", help="add a coral (repeatable)")
    ap.add_argument("--registry", default="", help="registry.yaml listing several corals")
    ap.add_argument("--init", default="", metavar="DIR",
                    help="scaffold a new coral and exit")
    ap.add_argument("--init-name", default="")
    ap.add_argument("--desc", default="")
    ap.add_argument("--owner", default="")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--bind", default="0.0.0.0")
    a = ap.parse_args()

    if a.init:
        root = Path(a.init).expanduser()
        r = init_coral(root, a.init_name or root.name, a.desc, a.owner)
        print(json.dumps(r, ensure_ascii=False))
        return 0 if r["ok"] else 1

    roots: dict[str, Path] = {}
    if a.registry:
        reg = Path(a.registry).expanduser()
        roots.update({n: Path(p).expanduser() for n, p in load_registry(reg).items()})
    for spec in a.coral:
        if "=" not in spec:
            ap.error(f"--coral wants NAME=PATH, got {spec!r}")
        n, p = spec.split("=", 1)
        roots[n.strip()] = Path(p).expanduser()
    if a.root:
        roots["default"] = Path(a.root).expanduser()
    if not roots:
        roots["default"] = Path.home() / "corals" / "mqsim"

    read_token = os.environ.get("CORALLIUM_READ_TOKEN", "")
    write_token = os.environ.get("CORALLIUM_WRITE_TOKEN", "")

    corals: dict[str, Store] = {}
    for name, root in roots.items():
        root.mkdir(parents=True, exist_ok=True)
        corals[name] = Store(root)

    default = "default" if "default" in corals else (next(iter(corals)) if len(corals) == 1 else None)

    # The default backlog is 5. With more than a handful of agents connecting
    # at once the SYN queue overflows and clients retry for whole seconds, which
    # the caller sees as "no response" — measured as 7-12% silent write loss at
    # 100 concurrent agents. The store was never the bottleneck.
    class CoralServer(ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 256

    srv = CoralServer((a.bind, a.port),
                              make_handler(corals, read_token, write_token, default))
    print(f"corallium on {a.bind}:{a.port} — {len(corals)} coral(s), default={default}", flush=True)
    for n, s in corals.items():
        print(f"  /c/{n}  ->  {s.root}  (max_seq={s.max_seq()})", flush=True)
    print(f"  tokens: read={'set' if read_token else 'open'} "
          f"write={'set' if write_token else 'open'}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


