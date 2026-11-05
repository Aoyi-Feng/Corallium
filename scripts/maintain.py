#!/usr/bin/env python3
"""Corallium maintenance mode (PROTOCOL §20).

Research mode adds knowledge; this mode repairs its shape. Three actions, by
how much they destroy:

  L1  link / redundant vote / significance correction   nothing destroyed
  L2  merge   two nodes about the same thing -> one       content kept as history
  L3  retire  destroy content, leave a tombstone          irreversible

`scan` only reports. It never recommends, because the recommendation would then
be laundered into the record as if it were a fact.

Usage
  python3 scripts/maintain.py scan [--coral NAME] [--json]
  python3 scripts/maintain.py merge --keep ID --drop ID --reason "..."
  python3 scripts/maintain.py retire --id ID --reason "..." [--yes]

Environment: CORALLIUM_URL (default http://127.0.0.1:8731), CORALLIUM_CORAL.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

URL = os.environ.get("CORALLIUM_URL", "http://127.0.0.1:8731").rstrip("/")
AUTHOR = os.environ.get("CORALLIUM_AUTHOR", "curator")


def coral_path(name: str | None) -> str:
    if not name:
        name = os.environ.get("CORALLIUM_CORAL", "")
    if not name:
        root = Path.home() / "corals"
        names = sorted(p.name for p in root.iterdir() if (p / "nodes").is_dir()) if root.is_dir() else []
        if len(names) == 1:
            name = names[0]
        else:
            sys.exit("pass --coral (available: %s)" % ", ".join(names) if names else "no corals found")
    return "/c/" + name


def call(path: str, payload: dict | None = None) -> dict:
    url = URL + path
    data = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET")
    req.add_header("Content-Type", "application/json")
    tok = os.environ.get("CORALLIUM_WRITE_TOKEN") or os.environ.get("CORALLIUM_READ_TOKEN")
    if tok:
        req.add_header("Authorization", "Bearer " + tok)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            detail = json.loads(body)
        except Exception:
            detail = {"error": body[:300]}
        sys.exit(f"error {e.code}: {detail.get('error', detail)}")
    except Exception as e:
        sys.exit(f"error: cannot reach {URL} ({e}) — maintenance needs the server; "
                 "reads are not degraded here, because a partial view would mislead the plan")


def cmd_scan(a) -> int:
    r = call(coral_path(a.coral) + "/maintenance/scan")
    if a.json:
        print(json.dumps(r, ensure_ascii=False, indent=2))
        return 0
    c = r["counts"]
    print(f"nodes {c['nodes']}   roots {c['roots']}   withdrawn(explicit) {c['withdrawn']}   "
          f"duplicate pairs {c['duplicate_pairs']}   junk candidates {c['junk']}")
    if r["duplicates"]:
        print("\nduplicates (L2: merge)")
        for d in r["duplicates"]:
            flag = "already linked" if d["linked"] else "unlinked"
            print(f"  {d['similarity']:.2f}  {d['a']}  {d['b']}  [{flag}]")
    if r["junk"]:
        print("\njunk candidates — structural gates only. L3 also needs a `junk` vote,")
        print("because structure proves nobody would miss it, never that it is wrong.")
        for j in r["junk"]:
            mark = "retirable" if j.get("retirable") else "needs a junk vote"
            print(f"  {j['id']}  {mark:18s} {j['why']:26s}  {j['o']}")
    if r["broken_refs"]:
        print("\nbroken references (not removable — fix the evidence instead)")
        for b in r["broken_refs"]:
            print(f"  {b['node']}  {b['path']}")
    if r["orphans"]:
        print(f"\nroots without parents: {len(r['orphans'])} "
              f"(L1: link them under the branch they belong to)")
    print("\nnothing was changed. apply one action at a time, with a reason.")
    return 0


def cmd_merge(a) -> int:
    r = call(coral_path(a.coral) + "/maintenance/merge",
             {"keep": a.keep, "drop": a.drop, "reason": a.reason, "author": a.author})
    print(f"merged {a.keep} + {a.drop} -> {r['merged']}"
          f"{'' if r.get('created') else ' (already existed)'}")
    print("both parents leave tip; nothing was destroyed")
    return 0


def cmd_retire(a) -> int:
    if not a.yes:
        sys.exit("refusing without --yes: retire destroys content irreversibly")
    r = call(coral_path(a.coral) + "/maintenance/retire",
             {"id": a.id, "reason": a.reason, "author": a.author})
    print(f"retired {a.id} -> tombstone {r['node']['id']} ({r['node'].get('reason')})")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Corallium maintenance mode (PROTOCOL §20)")
    ap.add_argument("--coral", default="")
    ap.add_argument("--author", default=AUTHOR)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scan", help="report duplicates, junk, broken refs, orphans")
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_scan)

    m = sub.add_parser("merge", help="L2: one node for two that say the same thing")
    m.add_argument("--keep", required=True)
    m.add_argument("--drop", required=True)
    m.add_argument("--reason", required=True)
    m.add_argument("--author", default=argparse.SUPPRESS)
    m.set_defaults(fn=cmd_merge)

    r = sub.add_parser("retire", help="L3: destroy a junk node, leave a tombstone")
    r.add_argument("--id", required=True)
    r.add_argument("--reason", required=True)
    r.add_argument("--yes", action="store_true")
    r.add_argument("--author", default=argparse.SUPPRESS)
    r.set_defaults(fn=cmd_retire)

    a = ap.parse_args()
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
