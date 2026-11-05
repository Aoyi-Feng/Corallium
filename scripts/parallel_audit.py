#!/usr/bin/env python3
"""Audit one coral for the evidence that parallel agents were here at all.

Answers five questions, each from the data rather than from trust:

  1. Did every write land?            seq contiguous, one file per seq, no gaps
  2. Did concurrent identical writes   duplicate (s,p,o) groups that are *distinct
     fork?                            ids* — the hash is supposed to prevent this
  3. Did claims serialize?            one holder per question in claims.json
  4. Is the timeline readable?        作者 / 时间 / 动作，按 seq 排
  5. Did agents validate each other?  votes whose author != claim author, and
                                      whether they passed the two gates

Usage:  python3 scripts/parallel_audit.py --coral ~/corals/corallium
        python3 scripts/parallel_audit.py --coral ~/corals/corallium --since 40
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path


def load(root: Path) -> list[dict]:
    nodes = []
    for f in sorted(root.glob("nodes/*.json")):
        try:
            nodes.append(json.loads(f.read_text()))
        except Exception as e:
            print(f"  ! unreadable node file {f.name}: {e}")
    nodes.sort(key=lambda n: n.get("seq", 0))
    return nodes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coral", default=str(Path.home() / "corals" / "corallium"))
    ap.add_argument("--since", type=int, default=0)
    a = ap.parse_args()
    root = Path(a.coral).expanduser()
    if not (root / "nodes").is_dir():
        print(f"not a coral: {root}")
        return 2

    nodes = load(root)
    live = [n for n in nodes if n.get("seq", 0) >= a.since]
    bad = 0

    print(f"coral     {root.name}")
    print(f"nodes     {len(nodes)} total, {len(live)} since seq {a.since}")
    print(f"seq range {nodes[0]['seq'] if nodes else '-'} … {nodes[-1]['seq'] if nodes else '-'}")

    # 1 — did every write land
    seqs = [n["seq"] for n in nodes]
    expect = list(range(1, (max(seqs) if seqs else 0) + 1))
    missing = sorted(set(expect) - set(seqs))
    dupes = [s for s, c in Counter(seqs).items() if c > 1]
    files = len(list((root / "nodes").glob("*.json")))
    print(f"\n[1] writes landed: {files} files / {len(nodes)} rows / seq holes={missing or 'none'} "
          f"reused={dupes or 'none'}")
    if missing or dupes or files != len(nodes):
        bad += 1
        print("    FAIL — the record has a hole; since(cursor) would skip it forever")

    # 2 — duplicate propositions
    groups = defaultdict(list)
    for n in nodes:
        groups[(n.get("s"), n.get("p"), n.get("o"))].append(n)
    def linked(v):
        """A supersedes edge reuses (s,p,o) on purpose: same proposition, new id,
        because rel and parents are part of the hash. That is not a fork."""
        ids = {x["id"] for x in v}
        for x in v:
            if set(x.get("parents") or []) & ids:
                return True
        return False

    forks = {k: v for k, v in groups.items()
             if len({x["id"] for x in v}) > 1 and not linked(v)}
    print(f"[2] idempotence: {len(forks)} propositions stored twice under different ids")
    for (s, p, o), v in list(forks.items())[:3]:
        print(f"    {p} = {o}  ids={[x['id'] for x in v]}")

    # 3 — claims
    cf = root / "claims.json"
    claims = json.loads(cf.read_text()) if cf.exists() else {}
    print(f"[3] claims: {len(claims)} question(s) on record")
    for k, v in (claims.items() if isinstance(claims, dict) else []):
        print(f"    {v.get('question', k)[:60]:60s} holder={v.get('author')} "
              f"status={v.get('status')}")

    # 4 — timeline
    print("\n[4] timeline")
    authors = Counter(n.get("author", "?") for n in nodes)
    for n in live[-25:]:
        kind = n.get("type", "claim")
        extra = ""
        if kind == "review":
            extra = f" → {str(n.get('claim'))[:12]} as {n.get('kind') or n.get('p')}"
        print(f"    seq {n['seq']:>4} {n.get('ts','')[11:19]} {n.get('author','?'):16.16s} "
              f"{kind:9s} {str(n.get('p',''))[:26]:26s} {str(n.get('o',''))[:34]}{extra}")
    print("\n    by author: " + ", ".join(f"{k}={v}" for k, v in authors.most_common()))

    # 5 — cross-validation
    by_id = {n["id"]: n for n in nodes}
    votes = [n for n in nodes if n.get("type") == "review"]
    cross = [v for v in votes if by_id.get(v.get("claim"), {}).get("author") != v.get("author")]
    gated = [v for v in cross if v.get("prior") and (v.get("artifact") or v.get("method"))]
    print(f"\n[5] votes: {len(votes)} total, {len(cross)} by someone other than the claim's author, "
          f"{len(gated)} carrying prior+artifact (full weight)")
    for v in cross[-6:]:
        gate = "full" if (v.get("prior") and (v.get("artifact") or v.get("method"))) else "reduced"
        print(f"    {v['author']:14.14s} {str(v.get('kind')):11s} on {str(v.get('claim'))[:12]} [{gate}] "
              f"«{str(v.get('o',''))[:40]}»")

    # hypotheses that the parallel run should have produced
    print("\n[h] did parallelism actually happen")
    distinct = len({n.get("author") for n in nodes if n.get("author") not in (None, "seed")})
    print(f"    distinct non-seed authors: {distinct}")
    print(f"    interleaved (author changes within one run): "
          f"{sum(1 for i in range(1, len(live)) if live[i].get('author') != live[i-1].get('author'))} switches")
    print(f"    votes after the last claim publication: "
          f"{sum(1 for v in votes if v.get('seq', 0) > max((n.get('seq', 0) for n in live), default=0) - 50)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
