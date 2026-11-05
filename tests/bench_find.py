#!/usr/bin/env python3
"""Measure `find` at scale, so nobody guesses about the index.

    python3 bench_find.py            # 200k synthetic nodes (fast)
    python3 bench_find.py 500000     # the number quoted in SKILL.md

It builds a throwaway coral under /tmp, inserts synthetic nodes straight into SQLite
(faster than POSTing), then times:

  1. raw `LIKE '%q%'` over nodes        — the grep-equivalent full scan
  2. FTS5 trigram MATCH                 — what the server uses
  3. FTS5 MATCH + ORDER BY rank         — the trap: bm25 scores every match
  4. Store.find(q) end to end           — what an agent actually calls

Run it before adding any index, and again before claiming a query is slow.
"""
import json
import pathlib
import random
import shutil
import sqlite3
import sys
import time

ROOT = pathlib.Path("/tmp/corallium-bench")
sys.path.insert(0, str(pathlib.Path.home() / "corallium"))
from server import Store  # noqa: E402

WORDS = ["MQSim", "970pro", "geometry", "write", "latency", "GC", "channel",
         "die", "plane", "trace", "读出", "顺序写", "缓存", "校准", "闪存"]


def build(n: int) -> Store:
    shutil.rmtree(ROOT, ignore_errors=True)
    ROOT.mkdir(parents=True)
    st = Store(ROOT)
    c = st._conn()
    rnd = random.Random(11)
    c.execute("BEGIN")
    for i in range(n):
        s = rnd.choice(WORDS)
        p = f"pred_{i % 500}"
        o = f"{rnd.choice(WORDS)} {rnd.randint(1, 9999)} MB/s"
        note = " ".join(rnd.choices(WORDS, k=4))
        node = {"s": s, "p": p, "o": o, "note": note,
                "type": "claim", "author": "bench", "topics": ["MQSim"]}
        c.execute(
            "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (f"id{i:06d}", i + 1, "2026-01-01", "bench", "claim", s, p, o,
             "open", json.dumps(["MQSim"]), json.dumps(node, ensure_ascii=False)),
        )
        c.execute("INSERT INTO nodes_fts(nid, txt) VALUES (?,?)",
                  (f"id{i:06d}", st._fts_text(node)))
    c.commit()
    c.close()
    return st


def timeit(fn, n=3):
    best = 9e9
    for _ in range(n):
        t = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t)
    return best * 1000


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
    st = build(n)
    c = sqlite3.connect(ROOT / "index.db")

    def like(q):
        c.execute(
            "SELECT count(*) FROM nodes WHERE lower(coalesce(s,'')||' '||coalesce(p,'')||' '"
            "||coalesce(o,'')||' '||coalesce(body,'')) LIKE ?", (f"%{q.lower()}%",)).fetchone()

    def fts(q):
        c.execute("SELECT nid FROM nodes_fts WHERE nodes_fts MATCH ? LIMIT 20",
                  (f'"{q}"',)).fetchall()

    def fts_rank(q):
        c.execute("SELECT nid FROM nodes_fts WHERE nodes_fts MATCH ? ORDER BY rank LIMIT 20",
                  (f'"{q}"',)).fetchall()

    print(f"{n} nodes\n")
    print(f"{'query':14s} {'LIKE':>9s} {'FTS':>8s} {'FTS+rank':>10s} {'Store.find':>11s}")
    for q in ["571", "970pro", "顺序写", "no-such-token-xyz"]:
        a = timeit(lambda: like(q))
        b = timeit(lambda: fts(q))
        d = timeit(lambda: fts_rank(q))
        e = timeit(lambda: st.find(q, 5, ""))
        print(f"{q:14s} {a:8.1f}ms {b:7.1f}ms {d:9.1f}ms {e:10.1f}ms")

    # the point of the table: FTS+rank is the slow one, and a zero-hit query must
    # NOT fall back to LIKE or every miss costs a full scan.
    print("\nRead: LIKE ~100ms at 500k; FTS <15ms; ORDER BY rank adds 35-50ms;")
    print("      a miss must stay on the FTS path, not fall back to LIKE.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
