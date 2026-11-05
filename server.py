#!/usr/bin/env python3
"""Corallium server — stdlib only. See PROTOCOL §13.

Truth is nodes/*.json (immutable, one file per node, carrying its own seq).
SQLite is a derived index plus the only non-rebuildable state: claims (leases).

Endpoints
    GET  /health
    GET  /since?cursor=N&topics=a,b
    GET  /tip?topic=T
    GET  /find?q=TEXT
    POST /nodes          {s,p,o,rel?,parents?,type?,status?,volatility?,topics?,method?,attempts?,note?,author}
    POST /claims         {question,ttl?,author}
    POST /nodes/{id}/replies  {kind,text,author,claim?}
    POST /purge          {id,reason,author}   — 销毁内容，留墓碑 (PROTOCOL §14.3/§19)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue
from urllib.parse import parse_qs, urlparse

DEFAULT_PORT = 8731

# PROTOCOL §4.2. `agree` is 0 on purpose: LLMs agree by default, so counting
# agreement turns confidence into a prose-quality contest.
REQUIRED_ARTIFACT = {"refuted", "redundant"}
WEIGHTS = {"adversarial": 4, "replicated": 3, "sourced": 2, "derived": 1,
           "agree": 0, "refuted": -3, "redundant": -2, "junk": 0}

# A tombstone occupies the same slot the node occupied: same id, same seq, same
# ts. Everything that was content is gone. `status: withdrawn` is not decoration:
# `_dead_sets` keys on it, so children become `suspect` with no extra machinery.
TOMBSTONE_KEYS = ("id", "seq", "ts", "type", "status", "purged_at", "reason", "author")


class ApiError(Exception):
    """An error that carries its own HTTP status, raised from inside a writer job.

    `_run` must not flatten these into RuntimeError: "no such node" is a 404,
    not a 500, and the caller decides what to do next from that code.
    """

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


# ── storage ────────────────────────────────────────────────────────────────
class Store:
    """Single writer: every mutation goes through one queue and one thread."""

    def __init__(self, root: Path):
        self.root = root
        self.nodes_dir = root / "nodes"
        self.nodes_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = root / "index.db"
        self.claims_path = root / "claims.json"  # leases must survive rm index.db
        self._q: Queue = Queue()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._writer, daemon=True)
        self._thread.start()
        self._ready.wait(10)
        self._rebuild()
        self._load_claims()

    # -- db ---------------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path, timeout=10)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        # Deleting a row is not the same as deleting bytes: freed pages keep the
        # old payload readable in the file. A purge that leaves the secret in
        # freelist pages has not purged anything.
        c.execute("PRAGMA secure_delete=ON")
        return c

    def _init_schema(self, c: sqlite3.Connection) -> None:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS nodes(
                id TEXT PRIMARY KEY, seq INTEGER UNIQUE, ts TEXT,
                author TEXT, type TEXT, s TEXT, p TEXT, o TEXT,
                status TEXT, topics TEXT, body TEXT);
            CREATE INDEX IF NOT EXISTS nodes_seq ON nodes(seq);
            CREATE TABLE IF NOT EXISTS claims(
                task_key TEXT PRIMARY KEY, question TEXT, author TEXT,
                lease_until REAL, status TEXT, ts TEXT);
            CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
            """
        )
        # FTS5 + trigram: substring search that works for CJK and for partial
        # alphanumerics ("970" matches "970pro"). Default tokenizer splits on
        # whitespace and would be useless on Chinese text.
        try:
            c.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts "
                "USING fts5(nid UNINDEXED, txt, tokenize='trigram')"
            )
            self.fts_ok = True
        except sqlite3.OperationalError:
            self.fts_ok = False  # old sqlite without trigram: find() uses LIKE

    def _rebuild(self) -> None:
        """Derived index is disposable: rebuild from node files on start."""
        c = self._conn()
        self._init_schema(c)
        try:
            c.execute("DELETE FROM nodes_fts")  # rebuild means rebuild, not append
        except sqlite3.OperationalError:
            pass
        rows = list(self.nodes_dir.glob("*.json"))
        for f in rows:
            try:
                n = json.loads(f.read_text())
            except Exception:
                continue
            c.execute(
                "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET seq=excluded.seq, ts=excluded.ts, "
                "author=excluded.author, type=excluded.type, s=excluded.s, p=excluded.p, "
                "o=excluded.o, status=excluded.status, topics=excluded.topics, body=excluded.body",
                (n["id"], n.get("seq", 0), n.get("ts"), n.get("author"),
                 n.get("type"), n.get("s"), n.get("p"), n.get("o"),
                 n.get("status"), json.dumps(n.get("topics", [])),
                 json.dumps(n, ensure_ascii=False)),
            )
            self._fts_put(c, n)
        c.commit()
        c.close()

    @staticmethod
    def _fts_text(n: dict) -> str:
        parts = [str(n.get(k) or "") for k in ("s", "p", "o", "note", "type", "author")]
        parts += [str(t) for t in (n.get("topics") or [])]
        return " ".join(p for p in parts if p)

    def _load_claims(self) -> None:
        """Leases live in claims.json, not only in the derived index.

        `rm index.db*` is documented as harmless; if leases existed only in the
        database, that harmless delete would silently let two agents claim the
        same question. Fail-open is acceptable for an expired lease, never for
        a live one.
        """
        if not self.claims_path.exists():
            return
        try:
            items = json.loads(self.claims_path.read_text())
        except Exception:
            return
        c = self._conn()
        for it in items:
            c.execute(
                "INSERT INTO claims VALUES (?,?,?,?,?,?) ON CONFLICT(task_key) DO UPDATE SET "
                "author=excluded.author, lease_until=excluded.lease_until, status=excluded.status",
                (it["task_key"], it.get("question", ""), it.get("author", "unknown"),
                 it.get("lease_until", 0), it.get("status", "claimed"), it.get("ts", "")),
            )
        c.commit()
        c.close()

    def _save_claims(self, c: sqlite3.Connection | None = None) -> None:
        """Call it with the writer's own connection: a fresh connection cannot see
        an uncommitted INSERT, so saving from outside the transaction writes an
        empty file and the lease dies on the next restart."""
        own = c is None
        if own:
            c = self._conn()
        rows = c.execute(
            "SELECT task_key, question, author, lease_until, status, ts FROM claims"
        ).fetchall()
        if own:
            c.close()
        items = [dict(zip(("task_key", "question", "author", "lease_until", "status", "ts"), r))
                 for r in rows]
        # Unique temp name: two threads saving at once used to race on a single
        # `claims.json.tmp`, one rename won, the other raised FileNotFoundError
        # and returned 500 — while the lease had already been taken in SQLite.
        tmp = self.claims_path.with_name(
            f"claims.{os.getpid()}.{threading.get_ident()}.tmp")
        tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2))
        os.replace(tmp, self.claims_path)

    def _fts_put(self, c: sqlite3.Connection, n: dict) -> None:
        try:
            c.execute("INSERT INTO nodes_fts(nid, txt) VALUES (?,?)",
                      (n["id"], self._fts_text(n)))
        except sqlite3.OperationalError:
            pass

    def _writer(self) -> None:
        c = self._conn()
        self._init_schema(c)
        self._ready.set()
        while True:
            job, box = self._q.get()
            try:
                box["result"] = job(c)
                c.commit()
            except Exception as e:  # never leave the writer thread dead
                box["error"] = f"{type(e).__name__}: {e}"
                box["exc"] = e
                try:
                    c.rollback()
                except Exception:
                    pass
            finally:
                box["done"].set()
                self._q.task_done()

    def _run(self, fn):
        box = {"done": threading.Event(), "result": None, "error": None, "exc": None}
        self._q.put((fn, box))
        if not box["done"].wait(30):
            raise TimeoutError("writer queue timeout")
        if box["error"]:
            # validation and "not found" must reach the caller as themselves,
            # otherwise every bad request is indistinguishable from a crash
            if isinstance(box["exc"], (ApiError, ValueError)):
                raise box["exc"]
            raise RuntimeError(box["error"])
        return box["result"]

    # -- reads (no queue) --------------------------------------------------
    def since(self, cursor: int, topics: list[str]) -> list[dict]:
        c = self._conn()
        cur = c.execute("SELECT body FROM nodes WHERE seq > ? ORDER BY seq", (cursor,))
        out = []
        for (body,) in cur.fetchall():
            n = json.loads(body)
            if topics and not (set(topics) & set(n.get("topics", []))):
                continue
            out.append(n)
        c.close()
        return out

    def find(self, q: str, limit: int = 5, root: str = "",
             order: str = "recent") -> tuple[list[dict], int]:
        """Returns (hits, outside_count). outside = matched but filtered by --root."""
        c = self._conn()
        q = (q or "").strip()
        out: list[dict] = []
        outside = 0
        scoped = False
        if len(q) >= 3:  # trigram needs 3+ chars
            try:
                phrase = '"' + q.replace('"', '""') + '"'
                if root:
                    # Scope before slicing. Filtering the twenty newest matches by
                    # topic made `find q --root X` answer "no hits" while a hundred
                    # in-scope nodes sat further back, and made `outside` a count of
                    # the sample rather than of the matches (§11.5 says report it).
                    rows = c.execute(
                        "SELECT n.id, n.body, n.topics FROM nodes n WHERE n.id IN "
                        "(SELECT nid FROM nodes_fts WHERE nodes_fts MATCH ?) "
                        "ORDER BY n.seq DESC LIMIT 5000", (phrase,)).fetchall()
                    for rid, b, topics in rows:
                        if root in json.loads(topics or "[]"):
                            if len(out) < max(limit * 8, 40):
                                n = json.loads(b)
                                n.setdefault("id", rid)
                                out.append(n)
                        else:
                            outside += 1
                    scoped = True
                else:
                    # No ORDER BY rank: bm25 scoring costs 36-49 ms per query
                    # because it scores every match; we sort the handful in Python.
                    ids = [r[0] for r in c.execute(
                        "SELECT nid FROM nodes_fts WHERE nodes_fts MATCH ? LIMIT ?",
                        (phrase, max(limit * 4, 20)),
                    ).fetchall()]
                    ids = list(dict.fromkeys(ids))  # FTS rows are per-insert, not per-id
                    if ids:
                        marks = ",".join("?" * len(ids))
                        got = {}
                        for rid, b in c.execute(
                                f"SELECT id, body FROM nodes WHERE id IN ({marks})", ids):
                            n = json.loads(b)
                            n.setdefault("id", rid)  # body may predate the id field
                            got[rid] = n
                        out = [got[i] for i in ids if i in got]
            except sqlite3.OperationalError:
                out = []
        # LIKE is a full scan (161-181 ms at 500k). Only use it when FTS cannot
        # answer: no trigram support, or a query shorter than 3 chars. Falling
        # back on "no hits" would make every miss the slowest possible query.
        if not out and not scoped and (not getattr(self, "fts_ok", False) or len(q) < 3):
            like = f"%{q.lower()}%"
            cur = c.execute(
                """SELECT body FROM nodes
                   WHERE lower(coalesce(s,'')||' '||coalesce(p,'')||' '||coalesce(o,'')
                               ||' '||coalesce(body,'')) LIKE ?
                   ORDER BY seq DESC LIMIT ?""",
                (like, max(limit * 20, 100)),
            )
            out = [json.loads(b) for (b,) in cur.fetchall()]
        c.close()

        out = self.annotate(out)
        if root and not scoped:
            keep = [n for n in out if root in (n.get("topics") or [])]
            outside = len(out) - len(keep)
            out = keep
        # claims before review nodes: a vote mentioning "571" must not displace
        # the conclusion itself in a five-row answer.
        out.sort(key=lambda n: (n.get("type") == "review", -n.get("seq", 0)))
        if order == "significance":
            out.sort(key=lambda n: (n.get("significance", 3), n.get("score", 0)), reverse=True)
        elif order == "score":
            out.sort(key=lambda n: n.get("score", 0), reverse=True)
        return out[:limit], outside

    @staticmethod
    def _dead_sets(all_nodes: list[dict]) -> tuple[set, set]:
        """Two different kinds of "gone", because they cascade differently.

        `dead`   — explicitly retracted (status: withdrawn) plus whatever such a
                   node supersedes. This one poisons descendants → `suspect`.
        `replaced` — merely superseded by a newer answer. The old value is no
                   longer the tip, but it was not wrong, so it must not make its
                   successors suspect. A successor inherits nothing but lineage.
        """
        dead = {n["id"] for n in all_nodes if n.get("status") == "withdrawn"}
        for n in all_nodes:
            if n.get("status") == "withdrawn" and n.get("rel") == "supersedes":
                dead.update(n.get("parents") or [])
        replaced = set()
        for n in all_nodes:
            if n.get("rel") == "supersedes":
                replaced.update(n.get("parents") or [])
        replaced -= dead
        return dead, replaced

    def derive_status(self, node: dict, all_nodes: list[dict],
                      by_id: dict | None = None, dead: set | None = None,
                      replaced: set | None = None, merged: set | None = None) -> str:
        """Status is computed, never written back.

        Precedence matters: withdrawn > suspect > contested > confirmed > open.
        `suspect` is not `withdrawn` — a conclusion can still be true after
        losing its support, so it is flagged, never auto-retracted.
        """
        if dead is None or replaced is None:
            dead, replaced = self._dead_sets(all_nodes)
        if merged is None:
            merged = self._merged_pairs(all_nodes)
        by_id = by_id or {n["id"]: n for n in all_nodes}
        if node["id"] in dead or node.get("status") == "withdrawn":
            return "withdrawn"
        if node["id"] in replaced:
            return "withdrawn"  # superseded: replaced, not refuted
        # Check existence live, not the flag stored at write time: a file that
        # existed then and is gone now is exactly the case this is for. One stat
        # per ref is cheap; hashing is left to GET /verify.
        for r in node.get("refs") or []:
            if r.get("path") and not Path(r["path"]).is_file():
                return "suspect"
            if r.get("node") and not r.get("coral") and r["node"] not in by_id:
                return "suspect"  # a node ref to an id that is not here cannot be opened
        if any(r.get("missing") for r in (node.get("refs") or [])):
            return "suspect"  # the evidence file is gone; the claim is uncheckable

        # retraction cascades transitively: a node whose support was withdrawn
        # is `suspect`, however deep the withdrawal sits above it.
        stack, seen = list(node.get("parents") or []), set()
        while stack:
            pid = stack.pop()
            if pid in seen:
                continue
            seen.add(pid)
            parent = by_id.get(pid)
            if not parent:
                continue
            if pid in dead:
                return "suspect"
            stack.extend(parent.get("parents") or [])

        votes = [n for n in all_nodes if n.get("type") == "review"
                 and n.get("claim") == node["id"]
                 and n.get("author") != node.get("author")]
        kinds = [(v.get("kind") or v.get("p")) for v in votes]
        if "refuted" in kinds:
            return "contested"
        rivals = [n for n in all_nodes
                  if n.get("type") == "claim" and n.get("s") == node.get("s")
                  and n.get("p") == node.get("p") and n.get("o") != node.get("o")
                  and n["id"] not in dead and n["id"] not in replaced
                  # §6.2: a `merges` decision adjudicated exactly this pair.
                  # Counting it as a live rival afterwards left both branches
                  # `contested` forever — the graph recorded a resolution that
                  # the derived status never noticed.
                  and frozenset((node["id"], n["id"])) not in merged]
        if rivals:
            return "contested"  # two live answers to the same question

        strong = [v for v in votes
                  if (v.get("kind") or v.get("p")) == "adversarial"
                  or ((v.get("kind") or v.get("p")) == "replicated" and v.get("prior"))]
        # §4.4: two independent `sourced` votes are corroboration of the same
        # strength as one replication — two sources that never saw each other
        # are independent in the sense the protocol cares about.
        sourced = [v for v in votes if (v.get("kind") or v.get("p")) == "sourced"]
        if strong or len(sourced) >= 2:
            # multiple-comparisons gate: tried 6+ configurations without anyone
            # trying to kill it? Then it is the winner of a search, not a fact.
            attempts = int(node.get("attempts", 1) or 1)
            if attempts > 5 and "adversarial" not in kinds:
                return "open"
            return "confirmed"
        return "open"

    @staticmethod
    def _merged_pairs(all_nodes: list[dict]) -> set:
        """Pairs of nodes a `merges` decision has adjudicated (§6.2).

        All pairs, not just the first two: a decision may name several branches
        at once, and every pair it names has been settled by it.
        """
        out = set()
        for n in all_nodes:
            if n.get("rel") != "merges":
                continue
            ps = n.get("parents") or []
            for i in range(len(ps)):
                for j in range(i + 1, len(ps)):
                    out.add(frozenset((ps[i], ps[j])))
        return out

    @staticmethod
    def _gate(v: dict, target_author: str | None):
        """Base weight from kind, then the two gates. Returns (weight, caps, kind)."""
        kind = v.get("kind") or v.get("p", "agree")
        w = WEIGHTS.get(kind, 0)
        caps = []
        if kind in ("replicated", "adversarial", "refuted", "redundant"):
            # `refuted` and `redundant` stand on their own evidence: a counter-case
            # or a duplicate is visible without having published anything first.
            if kind not in ("refuted", "redundant") and not v.get("prior"):
                caps.append("no-prior")
            if not (v.get("artifact") or v.get("method")):
                caps.append("not-checkable")
            if caps:
                w = 1 if w > 0 else -1
        return w, caps, kind

    def _scores(self, alln: list[dict]) -> dict:
        """Score every node from the vote graph. Nothing is stored.

        Votes on votes are allowed, but the recursion stops after one level:
        a vote's weight may be damped by what others said about it, while votes
        on those meta-votes are recorded and ignored for scoring.
        """
        reviews = [n for n in alln if n.get("type") == "review"]
        by_target: dict[str, list[dict]] = {}
        for r in reviews:
            by_target.setdefault(r.get("claim"), []).append(r)

        out: dict[str, dict] = {}
        for n in alln:  # reviews are scored too, so votes on a vote are listable
            votes, total = [], 0.0
            for v in by_target.get(n["id"], []):
                w, caps, kind = self._gate(v, n.get("author"))
                meta = [m for m in by_target.get(v["id"], [])
                        if m.get("author") != v.get("author")]
                meta_score = 0.0
                for m in meta:
                    mw, _, _ = self._gate(m, v.get("author"))
                    meta_score += mw
                mult = max(1.0 / (1.0 + max(0.0, -meta_score) / 4.0), 0.25)
                counted = v.get("author") != n.get("author")
                votes.append({
                    "id": v["id"], "seq": v.get("seq"), "ts": v.get("ts"),
                    "author": v.get("author"), "kind": kind, "text": v.get("o", ""),
                    "prior": v.get("prior"), "artifact": v.get("artifact"),
                    "weight": w, "capped": caps,
                    "meta_score": meta_score, "meta_votes": len(meta),
                    "multiplier": round(mult, 2),
                    "effective": round(w * mult, 2) if counted else 0,
                    "counted": counted,
                })
                if counted:
                    total += w * mult
            if votes:
                out[n["id"]] = {"score": round(total, 2), "votes": len(votes),
                                "detail": votes}
        return out

    def tasks(self, include_closed: bool = False) -> list[dict]:
        """Open obligations. A task closes when a node lists it in `parents`."""
        c = self._conn()
        alln = [json.loads(b) for (b,) in c.execute("SELECT body FROM nodes").fetchall()]
        c.close()
        children = {p for n in alln for p in (n.get("parents") or [])}
        dead, replaced = self._dead_sets(alln)
        out = []
        for n in alln:
            if n.get("type") != "task":
                continue
            closed = n["id"] in children or n["id"] in dead
            if closed and not include_closed:
                continue
            m = dict(n)
            m["derived_status"] = self.derive_status(n, alln, None, dead, replaced)
            m["closed"] = closed
            out.append(m)
        out.sort(key=lambda n: (-n.get("significance", 3), n.get("seq", 0)))
        return out

    def claims(self, live_only: bool = True) -> list[dict]:
        """Leases are the one piece of state that is not derived, and until now
        they were write-only: an agent could take one, and nobody — not even the
        holder — could ask what was currently claimed. That makes §7 useless in
        exactly the situation it exists for (two agents, same question).
        """
        c = self._conn()
        rows = c.execute(
            "SELECT task_key, question, author, lease_until, status, ts FROM claims "
            "ORDER BY lease_until DESC").fetchall()
        c.close()
        now = time.time()
        out = [dict(zip(("task_key", "question", "author", "lease_until", "status", "ts"), r))
               for r in rows]
        if live_only:
            out = [r for r in out if (r.get("lease_until") or 0) > now]
        return out

    def reviews(self, target_id: str) -> dict:
        """The vote list for any node — claim or another vote — as a timeline."""
        c = self._conn()
        alln = [json.loads(b) for (b,) in c.execute("SELECT body FROM nodes").fetchall()]
        c.close()
        target = next((n for n in alln if n["id"] == target_id), None)
        if not target:
            return {"error": f"no such node: {target_id}"}
        sc = self._scores(alln).get(target_id, {"score": 0, "votes": 0, "detail": []})
        tally: dict[str, int] = {}
        for v in sc["detail"]:
            if v["counted"]:
                tally[v["kind"]] = tally.get(v["kind"], 0) + 1
        return {"node": target, "score": sc["score"], "tally": tally, "votes": sc["detail"]}

    def annotate(self, nodes: list[dict]) -> list[dict]:
        """Fill in everything that is derived: status, score, votes, dependents.

        This re-reads every node body on every query: 28 ms at 5k nodes and a
        250 ms p50 with six concurrent readers, measured by
        tests/concurrency_test.py. It is the store's real scaling limit.
        """
        c = self._conn()
        alln = [json.loads(b) for (b,) in c.execute("SELECT body FROM nodes").fetchall()]
        c.close()
        # No cache. A purge rewrites a node in place without changing seq, and a
        # deleted evidence file changes a status without touching the database —
        # any key that stays cheap is therefore wrong. The fix is to stop loading
        # every node per query (see the perf task), not to memoise a full scan.
        table = self._annotate_all(alln)
        return [dict(table[n["id"]]) for n in nodes if n["id"] in table]

    def _annotate_all(self, alln: list[dict]) -> dict:
        scores = self._scores(alln)
        by_id = {n["id"]: n for n in alln}
        dead, replaced = self._dead_sets(alln)
        merged = self._merged_pairs(alln)
        deps: dict[str, int] = {}
        for n in alln:
            for p_ in (n.get("parents") or []):
                deps[p_] = deps.get(p_, 0) + 1
        red_targets = {v.get("claim") for v in alln
                       if v.get("type") == "review"
                       and (v.get("kind") or v.get("p")) == "redundant"}
        # §4.4: `confirmed` has two levels and `tip` must show which one.
        # "Nobody objected" is not "survived attack"; computing the set once is
        # what keeps this from being another full scan per node.
        challenged = {v.get("claim") for v in alln
                      if v.get("type") == "review"
                      and (v.get("kind") or v.get("p")) == "adversarial"
                      and v.get("author") != (by_id.get(v.get("claim")) or {}).get("author")}
        table: dict[str, dict] = {}
        for n in alln:
            m = dict(n)
            m["derived_status"] = self.derive_status(n, alln, by_id, dead, replaced, merged)
            if m["derived_status"] == "confirmed":
                m["confirmed_level"] = "challenged" if n["id"] in challenged else "unchallenged"
            if n["id"] in red_targets:
                m["merge_candidate"] = True
            rs = n.get("refs") or []
            if rs:
                # Do not overwrite `refs` with a number: a caller that reads the
                # hit then tries to open the evidence gets `2` instead of a list,
                # and a ref with `node` instead of `path` (Path("") == ".") used
                # to be counted as a missing file.
                m["refs_count"] = len(rs)
                m["refs_missing"] = sum(1 for r in rs
                                        if r.get("path") and not Path(r["path"]).is_file())
            if n.get("type") not in ("review", "tombstone"):
                # Every proposition can be voted on (§4.2), not only `claim`.
                # A `question` or `anomaly` that carries a real `sourced` vote
                # used to render score=None and votes=None, so the vote existed
                # in the graph and was invisible in every read — the same silent
                # loss as a vote aimed at a typo'd id.
                sc = scores.get(n["id"], {"score": 0, "votes": 0})
                m["score"] = sc["score"]
                m["votes"] = sc["votes"]
                m["significance"] = n.get("significance", 3)
                m["dependents"] = deps.get(n["id"], 0)
            table[n["id"]] = m
        return table

    def _seq_gen(self) -> int:
        c = self._conn()
        row = c.execute("SELECT coalesce(max(seq),0) FROM nodes").fetchone()
        c.close()
        return int(row[0] if row else 0)

    def tip(self, topic: str) -> list[dict]:
        """Leaves of the edge graph, not a text match on the topic.

        `parents` is the only structure that says "this replaced / extended
        that", so a tip is a node that nothing else points at. Dropping
        `withdrawn` here is what makes `tip` an answer to "what do we believe
        now" rather than "what did we ever write".
        """
        c = self._conn()
        alln = [json.loads(b) for (b,) in c.execute("SELECT body FROM nodes").fetchall()]
        c.close()
        if topic:
            low = topic.lower()
            # Topic labels are the aggregation axis (§2.3). Matching the label
            # anywhere in the JSON meant `tip protocol` pulled in every node whose
            # note merely mentioned the word, so the "current beliefs" list was
            # mostly history. Free text is a fallback for a label that is not in
            # use, not the primary interpretation.
            scope = [n for n in alln
                     if low in [str(t).lower() for t in (n.get("topics") or [])]]
            if not scope:
                scope = [n for n in alln if low in json.dumps(n, ensure_ascii=False).lower()]
        else:
            scope = alln
        if not scope:
            return []
        ids = {n["id"] for n in scope}
        child_of = {p for n in scope for p in (n.get("parents") or []) if p in ids}
        tips = [n for n in scope if n["id"] not in child_of]
        dead, replaced = self._dead_sets(alln)
        live = [n for n in tips if n["id"] not in dead and n["id"] not in replaced]
        return self.annotate(live or tips)

    def max_seq(self) -> int:
        c = self._conn()
        v = c.execute("SELECT coalesce(max(seq),0) FROM nodes").fetchone()[0]
        c.close()
        return int(v)

    # -- writes (queued) ---------------------------------------------------
    def _pin_refs(self, node: dict) -> None:
        """A reference is a path plus the hash of what was there when it was made.

        Pinning at write time is the point: the author cannot later claim the file
        said something it never said, and months later we can tell whether the
        evidence still exists. Files change; nodes do not.
        """
        refs = node.get("refs") or []
        if not isinstance(refs, list):
            raise ValueError("refs must be a list")
        pinned = []
        for r in refs:
            if not isinstance(r, dict) or not (r.get("path") or r.get("node")):
                raise ValueError("each ref needs path or node")
            r = dict(r)
            if r.get("node"):
                pinned.append(r)  # node-to-node: the store resolves it, not the filesystem
                continue
            f = Path(r["path"]).expanduser()
            if not f.is_absolute():
                f = self.root / f
            r["path"] = str(f)
            if f.is_file():
                r["sha256"] = hashlib.sha256(f.read_bytes()).hexdigest()
                r["size"] = f.stat().st_size
                r["missing"] = False
            else:
                r["missing"] = True
                r.pop("sha256", None)
            pinned.append(r)
        node["refs"] = pinned

    def verify_refs(self, node_id: str | None = None) -> dict:
        """Re-hash every reference: is the evidence still there, and still the same?"""
        c = self._conn()
        rows = (c.execute("SELECT body FROM nodes WHERE id=?", (node_id,)).fetchall()
                if node_id else c.execute("SELECT body FROM nodes").fetchall())
        ids = {r[0] for r in c.execute("SELECT id FROM nodes").fetchall()}
        c.close()
        out = []
        for (b,) in rows:
            n = json.loads(b)
            for i, r in enumerate(n.get("refs") or []):
                if r.get("node"):
                    # A node reference is a pointer too. It used to answer
                    # "node" — counted as ok — without ever looking the target
                    # up, so a ref to a mistyped id was evidence that could not
                    # be opened, reported as present. Cross-coral refs carry
                    # `coral` and are not in this index at all.
                    state = "node" if r.get("coral") or r["node"] in ids else "missing"
                    out.append({"node": n["id"], "i": i, "ref": r, "state": state})
                    continue
                f = Path(r["path"])
                if not f.is_file():
                    out.append({"node": n["id"], "i": i, "ref": r, "state": "missing"})
                    continue
                cur = hashlib.sha256(f.read_bytes()).hexdigest()
                out.append({"node": n["id"], "i": i, "ref": r,
                            "state": "ok" if cur == r.get("sha256") else "changed",
                            "now": cur})
        return {"refs": out,
                "ok": sum(1 for x in out if x["state"] in ("ok", "node")),
                "changed": sum(1 for x in out if x["state"] == "changed"),
                "missing": sum(1 for x in out if x["state"] == "missing")}

    @staticmethod
    def _grams(text: str, n: int = 3) -> set:
        t = " ".join(str(text or "").lower().split())
        t = t.replace(" ", "")
        return {t[i:i + n] for i in range(max(0, len(t) - n + 1))}

    def _candidates(self, p: str, o: str, limit: int = 30) -> list[dict]:
        """Cheap prefilter through the FTS index, then trigram Jaccard.

        The index answers "which nodes share a token", which at 500k is a few
        hundred microseconds; the Jaccard pass only ever sees the handful it
        returns. Scanning every node to decide whether to reject a write would
        make publishing O(coral), and publishing is the thing we want to be easy.
        """
        c = self._conn()
        terms = list(dict.fromkeys(re.findall(r"[A-Za-z0-9_]{3,}", f"{p} {o}")))
        match = " OR ".join('"' + t + '"' for t in terms[:6])
        if not terms:
            # CJK text has no 3-char ASCII token, but it does have trigrams. The
            # old fallback compared every Chinese write against the *first* N rows
            # in the store — an arbitrary sample — so the gate that §3.4 relies on
            # was silently useless on a Chinese coral. Query the same index with
            # evenly spaced 3-char windows of the proposition instead.
            text = re.sub(r"\s+", "", f"{p} {o}")
            windows = list(dict.fromkeys(text[i:i + 3] for i in range(max(0, len(text) - 2))))
            if len(windows) > 8:
                step = len(windows) / 8
                windows = [windows[int(i * step)] for i in range(8)]
            match = " OR ".join('"' + w + '"' for w in windows if w.strip())
        rows = []
        if match:
            try:
                rows = c.execute(
                    "SELECT nid FROM nodes_fts WHERE nodes_fts MATCH ? LIMIT ?",
                    (match, max(limit * 4, 20))).fetchall()
            except sqlite3.OperationalError:
                rows = []
        ids = list(dict.fromkeys(r[0] for r in rows))
        if not ids:  # nothing shares a window: compare against the newest writes
            ids = [r[0] for r in c.execute(
                "SELECT id FROM nodes ORDER BY seq DESC LIMIT ?", (max(limit * 4, 20),))]
        marks = ",".join("?" * len(ids))
        got = [json.loads(b) for (b,) in c.execute(
            f"SELECT body FROM nodes WHERE id IN ({marks})", ids).fetchall()] if ids else []
        c.close()
        mine_o, mine_po = self._grams(o), self._grams(f"{p} {o}")
        out = []
        for n in got:
            if n.get("type") in ("review", "tombstone"):
                continue
            theirs_o = self._grams(n.get("o", ""))
            theirs_po = self._grams(f"{n.get('p','')} {n.get('o','')}")
            if not theirs_o or not mine_o:
                continue
            # Two axes, because the same knowledge recorded under a different
            # predicate name is what this gate is most needed for: `ref_support`
            # vs `refs_pinned` share almost no predicate grams but the object is
            # identical. Judging on `p + o` alone scores that pair below 0.62.
            j = max(len(mine_o & theirs_o) / len(mine_o | theirs_o),
                    len(mine_po & theirs_po) / len(mine_po | theirs_po))
            if j >= 0.45:
                out.append({"id": n["id"], "p": n.get("p"), "o": n.get("o"),
                            "status": n.get("status"),
                            "parents": n.get("parents") or [],
                            "similarity": round(j, 3)})
        out.sort(key=lambda x: -x["similarity"])
        return out[:5]

    def add_node(self, payload: dict) -> dict:
        def job(c):
            s, p, o = payload.get("s"), payload.get("p"), payload.get("o")
            if not (s and p and o):
                raise ValueError("s, p, o are required")
            rel = payload.get("rel", "root")
            parents = payload.get("parents") or []
            ntype = payload.get("type", "claim")
            # A parent id that resolves to nothing is not a weaker edge, it is no
            # edge at all: the child silently becomes a root, never cascades when
            # its support is retracted, and — because the write gate is skipped
            # whenever `parents` is non-empty — it also buys a free pass around
            # §3.4. A typo must fail loudly here, while the writer still has the
            # candidate list in front of them.
            if parents:
                missing = [p for p in parents
                           if not c.execute("SELECT 1 FROM nodes WHERE id=?", (p,)).fetchone()]
                if missing:
                    raise ApiError(400, "unknown parent id(s): " + ", ".join(missing[:3]) +
                                        " — get the id from /find; a node must grow on a node "
                                        "that exists (PROTOCOL §3.4.2)")
            # A vote's identity includes who cast it and what it rests on.
            # Without author+artifact in the hash, two agents who write the same
            # words collapse into one vote and the second disappears silently —
            # observed live when two of four parallel agents cast the same
            # `redundant` verdict.
            if payload.get("type") == "review":
                target = payload.get("claim")
                # A vote id typo'd by one character used to land anyway: it is a
                # valid node, it appears in `since`, and it counts toward nothing
                # forever. Nodes are immutable, so there is no way to re-attach it.
                if not target or not c.execute("SELECT 1 FROM nodes WHERE id=?",
                                               (target,)).fetchone():
                    raise ApiError(404, f"no such node: {target} — a vote must point at "
                                        "a node that exists (PROTOCOL §4.1.2)")
                blob = json.dumps([s, p, o, rel, sorted(parents),
                                   payload.get("author", ""), payload.get("prior", ""),
                                   payload.get("artifact", "")], ensure_ascii=False)
            else:
                blob = json.dumps([s, p, o, rel, sorted(parents)], ensure_ascii=False)
            nid = hashlib.sha256(blob.encode()).hexdigest()[:12]

            hit = c.execute("SELECT body FROM nodes WHERE id=?", (nid,)).fetchone()
            if hit:  # idempotent: concurrent identical publishes collapse
                return {"created": False, "node": json.loads(hit[0])}

            # §3.4.1 — the gate that stops the graph from spreading sideways.
            # It only runs for propositions: a review points at its claim by
            # construction, and a task is an obligation, not a finding.
            gateable = ntype in ("claim", "anomaly", "question", "decision")
            if gateable and not parents:
                cand = self._candidates(p, o)
                top = cand[0] if cand else None
                if top and top["similarity"] >= 0.62 and not payload.get("force"):
                    return {"conflict": "similar", "candidates": cand,
                            "hint": "link it (rel=extends|supersedes, parents=[id]) "
                                    "or pass force=true with why_new"}
                if top and top["similarity"] >= 0.62 and not (payload.get("why_new") or "").strip():
                    raise ValueError("force=true needs a non-empty why_new: "
                                     "say how this differs from " + top["id"])

            seq = int(c.execute("SELECT coalesce(max(seq),0) FROM nodes").fetchone()[0]) + 1
            node = {
                "id": nid, "seq": seq,
                "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                "author": payload.get("author", "unknown"),
                "type": payload.get("type", "claim"),
                "s": s, "p": p, "o": o, "rel": rel, "parents": parents,
                "status": payload.get("status", "open"),
                "volatility": payload.get("volatility", "static"),
                "topics": payload.get("topics") or [],
                "attempts": payload.get("attempts", 1),
                # declared, not voted: significance is how much it would matter
                # if true, which is a different axis from whether it is true.
                "significance": max(1, min(5, int(payload.get("significance", 3) or 3))),
                "note": payload.get("note", ""),
            }
            if payload.get("method"):
                node["method"] = payload["method"]
            # review nodes must carry what they are evidence *for*, and the
            # prior node that proves the reviewer had not seen the conclusion.
            for k in ("claim", "prior", "kind", "artifact"):
                if payload.get(k):
                    node[k] = payload[k]
            if payload.get("force"):
                node["forced"] = True
                node["why_new"] = payload.get("why_new", "")
            if payload.get("refs"):
                node["refs"] = payload["refs"]
                self._pin_refs(node)

            # file first: it is the truth, and O_EXCL makes the write atomic
            path = self.nodes_dir / f"{nid}.json"
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                with os.fdopen(fd, "w") as fh:
                    json.dump(node, fh, ensure_ascii=False, indent=2)
            except FileExistsError:
                existing = json.loads(path.read_text())
                return {"created": False, "node": existing}

            c.execute(
                "INSERT OR IGNORE INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (nid, seq, node["ts"], node["author"], node["type"], s, p, o,
                 node["status"], json.dumps(node["topics"]), json.dumps(node, ensure_ascii=False)),
            )
            self._fts_put(c, node)
            out = {"created": True, "node": node}
            if gateable and parents:
                # §3.4.1 promises `linked_to` on the parents exit: the writer gets
                # to see which existing nodes this one now sits next to, so the
                # next agent's search starts from the right branch. It was in the
                # table and not in the response.
                out["linked_to"] = [x["id"] for x in self._candidates(p, o)
                                    if x["similarity"] >= 0.62]
            return out

        return self._run(job)

    def claim(self, question: str, author: str, ttl_min: int) -> dict:
        def job(c):
            now = time.time()
            if not (question or "").strip():
                # An empty question hashes to one key, so every agent that ever
                # omits it contends for the same nameless lease.
                raise ValueError("question is required: a lease with no question "
                                 "locks a name nobody can describe")
            key = hashlib.sha256(question.strip().lower().encode()).hexdigest()[:12]
            row = c.execute(
                "SELECT author, lease_until FROM claims WHERE task_key=?", (key,)
            ).fetchone()
            if ttl_min <= 0:
                # ttl<=0 is a release, not a claim: the CLI had no other way to
                # hand a question back, and `ttl=0` used to mean "expires now",
                # which let 8 of 20 contenders all win at once.
                # It is checked before the "busy" answer so that a refused
                # release says `released: false` + holder instead of looking
                # like a claim that merely lost the race.
                if not row:
                    return {"released": False, "reason": "no such question"}
                if row[1] and row[1] > now and row[0] != author:
                    # Holder-only: otherwise any agent can drop someone else's
                    # lease mid-run and silently start the same research twice.
                    return {"released": False, "holder": row[0],
                            "reason": "held by someone else"}
                c.execute("DELETE FROM claims WHERE task_key=?", (key,))
                self._save_claims(c)
                return {"released": True, "task_key": key, "question": question}
            if row and row[1] and row[1] > now and row[0] != author:
                return {"acquired": False, "holder": row[0], "lease_until": row[1]}
            c.execute(
                "INSERT INTO claims VALUES (?,?,?,?,?,?) ON CONFLICT(task_key) DO UPDATE SET "
                "author=excluded.author, lease_until=excluded.lease_until, status='claimed'",
                (key, question, author, now + ttl_min * 60, "claimed",
                 datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")),
            )
            if ttl_min > 0:
                self._save_claims(c)  # same connection: the INSERT is uncommitted
            return {"acquired": True, "task_key": key, "lease_until": now + ttl_min * 60}

        return self._run(job)

    def reply(self, claim_id: str, payload: dict) -> dict:
        merged = dict(payload)
        merged.update({"type": "review", "claim": claim_id,
                       "s": payload.get("s", claim_id),
                       "p": payload.get("kind", "confirm"),
                       "o": payload.get("text", "")})
        return self.add_node(merged)

    # ── 维护模式 (§20) ──────────────────────────────────────────────────────

    def _all(self) -> list[dict]:
        c = self._conn()
        out = [json.loads(b) for (b,) in c.execute("SELECT body FROM nodes").fetchall()]
        c.close()
        out.sort(key=lambda n: n.get("seq", 0))
        return out

    def scan(self) -> dict:
        """Facts only. Which duplicate to merge is a judgement, and judgements
        leave a node behind — a scan that also recommends would launder that."""
        alln = self._all()
        children: dict[str, int] = {}
        votes: dict[str, int] = {}
        for n in alln:
            for p_ in (n.get("parents") or []):
                children[p_] = children.get(p_, 0) + 1
            if n.get("type") == "review" and n.get("claim"):
                votes[n["claim"]] = votes.get(n["claim"], 0) + 1
        # Already-resolved nodes are not candidates: a merge or a supersede is a
        # decision, and re-listing its inputs every scan turns the report into
        # noise that hides the pairs that still need someone to decide.
        resolved = {p_ for n in alln
                    if n.get("rel") in ("merges", "supersedes")
                    for p_ in (n.get("parents") or [])}
        # Derived status, not the stored field: `status` is `open` on every node
        # ever written (v0.1.6), so filtering on it kept superseded nodes in the
        # candidate list forever — a duplicate pair stays in the report after a
        # `supersedes` because the old node's stored status never changes.
        table = self._annotate_all(alln)
        live = [n for n in alln if n.get("type") in ("claim", "anomaly", "question")
                and table[n["id"]]["derived_status"] != "withdrawn"
                and n["id"] not in resolved]
        dups, seen = [], set()
        for n in live:
            for cand in self._candidates(n.get("p", ""), n.get("o", ""), limit=12):
                if cand["id"] == n["id"] or cand["similarity"] < 0.62:
                    continue
                key = tuple(sorted((n["id"], cand["id"])))
                if key in seen:
                    continue
                seen.add(key)
                dups.append({"a": n["id"], "b": cand["id"],
                             "similarity": cand["similarity"],
                             "linked": (cand["id"] in (n.get("parents") or [])
                                        or n["id"] in (cand.get("parents") or []))})
        roots = [n for n in live if not (n.get("parents") or [])]
        # Only live nodes' refs: a retracted claim's missing evidence is not a
        # maintenance item — nobody should re-check it, and re-listing it every
        # scan (as the superseded node kept doing) is exactly the noise v0.1.16
        # removed from `duplicates`.
        broken = [{"node": n["id"], "path": r.get("path")}
                  for n in alln
                  if table[n["id"]]["derived_status"] != "withdrawn"
                  for r in (n.get("refs") or [])
                  if r.get("path") and not Path(r["path"]).is_file()]
        junk = []
        for n in live:
            if n.get("type") == "decision":
                continue
            if (children.get(n["id"], 0) == 0 and votes.get(n["id"], 0) == 0
                    and int(n.get("significance", 3)) <= 2 and not (n.get("refs"))):
                jv = sum(1 for x in alln if x.get("type") == "review"
                         and x.get("claim") == n["id"]
                         and (x.get("kind") or x.get("p")) == "junk"
                         and x.get("author") != n.get("author"))
                junk.append({"id": n["id"], "o": n.get("o", "")[:60],
                             "why": "no children, no votes, ★%s" % n.get("significance", 3),
                             "author": n.get("author"), "junk_votes": jv,
                             "retirable": bool(jv)})
        by_status: dict[str, int] = {}
        for n in alln:
            st = table[n["id"]]["derived_status"]
            by_status[st] = by_status.get(st, 0) + 1
        return {"duplicates": dups[:50], "orphans": [n["id"] for n in roots][:100],
                "broken_refs": broken, "junk": junk[:50],
                "counts": {"nodes": len(alln), "roots": len(roots),
                           # stored `status: withdrawn` is nearly always 0; this
                           # is the derived count (explicit retractions included)
                           "withdrawn": by_status.get("withdrawn", 0),
                           "by_status": by_status,
                           "duplicate_pairs": len(dups), "junk": len(junk)}}

    def merge(self, keep: str, drop: str, reason: str, author: str) -> dict:
        """L2: one node that stands for both. Nothing is destroyed — the merge
        node's `parents` are exactly what makes tip stop showing either."""
        alln = {n["id"]: n for n in self._all()}
        for i in (keep, drop):
            if i not in alln:
                raise ApiError(404, f"unknown node {i}")
        if keep == drop:
            raise ValueError("keep and drop are the same node")
        if not reason.strip():
            raise ValueError("reason is required: a merge without a reason cannot be reviewed")
        a, b = alln[keep], alln[drop]
        r = self.add_node({"s": a.get("s", ""), "p": a.get("p", ""), "o": a.get("o", ""),
                           "author": author, "type": "decision", "rel": "merges",
                           "parents": [keep, drop], "topics": a.get("topics") or [],
                           "significance": max(int(a.get("significance", 3)),
                                               int(b.get("significance", 3))),
                           "note": reason, "force": True,
                           "why_new": "merge of %s and %s" % (keep, drop)})
        return {"merged": r["node"]["id"], "keep": keep, "drop": drop,
                "created": r["created"]}

    def retire(self, node_id: str, reason: str, author: str) -> dict:
        """L3: destroy content. Four machine-checkable conditions, because the
        cost of being wrong is asymmetric: leftover junk is noise, deleting a
        node someone builds on is an irreversible loss."""
        alln = self._all()
        by = {n["id"]: n for n in alln}
        n = by.get(node_id)
        if not n:
            raise ApiError(404, f"unknown node {node_id}")
        children = [x["id"] for x in alln if node_id in (x.get("parents") or [])]
        # A `junk` vote is the accusation, not a defence: it must not close the
        # "nobody has judged this" gate, or the accusation would veto itself.
        votes = [x["id"] for x in alln if x.get("type") == "review"
                 and x.get("claim") == node_id
                 and (x.get("kind") or x.get("p")) != "junk"]
        fails = []
        if children:
            fails.append(f"has {len(children)} child node(s): {children[:3]}")
        if votes:
            fails.append(f"has {len(votes)} vote(s): {votes[:3]}")
        if int(n.get("significance", 3)) > 2:
            fails.append(f"significance is {n.get('significance')} (retire allows <=2)")
        if n.get("refs"):
            fails.append("has file references: it is evidence-backed, not junk")
        junk_votes = [x["id"] for x in alln if x.get("type") == "review"
                      and x.get("claim") == node_id
                      and (x.get("kind") or x.get("p")) == "junk"
                      and x.get("author") != n.get("author")]
        if not junk_votes:
            # Structural gates say "nobody would miss it"; they cannot say
            # "it is wrong". That claim has to be made by someone, on the
            # record, before anything is destroyed.
            fails.append("no `junk` vote: nothing has accused this node of being a mistake")
        if fails:
            raise ApiError(409, "refusing to retire: " + "; ".join(fails))
        return self.purge(node_id, reason, author)

    def purge(self, node_id: str, reason: str, author: str) -> dict:
        """Destroy one node's payload, keep its place in the graph (PROTOCOL §14.3).

        Nodes are immutable; this is the single sanctioned exception, and only
        for secrets. Three things must all be true afterwards, or the purge is a
        lie:

          1. the payload is not in the file (rewritten, not appended to),
          2. the payload is not in the index (FTS rows deleted, not shadowed),
          3. the id still resolves, so `parents: [id]` does not dangle.

        (3) is why this is a tombstone and not a delete: a child that lost its
        parent silently becomes an orphan that still looks confirmed. Status
        `withdrawn` feeds `_dead_sets`, which is what turns those children
        `suspect` — the retraction cascade falls out of the state machine instead
        of being re-implemented here.
        """
        node_id = (node_id or "").strip()
        reason = (reason or "").strip()
        author = (author or "").strip() or "unknown"
        # Validate outside the queue: a missing reason is the caller's mistake,
        # and must not reach the index or the disk at all.
        if not node_id:
            raise ApiError(400, "id is required")
        if not reason:
            raise ApiError(400, "reason is required: a tombstone without a reason "
                                "is an unexplained hole in the record")

        def job(c):
            row = c.execute("SELECT body FROM nodes WHERE id=?", (node_id,)).fetchone()
            if not row:
                raise ApiError(404, f"no such node: {node_id}")
            old = json.loads(row[0])
            if old.get("type") == "tombstone":
                # second purge is a conflict, not a second tombstone: the first
                # reason is the record, and overwriting it would erase the audit
                raise ApiError(409, f"already a tombstone: {node_id}")

            tomb = {
                "id": old["id"], "seq": old.get("seq", 0), "ts": old.get("ts"),
                "type": "tombstone", "status": "withdrawn",
                "purged_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                "reason": reason, "author": author,
            }
            assert tuple(tomb) == TOMBSTONE_KEYS  # no field creeps back in

            # file first: it is the truth, as on the write path. Same directory,
            # so os.replace is atomic — a reader sees the old node or the
            # tombstone, never half of each.
            path = self.nodes_dir / f"{old['id']}.json"
            tmp = path.with_name(path.name + ".purge")
            fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
            with os.fdopen(fd, "w") as fh:
                json.dump(tomb, fh, ensure_ascii=False, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)

            body = json.dumps(tomb, ensure_ascii=False)
            c.execute(
                "UPDATE nodes SET author=?, type=?, s=NULL, p=NULL, o=NULL, "
                "status=?, topics=?, body=? WHERE id=?",
                (author, "tombstone", "withdrawn", json.dumps([]), body, old["id"]),
            )
            # FTS rows are per-insert, not per-id: the old text lives in its own
            # row, and UPDATE nodes does not touch it. Delete by nid; a bare
            # re-insert would leave the secret searchable forever.
            c.execute("DELETE FROM nodes_fts WHERE nid=?", (old["id"],))
            self._fts_put(c, tomb)
            return {"purged": True, "node": tomb}

        r = self._run(job)
        self._scrub()
        return r

    def _scrub(self) -> None:
        """Overwrite what the delete left behind. Best effort, after commit.

        SQLite does not zero freed pages (hence `secure_delete`), FTS5 keeps the
        deleted text in unmerged segments until `optimize`, and the WAL still
        holds the pre-purge page images until a truncating checkpoint. None of
        these change what `find` returns — they change what `grep index.db`
        finds, which for a secret is the whole point.
        """
        try:
            c = self._conn()
            if getattr(self, "fts_ok", False):
                c.execute("INSERT INTO nodes_fts(nodes_fts) VALUES('optimize')")
            c.commit()
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            c.close()
        except sqlite3.Error:
            pass


# ── http ───────────────────────────────────────────────────────────────────
def make_handler(corals: dict, read_token: str, write_token: str, default: str | None):
    """One process, many corals. `/c/<name>/<endpoint>` addresses them explicitly;
    bare `/<endpoint>` goes to the default coral so a single-coral install needs
    no prefix."""
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):  # quiet
            pass

        def _path(self) -> str:
            """The request target, decoded as UTF-8 when it is not ASCII.

            http.server decodes the request line as latin-1, so `?q=写缓存`
            (what `curl` sends when the query is pasted raw) arrives as mojibake
            and matches nothing — a silent zero-hit answer for a library whose
            contents are mostly Chinese. Percent-encoded clients are unaffected.
            """
            p = self.path
            if not p.isascii():
                try:
                    p = p.encode("latin-1").decode("utf-8")
                except (UnicodeEncodeError, UnicodeDecodeError):
                    pass
            return p

        def _send(self, code: int, obj) -> None:
            body = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _auth(self, write: bool) -> bool:
            got = (self.headers.get("Authorization") or "").replace("Bearer ", "").strip()
            want = write_token if write else read_token
            if write and not write_token and read_token:
                # A deployment that set only a read token is declaring "read only".
                # Treating the missing write token as "no auth needed" turned that
                # declaration into the opposite: the one credential handed out was
                # a read token, and every write was open to anyone.
                self._send(401, {"error": "read-only deployment: no write token is configured"})
                return False
            if not want:  # nothing configured → local trust
                return True
            if got != want:
                self._send(401, {"error": "unauthorized"})
                return False
            return True

        def _resolve(self, path: str):
            """-> (store, endpoint, coral_name) or (None, None, None) if unknown."""
            parts = [p for p in path.split("/") if p]
            if parts and parts[0] == "c":
                if len(parts) < 2:
                    return None, None, None
                name = parts[1]
                if name not in corals:
                    self._send(404, {"error": f"unknown coral '{name}'",
                                     "corals": sorted(corals)})
                    return None, None, None
                return corals[name], "/" + "/".join(parts[2:]), name
            if default is None:
                self._send(400, {"error": "no default coral; use /c/<name>/...",
                                 "corals": sorted(corals)})
                return None, None, None
            return corals[default], path, default

        def do_GET(self):
            u = urlparse(self._path())
            q = parse_qs(u.query)
            # /health stays unauthenticated: systemd and monitors probe it, and
            # it leaks nothing but coral names and sequence numbers.
            if u.path == "/health":
                # Unauthenticated on purpose (systemd and monitors probe it), so it
                # may say which corals exist and how far they are — never where
                # they live on disk. The path is served by the authenticated
                # /corals endpoint instead.
                meta = {n: {"max_seq": s.max_seq(),
                            **{k: v for k, v in coral_meta(s.root).items()
                               if k in ("description", "owner", "protocol")}}
                        for n, s in corals.items()}
                return self._send(200, {
                    "ok": True, "default": default,
                    "protocol": server_version(),
                    "max_seq": meta[default]["max_seq"] if default else 0,
                    "corals": meta,
                })
            if not self._auth(write=False):
                return
            try:
                if u.path == "/corals":
                    meta = {n: {"path": str(s.root), "max_seq": s.max_seq(),
                                **{k: v for k, v in coral_meta(s.root).items()
                                   if k in ("description", "owner", "protocol")}}
                            for n, s in corals.items()}
                    return self._send(200, {
                        "ok": True,
                        "default": default,
                        "protocol": server_version(),
                        # top-level max_seq keeps single-coral clients working
                        "max_seq": meta[default]["max_seq"] if default else 0,
                        "corals": meta,
                    })
                store, ep, name = self._resolve(u.path)
                if store is None:
                    return
                if ep == "/health":
                    return self._send(200, {"ok": True, "coral": name,
                                            "max_seq": store.max_seq()})
                if ep == "/since":
                    cursor = int(q.get("cursor", ["0"])[0] or 0)
                    topics = [t for t in (q.get("topics", [""])[0] or "").split(",") if t]
                    return self._send(200, {"nodes": store.since(cursor, topics)})
                if ep == "/tip":
                    return self._send(200, {"tips": store.tip(q.get("topic", [""])[0])})
                if ep == "/verify":
                    return self._send(200, store.verify_refs(q.get("node", [None])[0]))
                if ep == "/maintenance/scan":
                    return self._send(200, store.scan())
                if ep == "/claims":
                    return self._send(200, {"claims": store.claims(
                        live_only=(q.get("all", ["0"])[0] != "1"))})
                if ep == "/tasks":
                    return self._send(200, {"tasks": store.tasks(
                        include_closed=(q.get("all", ["0"])[0] == "1"))})
                if ep == "/reviews":
                    r = store.reviews(q.get("claim", [""])[0])
                    return self._send(404 if "error" in r else 200, r)
                if ep == "/find":
                    hits, outside = store.find(q.get("q", [""])[0],
                                               root=(q.get("root", [""])[0] or ""),
                                               order=(q.get("sort", ["recent"])[0] or "recent"))
                    return self._send(200, {"hits": hits, "outside": outside})
                return self._send(404, {"error": "not found"})
            except ValueError as e:  # ?cursor=abc is the caller's fault, not a crash
                return self._send(400, {"error": str(e)})
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

        def do_POST(self):
            if not self._auth(write=True):
                return
            u = urlparse(self._path())
            try:
                ln = int(self.headers.get("Content-Length") or 0)
                payload = json.loads(self.rfile.read(ln) or b"{}")
                store, ep, name = self._resolve(u.path)
                if store is None:
                    return
                if ep == "/nodes":
                    r = store.add_node(payload)
                    if r.get("conflict") == "similar":
                        # 409, not 201: the write did not happen and the caller
                        # has to decide what this node is *in relation to*.
                        return self._send(409, r)
                    return self._send(201 if r["created"] else 200, r)
                if ep == "/claims":
                    return self._send(200, store.claim(
                        payload.get("question", ""), payload.get("author", "unknown"),
                        int(payload.get("ttl", 30))))
                if ep.startswith("/nodes/") and ep.endswith("/replies"):
                    nid = ep[len("/nodes/"):-len("/replies")]
                    r = store.reply(nid, payload)
                    return self._send(201 if r.get("created") else 200, r)
                if ep == "/maintenance/merge":
                    return self._send(201, store.merge(payload.get("keep", ""),
                                                       payload.get("drop", ""),
                                                       payload.get("reason", ""),
                                                       payload.get("author", "curator")))
                if ep == "/maintenance/retire":
                    return self._send(200, store.retire(payload.get("id", ""),
                                                        payload.get("reason", ""),
                                                        payload.get("author", "curator")))
                if ep == "/purge":
                    r = store.purge(payload.get("id", ""), payload.get("reason", ""),
                                    payload.get("author", "unknown"))
                    return self._send(200, r)
                return self._send(404, {"error": "not found"})
            except ApiError as e:  # carries its own status: 400 / 404 / 409
                return self._send(e.code, {"error": str(e)})
            except ValueError as e:  # bad input is the caller's fault
                return self._send(400, {"error": str(e)})
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    return H


def server_version() -> str:
    """The protocol this process implements, from VERSION.

    `/health` also carries a per-coral `protocol` taken from coral.yaml, which is
    the version the coral was *scaffolded* under — reading that as the server's
    version made every old coral look like an old server. Both are now explicit.
    """
    vf = Path(__file__).with_name("VERSION")
    try:
        return vf.read_text().strip()
    except OSError:
        return "unknown"


def coral_meta(root: Path) -> dict:
    """coral.yaml: the identity of one coral, readable by a human in 10 seconds."""
    f = root / "coral.yaml"
    meta = {"name": root.name, "description": ""}
    if not f.exists():
        return meta
    for line in f.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip()
    return meta


CORAL_GITIGNORE = """index.db
index.db-wal
index.db-shm
claims.json
__pycache__/
"""

CORAL_README = """# {name}

{desc}

## What belongs here

One falsifiable proposition per node. If you cannot say what would make it false,
it is an opinion and does not belong.

## Query

```bash
curl 'http://127.0.0.1:8731/c/{name}/find?q=<text>'
curl 'http://127.0.0.1:8731/c/{name}/since?cursor=0'
curl 'http://127.0.0.1:8731/c/{name}/tip?topic=<topic>'
```

## Layout

- `nodes/<id>.json` — immutable; the id is a hash of the proposition
- `claims.json` — live leases (not in git)
- `index.db` — derived search index; delete it freely

Protocol: see the Corallium repository, `PROTOCOL.md`.
"""


def init_coral(root: Path, name: str, desc: str, owner: str = "") -> dict:
    if (root / "coral.yaml").exists():
        return {"ok": False, "error": f"{root} is already a coral"}
    root.mkdir(parents=True, exist_ok=True)
    (root / "nodes").mkdir(exist_ok=True)
    version = "0.1.10"
    vf = Path(__file__).with_name("VERSION")
    if vf.exists():
        version = vf.read_text().strip()
    (root / "coral.yaml").write_text(
        f"name: {name}\n"
        f"description: {desc}\n"
        f"owner: {owner}\n"
        f"created: {datetime.now(timezone.utc).astimezone().strftime('%Y-%m-%d')}\n"
        f"protocol: {version}\n"
    )
    (root / ".gitignore").write_text(CORAL_GITIGNORE)
    (root / "README.md").write_text(CORAL_README.format(name=name, desc=desc))
    return {"ok": True, "root": str(root), "name": name}


def load_registry(path: Path) -> dict[str, str]:
    """registry.yaml, deliberately parsed with a 10-line reader.

        mqsim: 仿真器、SSD、存储栈
        trading: 动量模型、组合、因子

    Paths resolve relative to ~/corals/. A JSON file is also accepted.
    """
    text = path.read_text()
    if text.lstrip().startswith("{") or text.lstrip().startswith("["):
        data = json.loads(text)
        if isinstance(data, list):
            return {d["name"]: d["path"] for d in data}
        return {k: (v if isinstance(v, str) else v.get("path")) for k, v in data.items()}
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        name, val = line.split(":", 1)
        val = val.strip()
        first = val.split()[0] if val else ""
        # a bare description (no path-looking token) means ~/corals/<name>
        path_str = first if ("/" in first or first.startswith("~")) else name.strip()
        base = Path(path_str).expanduser()
        if not base.is_absolute():
            base = path.parent / path_str
        out[name.strip()] = str(base)
    return out


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


if __name__ == "__main__":
    sys.exit(main())
