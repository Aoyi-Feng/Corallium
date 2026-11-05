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
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
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
WEIGHTS = {"adversarial": 4, "replicated": 3, "sourced": 2, "derived": 1,
           "agree": 0, "refuted": -3}


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
                "INSERT OR IGNORE INTO nodes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
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

    def _save_claims(self) -> None:
        c = self._conn()
        rows = c.execute(
            "SELECT task_key, question, author, lease_until, status, ts FROM claims"
        ).fetchall()
        c.close()
        items = [dict(zip(("task_key", "question", "author", "lease_until", "status", "ts"), r))
                 for r in rows]
        tmp = self.claims_path.with_suffix(".json.tmp")
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
                try:
                    c.rollback()
                except Exception:
                    pass
            finally:
                box["done"].set()
                self._q.task_done()

    def _run(self, fn):
        box = {"done": threading.Event(), "result": None, "error": None}
        self._q.put((fn, box))
        if not box["done"].wait(30):
            raise TimeoutError("writer queue timeout")
        if box["error"]:
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
        if len(q) >= 3:  # trigram needs 3+ chars
            try:
                # Order by FTS relevance (bm25), NOT by seq: ordering by seq forces
                # a JOIN over every match (246 ms at 500k rows) while rank is free.
                # No ORDER BY rank: bm25 scoring costs 36-49 ms per query because
                # it scores every match. Plain LIMIT is 0.0-0.3 ms; we sort the
                # handful of rows in Python instead.
                ids = [r[0] for r in c.execute(
                    "SELECT nid FROM nodes_fts WHERE nodes_fts MATCH ? LIMIT ?",
                    ('"' + q.replace('"', '""') + '"', max(limit * 4, 20)),
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
                    # claims before review nodes: a vote mentioning "571" must
                    # not displace the conclusion itself in a 5-row answer.
                    out.sort(key=lambda n: (n.get("type") == "review", -n.get("seq", 0)))
            except sqlite3.OperationalError:
                out = []
        # LIKE is a full scan (161-181 ms at 500k). Only use it when FTS cannot
        # answer: no trigram support, or a query shorter than 3 chars. Falling
        # back on "no hits" would make every miss the slowest possible query.
        if not out and (not getattr(self, "fts_ok", False) or len(q) < 3):
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
        outside = 0
        if root:
            keep = [n for n in out if root in (n.get("topics") or [])]
            outside = len(out) - len(keep)
            out = keep
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
                      replaced: set | None = None) -> str:
        """Status is computed, never written back.

        Precedence matters: withdrawn > suspect > contested > confirmed > open.
        `suspect` is not `withdrawn` — a conclusion can still be true after
        losing its support, so it is flagged, never auto-retracted.
        """
        if dead is None or replaced is None:
            dead, replaced = self._dead_sets(all_nodes)
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
                  and n["id"] not in dead and n["id"] not in replaced]
        if rivals:
            return "contested"  # two live answers to the same question

        strong = [v for v in votes
                  if (v.get("kind") or v.get("p")) == "adversarial"
                  or ((v.get("kind") or v.get("p")) == "replicated" and v.get("prior"))]
        if strong:
            # multiple-comparisons gate: tried 6+ configurations without anyone
            # trying to kill it? Then it is the winner of a search, not a fact.
            attempts = int(node.get("attempts", 1) or 1)
            if attempts > 5 and "adversarial" not in kinds:
                return "open"
            return "confirmed"
        return "open"

    @staticmethod
    def _gate(v: dict, target_author: str | None):
        """Base weight from kind, then the two gates. Returns (weight, caps, kind)."""
        kind = v.get("kind") or v.get("p", "agree")
        w = WEIGHTS.get(kind, 0)
        caps = []
        if kind in ("replicated", "adversarial", "refuted"):
            if kind != "refuted" and not v.get("prior"):
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
        c = self._conn()
        alln = [json.loads(b) for (b,) in c.execute("SELECT body FROM nodes").fetchall()]
        c.close()
        scores = self._scores(alln)
        by_id = {n["id"]: n for n in alln}
        dead, replaced = self._dead_sets(alln)
        deps: dict[str, int] = {}
        for n in alln:
            for p_ in (n.get("parents") or []):
                deps[p_] = deps.get(p_, 0) + 1
        out = []
        for n in nodes:
            m = dict(n)
            m["derived_status"] = self.derive_status(n, alln, by_id, dead, replaced)
            if n.get("refs"):
                m["refs"] = len(n["refs"])
                m["refs_missing"] = sum(1 for r in n["refs"] if r.get("missing"))
            rs = n.get("refs") or []
            if rs:
                m["refs"] = len(rs)
            if n.get("type") == "claim":
                sc = scores.get(n["id"], {"score": 0, "votes": 0})
                m["score"] = sc["score"]
                m["votes"] = sc["votes"]
                m["significance"] = n.get("significance", 3)
                m["dependents"] = deps.get(n["id"], 0)
            out.append(m)
        return out

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
            scope = [n for n in alln
                     if low in (n.get("topics") or [])
                     or low in json.dumps(n, ensure_ascii=False).lower()]
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
        c.close()
        out = []
        for (b,) in rows:
            n = json.loads(b)
            for i, r in enumerate(n.get("refs") or []):
                if r.get("node"):
                    out.append({"node": n["id"], "i": i, "ref": r, "state": "node"})
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

    def add_node(self, payload: dict) -> dict:
        def job(c):
            s, p, o = payload.get("s"), payload.get("p"), payload.get("o")
            if not (s and p and o):
                raise ValueError("s, p, o are required")
            rel = payload.get("rel", "root")
            parents = payload.get("parents") or []
            blob = json.dumps([s, p, o, rel, sorted(parents)], ensure_ascii=False)
            nid = hashlib.sha256(blob.encode()).hexdigest()[:12]

            hit = c.execute("SELECT body FROM nodes WHERE id=?", (nid,)).fetchone()
            if hit:  # idempotent: concurrent identical publishes collapse
                return {"created": False, "node": json.loads(hit[0])}

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
            return {"created": True, "node": node}

        return self._run(job)

    def claim(self, question: str, author: str, ttl_min: int) -> dict:
        def job(c):
            now = time.time()
            key = hashlib.sha256(question.strip().lower().encode()).hexdigest()[:12]
            row = c.execute(
                "SELECT author, lease_until FROM claims WHERE task_key=?", (key,)
            ).fetchone()
            if row and row[1] and row[1] > now and row[0] != author:
                return {"acquired": False, "holder": row[0], "lease_until": row[1]}
            c.execute(
                "INSERT INTO claims VALUES (?,?,?,?,?,?) ON CONFLICT(task_key) DO UPDATE SET "
                "author=excluded.author, lease_until=excluded.lease_until, status='claimed'",
                (key, question, author, now + ttl_min * 60, "claimed",
                 datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")),
            )
            return {"acquired": True, "task_key": key, "lease_until": now + ttl_min * 60}

        r = self._run(job)
        if r.get("acquired"):
            self._save_claims()  # lease must survive an index wipe
        return r

    def reply(self, claim_id: str, payload: dict) -> dict:
        merged = dict(payload)
        merged.update({"type": "review", "claim": claim_id,
                       "s": payload.get("s", claim_id),
                       "p": payload.get("kind", "confirm"),
                       "o": payload.get("text", "")})
        return self.add_node(merged)


# ── http ───────────────────────────────────────────────────────────────────
def make_handler(corals: dict, read_token: str, write_token: str, default: str | None):
    """One process, many corals. `/c/<name>/<endpoint>` addresses them explicitly;
    bare `/<endpoint>` goes to the default coral so a single-coral install needs
    no prefix."""
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):  # quiet
            pass

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
            if not want:  # no token configured → local trust
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
            u = urlparse(self.path)
            q = parse_qs(u.query)
            # /health stays unauthenticated: systemd and monitors probe it, and
            # it leaks nothing but coral names and sequence numbers.
            if u.path == "/health":
                meta = {n: {"path": str(s.root), "max_seq": s.max_seq(),
                            **{k: v for k, v in coral_meta(s.root).items()
                               if k in ("description", "owner", "protocol")}}
                        for n, s in corals.items()}
                return self._send(200, {
                    "ok": True, "default": default,
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
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

        def do_POST(self):
            if not self._auth(write=True):
                return
            u = urlparse(self.path)
            try:
                ln = int(self.headers.get("Content-Length") or 0)
                payload = json.loads(self.rfile.read(ln) or b"{}")
                store, ep, name = self._resolve(u.path)
                if store is None:
                    return
                if ep == "/nodes":
                    r = store.add_node(payload)
                    return self._send(201 if r["created"] else 200, r)
                if ep == "/claims":
                    return self._send(200, store.claim(
                        payload.get("question", ""), payload.get("author", "unknown"),
                        int(payload.get("ttl", 30))))
                if ep.startswith("/nodes/") and ep.endswith("/replies"):
                    nid = ep[len("/nodes/"):-len("/replies")]
                    return self._send(201, store.reply(nid, payload))
                return self._send(404, {"error": "not found"})
            except ValueError as e:  # bad input is the caller's fault
                return self._send(400, {"error": str(e)})
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    return H


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

    srv = ThreadingHTTPServer((a.bind, a.port),
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
