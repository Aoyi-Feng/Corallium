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

    def find(self, q: str, limit: int = 5, root: str = "") -> tuple[list[dict], int]:
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
        return out[:limit], outside

    def derive_status(self, node: dict, all_nodes: list[dict]) -> str:
        """Status is computed, never written back: nodes stay immutable and the
        answer to "what do we believe now" tracks the graph, not a stale field."""
        if node.get("status") == "withdrawn":
            return "withdrawn"
        votes = [n for n in all_nodes if n.get("type") == "review"
                 and n.get("claim") == node["id"]
                 and n.get("author") != node.get("author")]
        if any((v.get("kind") or v.get("p")) == "refuted" for v in votes):
            return "contested"
        rivals = [n for n in all_nodes
                  if n.get("type") == "claim" and n.get("s") == node.get("s")
                  and n.get("p") == node.get("p") and n.get("o") != node.get("o")
                  and n.get("status") != "withdrawn"]
        if rivals:
            return "contested"  # two live answers to the same question
        strong = [v for v in votes if (v.get("kind") or v.get("p")) == "adversarial"
                  or ((v.get("kind") or v.get("p")) == "replicated" and v.get("prior"))]
        if strong:
            return "confirmed"
        return "open"

    def annotate(self, nodes: list[dict]) -> list[dict]:
        c = self._conn()
        alln = [json.loads(b) for (b,) in c.execute("SELECT body FROM nodes").fetchall()]
        c.close()
        scores = self._scores(alln)
        out = []
        for n in nodes:
            m = dict(n)
            m["derived_status"] = self.derive_status(n, alln)
            if n.get("type") == "claim":
                sc = scores.get(n["id"], {"score": 0, "votes": 0})
                m["score"] = sc["score"]
                m["votes"] = sc["votes"]
            out.append(m)
        return out

    def tip(self, topic: str) -> list[dict]:
        c = self._conn()
        like = f"%{topic.lower()}%"
        cur = c.execute(
            "SELECT body FROM nodes WHERE lower(coalesce(body,'')) LIKE ? ORDER BY seq DESC",
            (like,),
        )
        rows = [json.loads(b) for (b,) in cur.fetchall()]
        c.close()
        if not rows:
            return []
        ids = {n["id"] for n in rows}
        parented = {p for n in rows for p in (n.get("parents") or []) if p in ids}
        tips = [n for n in rows if n["id"] not in parented and n.get("status") != "withdrawn"]
        return self.annotate(tips or rows)

    @staticmethod
    def _gate(v: dict, target_author: str | None):
        """Base weight from kind, then the two gates. Returns (weight, caps, kind)."""
        kind = v.get("kind") or v.get("p", "agree")
        w = WEIGHTS.get(kind, 0)
        caps = []
        if kind in ("replicated", "adversarial", "refuted"):
            # `prior` proves a replicator had not seen the conclusion first.
            # A refutation needs no warning shot: the counter-case is the point.
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
        a vote's own weight may be damped by what others said about it, while
        votes *on those meta-votes* are recorded and ignored for scoring.
        Without a floor the graph has no fixed point (A rates B, B rates A).
        """
        reviews = [n for n in alln if n.get("type") == "review"]
        by_target: dict[str, list[dict]] = {}
        for r in reviews:
            by_target.setdefault(r.get("claim"), []).append(r)

        authors = {n["id"]: n.get("author") for n in alln}
        out: dict[str, dict] = {}
        for n in alln:  # reviews are scored too, so votes on a vote are listable
            votes = []
            total = 0.0
            for v in by_target.get(n["id"], []):
                w, caps, kind = self._gate(v, n.get("author"))
                meta = [m for m in by_target.get(v["id"], [])
                        if m.get("author") != v.get("author")]
                meta_score = 0.0
                for m in meta:
                    mw, mcaps, mkind = self._gate(m, v.get("author"))
                    meta_score += mw
                # -4 → ×0.5, -8 → ×0.33, floored at ×0.25 so a heavily
                # contested piece of evidence still leaves a trace.
                mult = 1.0 / (1.0 + max(0.0, -meta_score) / 4.0)
                mult = max(mult, 0.25)
                eff = w * mult
                counted = v.get("author") != n.get("author")
                votes.append({
                    "id": v["id"], "seq": v.get("seq"), "ts": v.get("ts"),
                    "author": v.get("author"), "kind": kind, "text": v.get("o", ""),
                    "prior": v.get("prior"), "artifact": v.get("artifact"),
                    "weight": w, "capped": caps,
                    "meta_score": meta_score, "meta_votes": len(meta),
                    "multiplier": round(mult, 2),
                    "effective": round(eff, 2) if counted else 0,
                    "counted": counted,
                })
                if counted:
                    total += eff
            if votes:
                out[n["id"]] = {"score": round(total, 2), "votes": len(votes),
                                "detail": votes}
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
        return {"node": target, "score": sc["score"], "tally": tally,
                "votes": sc["detail"]}

    def max_seq(self) -> int:
        c = self._conn()
        v = c.execute("SELECT coalesce(max(seq),0) FROM nodes").fetchone()[0]
        c.close()
        return int(v)

    # -- writes (queued) ---------------------------------------------------
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
                "note": payload.get("note", ""),
            }
            if payload.get("method"):
                node["method"] = payload["method"]
            # review nodes must carry what they are evidence *for*, and the
            # prior node that proves the reviewer had not seen the conclusion.
            for k in ("claim", "prior", "kind", "artifact"):
                if payload.get(k):
                    node[k] = payload[k]

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
                meta = {n: {"path": str(s.root), "max_seq": s.max_seq()}
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
                    meta = {n: {"path": str(s.root), "max_seq": s.max_seq()}
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
                if ep == "/reviews":
                    r = store.reviews(q.get("claim", [""])[0])
                    return self._send(404 if "error" in r else 200, r)
                if ep == "/find":
                    hits, outside = store.find(q.get("q", [""])[0],
                                               root=(q.get("root", [""])[0] or ""))
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
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    return H


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
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--bind", default="0.0.0.0")
    a = ap.parse_args()

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
