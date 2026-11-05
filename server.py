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


# ── storage ────────────────────────────────────────────────────────────────
class Store:
    """Single writer: every mutation goes through one queue and one thread."""

    def __init__(self, root: Path):
        self.root = root
        self.nodes_dir = root / "nodes"
        self.nodes_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = root / "index.db"
        self._q: Queue = Queue()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._writer, daemon=True)
        self._thread.start()
        self._ready.wait(10)
        self._rebuild()

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
                    out.sort(key=lambda n: n.get("seq", 0), reverse=True)
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

        outside = 0
        if root:
            keep = [n for n in out if root in (n.get("topics") or [])]
            outside = len(out) - len(keep)
            out = keep
        return out[:limit], outside

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
        return tips or rows

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

        return self._run(job)

    def reply(self, claim_id: str, payload: dict) -> dict:
        merged = dict(payload)
        merged.update({"type": "review", "claim": claim_id,
                       "s": payload.get("s", claim_id),
                       "p": payload.get("kind", "confirm"),
                       "o": payload.get("text", "")})
        return self.add_node(merged)


# ── http ───────────────────────────────────────────────────────────────────
def make_handler(store: Store, read_token: str, write_token: str):
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

        def do_GET(self):
            if not self._auth(write=False):
                return
            u = urlparse(self.path)
            q = parse_qs(u.query)
            try:
                if u.path == "/health":
                    return self._send(200, {"ok": True, "max_seq": store.max_seq()})
                if u.path == "/since":
                    cursor = int(q.get("cursor", ["0"])[0] or 0)
                    topics = [t for t in (q.get("topics", [""])[0] or "").split(",") if t]
                    return self._send(200, {"nodes": store.since(cursor, topics)})
                if u.path == "/tip":
                    return self._send(200, {"tips": store.tip(q.get("topic", [""])[0])})
                if u.path == "/find":
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
                if u.path == "/nodes":
                    r = store.add_node(payload)
                    return self._send(201 if r["created"] else 200, r)
                if u.path == "/claims":
                    return self._send(200, store.claim(
                        payload.get("question", ""), payload.get("author", "unknown"),
                        int(payload.get("ttl", 30))))
                if u.path.startswith("/nodes/") and u.path.endswith("/replies"):
                    nid = u.path[len("/nodes/"):-len("/replies")]
                    return self._send(201, store.reply(nid, payload))
                return self._send(404, {"error": "not found"})
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    return H


def main() -> int:
    ap = argparse.ArgumentParser(prog="coral-serve")
    ap.add_argument("--root", default=str(Path.home() / "corals" / "mqsim"))
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--bind", default="0.0.0.0")
    a = ap.parse_args()

    root = Path(a.root).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    read_token = os.environ.get("CORALLIUM_READ_TOKEN", "")
    write_token = os.environ.get("CORALLIUM_WRITE_TOKEN", "")

    store = Store(root)
    srv = ThreadingHTTPServer((a.bind, a.port), make_handler(store, read_token, write_token))
    print(f"corallium serving {root} on {a.bind}:{a.port} "
          f"(read_token={'set' if read_token else 'open'}, "
          f"write_token={'set' if write_token else 'open'})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
