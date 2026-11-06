"""Storage: one immutable file per node, SQLite as a derived index (PROTOCOL §5).

`Store` is the only object the rest of the server sees. It is assembled from
four mixins so no single file holds the whole server:

    Storage  this module      connection, index, leases, reader queue, refs
    Derive   derive.py        status, score, tip, tasks, votes
    Write    write.py         the write gate, publish, claims, replies
    Maintenance  maintenance.py  scan / merge / retire / purge (PROTOCOL §20)

Every mutation runs on one writer thread through one queue: that is the
concurrency contract in §13.10, not an implementation detail.
"""

from .errors import ApiError
import hashlib
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue

from .derive import Derive
from .maintenance import Maint
from .write import Write


def _tri(text: str) -> set:
    t = " ".join((text or "").lower().split())
    return {t[i:i + 3] for i in range(max(0, len(t) - 2))} or ({t} if t else set())


def _jac(a: str, b: str) -> float:
    A, B = _tri(a), _tri(b)
    if not A or not B:
        return 0.0
    return len(A & B) / len(A | B)


def conflicts(hits: list[dict], thr: float = 0.62) -> list[dict]:
    """Pairs in a result set that talk about the same thing but not the same way.

    The write gate stops a near-duplicate at 0.62; it cannot stop two nodes that
    are near-duplicates of each other and disagree, because each was written
    alone. `12 ms` and `120 ms` are 0.95 similar and cannot both be true, so the
    reader is the only place this can be caught — and if it is not shown, the
    second hit simply looks like corroboration.
    """
    out = []
    for i in range(len(hits)):
        for j in range(i + 1, len(hits)):
            a, b = hits[i], hits[j]
            if a.get("type") in ("link", "review", "suggestion"):
                continue
            if b.get("type") in ("link", "review", "suggestion"):
                continue
            sa = f"{a.get('s','')} {a.get('p','')} {a.get('o','')}"
            sb = f"{b.get('s','')} {b.get('p','')} {b.get('o','')}"
            sim = _jac(sa, sb)
            if sim < thr:
                continue
            same_o = " ".join(str(a.get("o", "")).lower().split()) == \
                     " ".join(str(b.get("o", "")).lower().split())
            if same_o:
                continue  # restatement, not a disagreement
            out.append({"a": a.get("id"), "b": b.get("id"), "similarity": round(sim, 3),
                        "a_says": str(a.get("o", ""))[:120], "b_says": str(b.get("o", ""))[:120]})
    return out


class Storage:
    """Single writer: every mutation goes through one queue and one thread."""

    def __init__(self, root: Path):
        self.root = root
        # §20.9: refs are stored relative to the repo when they live inside it,
        # so a coral survives being copied to another machine. `repo:` in
        # coral.yaml names that root; without it paths stay absolute and this
        # is exactly what v0.1.25 did.
        self.repo = None
        meta = root / "coral.yaml"
        if meta.exists():
            for line in meta.read_text().splitlines():
                if line.strip().startswith("repo:"):
                    self.repo = Path(line.split(":", 1)[1].strip()).expanduser()
                    break
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


    def _ref_path(self, p: str) -> Path:
        """Resolve a stored ref path. Relative means "inside the repo"; `~/`
        means "inside home"; absolute means what it says."""
        f = Path(p).expanduser()
        if f.is_absolute():
            return f
        return (self.repo or self.root) / f


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


    def _all(self) -> list[dict]:
        c = self._conn()
        out = [json.loads(b) for (b,) in c.execute("SELECT body FROM nodes").fetchall()]
        c.close()
        out.sort(key=lambda n: n.get("seq", 0))
        return out


    def _seq_gen(self) -> int:
        c = self._conn()
        row = c.execute("SELECT coalesce(max(seq),0) FROM nodes").fetchone()
        c.close()
        return int(row[0] if row else 0)


    def max_seq(self) -> int:
        c = self._conn()
        v = c.execute("SELECT coalesce(max(seq),0) FROM nodes").fetchone()[0]
        c.close()
        return int(v)


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
            raw = r["path"]
            f = Path(raw).expanduser()
            if not f.is_absolute():
                f = (self.repo or self.root) / f
            # Store the shortest stable form: repo-relative inside the repo,
            # home-relative inside home, absolute otherwise. The hash is of the
            # bytes, not of the string, so re-pinning does not change identity.
            if self.repo:
                try:
                    r["path"] = str(f.relative_to(self.repo))
                except ValueError:
                    pass
            if r["path"].startswith("/home/") or r["path"].startswith("/Users/"):
                home = Path.home()
                try:
                    r["path"] = "~/" + str(f.relative_to(home))
                except ValueError:
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
                f = self._ref_path(r["path"])
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



class Store(Storage, Derive, Write, Maint):
    """The composed store: storage + derivation + writes + maintenance."""
