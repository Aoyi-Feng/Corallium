"""Maintenance mode: repairing the shape, not adding knowledge (PROTOCOL §20).

L1 link/vote, L2 merge, L3 retire. L3 destroys the payload and leaves a
tombstone; it needs a recorded non-author accusation, because structure can
prove "nobody would miss it" and never "it is wrong".
"""

from .errors import ApiError
import hashlib
import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue

# §20.6: records are not propositions. A node whose predicate is a version, a
# `*_landed` marker, a curation or a merge record says that something happened;
# it cannot be re-checked, nobody builds on it, and every one of them takes a
# row from the fixed candidate window in `find`. Matched by name, restricted
# further by "no children" at scan time — the shape is cheap to test, the
# judgement stays with the curator who must still cast a vote.
ADMIN_P = re.compile(r"^(v\d+(\.\d+)*$|.*_(landed|shipped)$|curation[:_]|merge[:_])", re.I)

# A tombstone occupies the same slot the node occupied: same id, same seq, same
# ts. Everything that was content is gone. `status: withdrawn` is not decoration:
# `_dead_sets` keys on it, so children become `suspect` with no extra machinery.
TOMBSTONE_KEYS = ("id", "seq", "ts", "type", "status", "purged_at", "reason", "author")


class Maint:
    """Curator actions: scan, merge, retire, purge."""

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
        # A withdrawn node is settled history, whether it appears as `a` or as
        # `b`: `live` filters the left side, but `_candidates` could still return
        # a superseded node on the right, re-surfacing exactly the pair a
        # supersede had resolved.
        gone = {i for i, v in table.items() if v["derived_status"] == "withdrawn"}
        # `resolved` is the wider set: a parent of a `merges` decision is settled
        # but not withdrawn, so its old evidence and its old text keep coming
        # back as "broken" and "duplicate" every scan — the same noise v0.1.16
        # removed for `supersedes`. A decision was made; stop re-offering it.
        settled = gone | resolved
        for n in live:
            for cand in self._candidates(n.get("p", ""), n.get("o", ""), limit=12):
                if (cand["id"] == n["id"] or cand["id"] in settled
                        or cand["similarity"] < 0.62):
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
        # §20.2 puts an orphan under a branch with a `link` (s=child, p=under,
        # o=branch), which is deliberately not a parent edge. So the orphan list
        # cannot shrink; what must be visible instead is how many of those roots
        # have already been placed somewhere — otherwise every scan re-offers
        # work that was done, and the next curator redoes it.
        linked = {n.get("s") for n in alln
                  if n.get("type") == "link" and n.get("p") == "under"}
        roots_linked = [n for n in roots if n["id"] in linked]
        # Only live nodes' refs: a retracted claim's missing evidence is not a
        # maintenance item — nobody should re-check it, and re-listing it every
        # scan (as the superseded node kept doing) is exactly the noise v0.1.16
        # removed from `duplicates`.
        broken = [{"node": n["id"], "path": r.get("path")}
                  for n in alln
                  if table[n["id"]]["derived_status"] != "withdrawn"
                  and n["id"] not in settled
                  for r in (n.get("refs") or [])
                  if r.get("path") and not self._ref_path(r["path"]).is_file()]
        # Not "junk": the four tests below (no children, no votes, low
        # significance, no refs) measure *unused*, not *worthless*. The name
        # was teaching curators to delete unanswered questions and buried
        # findings — §20.8 now refuses most of those retires anyway.
        unreferenced = []
        for n in live:
            if n.get("type") == "decision":
                continue
            if (children.get(n["id"], 0) == 0 and votes.get(n["id"], 0) == 0
                    and int(n.get("significance", 3)) <= 2 and not (n.get("refs"))):
                jv = sum(1 for x in alln if x.get("type") == "review"
                         and x.get("claim") == n["id"]
                         and (x.get("kind") or x.get("p")) == "junk"
                         and x.get("author") != n.get("author"))
                unreferenced.append({"id": n["id"], "o": n.get("o", "")[:60],
                             "why": "no children, no votes, ★%s" % n.get("significance", 3),
                             "author": n.get("author"), "junk_votes": jv,
                             "retirable": bool(jv)})
        by_status: dict[str, int] = {}
        for n in alln:
            st = table[n["id"]]["derived_status"]
            by_status[st] = by_status.get(st, 0) + 1
        # §20.6: a record of an action, not a proposition. Listed only when
        # nobody has built on it — a bookkeeping node that acquired a child has
        # become part of the argument and is no longer overhead.
        admin = [{"id": n["id"], "p": n.get("p", ""), "author": n.get("author"),
                  "why": "record-shaped predicate, no children"}
                 for n in alln
                 if n.get("type") in ("claim", "anomaly", "decision", "task")
                 and table[n["id"]]["derived_status"] != "withdrawn"
                 and n["id"] not in settled
                 and children.get(n["id"], 0) == 0
                 and ADMIN_P.match((n.get("p") or "").strip())]
        # §20.7: proposals filed by agents while working. They are not work
        # items until a curator accepts one; listing them with their age is
        # what keeps "someone should look at this" from dying in a node.
        max_seq = max((n.get("seq", 0) for n in alln), default=0)
        sugg = [{"id": n["id"], "action": n.get("action"), "targets": n.get("targets") or [],
                 "o": (n.get("o") or "")[:80], "author": n.get("author"),
                 "seq": n.get("seq"), "age_seq": max_seq - n.get("seq", 0)}
                for n in alln
                if n.get("type") == "suggestion"
                and table[n["id"]]["derived_status"] != "withdrawn"
                and children.get(n["id"], 0) == 0]
        return {"duplicates": dups[:50], "orphans": [n["id"] for n in roots][:100],
                "broken_refs": broken, "unreferenced": unreferenced[:50], "admin": admin[:50],
                "suggestions": sugg[:50],
                "counts": {"nodes": len(alln), "roots": len(roots),
                           "roots_linked": len(roots_linked),
                           "links": sum(1 for n in alln if n.get("type") == "link"),
                           # stored `status: withdrawn` is nearly always 0; this
                           # is the derived count (explicit retractions included)
                           "withdrawn": by_status.get("withdrawn", 0),
                           "by_status": by_status,
                           "admin": len(admin),
                           # the share that says whether growth is buying
                           # knowledge or bookkeeping (§20.6 budget: 0.05)
                           "admin_ratio": round(len(admin) / max(len(alln), 1), 3),
                           "suggestions": len(sugg),
                           "suggestions_stale": sum(1 for s in sugg if s["age_seq"] > 20),
                           "duplicate_pairs": len(dups),
                           "unreferenced": len(unreferenced)}}



    LEASE = "maintenance_lease"

    def _now(self) -> float:
        import time
        return time.time()

    def lease_acquire(self, author: str, ttl: int = 600) -> dict:
        """§20.10: maintenance runs one curator at a time.

        Suggestions may be written in parallel — they are append-only opinions.
        Executing them is not: merging A while someone retires A's twin produces
        a shape nobody chose, and the two records each look correct on their own.
        The lease is the unit of that decision, and it expires, so a crashed
        curator does not lock the coral forever.
        """
        author = (author or "").strip()
        if not author:
            raise ValueError("lease needs an author")
        ttl = max(30, min(int(ttl or 600), 3600))

        def job(c):
            now = self._now()
            row = c.execute("SELECT v FROM meta WHERE k=?", (self.LEASE,)).fetchone()
            cur = json.loads(row[0]) if row else None
            if cur and cur["until"] > now and cur["author"] != author:
                raise ApiError(409, "maintenance is held by %s for another %d s; "
                                    "propose a suggestion instead of acting (§20.7)"
                               % (cur["author"], int(cur["until"] - now)))
            rec = {"author": author, "since": now, "until": now + ttl, "ttl": ttl}
            c.execute("INSERT OR REPLACE INTO meta(k, v) VALUES(?,?)",
                      (self.LEASE, json.dumps(rec)))
            return rec

        return self._run(job)

    def lease_release(self, author: str) -> dict:
        def job(c):
            row = c.execute("SELECT v FROM meta WHERE k=?", (self.LEASE,)).fetchone()
            if not row:
                return {"released": False, "holder": None}
            cur = json.loads(row[0])
            if cur["author"] != author:
                raise ApiError(409, "the lease is held by %s" % cur["author"])
            c.execute("DELETE FROM meta WHERE k=?", (self.LEASE,))
            return {"released": True, "holder": author}

        return self._run(job)

    def lease_state(self) -> dict:
        row = self._conn().execute("SELECT v FROM meta WHERE k=?", (self.LEASE,)).fetchone()
        cur = json.loads(row[0]) if row else None
        if not cur:
            return {"held": False}
        return {"held": cur["until"] > self._now(), "author": cur["author"],
                "seconds_left": max(0, int(cur["until"] - self._now()))}

    def _need_lease(self, c, author: str) -> dict:
        row = c.execute("SELECT v FROM meta WHERE k=?", (self.LEASE,)).fetchone()
        cur = json.loads(row[0]) if row else None
        now = self._now()
        if not cur or cur["until"] <= now:
            raise ApiError(409, "no maintenance lease: POST /maintenance/lease {author} "
                                "first. Maintenance is serial by design (§20.10) — "
                                "parallel merges produce a shape nobody chose")
        if cur["author"] != (author or "").strip():
            raise ApiError(409, "maintenance lease is held by %s (%d s left); "
                                "another curator is acting" % (cur["author"],
                                                               int(cur["until"] - now)))
        return cur

    def merge(self, keep: str, drop: str, reason: str, author: str,
              suggestion: str = "") -> dict:
        """L2: one node that stands for both. Nothing is destroyed — the merge
        node's `parents` are exactly what makes tip stop showing either."""
        self._need_lease(self._conn(), author)
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
                           "parents": ([keep, drop] + ([suggestion] if suggestion else [])),
                           "topics": a.get("topics") or [],
                           "significance": max(int(a.get("significance", 3)),
                                               int(b.get("significance", 3))),
                           "note": reason, "force": True,
                           "why_new": "merge of %s and %s" % (keep, drop)})
        return {"merged": r["node"]["id"], "keep": keep, "drop": drop,
                "created": r["created"]}


    def retire(self, node_id: str, reason: str, author: str,
               suggestion: str = "") -> dict:
        """L3: destroy content. Four machine-checkable conditions, because the
        cost of being wrong is asymmetric: leftover junk is noise, deleting a
        node someone builds on is an irreversible loss."""
        self._need_lease(self._conn(), author)
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
        mode = reason.strip().lower()
        twins = [x["id"] for x in alln
                 if x["id"] != node_id and x.get("type") != "tombstone"
                 and (x.get("s"), x.get("p"), x.get("o"))
                 == (n.get("s"), n.get("p"), n.get("o"))]
        if mode == "duplicate":
            # The copy is the proof. §20.8 asks one question — does the
            # proposition survive? — and an identical (s,p,o) answers it. A
            # tombstone keeps the id, so a child that pointed here does not
            # dangle, and the chain's newest copy still carries the text. The
            # other gates protect knowledge, and there is no knowledge here
            # that exists only once.
            if not twins:
                fails.append("reason=duplicate needs an identical s/p/o copy; none exists, "
                             "so this is the only copy of the proposition")
        else:
            if children:
                fails.append(f"has {len(children)} child node(s): {children[:3]}")
            if votes:
                fails.append(f"has {len(votes)} vote(s): {votes[:3]}")
            if int(n.get("significance", 3)) > 2 and mode != "admin":
                fails.append(f"significance is {n.get('significance')} (retire allows <=2)")
            if n.get("refs") and mode != "admin":
                fails.append("has file references: it is evidence-backed, not junk")
            junk_votes = [x["id"] for x in alln if x.get("type") == "review"
                          and x.get("claim") == node_id
                          and (x.get("kind") or x.get("p")) == "junk"
                          and x.get("author") != n.get("author")]
            if not junk_votes:
                fails.append("no `junk` vote: nothing has accused this node of being a mistake")
            if mode != "admin":
                near = [c for c in self._candidates(n.get("p", ""), n.get("o", ""), limit=8)
                        if c["id"] != node_id and c["similarity"] >= 0.90]
                if not twins and not near:
                    fails.append("this is the only copy of the proposition: retire would lose "
                                 "knowledge — use merge or supersedes, or reason=admin if it is a record")
        if fails:
            raise ApiError(409, "refusing to retire: " + "; ".join(fails))
        # Applying a proposal closes it: the record's parents are the applied
        # suggestion, so it leaves `tip` and stops appearing in `scan.suggestions`
        # without a status field to flip.
        if suggestion:
            if suggestion not in by:
                raise ApiError(404, f"unknown suggestion {suggestion}")
            sug = by[suggestion]
            if sug.get("type") != "suggestion":
                raise ApiError(400, f"{suggestion} is a {sug.get('type')}, not a suggestion")
            once = self.add_node({"s": sug.get("s", ""), "p": "applied:" + str(sug.get("action")),
                                  "o": f"applied {sug.get('action')} to {node_id}",
                                  "author": author, "type": "decision", "rel": "extends",
                                  "parents": [suggestion], "force": True,
                                  "why_new": "closure of suggestion " + suggestion})
            del once
        return self.purge(node_id, reason, author)


    def repin(self, old: str, new: str, author: str, reason: str = "",
              nodes: list | None = None) -> dict:
        """§20.9: move a reference in place, without copying the node.

        Nodes are immutable except for refs: a path is a pointer, not a
        proposition. Before this, the only way to fix a moved file was to
        publish the same claim again with `supersedes` — which is why one
        proposition ended up as 118→276→315 and why 72 nodes became `replaced`
        after a single rename. This rewrites the ref (path + sha256 + size) in
        the same file, same id, same seq, and leaves one decision as the record.
        """
        old = (old or "").strip()
        new = (new or "").strip()
        if not old or not new:
            raise ValueError("repin needs both `from` and `to`")
        if old == new:
            raise ValueError("from and to are the same path")
        self._need_lease(self._conn(), author)
        want = set(nodes or [])

        def job(c):
            rows = c.execute("SELECT id, body FROM nodes").fetchall()
            touched, skipped = [], 0
            for nid, body in rows:
                if want and nid not in want:
                    continue
                n = json.loads(body)
                if n.get("type") == "tombstone":
                    continue
                changed = False
                for r in (n.get("refs") or []):
                    pth = r.get("path") or ""
                    if not pth:
                        continue
                    if pth == old or pth.startswith(old.rstrip("/") + "/"):
                        r["path"] = new + pth[len(old):]
                        f = self._ref_path(r["path"])
                        if f.is_file():
                            r["sha256"] = hashlib.sha256(f.read_bytes()).hexdigest()
                            r["size"] = f.stat().st_size
                            r["missing"] = False
                        else:
                            r["missing"] = True
                            r.pop("sha256", None)
                        changed = True
                if not changed:
                    continue
                path = self.nodes_dir / f"{nid}.json"
                tmp = path.with_name(path.name + ".repin")
                fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
                with os.fdopen(fd, "w") as fh:
                    json.dump(n, fh, ensure_ascii=False, indent=2)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, path)
                c.execute("UPDATE nodes SET body=? WHERE id=?",
                          (json.dumps(n, ensure_ascii=False), nid))
                touched.append(nid)
            return touched

        # The record is written *after* the job, not inside it: add_node enqueues
        # its own write, and a job that waits on the queue it is running on
        # deadlocks the single writer until the timeout (measured, §20.4).
        touched = self._run(job)
        if not touched:
            return {"from": old, "to": new, "nodes": [], "created": False}
        rec = self.add_node({
            "s": old, "p": "maintenance:repin",
            "o": "%s -> %s (%d node(s))" % (old, new, len(touched)),
            "author": author, "type": "decision", "topics": ["maintenance"],
            "significance": 2, "force": True,
            "why_new": "one record for a batch ref repair, instead of %d supersede copies" % len(touched),
            "note": (reason or "") + " | nodes: " + ", ".join(touched[:40]),
        })
        return {"from": old, "to": new, "nodes": touched, "record": rec["node"]["id"]}


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


