"""The write path: similarity gate, publish, leases, replies (PROTOCOL §3).

The server accepts almost anything, so the regulation lives in the writer (the
agent), not in a gate: the only hard refusals here are the near-duplicate 409,
an unknown parent id, and a vote with no target.
"""

from .errors import ApiError
from .types import normalize as normalize_type
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


def _trigrams(text: str) -> set[str]:
    t = re.sub(r"\s+", " ", (text or "").strip().lower())
    return {t[i:i + 3] for i in range(max(0, len(t) - 2))} or ({t} if t else set())


def _trigram_jaccard(a: str, b: str) -> float:
    A, B = _trigrams(a), _trigrams(b)
    if not A or not B:
        return 0.0
    return len(A & B) / len(A | B)


class Write:
    """Publishing propositions and casting votes."""

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
            # An anonymous write fixed the node's id to a shared identity: claim
            # ids exclude the author, so the first `unknown` write owned that id
            # and every later author's vote on it counted as a self-vote.
            if not str(payload.get("author") or "").strip():
                raise ApiError(400, "author is required: an anonymous node cannot be "
                                    "distinguished from a self-review (PROTOCOL §4.1)")
            rel = payload.get("rel", "root")
            parents = payload.get("parents") or []
            try:
                ntype = normalize_type(payload.get("type", "claim"))
            except ValueError as e:
                raise ApiError(400, str(e))
            # §3.2 — a claim must say what would make it wrong. Without that the
            # node is unfalsifiable by construction: a later reader cannot tell
            # whether it was ever tested, and `tip` cannot separate a guess from
            # a finding. The check is mechanical on purpose (length + not a
            # paraphrase of the claim) because judging whether a falsifier is
            # *good* is not a thing a write path can honestly do.
            if ntype == "claim":
                fals = (payload.get("falsifier") or "").strip()
                if not fals:
                    raise ApiError(400, "type=claim requires falsifier: one sentence naming an "
                                        "observation that would make this false (PROTOCOL §3.2). "
                                        "If nothing could, it is not a claim.")
                if len(fals) < 15:
                    raise ApiError(400, f"falsifier too short ({len(fals)} chars): name what would be "
                                        "observed, not that it might be wrong")
                # "restates the claim" must catch the common cheat — copy the claim
                # and append "of the time" — so compare containment first, then
                # trigram overlap. Jaccard alone passes those (0.85), containment
                # does not.
                norm = lambda t: re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", t.lower())
                n_o = norm(o)
                # containment only makes sense for a claim long enough to be
                # quoted: for `o="x"` every sentence containing an x matches.
                if (len(n_o) >= 12 and n_o[:24] in norm(fals)) or _trigram_jaccard(fals, f"{s} {o}") >= 0.85:
                    raise ApiError(400, "falsifier restates the claim; it must name a different "
                                        "observation, otherwise it can never fail")
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
            # §20.9: a `supersedes` that keeps s/p/o identical adds a node and no
            # knowledge — it is how one proposition became three (118→276→315)
            # every time a file moved. Ref repairs now go through /maintenance/repin.
            if rel == "supersedes" and parents:
                for pid in parents:
                    row = c.execute("SELECT body FROM nodes WHERE id=?", (pid,)).fetchone()
                    if not row:
                        continue
                    old = json.loads(row[0])
                    if (old.get("s"), old.get("p"), old.get("o")) == (s, p, o):
                        raise ApiError(409,
                            "supersedes with an identical s/p/o adds no knowledge: %s already "
                            "says this. If a referenced file moved, repair it in place with "
                            "POST /maintenance/repin {\"from\": old, \"to\": new} — one record "
                            "for the whole batch (PROTOCOL §20.9)" % pid)
            nid = hashlib.sha256(blob.encode()).hexdigest()[:12]

            hit = c.execute("SELECT body FROM nodes WHERE id=?", (nid,)).fetchone()
            if hit:  # idempotent: concurrent identical publishes collapse
                return {"created": False, "node": json.loads(hit[0])}

            # §3.4.1 — the gate that stops the graph from spreading sideways.
            # It only runs for propositions: a review points at its claim by
            # construction, and a task is an obligation, not a finding.
            #
            # v0.1.18: a parent edge is NOT a pass. The old rule ("parents ⇒ 201")
            # was exploited as a free channel — one author wrote the same claim
            # twice with rel=extends, similarity 1.00, and nothing objected.
            # Parented propositions are still gated, but at 0.90: `extends`
            # legitimately restates part of its father, while a near-verbatim
            # restatement adds no information at either threshold.
            gateable = ntype in ("claim", "anomaly", "question", "decision")
            if gateable:
                thr = 0.90 if parents else 0.62
                cand = self._candidates(p, o)
                for x in cand:
                    x["similar_to_parent"] = x["id"] in parents
                top = cand[0] if cand else None
                if top and top["similarity"] >= thr:
                    if not payload.get("force"):
                        hint = ("this restates an existing node almost word for word; "
                                "a parent edge does not exempt it — pass force=true with "
                                "why_new saying what it adds, or rel=supersedes"
                                if parents else
                                "link it (rel=extends|supersedes, parents=[id]) "
                                "or pass force=true with why_new")
                        return {"conflict": "similar", "candidates": cand,
                                "threshold": thr, "hint": hint}
                    if not (payload.get("why_new") or "").strip():
                        raise ValueError("force=true needs a non-empty why_new: "
                                         "say how this differs from " + top["id"])

            seq = int(c.execute("SELECT coalesce(max(seq),0) FROM nodes").fetchone()[0]) + 1
            node = {
                "id": nid, "seq": seq,
                "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                "author": payload.get("author", "unknown"),
                "type": ntype,
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
            # stored only where it is required: legacy nodes carry no falsifier,
            # and their absence means "written before v0.1.29", not "unfalsifiable"
            if ntype == "claim":
                node["falsifier"] = fals
            if payload.get("method"):
                node["method"] = payload["method"]
            # A vote inherits the topics of what it votes on. Without this,
            # `since?topics=protocol` returned 0 reviews while 24 votes targeted
            # protocol-tagged claims: a follower of a topic never learned that
            # its claims had been replicated or refuted.
            if ntype == "review" and not node["topics"] and payload.get("claim"):
                row = c.execute("SELECT body FROM nodes WHERE id = ?",
                                (payload["claim"],)).fetchone()
                if row:
                    node["topics"] = json.loads(row[0]).get("topics") or []
            # review nodes must carry what they are evidence *for*, and the
            # prior node that proves the reviewer had not seen the conclusion.
            for k in ("claim", "prior", "kind", "artifact"):
                if payload.get(k):
                    node[k] = payload[k]
            # §20.7: a suggestion without its action and targets is an opinion.
            # These were dropped here until v0.1.26 — the plugin sent them, the
            # store discarded them, and scan listed every proposal as `action:
            # null`. Unknown actions are refused for the same reason `parents`
            # are: a typo must fail while the writer still sees the candidate.
            if ntype == "suggestion":
                act = (payload.get("action") or "").strip()
                if act not in ("merge", "retire", "link", "split", "reindex"):
                    raise ApiError(400, "suggestion needs action one of "
                                        "merge|retire|link|split|reindex (got %r)" % act)
                tg = payload.get("targets") or []
                if not isinstance(tg, list) or not tg:
                    raise ApiError(400, "suggestion needs targets: the node ids it would touch")
                bad = [x for x in tg if not c.execute("SELECT 1 FROM nodes WHERE id=?", (x,)).fetchone()]
                if bad:
                    raise ApiError(400, "unknown target id(s): " + ", ".join(bad[:3]))
                node["action"] = act
                node["targets"] = tg
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


