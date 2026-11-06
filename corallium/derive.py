"""Derivation: status, scores, tip, tasks, votes (PROTOCOL §4.4, §5).

Nothing here is stored. `status` is computed on every read from the immutable
nodes and their review nodes, so "what did we believe at seq 100" stays
answerable; writing the number back would destroy that history.
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

# PROTOCOL §4.2. `agree` is 0 on purpose: LLMs agree by default, so counting
# agreement turns confidence into a prose-quality contest.
REQUIRED_ARTIFACT = {"refuted", "redundant"}
WEIGHTS = {"adversarial": 4, "replicated": 3, "sourced": 2, "derived": 1,
           "agree": 0, "refuted": -3, "redundant": -2, "junk": 0}


class Derive:
    """Read-side semantics: derived status, weighted score, leaves, obligations."""

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
            if r.get("path") and not self._ref_path(r["path"]).is_file():
                return "suspect"
            if r.get("node") and not r.get("coral") and r["node"] not in by_id:
                return "suspect"  # a node ref to an id that is not here cannot be opened
        if any(r.get("missing") for r in (node.get("refs") or [])):
            return "suspect"  # the evidence file is gone; the claim is uncheckable

        # retraction cascades transitively: a node whose support was withdrawn
        # is `suspect`, however deep the withdrawal sits above it.
        #
        # Except through a `supersedes` edge: that edge is a replacement, not
        # support. Without this, the record of retiring a node inherits the
        # node's death and becomes `suspect` for having done its job — and every
        # child of that record inherits it in turn.
        if node.get("rel") != "supersedes":
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
                if parent.get("rel") == "supersedes":
                    continue  # do not walk through a replacement record
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
                                        if r.get("path") and not self._ref_path(r["path"]).is_file())
            if n.get("type") not in ("review", "tombstone"):
                # Every proposition can be voted on (§4.2), not only `claim`.
                # A `question` or `anomaly` that carries a real `sourced` vote
                # used to render score=None and votes=None, so the vote existed
                # in the graph and was invisible in every read — the same silent
                # loss as a vote aimed at a typo'd id.
                sc = scores.get(n["id"], {"score": 0, "votes": 0})
                m["score"] = sc["score"]
                m["votes"] = sc["votes"]
                # §4.1: a proposition nobody has checked is not a weaker belief,
                # it is an unchecked one. Before this flag the reader had to
                # infer that from `votes: 0` buried in the payload, so an `open`
                # claim with no review read exactly like one that had survived a
                # replication. Say it in the line.
                m["unreviewed"] = sc["votes"] == 0
                m["significance"] = n.get("significance", 3)
                m["dependents"] = deps.get(n["id"], 0)
            table[n["id"]] = m
        return table


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
        # §20.2: a `link` node is a directory entry, not a belief. It has no
        # parents, so without this filter every link would itself be a leaf and
        # `tip` would answer "what do we believe now" with a list of pointers.
        # §20.7: a `suggestion` is a proposal about the shape, not a belief:
        # it must not answer "what do we believe now" either.
        scope = [n for n in scope if n.get("type") not in ("link", "suggestion")]
        ids = {n["id"] for n in scope}
        child_of = {p for n in scope for p in (n.get("parents") or []) if p in ids}
        tips = [n for n in scope if n["id"] not in child_of]
        dead, replaced = self._dead_sets(alln)
        live = [n for n in tips if n["id"] not in dead and n["id"] not in replaced]
        return self.annotate(live or tips)


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


