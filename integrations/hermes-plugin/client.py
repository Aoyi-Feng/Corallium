"""Corallium client for the Hermes plugin — stdlib only.

Two contracts, and they differ on purpose:

* **Reads degrade.** If the server is not answering, `find`/`tip`/`state` fall back
  to scanning `~/corals/<name>/nodes/*.json`. A knowledge base that is unreachable
  is still a folder full of true statements; refusing to read it helps nobody.
* **Writes never degrade.** If the server is down, a publish fails with an error the
  model can see. Silently queueing or dropping it means the discovery exists in one
  context window only, and the next agent re-discovers it.

The second rule is why `publish` returns a hard failure instead of a helpful note.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_URL = "http://127.0.0.1:8731"
TIMEOUT = 3.0


# ── configuration ────────────────────────────────────────────────────────────


def _hermes_home() -> Path:
    return Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))


def _read_config() -> dict:
    """Parse only the block we need. The full config is large and has keys this
    plugin must not touch; a tolerant line parser avoids depending on its schema."""
    out: dict = {}
    f = _hermes_home() / "config.yaml"
    if not f.exists():
        return out
    inside = False
    for raw in f.read_text().splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        if not raw.startswith((" ", "\t")):
            inside = raw.strip() == "corallium:"
            continue
        if not inside:
            continue
        k, _, v = raw.strip().partition(":")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def settings() -> dict:
    cfg = _read_config()
    corals_dir = Path(os.environ.get("CORALLIUM_CORALS_DIR")
                      or cfg.get("corals_dir") or (Path.home() / "corals"))
    return {
        "url": (os.environ.get("CORALLIUM_URL") or cfg.get("url") or DEFAULT_URL).rstrip("/"),
        "read_token": os.environ.get("CORALLIUM_READ_TOKEN") or cfg.get("read_token") or "",
        "write_token": os.environ.get("CORALLIUM_WRITE_TOKEN") or cfg.get("write_token") or "",
        "coral": os.environ.get("CORALLIUM_CORAL") or cfg.get("coral") or "",
        "corals_dir": corals_dir,
        "author": os.environ.get("CORALLIUM_AUTHOR") or cfg.get("author") or "",
        # A declaration, not a hint: a client told to read-only must not write,
        # even if a write token happens to sit in the environment. It used to be
        # documented and ignored, which is worse than not offering it.
        "read_only": (os.environ.get("CORALLIUM_READ_ONLY", "")
                      or cfg.get("read_only", "")).strip().lower() in ("1", "true", "yes", "on"),
    }


def author() -> str:
    """Who is publishing. Not an identity claim — the protocol's independence test is
    what you had seen (`prior`), not who you say you are. This only keeps two agents
    in the same session from counting as the same voter.

    The suffix is a hash of the whole session id, not its first 8 characters: ids are
    `20261105_152403_0b147a`, so `sid[:8]` gave every session of the day the same
    author string (`hermes-20261105`). Votes between them were then counted as
    self-votes and silently dropped.
    """
    a = settings()["author"]
    if a:
        return a
    sid = os.environ.get("HERMES_SESSION_ID", "")
    if not sid:
        return "hermes"
    return "hermes-" + hashlib.sha256(sid.encode()).hexdigest()[:8]


def _corals() -> list[str]:
    d = settings()["corals_dir"]
    if not d.is_dir():
        return []
    return sorted(p.name for p in d.iterdir() if (p / "nodes").is_dir())


def pick_coral(name: str) -> str:
    """Explicit name wins; otherwise the configured default; otherwise the only coral
    that exists. With several corals and no default, refuse: guessing which knowledge
    domain a question belongs to is exactly the error the split exists to prevent."""
    if name:
        return name
    cfg = settings()["coral"]
    if cfg:
        return cfg
    names = _corals()
    if len(names) == 1:
        return names[0]
    if not names:
        raise ValueError("no corals found under " + str(settings()["corals_dir"]))
    raise ValueError("several corals exist (%s); pass coral=<name>" % ", ".join(names))


# ── http ─────────────────────────────────────────────────────────────────────


def _request(path: str, method: str = "GET", payload: dict | None = None,
             write: bool = False) -> dict:
    cfg = settings()
    if write and cfg["read_only"]:
        # Fail before the socket, with a sentence the model can quote. Sending it
        # and letting the server 401 would blame the credential for a local
        # configuration choice.
        raise PermissionError("corallium.read_only is set for this client: "
                              "refusing to write (reads are unaffected)")
    url = cfg["url"] + path
    data = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    tok = cfg["write_token"] if write else (cfg["read_token"] or cfg["write_token"])
    if tok:
        req.add_header("Authorization", "Bearer " + tok)
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read() or b"{}")


def _coral_path(coral: str, ep: str = "") -> str:
    return "/c/%s%s" % (coral, ep)


def alive() -> bool:
    try:
        _request("/health")
        return True
    except Exception:
        return False


# ── offline reads ────────────────────────────────────────────────────────────


def _scan(coral: str) -> list[dict]:
    d = settings()["corals_dir"] / coral / "nodes"
    if not d.is_dir():
        return []
    out = []
    for f in d.glob("*.json"):
        try:
            out.append(json.loads(f.read_text()))
        except Exception:
            continue
    out.sort(key=lambda n: n.get("seq", 0))
    return out


def _offline_find(coral: str, q: str, limit: int) -> list[dict]:
    ql = q.lower()
    alln = _scan(coral)
    hits = []
    for n in alln:
        blob = " ".join(str(n.get(k, "")) for k in ("s", "p", "o", "note", "method", "type"))
        blob += " " + " ".join(n.get("topics") or [])
        if ql in blob.lower():
            hits.append(n)
    hits.sort(key=lambda n: n.get("seq", 0), reverse=True)
    return _offline_annotate(hits[:limit], alln)


def _offline_tip(coral: str, topic: str) -> list[dict]:
    nodes = _scan(coral)
    parents = {p for n in nodes for p in (n.get("parents") or [])}
    gone = {n["id"] for n in nodes if n.get("status") == "withdrawn"}
    for n in nodes:
        if n.get("rel") == "supersedes":
            gone.update(n.get("parents") or [])
    tips = []
    for n in nodes:
        if n["id"] in parents or n["id"] in gone:
            continue
        # label-only, same as the server: a note that happens to mention the
        # word is not in the topic (§11.1 is about a directory, not a grep).
        if topic and topic.lower() not in [t.lower() for t in (n.get("topics") or [])]:
            continue
        tips.append(n)
    tips.sort(key=lambda n: n.get("seq", 0), reverse=True)
    return _offline_annotate(tips, nodes)


def _offline_sets(nodes: list[dict]) -> tuple[set, set]:
    dead = {n["id"] for n in nodes if n.get("status") == "withdrawn"}
    for n in nodes:
        if n.get("status") == "withdrawn" and n.get("rel") == "supersedes":
            dead.update(n.get("parents") or [])
    replaced = {p for n in nodes if n.get("rel") == "supersedes"
                for p in (n.get("parents") or [])} - dead
    return dead, replaced


def _offline_status(n: dict, alln: list[dict], dead: set, replaced: set) -> str:
    if n.get("status") == "withdrawn" or n["id"] in dead or n["id"] in replaced:
        return "withdrawn"
    for r in n.get("refs") or []:
        if r.get("missing") or (r.get("path") and not Path(r["path"]).is_file()):
            return "suspect"
    by_id = {x["id"]: x for x in alln}
    stack, seen = list(n.get("parents") or []), set()
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        if pid in dead:
            return "suspect"
        stack.extend((by_id.get(pid) or {}).get("parents") or [])
    votes = [v for v in alln if v.get("type") == "review" and v.get("claim") == n["id"]
             and v.get("author") != n.get("author")]
    kinds = [v.get("kind") or v.get("p") for v in votes]
    if "refuted" in kinds:
        return "contested"
    if any(x.get("type") == "claim" and x.get("s") == n.get("s") and x.get("p") == n.get("p")
           and x.get("o") != n.get("o") and x["id"] not in dead and x["id"] not in replaced
           for x in alln):
        return "contested"
    strong = any(k == "adversarial" or (k == "replicated" and v.get("prior"))
                 for k, v in ((v.get("kind") or v.get("p"), v) for v in votes))
    if strong or kinds.count("sourced") >= 2:
        return "open" if int(n.get("attempts") or 1) > 5 and "adversarial" not in kinds else "confirmed"
    return "open"


def _offline_annotate(nodes: list[dict], alln: list[dict]) -> list[dict]:
    """Attach what the server would have computed.

    Without this the plugin's degraded read printed `[open]` on every node —
    a coral with four confirmed claims looked like a flat store with no
    semantics, which is the single most misleading thing a fallback can do.
    This mirrors Store.derive_status/_scores; the server remains authoritative.
    """
    dead, replaced = _offline_sets(alln)
    weights = {"adversarial": 4, "replicated": 3, "sourced": 2, "derived": 1,
               "agree": 0, "refuted": -3, "redundant": -2, "junk": 0}
    scores: dict[str, float] = {}
    counted: dict[str, int] = {}
    for v in alln:
        if v.get("type") != "review":
            continue
        target = next((x for x in alln if x["id"] == v.get("claim")), None)
        if not target or v.get("author") == target.get("author"):
            continue
        kind = v.get("kind") or v.get("p")
        w = weights.get(kind, 0)
        if kind in ("replicated", "adversarial", "refuted", "redundant"):
            if kind not in ("refuted", "redundant") and not v.get("prior"):
                w = 1 if w > 0 else -1
            elif not (v.get("artifact") or v.get("method")):
                w = 1 if w > 0 else -1
        scores[v["claim"]] = scores.get(v["claim"], 0.0) + w
        counted[v["claim"]] = counted.get(v["claim"], 0) + 1
    out = []
    for n in nodes:
        m = dict(n)
        st = _offline_status(n, alln, dead, replaced)
        m["derived_status"] = st
        if st == "confirmed":
            m["confirmed_level"] = "challenged" if any(
                (v.get("kind") or v.get("p")) == "adversarial" and v.get("claim") == n["id"]
                and v.get("author") != n.get("author") for v in alln) else "unchallenged"
        m["score"] = round(scores.get(n["id"], 0.0), 2)
        m["votes"] = counted.get(n["id"], 0)
        out.append(m)
    return out


# ── formatting ───────────────────────────────────────────────────────────────


def _line(n: dict) -> str:
    stars = "★" * int(n.get("significance") or 3)
    st = n.get("derived_status") or n.get("status") or "open"
    head = "%s [%s] %-5s %s %s = %s" % (
        n.get("id", "?"), st, stars, n.get("type", "claim"),
        n.get("p", ""), n.get("o", ""))
    bits = []
    if n.get("score"):
        bits.append("%+.1f" % n["score"])
    if n.get("refs"):
        # annotated nodes report a count; raw /since nodes still carry the list
        rs = n["refs"]
        bits.append("%d refs" % (len(rs) if isinstance(rs, list) else rs))
    if n.get("note"):
        bits.append(n["note"][:90])
    if int(n.get("attempts") or 1) > 1:
        # §4.6: "the best of twenty" must not read as "the only one".
        bits.append("tried %d" % int(n["attempts"]))
    return head + ("  — " + "; ".join(bits) if bits else "")


def _setup_hint(coral: str) -> str:
    """A fresh install has neither a server nor a coral directory. Say which one is
    missing and the one command that fixes it, instead of a bare "(no hits)"."""
    d = settings()["corals_dir"] / coral
    if (d / "nodes").is_dir():
        return ""
    return ("[no coral at %s and no server at %s — a fresh install looks like this.\n"
            " Create one with: bash install.sh --self-hosted]"
            % (d, settings()["url"]))


def _sources(offline: bool, coral: str) -> str:
    hint = _setup_hint(coral) if offline else ""
    if hint:
        return hint
    return ("[offline: scanned %s/nodes]" % coral) if offline else ""


# ── operations ───────────────────────────────────────────────────────────────


def find(q: str, coral: str = "", limit: int = 5, sort: str = "recent") -> str:
    coral = pick_coral(coral)
    try:
        r = _request(_coral_path(coral, "/find") + "?" + urllib.parse.urlencode(
            {"q": q, "limit": limit, "sort": sort}))
        hits = r.get("hits") or []
        outside = r.get("outside") or 0
        body = "\n".join(_line(h) for h in hits) if hits else "(no hits)"
        if outside:
            body += "\n(%d more matched outside the scope you asked for)" % outside
        return "%s\n%s" % (coral, body)
    except Exception:
        hits = _offline_find(coral, q, limit)
        body = "\n".join(_line(h) for h in hits) if hits else "(no hits)"
        return "%s\n%s\n%s" % (coral, body, _sources(True, coral))


def tip(topic: str = "", coral: str = "") -> str:
    coral = pick_coral(coral)
    try:
        r = _request(_coral_path(coral, "/tip") + "?" + urllib.parse.urlencode({"topic": topic}))
        tips = r.get("tips") or []
        body = "\n".join(_line(t) for t in tips) if tips else "(nothing at the tip)"
        return "%s\n%s" % (coral, body)
    except Exception:
        tips = _offline_tip(coral, topic)
        body = "\n".join(_line(t) for t in tips) if tips else "(nothing at the tip)"
        return "%s\n%s\n%s" % (coral, body, _sources(True, coral))


def show(node: str, coral: str = "") -> str:
    """One node with its votes as a timeline, and why its status is what it is.

    `find` gives you a line; a line is not enough to decide whether to build on
    someone. The vote list is where the argument is: who checked it, with what
    artifact, and which votes were capped or not counted.
    """
    coral = pick_coral(coral)
    node = (node or "").strip()
    if not node:
        return "show needs a node id (12 hex chars, from coral_find)"
    try:
        r = _request(_coral_path(coral, "/reviews") + "?" + urllib.parse.urlencode({"claim": node}))
        n = r.get("node") or {}
        lines = [_line(n), "status: %s" % (r.get("derived_status") or n.get("status") or "?"),
                 "score: %s from %s vote(s) %s" % (r.get("score", 0), len(r.get("votes") or []),
                                                   json.dumps(r.get("tally") or {}, ensure_ascii=False))]
        for v in (r.get("votes") or []):
            lines.append("  %s %-10s %s%s  %s" % (v.get("kind"), v.get("author", "?"),
                                                  "w=%s" % v.get("weight", "?"),
                                                  "" if v.get("counted", True) else " (not counted)",
                                                  (v.get("text") or "")[:80]))
        for key in ("note", "method"):
            if n.get(key):
                lines.append("%s: %s" % (key, n[key]))
        return "\n".join(lines)
    except Exception:
        n = next((x for x in _scan(coral) if x.get("id") == node), None)
        if not n:
            return "no such node %s in %s (and the server is down)" % (node, coral)
        lines = [_line(n)]
        votes = [x for x in _scan(coral) if x.get("type") == "review" and x.get("claim") == node]
        lines += ["  %s %-10s %s" % (v.get("kind"), v.get("author", "?"), (v.get("text") or "")[:80])
                  for v in votes] or ["(no votes)"]
        return "\n".join(lines) + "\n" + _sources(True, coral)


def publish(s: str, p: str, o: str, *, coral: str = "", rel: str = "root",
            parents: list | None = None, type: str = "claim", topics: list | None = None,
            significance: int = 3, note: str = "", method: str = "",
            refs: list | None = None, artifacts: dict | None = None,
            force: bool = False, why_new: str = "",
            attempts: int | None = None, volatility: str = "",
            action: str = "", targets: list | None = None) -> str:
    coral = pick_coral(coral)
    if force and not (why_new or "").strip():
        return ("PUBLISH FAILED: force needs why_new — say how this differs from the node it "
                "resembles. Nothing was recorded.")
    payload = {"s": s, "p": p, "o": o, "rel": rel, "parents": parents or [],
               "type": type, "topics": topics or [], "significance": significance,
               "note": note, "author": author()}
    # §20.7: a suggestion carries the action it proposes and the nodes it
    # touches. Without these two fields the node is just an opinion.
    if action:
        payload["action"] = action
    if targets:
        payload["targets"] = targets
    if force:
        payload["force"] = True
        payload["why_new"] = why_new
    # §4.6: how many configurations were tried before this result. Omitting it
    # is not neutral — the node then reads as `attempts: 1`, which is exactly
    # the claim the anti-multiple-comparisons rule exists to make explicit.
    if attempts is not None:
        payload["attempts"] = int(attempts)
    if volatility:
        payload["volatility"] = volatility
    if method:
        payload["method"] = method
    if refs:
        payload["refs"] = refs
    if artifacts:
        payload.update(artifacts)
    try:
        r = _request(_coral_path(coral, "/nodes"), "POST", payload, write=True)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            obj = json.loads(raw)
        except Exception:
            obj = {}
        if e.code == 409 and obj.get("candidates"):
            # The third exit of §3.4.1 used to be unreachable from the plugin:
            # the candidate list arrived as a truncated JSON blob.
            lines = ["409 similar — this is already in the library; nothing was recorded:"]
            for c in obj["candidates"][:5]:
                lines.append("  %s sim=%.2f  %s = %s" % (c.get("id"), c.get("similarity", 0),
                                                          c.get("p"), c.get("o")))
            lines.append("Pick one: rel=extends/supersedes with parents=[id], or "
                         "force=true plus why_new saying what is different.")
            return "\n".join(lines)
        return "PUBLISH FAILED (%s): %s\nNothing was recorded. Retry once the server is up; " \
               "do not treat this as saved." % (e.code, raw[:300])
    except PermissionError as e:
        return "PUBLISH FAILED: %s. Nothing was recorded." % e
    except Exception as e:
        hint = _setup_hint(pick_coral(coral))
        return ("PUBLISH FAILED: %s\nNothing was recorded (server at %s unreachable). "
                "Say so in your answer instead of assuming it landed.%s"
                % (e, settings()["url"], ("\n" + hint) if hint else ""))
    node = r.get("node") or {}
    verb = "created" if r.get("created") else "already existed"
    # The id is the first token of the prose line, which is fine for a human and
    # a trap for an agent: passing the whole sentence back as `parents` yields a
    # 400 (observed). The trailing `node_id:` line is the machine-readable form.
    return "%s %s\nnode_id: %s" % (verb, _line(node), node.get("id", ""))


def _http_detail(e: Exception) -> str:
    """The server's own message, not `HTTP Error 404: Not Found` — a 404 that
    says *which* coral is unknown is actionable; the bare status code is not."""
    try:
        raw = e.read().decode(errors="replace")
        try:
            return json.loads(raw).get("error") or raw[:200]
        except Exception:
            return raw[:200] or str(e)
    except Exception:
        return str(e)


def claim(question: str, ttl: int = 30, coral: str = "") -> str:
    """Take a lease on one question. Returns the holder when it is refused — a
    skip with a name is useful, a skip without one looks like a bug."""
    coral = pick_coral(coral)
    try:
        r = _request(_coral_path(coral, "/claims"), "POST",
                     {"question": question, "author": author(), "ttl": int(ttl)},
                     write=True)
    except PermissionError as e:
        return "CLAIM FAILED: %s — nobody holds this question." % e
    except Exception as e:
        return "CLAIM FAILED: %s — nothing was recorded, so nobody holds this question." % _http_detail(e)
    if not r.get("acquired"):
        until = time.strftime("%H:%M", time.localtime(r.get("lease_until") or 0))
        return "busy: %s already holds this question until %s — skip it" % (
            r.get("holder"), until)
    until = time.strftime("%H:%M", time.localtime(r.get("lease_until") or 0))
    return "claimed %s for %s min (until %s)" % (r.get("task_key"), ttl, until)


def release(question: str, coral: str = "") -> str:
    coral = pick_coral(coral)
    try:
        r = _request(_coral_path(coral, "/claims"), "POST",
                     {"question": question, "author": author(), "ttl": -1}, write=True)
    except PermissionError as e:
        return "RELEASE FAILED: %s — the lease is still held." % e
    except Exception as e:
        return "RELEASE FAILED: %s — the lease is still held." % _http_detail(e)
    if r.get("released"):
        return "released %s" % r.get("task_key")
    if r.get("holder"):
        return "cannot release: %s holds this question" % r["holder"]
    return "no such question: %s" % question


def suggest(action: str, targets: list, why: str, *, topics: list | None = None,
            significance: int = 2, coral: str = "") -> str:
    """Propose maintenance without doing it (§20.7).

    The judgement is a node so it can be refuted; the action stays with the
    operator, so a wrong proposal costs a vote rather than a deletion.
    """
    return publish("maintenance", "suggest:%s" % action, why, coral=coral,
                   type="suggestion", topics=topics or ["maintenance"],
                   significance=significance, action=action, targets=list(targets or []))


def vote(node: str, kind: str, text: str, *, coral: str = "", prior: str = "",
         artifact: str = "") -> str:
    coral = pick_coral(coral)
    payload = {"kind": kind, "text": text, "author": author()}
    if prior:
        payload["prior"] = prior
    if artifact:
        payload["artifact"] = artifact
    try:
        r = _request(_coral_path(coral, "/nodes/%s/replies" % node), "POST", payload, write=True)
    except Exception as e:
        return "VOTE FAILED: %s — nothing was recorded." % e
    n = r.get("node") or {}
    kind = (kind or "").lower()
    # §4.2.1: tell the caller which gate it failed, and only when that gate
    # applies. "no prior: weighs 1" on a `sourced` vote was simply false — that
    # kind never needed a prior — and would teach an agent to distrust a vote
    # that scored exactly as intended.
    caps = []
    if kind in ("replicated", "adversarial"):
        if not prior:
            caps.append("no prior")
        if not artifact:
            caps.append("no artifact")
    elif kind == "refuted" and not artifact:
        caps.append("no artifact")
    elif kind == "redundant" and not artifact:
        caps.append("no artifact naming the duplicate")
    return "vote %s %s on %s%s" % (n.get("id", "?"), kind, node,
                                   ("  (capped at 1: %s)" % ", ".join(caps)) if caps else "")


def state(coral: str = "", cursor: int = 0, topic: str = "") -> str:
    coral = pick_coral(coral)
    lines = []
    try:
        h = _request("/health")
        seq = ((h.get("corals") or {}).get(coral) or {}).get("max_seq", h.get("max_seq"))
        lines.append("server up, %s at seq %s" % (coral, seq))
        r = _request(_coral_path(coral, "/since") + "?" + urllib.parse.urlencode(
            {"cursor": cursor, "topics": topic}))
        fresh = r.get("nodes") or []
        lines.append("%d new since cursor %s" % (len(fresh), cursor))
        lines += ["  " + _line(n) for n in fresh[-10:]]
        t = _request(_coral_path(coral, "/tasks"))
        tasks = t.get("tasks") or []
        lines.append("%d open tasks" % len(tasks))
        lines += ["  ★%d %s — %s" % (x.get("significance", 3), x.get("p", ""), x.get("o", ""))
                  for x in tasks[:6]]
    except Exception:
        nodes = _scan(coral)
        lines.append("server down — scanned %d nodes in %s" % (len(nodes), coral))
        lines += ["  " + _line(n) for n in nodes[-8:]]
    return "\n".join(lines)


def verify(node: str = "", coral: str = "") -> str:
    coral = pick_coral(coral)
    try:
        r = _request(_coral_path(coral, "/verify") + ("?node=" + node if node else ""))
    except Exception as e:
        return "verify unavailable: %s" % e
    out = ["%d ok / %d changed / %d missing" % (r.get("ok", 0), r.get("changed", 0),
                                                r.get("missing", 0))]
    for x in (r.get("refs") or []):
        if x["state"] != "ok":
            out.append("  [%s] %s" % (x["state"], x["ref"].get("path") or x["ref"].get("node")))
    return "\n".join(out)
