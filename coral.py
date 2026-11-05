#!/usr/bin/env python3
"""Corallium CLI — thin client for a v0.1.12 server. See PROTOCOL.md.

The server is the reference implementation; this file holds no storage model
of its own. Everything it knows comes from HTTP, and when the server cannot be
reached it reads the immutable truth directly (nodes/*.json, PROTOCOL §16.1).

Reads degrade (§13.5):
    find / tip / show / reviews / verify / tasks / since
    -> scan ~/corals/<name>/nodes/*.json, print "[offline: scanned N nodes]"
Writes never degrade (§13.5, §13.8):
    add / reply / claim / release
    -> hard error on stderr, exit non-zero, nothing is silently dropped.

Configuration (§17.2), first hit wins:
    url    --url > $CORALLIUM_URL > ~/.hermes/config.yaml corallium.url
           > http://127.0.0.1:8731
    coral  --coral > $CORALLIUM_CORAL > config corallium.coral > the only coral
           the server has. Several corals and no choice -> error listing them;
           guessing would write your claim into the wrong knowledge domain.
    author --author > $CORALLIUM_AUTHOR > config corallium.author > "hermes"
    tokens $CORALLIUM_READ_TOKEN / $CORALLIUM_WRITE_TOKEN (Bearer, optional)

Exit codes: 0 ok · 1 error · 2 nothing found (empty search, no tips, no task).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

DEFAULT_URL = "http://127.0.0.1:8731"
CONFIG_PATH = Path(os.environ.get("CORALLIUM_CONFIG", "~/.hermes/config.yaml")).expanduser()

WEIGHTS = {"adversarial": 4, "replicated": 3, "sourced": 2, "derived": 1,
           "agree": 0, "refuted": -3}
RELS = ("root", "extends", "supersedes", "merges")
TYPES = ("claim", "anomaly", "question", "decision", "task")
READ_TIMEOUT = 3.0   # local reads are sub-ms; fail fast if the server is gone
WRITE_TIMEOUT = 15.0  # the writer queue may serialise a burst of publishes


# ── config ────────────────────────────────────────────────────────────────
def load_config() -> dict:
    """Read only the `corallium:` block of the YAML. No PyYAML, no guessing.

    The keys used here are flat by design (url / coral / corals_dir / author);
    unknown keys are ignored, and a nested `corals:` map is not interpreted —
    this CLI talks to one url, which is what §17.2's remote mode needs.
    """
    if not CONFIG_PATH.exists():
        return {}
    try:
        text = CONFIG_PATH.read_text()
    except OSError:
        return {}
    out: dict[str, str] = {}
    inside = False
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip())
        if indent == 0:
            inside = raw.strip().startswith("corallium:")
            continue
        if not inside or ":" not in raw:
            continue
        k, v = raw.split(":", 1)
        k, v = k.strip(), v.strip().strip("\"'")
        if v and not v.startswith("{"):  # flow maps belong to `corals:`
            out.setdefault(k, v)
    return out


def resolve_url(a) -> str:
    return (a.url or os.environ.get("CORALLIUM_URL")
            or load_config().get("url") or DEFAULT_URL).rstrip("/")


def coral_in_url(url: str) -> str:
    m = re.search(r"/c/([^/]+)$", url)
    return urllib.parse.unquote(m.group(1)) if m else ""


def server_root(url: str) -> str:
    return url[: -len("/c/" + coral_in_url(url))] if coral_in_url(url) else url


def corals_dir() -> Path:
    return Path(os.environ.get("CORALLIUM_CORALS_DIR")
                or load_config().get("corals_dir")
                or "~/corals").expanduser()


# ── transport ─────────────────────────────────────────────────────────────
class Offline(Exception):
    """Server unreachable: transport-level failure, not an HTTP error."""


class ApiError(Exception):
    """Server reachable but said no (400/401/404/500...)."""

    def __init__(self, code: int, msg: str):
        self.code = code
        super().__init__(f"HTTP {code}: {msg}")


def _token(write: bool) -> str:
    if write:
        return os.environ.get("CORALLIUM_WRITE_TOKEN", "")
    return (os.environ.get("CORALLIUM_READ_TOKEN")
            or os.environ.get("CORALLIUM_WRITE_TOKEN", ""))


def api(url: str, ep: str, params: dict | None = None,
        body: dict | None = None, write: bool = False):
    """One request. Raises Offline only when nothing answered.

    An HTTPError means the server *did* answer — degrading to a local scan
    there would hide a real rejection (401, bad payload) behind stale data.
    """
    if params:
        ep += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
    req = urllib.request.Request(url + "/" + ep.lstrip("/"), method="POST" if write else "GET")
    tok = _token(write)
    if tok:
        req.add_header("Authorization", "Bearer " + tok)
    data = None
    if write:
        data = json.dumps(body or {}, ensure_ascii=False).encode()
        req.add_header("Content-Type", "application/json")
        req.data = data
    try:
        with urllib.request.urlopen(req, timeout=WRITE_TIMEOUT if write else READ_TIMEOUT) as r:
            raw = r.read()
            return json.loads(raw or b"{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            msg = json.loads(raw).get("error") or raw
        except Exception:
            msg = raw.strip() or e.reason
        raise ApiError(e.code, msg) from None
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
        raise Offline(str(getattr(e, "reason", e))) from None
    except json.JSONDecodeError as e:
        raise ApiError(200, f"server returned non-JSON ({e})") from None


# ── coral resolution ──────────────────────────────────────────────────────
def available_corals(url: str) -> list[str]:
    """Names from /health (unauthenticated), else from local data directories."""
    try:
        h = api(server_root(url), "health")
        return sorted(h.get("corals") or {})
    except (Offline, ApiError):
        pass
    try:
        return sorted(p.name for p in corals_dir().iterdir()
                      if (p / "nodes").is_dir() or (p / "coral.yaml").is_file())
    except OSError:
        return []


def resolve_coral(a, url: str) -> str:
    name = (a.coral or os.environ.get("CORALLIUM_CORAL")
            or load_config().get("coral") or coral_in_url(url))
    if name:
        return name
    names = available_corals(url)
    if len(names) == 1:
        return names[0]
    if not names:
        die("no coral found at " + url + " and none under " + str(corals_dir()) +
            "\n  -> set CORALLIUM_CORAL=<name> or pass --coral <name>")
    die("several corals and no default — refusing to guess:\n  " + ", ".join(names) +
        "\n  -> pass --coral <name> or set corallium.coral in " + str(CONFIG_PATH))
    raise AssertionError  # unreachable


def endpoint(url: str, coral: str) -> str:
    if coral_in_url(url):
        return url
    return f"{url}/c/{urllib.parse.quote(coral)}"


def die(msg: str, code: int = 1) -> None:
    print("error: " + msg, file=sys.stderr)
    raise SystemExit(code)


def offline_fail(what: str, url: str, err: Exception) -> None:
    print(f"error: corallium server unreachable at {url} ({err})", file=sys.stderr)
    print(f"error: {what} NOT saved — writes never degrade (PROTOCOL §13.5)", file=sys.stderr)
    raise SystemExit(1)


# ── local truth (degraded reads) ──────────────────────────────────────────
def local_dir(coral: str) -> Path:
    d = corals_dir() / coral
    if not d.is_dir():
        die(f"offline and no local coral at {d} (set corallium.corals_dir)")
    return d


def local_nodes(coral: str) -> list[dict]:
    out = []
    for f in sorted(local_dir(coral).glob("nodes/*.json")):
        try:
            n = json.loads(f.read_text())
            n.setdefault("id", f.stem)
            out.append(n)
        except Exception as e:
            print(f"warn: corrupt node {f.name}: {e}", file=sys.stderr)
    return out


def _kind(n: dict) -> str:
    return n.get("kind") or n.get("p") or "agree"


def _gate(v: dict) -> tuple[float, list[str]]:
    """Mirror of Store._gate (§4.2.1) so offline scores are not fantasy."""
    kind, w, caps = _kind(v), WEIGHTS.get(_kind(v), 0), []
    if kind in ("replicated", "adversarial", "refuted"):
        if kind != "refuted" and not v.get("prior"):
            caps.append("no-prior")
        if not (v.get("artifact") or v.get("method")):
            caps.append("not-checkable")
        if caps:
            w = 1 if w > 0 else -1
    return w, caps


def local_scores(alln: list[dict]) -> dict[str, float]:
    out: dict[str, float] = {}
    for n in alln:
        if n.get("type") != "review":
            continue
        w, _ = _gate(n)
        if n.get("author") != next((t.get("author") for t in alln
                                    if t.get("id") == n.get("claim")), None):
            out[n.get("claim")] = out.get(n.get("claim"), 0.0) + w
    return out


def local_status(n: dict, alln: list[dict], dead: set) -> str:
    if n.get("status") == "withdrawn" or n["id"] in dead:
        return "withdrawn"
    votes = [v for v in alln if v.get("type") == "review" and v.get("claim") == n["id"]
             and v.get("author") != n.get("author")]
    if any(_kind(v) == "refuted" for v in votes):
        return "contested"
    if any(x.get("type") == "claim" and x.get("s") == n.get("s") and x.get("p") == n.get("p")
           and x.get("o") != n.get("o") and x["id"] not in dead for x in alln):
        return "contested"
    if any(_kind(v) == "adversarial" or (_kind(v) == "replicated" and v.get("prior"))
           for v in votes):
        return "open" if int(n.get("attempts") or 1) > 5 else "confirmed"
    return "open"


def _dead(alln: list[dict]) -> set:
    dead = {n["id"] for n in alln if n.get("status") == "withdrawn"}
    for n in alln:
        if n.get("status") == "withdrawn" and n.get("rel") == "supersedes":
            dead.update(n.get("parents") or [])
    for n in alln:  # superseded nodes are no longer the tip
        if n.get("rel") == "supersedes":
            dead.update(n.get("parents") or [])
    return dead


def offline_annotate(nodes: list[dict], coral: str) -> list[dict]:
    alln = local_nodes(coral)
    scores, dead = local_scores(alln), _dead(alln)
    out = []
    for n in nodes:
        m = dict(n)
        m["derived_status"] = local_status(n, alln, dead)
        m["score"] = round(scores.get(n["id"], 0.0), 2)
        m["votes"] = sum(1 for v in alln if v.get("type") == "review"
                         and v.get("claim") == n["id"] and v.get("author") != n.get("author"))
        out.append(m)
    return out


def offline_marker(n: int) -> None:
    print(f"[offline: scanned {n} nodes, status/score computed locally]")


# ── formatting ────────────────────────────────────────────────────────────
def clip(s, n: int = 120) -> str:
    s = " ".join(str(s or "").split())
    return s if len(s) <= n else s[: n - 1] + "…"


def stars(n: dict) -> str:
    sig = max(1, min(5, int(n.get("significance") or 3)))
    return "★" * sig + "☆" * (5 - sig)


def st(n: dict) -> str:
    return n.get("derived_status") or n.get("status") or "open"


def fscore(n: dict) -> str:
    sc = n.get("score")
    return f"{sc:+.1f}" if isinstance(sc, (int, float)) else " n/a"


def summary(n: dict) -> str:
    s, p, o = n.get("s") or "", n.get("p") or "", n.get("o") or ""
    body = (f"{s} · {p} = {o}" if (s or p) else str(n.get("question") or o))
    return clip(body, 150)


def line(n: dict) -> str:
    return f"{n.get('id','?')} [{st(n)}] {fscore(n)} {stars(n)} {summary(n)}"


def print_node(n: dict, note: bool = True) -> None:
    print(line(n))
    if note and n.get("note"):
        print("    note: " + clip(n["note"], 200))


def load_target(url: str, coral: str, nid: str) -> dict:
    """One node + its votes. /reviews answers for any node id, claim or vote."""
    try:
        return api(endpoint(url, coral), "reviews", {"claim": nid})
    except ApiError as e:
        if e.code == 404:  # /reviews answers 404 for an id it has never seen
            print(f"not found: {nid}", file=sys.stderr)
            raise SystemExit(2)
        die(str(e))
    except Offline:
        d = local_dir(coral) / "nodes" / f"{nid}.json"
        if not d.is_file():
            print(f"not found: {nid} (offline)", file=sys.stderr)
            raise SystemExit(2)
        alln = local_nodes(coral)
        node = json.loads(d.read_text())
        votes = [v for v in alln if v.get("type") == "review" and v.get("claim") == nid]
        ann = offline_annotate([node], coral)[0]
        offline_marker(len(alln))
        return {"node": ann, "score": ann["score"], "votes": votes}


# ── commands: read ────────────────────────────────────────────────────────
def cmd_find(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    lim = a.limit
    try:
        r = api(endpoint(url, coral), "find",
                {"q": a.query, "root": a.root, "sort": a.sort})
        hits, outside = r.get("hits") or [], r.get("outside") or 0
    except Offline as e:
        alln = local_nodes(coral)
        q = a.query.lower()
        hits = [n for n in alln if q in " ".join(str(n.get(k) or "") for k in
                ("s", "p", "o", "note", "question", "type", "author")).lower()
                + " " + " ".join(n.get("topics") or []).lower()]
        if a.root:
            outside = len([n for n in hits if a.root not in (n.get("topics") or [])])
            hits = [n for n in hits if a.root in (n.get("topics") or [])]
        else:
            outside = 0
        hits = offline_annotate(hits, coral)
        key = {"significance": lambda n: (n.get("significance") or 3, n.get("score") or 0),
               "score": lambda n: n.get("score") or 0}.get(a.sort)
        if key:
            hits.sort(key=key, reverse=True)
        else:
            hits.sort(key=lambda n: n.get("ts") or "", reverse=True)
        offline_marker(len(alln))
    except ApiError as e:
        die(str(e))
    hits = hits[:lim]
    for n in hits:
        print_node(n)
    if outside:
        print(f"{outside} outside scope (not shown)")
    print(f"{len(hits)} match(es)")
    return 0 if hits else 2


def cmd_tip(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    try:
        tips = api(endpoint(url, coral), "tip", {"topic": a.topic}).get("tips") or []
    except Offline as e:
        alln = local_nodes(coral)
        low = (a.topic or "").lower()
        scope = [n for n in alln if not low or low in (n.get("topics") or [])
                 or low in json.dumps(n, ensure_ascii=False).lower()]
        ids = {n["id"] for n in scope}
        kids = {p for n in scope for p in (n.get("parents") or []) if p in ids}
        dead = _dead(alln)
        tips = [n for n in scope if n["id"] not in kids and n["id"] not in dead]
        tips = offline_annotate(tips or scope, coral)
        tips.sort(key=lambda n: n.get("seq") or 0, reverse=True)
        offline_marker(len(alln))
    except ApiError as e:
        die(str(e))
    tips.sort(key=lambda n: n.get("seq") or 0, reverse=True)
    for n in tips[: a.limit]:
        print_node(n)
        print(f"    {st(n)} · {n.get('votes', 0)} vote(s) · {n.get('dependents', 0)} dependent(s)")
    if any(st(n) == "contested" for n in tips):
        print(f"!! {sum(1 for n in tips if st(n)=='contested')} contested — unresolved disagreement")
    print(f"{min(len(tips), a.limit)} tip(s)")
    return 0 if tips else 2


def derive_quick(n: dict, votes: list[dict]) -> str:
    """Vote-based status for `show`, which only has /reviews to work with.

    /reviews returns the raw node; the server's full derivation also looks at
    supersedes chains and rival answers, which are only visible through
    find/tip. This covers the dimension show is actually about: the votes.
    """
    if n.get("status") == "withdrawn":
        return "withdrawn"
    counted = [v for v in votes if v.get("counted", True)]
    if any((v.get("kind") or v.get("p")) == "refuted" for v in counted):
        return "contested"
    strong = any((v.get("kind") or v.get("p")) == "adversarial"
                 or ((v.get("kind") or v.get("p")) == "replicated" and v.get("prior"))
                 for v in counted)
    if strong:
        return "open" if int(n.get("attempts") or 1) > 5 else "confirmed"
    return "open"


def cmd_show(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    r = load_target(url, coral, a.id)
    n = dict(r["node"])
    votes = r.get("votes") or []
    n.setdefault("score", r.get("score"))
    n["votes"] = n.get("votes", len(votes))
    had_derived = "derived_status" in n
    if not had_derived:
        n["derived_status"] = derive_quick(n, votes)
    if a.json:
        print(json.dumps(n, ensure_ascii=False, indent=2))
        return 0
    print(line(n) if had_derived
          else line(n).replace(f"[{st(n)}]", f"[{st(n)}*]", 1))
    print(f"    type={n.get('type')} author={n.get('author')} seq={n.get('seq')} ts={n.get('ts')}")
    src = ("derived" if had_derived
           else "* votes-only derivation; find/tip also check rivals and supersedes")
    print(f"    status {st(n)} ({src}) · score {fscore(n)} · {n.get('votes')} vote(s)"
          + (f" · {n.get('dependents')} dependent(s)" if n.get("dependents") is not None else ""))
    if n.get("topics"):
        print("    topics: " + ", ".join(n["topics"]))
    if n.get("note"):
        print("    note: " + clip(n["note"], 400))
    if n.get("method"):
        m = n["method"]
        print("    method: " + clip(m if isinstance(m, str) else json.dumps(m, ensure_ascii=False), 300))
    if n.get("refs"):
        refs = n["refs"] if isinstance(n["refs"], list) else []
        print(f"    refs: {len(refs)}" + ("" if refs else f" ({n['refs']} pinned)"))
        for rf in refs:
            print(f"      {rf.get('path') or rf.get('node')} {rf.get('lines') or ''}"
                  f" missing={rf.get('missing')}")
    if n.get("parents"):
        print(f"    parents: {', '.join(n['parents'])} ({n.get('rel')})")
    votes = r.get("votes") or []
    if votes:
        print(f"    votes ({len(votes)}):")
        for v in votes:
            print("      " + vote_line(v))
    return 0


def vote_line(v: dict) -> str:
    if "weight" in v:  # server shape
        caps = ",".join(v.get("capped") or []) or "-"
        eff = v.get("effective", v.get("weight"))
        return (f"{str(v.get('ts'))[:19]} {v.get('author','?'):<12} {v.get('kind','?'):<12} "
                f"{float(eff):+.2f} (w={v.get('weight')} capped={caps}) {clip(v.get('text'))}")
    w, caps = _gate(v)  # offline shape
    return (f"{str(v.get('ts'))[:19]} {v.get('author','?'):<12} {_kind(v):<12} "
            f"{w:+.2f} (local) {clip(v.get('o'))}")


def cmd_reviews(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    r = load_target(url, coral, a.id)
    n, votes = r["node"], r.get("votes") or []
    print(f"{n['id']} [{st(n)}] score {fscore({'score': r.get('score')})} — {len(votes)} vote(s)")
    if r.get("tally"):
        print("tally: " + ", ".join(f"{k}×{v}" for k, v in sorted(r["tally"].items())))
    for v in votes:
        print("  " + vote_line(v))
        if v.get("prior"):
            print(f"      prior={v['prior']}  artifact={clip(v.get('artifact'), 160)}")
        elif v.get("artifact"):
            print(f"      artifact={clip(v.get('artifact'), 160)}")
    return 0 if votes else 2


def cmd_tasks(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    try:
        tasks = api(endpoint(url, coral), "tasks", {"all": "1" if a.all else ""}).get("tasks") or []
    except Offline as e:
        alln = local_nodes(coral)
        kids = {p for n in alln for p in (n.get("parents") or [])}
        dead = _dead(alln)
        tasks = [dict(n, closed=(n["id"] in kids or n["id"] in dead))
                 for n in alln if n.get("type") == "task"]
        tasks = [t for t in tasks if a.all or not t["closed"]]
        offline_marker(len(alln))
    except ApiError as e:
        die(str(e))
    if not a.all:
        tasks = [t for t in tasks if not t.get("closed")]
    for t in tasks:
        print(f"{t['id']} {'[closed]' if t.get('closed') else '[open]'} {stars(t)} {clip(t.get('o'), 160)}")
        if t.get("note"):
            print("    note: " + clip(t["note"], 160))
    print(f"{len(tasks)} task(s)")
    return 0 if tasks else 2


def cmd_verify(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    try:
        r = api(endpoint(url, coral), "verify", {"node": a.node})
    except Offline as e:
        alln = local_nodes(coral)
        rows = []
        for n in alln:
            if a.node and n["id"] != a.node:
                continue
            for i, rf in enumerate(n.get("refs") or []):
                if rf.get("node"):
                    rows.append({"node": n["id"], "ref": rf, "state": "node"})
                    continue
                f = Path(rf.get("path", ""))
                if not f.is_file():
                    rows.append({"node": n["id"], "ref": rf, "state": "missing"})
                else:
                    cur = hashlib.sha256(f.read_bytes()).hexdigest()
                    rows.append({"node": n["id"], "ref": rf,
                                 "state": "ok" if cur == rf.get("sha256") else "changed"})
        r = {"refs": rows, "ok": sum(x["state"] in ("ok", "node") for x in rows),
             "changed": sum(x["state"] == "changed" for x in rows),
             "missing": sum(x["state"] == "missing" for x in rows)}
        offline_marker(len(alln))
    except ApiError as e:
        die(str(e))
    print(f"refs: {r.get('ok',0)} ok, {r.get('changed',0)} changed, {r.get('missing',0)} missing")
    for x in r.get("refs") or []:
        state = x.get("state")
        if state in ("ok", "node") and not a.all:
            continue
        rf = x.get("ref") or {}
        print(f"  {state:<8} {x.get('node')}  {rf.get('path') or rf.get('node')}"
              + (f"  lines={rf['lines']}" if rf.get("lines") else ""))
    bad = (r.get("changed") or 0) + (r.get("missing") or 0)
    return 1 if bad else 0


def cmd_since(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    try:
        nodes = api(endpoint(url, coral), "since",
                    {"cursor": a.cursor, "topics": a.topics}).get("nodes") or []
    except Offline as e:
        die(f"since needs the server ({e}); offline scan cannot honour a seq cursor")
    except ApiError as e:
        die(str(e))
    if not nodes:
        print("0 new")
        return 0
    for n in nodes:
        print(f"{n.get('seq'):>5} {n.get('id')} {n.get('type','?'):<8} {clip(summary(n), 110)}")
    print(f"{len(nodes)} new")
    return 0


def cmd_claims(a) -> int:
    """Local view only: the server has no GET /claims (leases are POST-only)."""
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    f = corals_dir() / coral / "claims.json"
    rows = []
    if f.exists():
        try:
            rows = json.loads(f.read_text())
        except Exception as e:
            die(f"cannot read {f}: {e}")
    now = datetime.now().timestamp()
    live = [r for r in rows if (r.get("lease_until") or 0) > now]
    for r in sorted(live, key=lambda r: -(r.get("lease_until") or 0)):
        mins = int((r["lease_until"] - now) / 60)
        print(f"{r.get('task_key')} {r.get('author','?'):<12} {mins:>3}m {clip(r.get('question'), 100)}")
    print(f"{len(live)} live lease(s)  (local claims.json; server has no GET /claims)")
    return 0 if live else 2


# ── commands: write (never degrade) ───────────────────────────────────────
def cmd_add(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    parents = a.parent or []
    if a.rel in ("extends", "supersedes") and len(parents) != 1:
        die(f"--rel {a.rel} needs exactly 1 --parent (got {len(parents)})")
    if a.rel == "merges" and len(parents) != 2:
        die(f"--rel merges needs exactly 2 --parent (got {len(parents)})")
    if a.rel == "root" and parents:
        die("--rel root takes no --parent")
    topics = [t.strip() for x in (a.topics or []) for t in x.split(",") if t.strip()]
    body = {
        "s": a.s, "p": a.p, "o": a.o, "rel": a.rel, "parents": parents,
        "type": a.type, "topics": topics, "significance": a.significance,
        "attempts": a.attempts, "volatility": a.volatility,
        "note": a.note or "", "author": a.author or default_author(),
    }
    method = parse_method(a.method)
    if method:
        body["method"] = method
    if a.ref:
        body["refs"] = [{"path": r} for r in a.ref]
    try:
        r = api(endpoint(url, coral), "nodes", body=body, write=True)
    except Offline as e:
        offline_fail("publish", url, e)
    except ApiError as e:
        die(f"publish rejected: {e}")
    n = r.get("node") or {}
    print(("added " if r.get("created") else "exists ") + f"{n.get('id')} seq={n.get('seq')}")
    print_node(dict(n, score=0))
    return 0


def cmd_reply(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    me = a.author or default_author()
    # PROTOCOL §4.3: one author, one vote per target. The server does not
    # enforce it, so the client refuses before writing a duplicate.
    try:
        existing = api(endpoint(url, coral), "reviews", {"claim": a.id}).get("votes") or []
    except Offline as e:
        offline_fail("vote", url, e)
    except ApiError as e:
        die(f"cannot vote: {e}")
    if any(v.get("author") == me for v in existing):
        die(f"{me} already voted on {a.id} (PROTOCOL §4.3: one vote per author)")
    body = {"kind": a.kind, "text": a.text, "author": me}
    if a.prior:
        body["prior"] = a.prior
    if a.artifact:
        body["artifact"] = a.artifact
    try:
        r = api(endpoint(url, coral), f"nodes/{a.id}/replies", body=body, write=True)
    except Offline as e:
        offline_fail("vote", url, e)
    except ApiError as e:
        die(f"vote rejected: {e}")
    n = r.get("node") or {}
    print(("added " if r.get("created") else "exists ") + f"{n.get('id')} on {a.id} ({a.kind})")
    return 0


def cmd_claim(a) -> int:
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    try:
        r = api(endpoint(url, coral), "claims", write=True,
                body={"question": a.question, "author": a.author or default_author(),
                      "ttl": a.ttl})
    except Offline as e:
        offline_fail("claim", url, e)
    except ApiError as e:
        die(f"claim rejected: {e}")
    if not r.get("acquired"):
        until = datetime.fromtimestamp(r.get("lease_until") or 0).strftime("%H:%M")
        print(f"busy: {r.get('holder')} holds this question until {until}", file=sys.stderr)
        return 1
    until = datetime.fromtimestamp(r.get("lease_until") or 0).strftime("%H:%M")
    print(f"claimed {r.get('task_key')} for {a.ttl} min (until {until}): {a.question}")
    return 0


def cmd_release(a) -> int:
    """No /release endpoint exists; re-claiming with ttl=-1 expires our own lease."""
    url, coral = resolve_url(a), resolve_coral(a, resolve_url(a))
    try:
        r = api(endpoint(url, coral), "claims", write=True,
                body={"question": a.question, "author": a.author or default_author(),
                      "ttl": -1})
    except Offline as e:
        offline_fail("release", url, e)
    except ApiError as e:
        die(f"release rejected: {e}")
    if not r.get("acquired"):
        die(f"{r.get('holder')} holds this question — cannot release someone else's lease")
    print(f"released {r.get('task_key')} ({a.status}): {a.question}")
    return 0


def default_author() -> str:
    return (os.environ.get("CORALLIUM_AUTHOR") or load_config().get("author") or "hermes")


def parse_method(s: str | None) -> dict | None:
    if not s:
        return None
    try:
        v = json.loads(s)
        if isinstance(v, dict):
            return v
    except Exception:
        pass
    if "=" not in s:
        return {"cmd": s}
    out = {}
    for part in s.split(","):
        if "=" in part:
            k, val = part.split("=", 1)
            out[k.strip()] = val.strip()
    return out or {"cmd": s}


# ── cli ───────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="coral", description="Corallium CLI — v0.1.12 thin client",
        epilog="exit codes: 0 ok · 1 error · 2 nothing found",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--coral", default=None, help="coral name (default: $CORALLIUM_CORAL/config)")
    p.add_argument("--url", default=None, help="server url (default: $CORALLIUM_URL/config)")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add(sp, author=True):
        # Repeated on every subparser so `coral find q --coral mqsim` works;
        # SUPPRESS keeps a value given before the subcommand from being reset.
        sp.add_argument("--coral", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
        sp.add_argument("--url", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
        if author:
            sp.add_argument("--author", default=None)

    s = sub.add_parser("add", help="publish one falsifiable proposition")
    s.add_argument("--s", required=True, help="subject")
    s.add_argument("--p", required=True, help="predicate")
    s.add_argument("--o", required=True, help="object / the proposition itself")
    s.add_argument("--rel", default="root", choices=RELS)
    s.add_argument("--parent", action="append", default=[], metavar="ID")
    s.add_argument("--type", default="claim", choices=TYPES)
    s.add_argument("--topics", action="append", default=[], metavar="A,B")
    s.add_argument("--significance", type=int, default=3, choices=range(1, 6), metavar="1-5")
    s.add_argument("--note", default="")
    s.add_argument("--method", default="", help="JSON object or k=v,k=v (config, cmd, version, seed)")
    s.add_argument("--ref", action="append", default=[], metavar="PATH",
                   help="file the server hashes at write time (repeatable)")
    s.add_argument("--attempts", type=int, default=1)
    s.add_argument("--volatility", default="static",
                   choices=["static", "slow", "fast", "live"])
    add(s)
    s.set_defaults(fn=cmd_add)

    s = sub.add_parser("find", help="search (substring, §11)")
    s.add_argument("query")
    s.add_argument("--limit", type=int, default=5)
    s.add_argument("--root", default="", help="only nodes with this topic")
    s.add_argument("--sort", default="recent", choices=["recent", "score", "significance"])
    add(s, author=False)
    s.set_defaults(fn=cmd_find)

    s = sub.add_parser("tip", help="leaf beliefs for a topic")
    s.add_argument("topic", nargs="?", default="")
    s.add_argument("--limit", type=int, default=5)
    add(s, author=False)
    s.set_defaults(fn=cmd_tip)

    s = sub.add_parser("show", help="one node and its votes")
    s.add_argument("id")
    s.add_argument("--json", action="store_true")
    add(s, author=False)
    s.set_defaults(fn=cmd_show)

    s = sub.add_parser("reviews", help="vote timeline for a node")
    s.add_argument("id")
    add(s, author=False)
    s.set_defaults(fn=cmd_reviews)

    s = sub.add_parser("reply", help="vote on someone else's node (§4)")
    s.add_argument("id")
    s.add_argument("--kind", default="replicated",
                   choices=["adversarial", "replicated", "sourced", "derived", "agree", "refuted"])
    s.add_argument("--text", default="")
    s.add_argument("--prior", default="", help="a node you published before seeing the claim")
    s.add_argument("--artifact", default="", help="command/config that replays your check")
    add(s)
    s.set_defaults(fn=cmd_reply)

    s = sub.add_parser("claim", help="lease a question (atomic, TTL)")
    s.add_argument("question")
    s.add_argument("--ttl", type=int, default=30)
    add(s)
    s.set_defaults(fn=cmd_claim)

    s = sub.add_parser("release", help="release your lease")
    s.add_argument("question")
    s.add_argument("--status", default="done", choices=["done", "abandoned"])
    add(s)
    s.set_defaults(fn=cmd_release)

    s = sub.add_parser("claims", help="live leases (local claims.json view)")
    add(s, author=False)
    s.set_defaults(fn=cmd_claims)

    s = sub.add_parser("tasks", help="open obligations (§2.2)")
    s.add_argument("--all", action="store_true")
    add(s, author=False)
    s.set_defaults(fn=cmd_tasks)

    s = sub.add_parser("task", help="task helpers")
    tsub = s.add_subparsers(dest="taskcmd", required=True)
    t = tsub.add_parser("list", help="open tasks")
    t.add_argument("--all", action="store_true")
    add(t, author=False)
    t.set_defaults(fn=cmd_tasks)

    s = sub.add_parser("verify", help="re-hash refs (§2.4)")
    s.add_argument("--node", default="")
    s.add_argument("--all", action="store_true")
    add(s, author=False)
    s.set_defaults(fn=cmd_verify)

    s = sub.add_parser("since", help="what is new after a cursor (§10.4)")
    s.add_argument("cursor", nargs="?", type=int, default=0)
    s.add_argument("--topics", default="")
    add(s, author=False)
    s.set_defaults(fn=cmd_since)
    return p


def main(argv=None) -> int:
    a = build_parser().parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
