#!/usr/bin/env python3
"""Corallium — append-only knowledge graph. See PROTOCOL.md. (v0.1.1)

Nodes are immutable files: nodes/<id>.json
Edges live inside nodes (parents + rel). No database; scan is fast enough.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(os.environ.get("CORALLIUM_HOME", Path.home() / "corallium"))
NODES = ROOT / "nodes"
CLAIMS = ROOT / "claims.json"

WEIGHTS = {"replicated": 3, "sourced": 2, "derived": 1, "agree": 0, "refuted": -3}
RELS = {"extends", "supersedes", "merges", "root"}


# ── helpers ────────────────────────────────────────────────────────────────
def now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def author(default: str = "hermes") -> str:
    return os.environ.get("CORAL_AUTHOR") or default


def load_all() -> list[dict]:
    if not NODES.is_dir():
        return []
    out = []
    for f in sorted(NODES.glob("*.json")):
        try:
            out.append(json.loads(f.read_text()))
        except Exception as e:  # a corrupt node must be visible, not silently skipped
            print(f"WARN corrupt node {f.name}: {e}", file=sys.stderr)
    return out


def node_id(s: str, p: str, o: str, rel: str, parents: list[str]) -> str:
    blob = json.dumps([s, p, o, rel, sorted(parents)], ensure_ascii=False)
    return hashlib.sha256(blob.encode()).hexdigest()[:12]


def save_node(n: dict) -> Path:
    NODES.mkdir(parents=True, exist_ok=True)
    path = NODES / f"{n['id']}.json"
    path.write_text(json.dumps(n, ensure_ascii=False, indent=2))
    return path


def score(n: dict) -> int:
    return sum(WEIGHTS.get(e.get("kind", "agree"), 0) for e in n.get("evidence", []))


def fmt(n: dict, wide: bool = False) -> str:
    head = f"{n['id']}  [{n.get('status','open')}]  {n.get('author','?')}  {n['ts'][:10]}"
    body = f"{n.get('s','')} · {n.get('p','')} = {n.get('o','')}"
    sc = score(n)
    line = f"{head}\n  {body}"
    if sc:
        line += f"   (score {sc:+d})"
    if n.get("note"):
        line += f"\n  {n['note']}"
    if wide:
        if n.get("question"):
            line += f"\n  Q: {n['question']}"
        if n.get("parents"):
            line += f"\n  parents: {', '.join(n['parents'])} ({n.get('rel','?')})"
        for e in n.get("evidence", []):
            line += f"\n  · {e.get('kind')} by {e.get('author','?')}: {e.get('ref','')}"
    return line


def children_map(nodes: list[dict]) -> dict[str, list[str]]:
    cm: dict[str, list[str]] = {}
    for n in nodes:
        for p in n.get("parents", []):
            cm.setdefault(p, []).append(n["id"])
    return cm


# ── commands ───────────────────────────────────────────────────────────────
def cmd_add(a) -> int:
    parents = a.parent or []
    rel = a.rel
    if rel in ("extends", "supersedes") and len(parents) != 1:
        print("error: extends/supersedes 需要恰好 1 个 --parent", file=sys.stderr)
        return 2
    if rel == "merges" and len(parents) != 2:
        print("error: merges 需要恰好 2 个 --parent", file=sys.stderr)
        return 2
    if rel == "root" and parents:
        print("error: root 不能有 parent", file=sys.stderr)
        return 2

    nodes = load_all()
    known = {n["id"] for n in nodes}
    for p in parents:
        if p not in known:
            print(f"error: 父节点 {p} 不存在", file=sys.stderr)
            return 2

    nid = node_id(a.s, a.p, a.o, rel, parents)
    existing = next((n for n in nodes if n["id"] == nid), None)
    if existing:
        print(f"already exists: {nid}")
        print(fmt(existing, wide=True))
        return 0

    node = {
        "id": nid, "ts": now(), "author": a.author or author(),
        "type": a.type, "s": a.s, "p": a.p, "o": a.o,
        "rel": rel, "parents": parents, "status": a.status,
        "evidence": [{"kind": a.kind, "ref": a.ref or "", "author": a.author or author(), "ts": now()}],
    }
    if a.question:
        node["question"] = a.question
    if a.note:
        node["note"] = a.note

    path = save_node(node)
    print(f"added {nid} -> {path.relative_to(ROOT)}")
    print(fmt(node))
    return 0


def cmd_link(a) -> int:
    nodes = {n["id"]: n for n in load_all()}
    if a.src not in nodes or a.dst not in nodes:
        missing = [x for x in (a.src, a.dst) if x not in nodes]
        print(f"error: 节点不存在 {missing}", file=sys.stderr)
        return 2
    rel = a.rel
    if rel == "contradicts":
        # 对立关系是对称的，但要两边都能查到：写在较新节点上即可
        target = nodes[a.dst]
        target.setdefault("contradicts", [])
        if a.src not in target["contradicts"]:
            target["contradicts"].append(a.src)
            if a.dst in (nodes[a.src].get("contradicts") or []):
                print("already linked")
                return 0
            save_node(target)
            print(f"linked {a.dst} contradicts {a.src}")
        else:
            print("already linked")
        return 0
    print(f"error: 未知 rel {rel}", file=sys.stderr)
    return 2


def cmd_show(a) -> int:
    nodes = {n["id"]: n for n in load_all()}
    n = nodes.get(a.id)
    if not n:
        print(f"not found: {a.id}", file=sys.stderr)
        return 1
    print(json.dumps(n, ensure_ascii=False, indent=2))
    cm = children_map(list(nodes.values()))
    kids = cm.get(a.id, [])
    if kids:
        print("\nchildren:")
        for k in kids:
            print(f"  {k}  {nodes[k].get('rel')}  {nodes[k].get('o','')[:60]}")
    return 0


def _match(nodes: list[dict], q: str) -> list[dict]:
    q = q.lower()
    return [n for n in nodes if q in " ".join(
        str(n.get(k, "")) for k in ("s", "p", "o", "question", "note")).lower()]


def cmd_search(a) -> int:
    hits = sorted(_match(load_all(), a.query), key=lambda n: n["ts"], reverse=True)
    if not hits:
        print("no match")
        return 1
    for n in hits[: a.limit]:
        print(fmt(n))
        print()
    print(f"{len(hits)} match(es)")
    return 0


def cmd_tip(a) -> int:
    nodes = load_all()
    hits = _match(nodes, a.topic) if a.topic else nodes
    if not hits:
        print("no match")
        return 1
    ids = {n["id"] for n in hits}
    has_child = {p for n in nodes for p in n.get("parents", []) if p in ids}
    tips = [n for n in hits if n["id"] not in has_child and n.get("status") != "withdrawn"]
    if not tips:
        tips = hits
    tips.sort(key=lambda n: n["ts"], reverse=True)
    contested = [n for n in tips if n.get("status") == "contested"]
    for n in tips[:5]:
        print(fmt(n, wide=True))
        print()
    if contested:
        print(f"!! {len(contested)} contested — 存在未裁决的分歧")
    return 0


def cmd_list(a) -> int:
    nodes = load_all()
    if a.status:
        nodes = [n for n in nodes if n.get("status") == a.status]
    if a.type:
        nodes = [n for n in nodes if n.get("type") == a.type]
    for n in sorted(nodes, key=lambda x: x["ts"], reverse=True)[: a.limit]:
        print(fmt(n))
    print(f"{len(nodes)} node(s)")
    return 0


def _claims() -> dict:
    if CLAIMS.exists():
        try:
            return json.loads(CLAIMS.read_text())
        except Exception:
            return {}
    return {}


def cmd_claim(a) -> int:
    claims = _claims()
    t = time.time()
    # 过期租约自动释放
    for k in list(claims):
        if claims[k].get("lease_until", 0) < t and claims[k].get("status") == "claimed":
            claims[k]["status"] = "expired"
    key = hashlib.sha256(a.question.strip().lower().encode()).hexdigest()[:12]
    cur = claims.get(key)
    if cur and cur.get("status") == "claimed":
        print(f"busy: {cur['author']} 已认领，租约至 {datetime.fromtimestamp(cur['lease_until']).strftime('%H:%M')}")
        return 1
    claims[key] = {"question": a.question, "author": a.author or author(),
                   "ts": now(), "lease_until": t + a.ttl * 60, "status": "claimed"}
    CLAIMS.write_text(json.dumps(claims, ensure_ascii=False, indent=2))
    print(f"claimed {key} for {a.ttl} min: {a.question}")
    return 0


def cmd_release(a) -> int:
    claims = _claims()
    key = hashlib.sha256(a.question.strip().lower().encode()).hexdigest()[:12]
    if key not in claims:
        print("not claimed")
        return 1
    claims[key]["status"] = a.status
    claims[key]["lease_until"] = 0
    CLAIMS.write_text(json.dumps(claims, ensure_ascii=False, indent=2))
    print(f"{key} -> {a.status}")
    return 0


def cmd_review(a) -> int:
    nodes = {n["id"]: n for n in load_all()}
    n = nodes.get(a.id)
    if not n:
        print(f"not found: {a.id}", file=sys.stderr)
        return 1
    who = a.author or author()
    if who == n.get("author"):
        print("error: 作者不能给自己投票 (PROTOCOL §3)", file=sys.stderr)
        return 2
    if any(e.get("author") == who for e in n.get("evidence", [])):
        print("error: 你已经投过票了", file=sys.stderr)
        return 2
    n.setdefault("evidence", []).append(
        {"kind": a.kind, "ref": a.note or "", "author": who, "ts": now()})
    if a.verdict == "down" and n.get("status") != "contested":
        n["status"] = "contested"
    save_node(n)
    print(f"reviewed {a.id} ({a.kind}, {a.verdict}) -> score {score(n):+d}, status {n.get('status')}")
    return 0


def cmd_log(a) -> int:
    nodes = sorted(load_all(), key=lambda n: n["ts"], reverse=True)[: a.limit]
    for n in nodes:
        print(f"{n['ts'][:16]}  {n['id']}  {n.get('rel','?'):10s} {n.get('s','')} · {n.get('p','')}")
    return 0


# ── cli ────────────────────────────────────────────────────────────────────
def main() -> int:
    p = argparse.ArgumentParser(prog="coral", description="Corallium — append-only knowledge graph")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--author", default=None)

    a1 = sub.add_parser("add", help="新增节点")
    a1.add_argument("--s", required=True)
    a1.add_argument("--p", required=True)
    a1.add_argument("--o", required=True)
    a1.add_argument("--rel", default="root", choices=sorted(RELS))
    a1.add_argument("--parent", action="append")
    a1.add_argument("--kind", default="sourced", choices=list(WEIGHTS))
    a1.add_argument("--ref", default="")
    a1.add_argument("--type", default="claim", choices=["claim", "anomaly", "question", "decision"])
    a1.add_argument("--status", default="open",
                    choices=["open", "confirmed", "contested", "withdrawn", "dead_end"])
    a1.add_argument("--question", default="")
    a1.add_argument("--note", default="")
    common(a1)
    a1.set_defaults(fn=cmd_add)

    a2 = sub.add_parser("link", help="补边")
    a2.add_argument("src")
    a2.add_argument("dst")
    a2.add_argument("--rel", default="contradicts", choices=["contradicts"])
    common(a2)
    a2.set_defaults(fn=cmd_link)

    a3 = sub.add_parser("show", help="看单个节点")
    a3.add_argument("id")
    a3.set_defaults(fn=cmd_show)

    a4 = sub.add_parser("search", help="全文查")
    a4.add_argument("query")
    a4.add_argument("--limit", type=int, default=10)
    a4.set_defaults(fn=cmd_search)

    a5 = sub.add_parser("tip", help="某主题当前结论")
    a5.add_argument("topic", nargs="?", default="")
    a5.set_defaults(fn=cmd_tip)

    a6 = sub.add_parser("list", help="列出节点")
    a6.add_argument("--status", default=None)
    a6.add_argument("--type", default=None)
    a6.add_argument("--limit", type=int, default=20)
    a6.set_defaults(fn=cmd_list)

    a7 = sub.add_parser("claim", help="认领任务（防重复）")
    a7.add_argument("question")
    a7.add_argument("--ttl", type=int, default=30, help="租约分钟数")
    common(a7)
    a7.set_defaults(fn=cmd_claim)

    a8 = sub.add_parser("release", help="释放认领")
    a8.add_argument("question")
    a8.add_argument("--status", default="done", choices=["done", "abandoned"])
    a8.set_defaults(fn=cmd_release)

    a9 = sub.add_parser("review", help="投票")
    a9.add_argument("id")
    a9.add_argument("--verdict", default="up", choices=["up", "down"])
    a9.add_argument("--kind", default="replicated", choices=list(WEIGHTS))
    a9.add_argument("--note", default="")
    common(a9)
    a9.set_defaults(fn=cmd_review)

    a10 = sub.add_parser("log", help="最近活动")
    a10.add_argument("--limit", type=int, default=15)
    a10.set_defaults(fn=cmd_log)

    args = p.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
