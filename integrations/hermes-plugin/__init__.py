"""Corallium tools for Hermes Agent.

Wiring goes only through the public plugin API (`ctx.register_tool`). The plugin
holds no state and no daemon: every call is an HTTP request to the coral server, or
a read of the coral directory when the server is down.

Tool ordering matters more than the tool bodies. An agent reaches for `web_search`
by reflex; if `coral_find` is not named in that reflex, the knowledge base is dead
weight. Every description therefore starts with the same sentence: check here first.
"""

from __future__ import annotations

from . import client as _c

TOOLSET = "corallium"

_STR = {"type": "string"}
_INT = {"type": "integer"}
_ARR = {"type": "array", "items": _STR}


def _schema(name: str, description: str, props: dict, required: tuple = ()) -> dict:
    return {"name": name, "description": description,
            "parameters": {"type": "object", "properties": props, "required": list(required)}}


FIND_SCHEMA = _schema(
    "coral_find",
    "Check the knowledge coral BEFORE running a web search or starting fresh research. "
    "Returns matching claims with their derived status (open/confirmed/contested/suspect/"
    "withdrawn), score, significance and notes. If a hit settles the question, cite its id "
    "instead of researching again.",
    {"q": {**_STR, "description": "Free text; matched against subject, predicate, object, note, topics"},
     "coral": {**_STR, "description": "Coral name; defaults to the configured one"},
     "limit": _INT,
     "sort": {"type": "string", "enum": ["recent", "score", "significance"]}},
    ("q",))

PUBLISH_SCHEMA = _schema(
    "coral_publish",
    "Record a finding in the coral as one falsifiable proposition, at the moment you find it "
    "(not at the end of the task — batching means every parallel agent duplicates you). "
    "Duplicate propositions merge by content hash, so publishing twice is safe. Fails loudly "
    "if the server is down; never assume it was saved. "
    "BEFORE WRITING: (1) run coral_find and link what you build on — rel=extends/supersedes/merges "
    "with parents=[id]; rel=root only for a genuinely new branch, and then say in note what existing "
    "node failed to cover it. (2) One falsifiable proposition per node; a status report ('X is "
    "implemented', 'tests pass') belongs on the task node it closes, not as a claim. (3) Always give "
    "method (command/config/version/seed) and refs (absolute paths). (4) topics from the fixed list: "
    "design, protocol, storage, retrieval, votes, plugin, cli, server, bench, org, deploy, experiment. "
    "(5) While you work: a duplicate, a record-shaped node, a wrong parent or an unreachable conclusion "
    "is a `suggestion` node (type=suggestion, action=merge|retire|link|split|reindex, targets=[ids], "
    "o = what breaks if nothing is done) — never a silent merge or retire, so the judgement stays refutable. "
    "Full regulation: ~/.hermes/plugins/corallium/GUIDE.md.",
    {"s": {**_STR, "description": "Subject, e.g. MQSim or Corallium"},
     "p": {**_STR, "description": "Predicate, e.g. 128K_seq_write"},
     "o": {**_STR, "description": "Object: the number or conclusion itself"},
     "coral": _STR,
     "rel": {"type": "string", "enum": ["root", "extends", "supersedes", "merges"]},
     "parents": {**_ARR, "description": "Node ids this builds on or replaces"},
     "type": {"type": "string", "enum": ["claim", "anomaly", "question", "decision", "task", "suggestion", "link"]},
     "action": {"type": "string", "enum": ["merge", "retire", "link", "split", "reindex"],
                "description": "suggestion only: the maintenance action you propose"},
     "targets": {**_ARR, "description": "suggestion only: node ids the proposed action would touch"},
     "topics": _ARR,
     "significance": {**_INT, "description": "1-5: how much it would matter if true (not how sure you are)"},
     "note": {**_STR, "description": "Caveats, or for anomalies: prediction / observation / deviation / probe"},
     "method": {**_STR, "description": "Exact command, config, version, seed — required for a vote to count"},
     "refs": {"type": "array",
              "items": {"type": "object",
                        "properties": {"path": _STR, "node": _STR, "lines": _STR}},
              "description": "Files backing this claim; the server hashes them at write time"},
     "force": {"type": "boolean",
               "description": "Third exit of the write gate: the proposition really is new despite the "
                              "similarity. Requires why_new. Use only after reading the 409 candidates."},
     "why_new": {**_STR,
                 "description": "Required with force: one sentence on how this differs from the candidate "
                                "(e.g. 'this is write-time hashing, the candidate is read-time checking')"},
     "attempts": {**_INT,
                  "description": "How many configurations/hypotheses were tried before this result (§4.6). "
                                 "Reported as `tried N`; >5 needs an adversarial vote to reach confirmed. "
                                 "Omit only if it really was the first attempt"},
     "volatility": {"type": "string", "enum": ["static", "slow", "fast", "live"],
                    "description": "How fast this fact expires: static (never) / slow (180d) / fast (1d) / live (1h)"}},
    ("s", "p", "o"))

TIP_SCHEMA = _schema(
    "coral_tip",
    "Ask what is currently believed about a topic: the leaf nodes of the graph, with contested "
    "and suspect ones flagged. Use it when opening a topic rather than reading the whole history.",
    {"topic": {**_STR, "description": "Topic filter, e.g. MQSim or write-path"},
     "coral": _STR})

VOTE_SCHEMA = _schema(
    "coral_vote",
    "Vote on an existing node: replicated (you reproduced it), adversarial (you tried to break it "
    "and failed), refuted (you found a counter-case), sourced, derived, redundant (it already exists "
    "elsewhere; needs artifact = the node id), or junk (an accusation of error; licenses deletion). "
    "`agree` weighs 0 — do not vote agree. Full weight needs an independent `prior` node published "
    "before you saw the claim, plus an artifact or method.",
    {"node": {**_STR, "description": "Node id being judged"},
     "kind": {"type": "string", "enum": ["replicated", "adversarial", "refuted", "sourced",
                                          "derived", "redundant", "junk", "agree"]},
     "text": {**_STR, "description": "What you actually did, in one or two sentences"},
     "prior": {**_STR, "description": "Id of a node you published before reading this claim"},
     "artifact": {**_STR, "description": "Command, script+commit, dataset hash or seed that replays it"},
     "coral": _STR},
    ("node", "kind", "text"))

STATE_SCHEMA = _schema(
    "coral_state",
    "Catch up on a coral: what is new since a cursor, and which tasks are still open. Run this at "
    "the start of a session on a topic already in the coral.",
    {"cursor": {**_INT, "description": "Last seq you saw; 0 means from the beginning"},
     "topic": _STR,
     "coral": _STR})

CLAIM_SCHEMA = _schema(
    "coral_claim",
    "Take a lease on one question before starting a multi-step investigation, so a parallel agent "
    "skips it instead of duplicating the work. Question-granular (§7.1), always with a TTL. "
    "If acquired=false, do not research it — the response names the holder. Release when done "
    "with coral_release.",
    {"question": {**_STR, "description": "The exact question, e.g. 'why is 128K seq write only 571 MB/s'"},
     "ttl": {**_INT, "description": "Lease minutes; default 30. 0 releases (use coral_release)"},
     "coral": _STR},
    ("question",))

RELEASE_SCHEMA = _schema(
    "coral_release",
    "Give a question lease back when the investigation is done or abandoned. Holder-only: another "
    "agent's live lease is not yours to drop. If abandoned, publish a dead_end/decision node saying "
    "why first — the next agent must not walk into the same hole (§7.2).",
    {"question": {**_STR, "description": "The exact question string used in coral_claim"},
     "coral": _STR},
    ("question",))


def _h_find(args, **kw):
    return _c.find(str(args.get("q") or ""), coral=str(args.get("coral") or ""),
                   limit=int(args.get("limit") or 5), sort=str(args.get("sort") or "recent"))


def _h_publish(args, **kw):
    return _c.publish(
        str(args.get("s") or ""), str(args.get("p") or ""), str(args.get("o") or ""),
        coral=str(args.get("coral") or ""), rel=str(args.get("rel") or "root"),
        parents=args.get("parents") or [], type=str(args.get("type") or "claim"),
        topics=args.get("topics") or [], significance=int(args.get("significance") or 3),
        note=str(args.get("note") or ""), method=str(args.get("method") or ""),
        refs=args.get("refs") or [], force=bool(args.get("force")),
        why_new=str(args.get("why_new") or ""),
        attempts=(int(args["attempts"]) if args.get("attempts") is not None else None),
        volatility=str(args.get("volatility") or ""),
        action=str(args.get("action") or ""), targets=args.get("targets") or [])


def _h_tip(args, **kw):
    return _c.tip(str(args.get("topic") or ""), coral=str(args.get("coral") or ""))


def _h_vote(args, **kw):
    return _c.vote(str(args.get("node") or ""), str(args.get("kind") or ""),
                   str(args.get("text") or ""), coral=str(args.get("coral") or ""),
                   prior=str(args.get("prior") or ""), artifact=str(args.get("artifact") or ""))


def _h_state(args, **kw):
    return _c.state(coral=str(args.get("coral") or ""),
                    cursor=int(args.get("cursor") or 0),
                    topic=str(args.get("topic") or ""))


def _h_claim(args, **kw):
    return _c.claim(str(args.get("question") or ""), coral=str(args.get("coral") or ""),
                    ttl=int(args.get("ttl") if args.get("ttl") is not None else 30))


def _h_release(args, **kw):
    return _c.release(str(args.get("question") or ""), coral=str(args.get("coral") or ""))


_TOOLS = (
    ("coral_find", FIND_SCHEMA, _h_find, "🪸"),
    ("coral_publish", PUBLISH_SCHEMA, _h_publish, "✍️"),
    ("coral_tip", TIP_SCHEMA, _h_tip, "🌊"),
    ("coral_vote", VOTE_SCHEMA, _h_vote, "⚖️"),
    ("coral_state", STATE_SCHEMA, _h_state, "🔄"),
    ("coral_claim", CLAIM_SCHEMA, _h_claim, "🔒"),
    ("coral_release", RELEASE_SCHEMA, _h_release, "🔓"),
)


def _check_env() -> bool:
    """Always available: with no server it still reads the coral folder, and a failed
    publish is a real error the model should see rather than a hidden tool."""
    return True


def register(ctx) -> None:
    for name, schema, handler, emoji in _TOOLS:
        ctx.register_tool(name=name, toolset=TOOLSET, schema=schema, handler=handler,
                          check_fn=_check_env, emoji=emoji)


__all__ = ["register"]
