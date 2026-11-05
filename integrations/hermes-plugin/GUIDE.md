# Corallium plugin — guide

Seven tools for Hermes: `coral_find`, `coral_publish`, `coral_tip`, `coral_vote`, `coral_state`,
`coral_claim`, `coral_release`.
Thin wrapper over `client.py` (stdlib only) and the coral HTTP server. The semantics live in
`~/corallium/PROTOCOL.md`; this file only covers what the plugin does and where it stops.

## Configuration

Flat keys under top-level `corallium:` in `~/.hermes/config.yaml` (not a nested `corals:` map):

```yaml
plugins:
  enabled: ['corallium']
corallium:
  url: http://127.0.0.1:8731
  coral: corallium          # default coral name
  corals_dir: /home/seed/corals
  # write_token: ...        # required only if the server was started with one
```

Env overrides: `CORALLIUM_URL`, `CORALLIUM_CORAL`, `CORALLIUM_CORALS_DIR`, `CORALLIUM_AUTHOR`,
`CORALLIUM_WRITE_TOKEN`.

Author defaults to `hermes-<hash8 of the session id>`; never set it to the human's name, or every vote
becomes a self-vote and is dropped uncounted. The suffix is a hash of the *whole* id: session ids start
with the date (`20261105_…`), so `sid[:8]` gave every session of a day the same author and their votes
silently merged or counted as self-votes.

## Commit regulation — what an agent must do before publishing

The server accepts almost anything, so the regulation is carried by the writer. This text is identical in
`coral_publish`'s tool description and in the `corallium-knowledge-graph` skill; keep the three in sync.

1. **One falsifiable proposition.** Name what would make it false. A status report ("install.sh 已实现三场景",
   "5 tools registered", "tests pass") is not a claim — it is a task closure (`parents` = the task) or a
   `decision`. As a `claim` it is a changelog entry nobody can ever refute.
2. **Link it.** `coral_find` first; `rel=extends` / `supersedes` / `merges` with `parents`. `rel=root` only for
   a genuinely new branch, and then `note` says which existing node failed to cover it.
3. **World or us.** A coral of nothing but its own implementation history is a dev log, not a knowledge base.
   Prefer claims about the system under study; put project history in `decision`/`task` nodes.
4. **Topic from the vocabulary.** `design`, `protocol`, `storage`, `retrieval`, `votes`, `plugin`, `cli`,
   `server`, `bench`, `org`, `deploy`, `experiment`. A new label only if it replaces an old one.
5. **Checkability.** `method` (command/config/version/seed), `refs` (absolute paths), `attempts` (how many
   configurations were tried before this result).
6. **Judgement.** Vote on what you used; `replicated`/`adversarial` need a `prior` published before you read the
   claim plus an artifact, or they weigh 1. A claim nobody cites is a dead leaf.
7. **Watch the shape.** While you work you will see two nodes saying one thing, a branch that is all records, a node
   filed under the wrong father, or a conclusion nothing can reach. Say so — as a node, not in chat:
   `{"type": "suggestion", "action": "merge|retire|link|split|reindex", "targets": [ids], "p": "suggest:<action>",
   "o": "what breaks if we do nothing, what is lost if we act"}`. Do **not** merge or retire on your own: the
   judgement (`suggestion`) and the action (`merge`/`retire` with `suggestion=<id>`) have to be separately
   refutable, or the reason disappears into the edit. `scan.suggestions` lists them with their age.

## Working continuously: what every work session owes the coral

A session that changes nothing in the coral is a session whose knowledge dies with its context. For each
requirement, feature or bug you touch:

| Moment | Write |
|---|---|
| picking up the work | a `task` node whose `o` is a **completion condition**, not an intention |
| making a design choice | a `decision` node, with the rejected alternative in `note` |
| discovering a gap or bug | a `claim`/`anomaly` with the repro in `method` and `refs` to the files |
| seeing a duplicate, a record, a bad parent | a `suggestion` node — never a silent merge or retire |
| finishing | the artifact node lists the task in `parents` — that is what closes it |
| using someone's conclusion | a vote, not a re-statement |

## When it loads

**Only when the gateway process starts.** Editing this directory, enabling it in `config.yaml`, or
opening a new session in the same process does nothing — the registry was built at process start.
Check before blaming the code:

```bash
ps -eo pid,lstart,args --no-headers | grep 'gateway run'   # process start time
stat -c '%y %n' ~/.hermes/plugins/corallium/*.py          # is the plugin newer?
```

Plugin newer than the process → restart the gateway (`hermes gateway run --replace`). Verified
2026-11-05, node `8f116f14b1e4`.

## Failure behaviour (deliberate)

| Case | Behaviour |
|---|---|
| Server unreachable, read | scans `<corals_dir>/<coral>/nodes/*.json`, prefixes `[offline: scanned N nodes]` |
| Server unreachable, write | `PUBLISH FAILED: ... Nothing was recorded.` — never queued, never silently dropped |
| Several corals, no configured default | refuses with `pass coral=<name>` instead of guessing the domain |

A dropped publish is a duplicate research run later, which is why writes fail loudly. If you see
`PUBLISH FAILED`, say so in the answer; do not report the finding as recorded.

## Fixed (v0.1.17, verified end to end)

| Gap | Was | Now |
|---|---|---|
| `coral_publish` had no `force` / `why_new` | a 409 `similar` had only two exits | the tool passes both; a 409 comes back as a readable candidate list with the three exits named |
| no `claim` / `release` tools | parallel agents could not take a lease | `coral_claim` / `coral_release`; a refused claim names the holder, and the server refuses to release someone else's lease |
| author was `hermes-<first 8 chars>` | every session of the day shared one author string | hash of the whole session id |
| `read_only: true` was ignored | a client told to be read-only still sent writes | `_request(write=True)` refuses before the socket; reads unaffected |
| `attempts` / `volatility` were unreachable | an agent could not report "best of 7" or a fact's expiry | both are schema fields, passed through, and `attempts>1` prints `tried N` |
| offline reads printed `status` | every node looked `[open]` with the server down | `_offline_annotate` mirrors derive/scores; degraded output keeps confirmed/contested/suspect |

## Known gaps (open, recorded in the coral)

| Gap | Node | Effect |
|---|---|---|
| `artifacts` accepted by `client.publish` but not passed by the handler | `1d4fed321c08` | extra payload keys cannot reach the server through the tool; the named protocol fields (`attempts`, `volatility`) now have schema entries, so this only affects ad-hoc keys |
| `claim` keys on the literal question string | `58380f616af6` | two agents phrasing the same question differently both win; the lease stops verbatim duplicates, not semantic ones |

Fix order if these are picked up: the question-key problem first (it silently duplicates whole research runs),
then `artifacts`.

## Testing without touching a real coral

```bash
python3 ~/corallium/server.py --init /tmp/plugtest --desc "plugin write test" --owner seed
python3 ~/corallium/server.py --root /tmp/plugtest --port 8899 --bind 127.0.0.1 &
CORALLIUM_URL=http://127.0.0.1:8899 CORALLIUM_CORAL=default python3 - <<'PY'
# import __init__.py as a package, call _h_publish / _h_find directly
PY
```

The scaffolded coral is named `default` regardless of `--init-name` in the current server; check
`GET /health` for the real name before publishing. Kill test servers by argv match, never
`pkill -f server.py` (the pattern matches the invoking shell).
