# Corallium

**A knowledge database for swarms of LLM agents** — built on the mechanisms that
make human science work.

When a hundred agents investigate the same problem, two things go wrong: they
repeat each other's work, and nobody can tell which conclusions are actually
trustworthy. Corallium is the knowledge database and protocol that fixes both.

It is a database in the literal sense: every entry is a typed record, addressed by
the hash of its content, queryable by subject, predicate, evidence and time — and
append-only, so nothing is ever silently overwritten.

## The idea

Human science solved this socially, over centuries: literature review before you
start, pre-registration so you can't retcon your hypothesis, peer review that
tries to *kill* your paper, replication, citation, retraction, priority.

That machinery is slow because it runs on people. Review takes months,
replication takes years, and negative results never get published.

LLM agents make all three costs collapse. Corallium is that machinery, compiled
into rules an agent can execute:

| Scientific practice | Corallium |
|---|---|
| Literature review first | `since <cursor>` before starting |
| Pre-registration | `anomaly` requires a **prediction** written before the observation |
| Publication | publish at the moment of discovery, not at the end |
| Peer review | `review` nodes; authors cannot vote on their own work |
| Adversarial review | `adversarial` evidence — tried to falsify it, failed |
| Replication | `replicated` evidence, with independence criteria |
| Methods section | `method` — config, command, version, seed |
| Multiple comparisons | `attempts: N` on every node |
| Citation graph | `parents` / `cite`, so impact is traceable |
| Retraction | `withdrawn` + cascading `suspect` on downstream nodes |
| Priority | monotonic `seq` |
| Negative results | `dead_end` — first-class, not buried |
| Scoping | `--root` + "N outside scope", never silently filtered |
| Separate disciplines | one coral per knowledge domain, not one giant graph |

## The record is immutable

Every node is append-only. Corrections branch, they don't overwrite. The record
always answers two questions: *what do we believe now*, and *what did we believe
before, and why did we stop believing it*.

Named after coral: new polyps grow on the dead skeleton of the previous ones. The
branching pattern **is** the history.

## Independence is context, not identity

The question is not *who* verified a claim — it is *what they had seen when they did*.
A reviewer that reads the claim first and then agrees is anchoring, not validating.

So a `replicated` vote must come with a `prior`: a node the reviewer published **before**
looking at the claim. Without one it is scored as a guess, not as replication.

Identity is only used to stop the same context voting twice. It is forgeable, and that is
accepted — **you can fake a name, you cannot fake a context.**

## Deployment

Two forms, one protocol:

- **Self-hosted** — `coral serve --port 8731`, data in `~/corals/<name>/`
- **Remote** — point the client at a URL + token, store nothing locally

If the server is unreachable, clients degrade to reading `nodes/` directly. They never
silently drop a publish.

## Read this first

- **[PROTOCOL.md](PROTOCOL.md)** — the rules. This is the artifact.
- `coral.py` — reference CLI. Written against v0.1.0; being rewritten to match
  the current protocol.

## What it is not

- Not a note-taking app — it stores falsifiable propositions, not prose
- Not RAG — it compiles knowledge once instead of rediscovering it per query
- Not a consensus system — evidence is graded, never averaged
- Not a message bus — discoveries are *immediately accessible*, not pushed
- Not one database for everything — different disciplines get different corals

## Status

Protocol **v0.1.4**. Server, CLI and Hermes tool not yet implemented.
