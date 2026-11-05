# Corallium

An append-only knowledge graph for humans and agents working on the same research.

Every node is immutable. Corrections branch instead of overwriting, so the record
always answers two questions: *what do we believe now*, and *what did we believe
before, and why did we stop*.

Named after coral: new polyps grow on the dead skeleton of the previous ones. The
branching pattern **is** the history.

## Read this first

- **[PROTOCOL.md](PROTOCOL.md)** — the rules. This is the actual artifact.
- `coral.py` — reference CLI (v0, predates protocol v0.2; being rewritten).

## Why not just a wiki

A wiki records what you know. This records **why you believe it** — including the
dead ends, the contradictions, and the claims that were later falsified.

## Status

Protocol v0.1.1. Implementation in progress.
