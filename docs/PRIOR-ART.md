# Prior art, and what it would change here

Written 2026-11-06, after a literature search prompted by "is this even new?".
Short answer: **the mechanism is forty years old and the concept is not novel in 2026.** What is not
taken is the specific combination — a *required* falsifier at write time plus retraction that
cascades to dependents — and, more importantly, a working installable artifact.

Every claim below is also a node in the `corallium` coral (`prior-art:*`), so it can be attacked.

## What exists

| Source | What it already solved | Where Corallium stands now |
|---|---|---|
| Doyle 1979, *A Truth Maintenance System*, AI 12(3):231-272 | justifications: a belief records **why** it is held, and retraction follows use | `parents` is an untyped id list; no AND/OR structure (`corallium/derive.py:81-92`) |
| Alchourrón, Gärdenfors, Makinson 1985, JSL 50(2):510-530 (AGM) | contraction = *minimal* change that restores consistency | every descendant of a withdrawn premise is flagged `suspect`; no minimality criterion |
| Dung 1995, *On the acceptability of arguments*, AI 77(2):321-357 | semantics over attack graphs (grounded, preferred, stable) | status from vote kinds + rival lookup (`derive.py:94-126`); no semantics, no uniqueness guarantee |
| arXiv 2606.00007, *Deliberative Curation* (2026) | reputation-weighted voting beats majority under adversarial agents: 0.807 vs 0.740 under stress, p<0.001, 100 agents, 30 seeds | every vote weighs the same |
| Zenodo 10.5281/zenodo.21535416, *Mnesion* (2026) | consensus over organisational rules; working deployment for 5-10 agents | no cross-coral layer at all |
| arXiv 2501.13956, Zep / Graphiti | temporal validity on edges (`valid_at` / `invalid_at`) | nodes carry `ts` and `seq` only — "no longer believed" and "never believed" look the same in history |
| Mem0, Cognee, Letta (~160k stars combined) | production memory layers, conflict resolution at write time | different product: they remember, they do not adjudicate claims |

Commercial products (Zep, Mem0) already invalidate old facts. The earlier line "no memory plugin
can do this" was wrong and is withdrawn. What Zep does not do is mark the **dependents** of the
invalidated fact as suspect, and it does not ask the author what would refute the claim.

## What each one would actually change

Ordered by cost, not by elegance.

**1. Validity intervals (Zep) — advisory, ~3h.**
Add optional `valid_at` / `invalid_at` to a node; derive nothing from them. The graph can then
answer "when did we stop believing this" without replaying `seq`. Additive, cannot break status
derivation. *Do this one first: it is a pure read-side gain.*

**2. A grounded-extension view (Dung) — ~1 day.**
Do **not** replace the vote rule. Add `GET /semantics` that computes the grounded extension over
`attack` edges (`refutes`, `adversarial` reviews) and shows it next to the vote-derived status.
Where the two disagree, that disagreement is the interesting object. Replacing the decision rule
outright would invalidate every status in the existing 469-node graph.

**3. Justification types (JTMS) — ~1-2 days, changes retraction.**
Today a node with three premises dies when *any* premise dies. A justification would separate
`AND` (all premises needed) from `OR` (any alternative suffices), so retraction follows actual use.
This is the single biggest semantic improvement available, and the riskiest: it changes the status
of existing nodes, so it needs a migration pass and a before/after diff.

**4. Vote weighting (Deliberative Curation) — ~1 day, but needs a simulation first.**
Weight a vote by the voter's own replication record. The paper's numbers come from simulation, not
from a live coral; adopting the weight without reproducing that simulation would be exactly the
kind of unearned change the protocol is supposed to reject. Publish the simulation as the node,
then decide.

**5. Federation / export-import (Mnesion) — weeks.**
Out of scope before 2026-11-11. Recorded so the README never implies it.

## What not to copy

- **AGM in full.** AGM assumes a logically closed belief set; Corallium stores a finite set of
  claims with no closure operator. Borrowing "minimal change" as a design constraint is useful;
  implementing AGM would require committing to a logic, which is a different project.
- **Reputation as a gate.** Weighting votes changes outcomes silently. If it is added, the
  unweighted count must stay visible, or the graph becomes unauditable.
- **Temporal invalidation as deletion.** Graphiti invalidates edges; it does not erase them. Any
  temporal field added here must be additive for the same reason.

## The honest positioning

Not "a new idea". The defensible sentence is: **the protocol implemented in full — falsifier
required, adversarial review, cascade to dependents — installable, with reads that keep working
when the server is down and writes that fail loudly instead of being dropped.**

## Coral nodes (the graph, not this file)

Each paper is a node, so a claim can cite it without leaving the coral. The DOI is in the
`o` field and the falsifier is "the DOI resolves elsewhere" — the citation itself is checkable.

| paper | node | cited by |
|---|---|---|
| Doyle 1979 | `e2109db257dc` | `21d074998c62` (parents lacks justification typing) |
| AGM 1985 | `0381a07375fd` | `194dcbc667a5` (no minimal change) |
| Dung 1995 | `c9acc08cf507` | `a68294e9a85f` (no argumentation semantics) |
| Deliberative Curation 2026 | `36baaaaaf263` | `3ad8e3c46de0`, `6fbff5d8ffd4`, `6cd4b211c521` |

Edges are `link` nodes with `p=cites`; they change no status, by design (§20.2).
