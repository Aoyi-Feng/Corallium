# Concurrency results — Corallium store

Measured 2026-11-05 ~14:05 CET against `server.py` v0.1.12,
sha256 `d02ea89653005c2d…` (hash identical before and after the published run).

```
python 3.14.8   sqlite 3.54.0   Linux 7.2.11-arch1-1   32 cores
server      a private instance on a random port, throwaway root under /tmp
clients     multiprocessing (fork) processes, NOT threads
transport   one TCP connection per request (what a fresh curl/agent does),
            except the explicitly-labelled keep-alive run
reproduce   python3 tests/concurrency_test.py
```

Every writer run starts all processes on a `multiprocessing.Barrier`, so the
first request of every process is in flight at the same moment. Latency is
measured inside each worker with `perf_counter`, so it includes TCP connect,
queue wait and response read — what an agent actually experiences. "lost"
below always means **no HTTP response at all** (`ConnectionResetError`,
i.e. the client cannot tell whether the write happened), never a 5xx.

## 1. Writer sweep — distinct claims

| writers | sent | 2xx | lost | 4xx | 5xx | seq gaps | rows = files | wall | writes/s | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|
| 8 | 1000 | 1000 | 0 | 0 | 0 | 0 | yes | 1.50 s | 668 | 11.1 ms | 19.4 ms | 22.4 ms |
| 32 | 992 | 979 | 13 (1.3%) | 0 | 0 | 0 | yes | 3.24 s | 306 | 13.2 ms | 26.2 ms | 1069 ms |
| 100 | 1000 | 928 | 72 (7.2%) | 0 | 0 | 0 | yes | 6.47 s | 155 | 16.2 ms | 1244 ms | 4295 ms |
| 100 keep-alive | 1000 | 961 | 39 (3.9%) | 0 | 0 | 0 | yes | 6.67 s | 150 | 44.8 ms | 1033 ms | 1288 ms |

* `seq` values returned were unique in every run (`seq_unique == seq_returned`),
  and the final store had **zero gaps and zero duplicate seq** across 5 187 nodes.
* `files_delta == rows_delta == 2xx` in every run: every request that got a 2xx
  produced exactly one file and one row; every lost request produced neither.
  No half-writes, no file/index divergence (`files_not_in_db = db_not_in_files = 0`).
* Total throughput **falls** as agents are added: 668 → 306 → 155 writes/s.
  Adding concurrency makes the system slower in aggregate, not just per request.

## 2. Accept path, zero store work — `GET /health`

`/health` only reads `max_seq`; it touches no queue, no file, no FTS. Same
barrier-synchronised burst:

| simultaneous new connections | ok | lost | p50 | p90 | p99 | > 500 ms |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 8 | 0 | 3.7 ms | 4.1 ms | 4.1 ms | 0 |
| 20 | 20 | 0 | 1039 ms | 1245 ms | 1246 ms | 12 |
| 50 | 50 | 0 | 1257 ms | 3095 ms | 3352 ms | 39 |
| 100 | 100 | 0 | 2068 ms | 5713 ms | 18 384 ms | 78 |

This is the load-bearing measurement in this document. With **no store work at
all**, 20 simultaneous agents already see a ~1 s median, and 100 see 2 s median
with an 18 s p99. The ~1 s / ~3 s / ~7 s / ~15 s / ~31 s plateaus are the Linux
SYN retransmission ladder: `socketserver.TCPServer.request_queue_size` is 5, and
`server.py` never overrides it, so `listen(5)` drops handshakes beyond a handful
of pending connections.

## 3. Idempotency — 50 processes, one proposition, 3 rounds

Same `s,p,o`, all 50 processes released together, node absent before the round
(id predicted locally from the documented hash so no probe creates it first):

| round | 201 created:true | 200 created:false | no response | distinct ids | files for id |
|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 28 | 21 | 1 | 1 |
| 1 | 1 | 31 | 18 | 1 | 1 |
| 2 | 1 | 38 | 11 | 1 | 1 |

The store invariant is exact: **exactly one node, one file, one id, in all three
rounds**, and the id matched the locally computed prediction. The protocol-level
claim "49 clients get `created:false`" did **not** hold: 11–21 of the 50 clients
got no response at all. They would be safe to retry (same hash → `created:false`),
but a client that does not retry cannot distinguish "someone else created it"
from "my publish vanished".

## 4. Claim contention — 20 processes, one question

| measurement | result |
|---|---|
| winners | **1 / 20** (`bench-c08`) |
| losers agreeing on holder | 19 / 19 |
| claim latency p50 / p99 | 1006 ms / 2032 ms |
| lost connections | 1 / 20 |
| same author re-claims own live lease | acquired:true (renewal, by design) |
| different author re-claims live lease | acquired:false, holder correct |
| handoff after expiry (`ttl=0`) | first:true, second:true |
| release endpoint | `POST /claims/<key>/release` → 404, `POST /claims/<key>` → 404, `POST /claims/release` → 404 |

Mutual exclusion itself is correct. Two operational holes:

* **There is no release over HTTP.** `coral.py release` only edits a local
  `claims.json`; the server exposes no way to drop a lease, so an agent that
  dies holds its question for the full TTL.
* **`ttl=0` is not a release, it is a stampede.** 20 processes claiming an
  immediately-expiring lease produced **14 winners** (8 and 11 in earlier runs).
  Every acquirer writes `lease_until = now` and returns `acquired:true`, so an
  expired claim can be owned by everyone at once.

### Concurrent claims on *different* questions — the worst result here

20 processes, 20 distinct questions, one barrier:

| acquired | HTTP 500 | lost | leases actually held afterwards |
|---:|---:|---:|---:|
| 11 | 7 | 2 | 18 |

All 7 five-hundreds carried the same body:

```
FileNotFoundError: [Errno 2] No such file or directory:
  <root>/claims.json.tmp -> <root>/claims.json
```

and **all 7 questions were still locked to the author who received the 500**
(`locked_after_500 = 7`). Cause: `Store.claim()` calls `_save_claims()` *outside*
the single-writer queue, and `_save_claims()` writes a fixed path
`claims.json.tmp` then `os.replace`s it. Concurrent claimers race on that one
temp file; one wins the replace, the others raise `FileNotFoundError`, the
request becomes a 500 — but the lease row was already committed in SQLite by the
writer thread. The client is told the claim failed and the question is locked to
it for the whole TTL. The lease also never reaches `claims.json`, so the
durability this file exists for is lost in exactly the case it was written for.

`claims.json` itself still parsed after the burst (23 entries), so the race did
not corrupt it in this run — it only lost writes.

## 5. Reads during writes

32 writers (1 280 posts) + 6 reader processes alternating `/find?q=measured`
and `/since?cursor=<fixed>` + 1 well-behaved cursor consumer, all concurrent:

| metric | value |
|---|---|
| reads | 288 |
| 5xx / lost / non-JSON / wrong shape | 0 / 0 / 0 / 0 |
| `/find` p50 / p99 | 250 ms / 318 ms |
| `/since` p50 / p99 | 256 ms / 326 ms |
| read throughput (6 readers) | 23 reads/s |
| writer 2xx / lost | 1272 / 8 |
| cursor consumer: nodes seen / lost / duplicated / backwards | 5143 / **0** / **0** / **0** |

No 500s, no truncated JSON, no lost or duplicated cursor nodes. The cost is
latency: `/find` is 250 ms under this load because `Store.annotate()` runs
`SELECT body FROM nodes` and `json.loads` every row **per query** — read cost is
O(total nodes), not O(hits).

Sequential read latency, no writers running (same server):

| rows | `/find` p50 | `/find` p99 | `/since?cursor=max-200` p50 |
|---:|---:|---:|---:|
| 3 868 | 21.4 ms | 28.3 ms | 2.7 ms |
| 5 187 | 28.6 ms | 41.4 ms | 2.1 ms |

≈ 5 µs per stored node per query. Extrapolated (not measured): ~0.3 s per
`/find` at 50 k nodes single-threaded, and the 6-reader run above already shows
a 9× degradation from concurrency alone.

## 6. Votes — 20 processes on one claim, twice

| round | created | lost | server score | recomputed from nodes | counted | capped | repeated call identical |
|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | 16 / 20 | 4 | 48.0 | 48.0 | 16 | 0 | yes |
| 1 | 20 / 20 | 0 | 60.0 | 60.0 | 20 | 0 | yes |

Scoring is deterministic and reproducible: the server score equals an
independent recomputation from raw nodes (20 × `replicated` weight 3 = 60),
two consecutive GETs agree, and the two rounds differ only by the votes that
were actually delivered. The score never drifted while votes were arriving.

One behaviour changed under this benchmark: vote identity now includes author,
prior and artifact. Two different authors posting byte-identical review text
produced **2 nodes and score 6.0**; an earlier build collapsed them into 1 node
and score 3.0.

## 7. Final integrity

```
rows 5187   files 5187   max_seq 5187
seq gaps 0   duplicate seq 0
files not in db 0   db not in files 0
server tracebacks 0
```

## What breaks first

1. **The TCP accept queue, not the store.** `listen(5)`. At 20 simultaneous
   agents the median `/health` is 1.0 s; at 100 it is 2.1 s with an 18 s p99 and
   a 5 s / 7 s / 15 s retransmission staircase. This is the first thing a fleet
   of agents hits, it happens before any Corallium code runs, and it produces
   the 1–7 % write loss in §1. A one-line `request_queue_size` override (or a
   real WSGI/async server) is the highest-leverage fix in the whole system.

2. **`claims.json` is written outside the writer queue.** 7 of 20 concurrent
   claims on *different* questions returned 500 `FileNotFoundError` on the
   shared `.json.tmp` path and **still took the lease**. The failure is worse
   than a plain error: the agent is told its claim failed, the question is
   locked to it for the TTL, and the lease is missing from the file that is
   supposed to survive an index wipe. Fix: move the save inside the queued job
   (it already has a consistent SQLite snapshot there) or use a temp name
   unique per call.

3. **No release, and expiry is not a handoff.** Every release-shaped URL is a
   404; a lease can only be waited out. `ttl=0` lets 8–14 of 20 contenders all
   "win". An operator who wants to abandon a question has no supported path.

4. **Reads are O(total nodes) per query.** `annotate()` loads every node body
   for every `/find`, so `/find` is 29 ms at 5 k nodes and 250 ms under six
   readers; `/since?cursor=0` is worse because it also serialises everything.
   Writes stay correct, but a few agents polling `/find` while others write
   dominate the CPU and drag writer p50 from 13 ms to 256 ms in §5.

5. **Aggregate throughput inverts.** 668 writes/s at 8 agents, 306 at 32,
   155 at 100. More agents means less total work completed. The single-writer
   queue is not the limit at these sizes (a queued write is ~1 ms); the accept
   path and per-request thread/GIL cost are.

What did **not** break, under every load tested: seq monotonicity and
contiguity, file/index parity, content-hash idempotency, cursor completeness
(`/since` lost nothing and duplicated nothing across 5 143 nodes during live
writes), and vote-score determinism.

### Measurement caveats

* `server.py` was edited by another agent at 13:59:34 while this work was in
  progress (a §3.4.1 similarity gate and purge/tombstone support were added).
  The published run is against hash `d02ea89…`, verified unchanged before and
  after the run; `claim()`, `_save_claims()` and the HTTP server setup in that
  file are the same code this benchmark was written against.
* Three earlier full runs on the pre-gate build lost 98/1000, 115/1000 and
  119/1000 writes at N=100 (9.8–11.9 %), so the accept-path failure predates the
  similarity gate. Write latency did move across builds: N=8 p50 was 2.5–3.0 ms
  then and is 11.1 ms now, consistent with the gate's per-write FTS lookup but
  not isolated from the other edits.
* The §3.4.1 gate also means a load generator whose propositions differ only by
  a digit measures the gate, not concurrency: it returned 409 for 992 of 1 000
  writes. This harness uses lexically distinct random objects for that reason.
* Single host, loopback, warm page cache, no fsync on the WAL commit path
  (`synchronous=NORMAL`). Numbers are for one machine, not a cluster.
* Run-to-run variance is real and is itself part of the result: across five
  full runs at N=100 the lost-write count was 72, 98, 104, 115 and 119 per 1 000
  (7.2–11.9 %); the concurrent-claims burst acquired 10–16 of 20; the `ttl=0`
  race produced 8–14 winners of 20. The zero values (seq gaps, file/index
  divergence, lost cursor nodes, score drift) were zero in every run.
