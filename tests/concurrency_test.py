#!/usr/bin/env python3
"""Cross-process concurrency benchmark for the Corallium store (PROTOCOL §13).

Why processes: the store's promises are about *agents*, which are separate
processes on separate machines. A thread pool inside one interpreter shares the
GIL, one socket pool and one exception context; it cannot show whether the
single-writer queue actually serialises anything.

What is measured, against a private server on a throwaway root:
  1. writer sweep        8 / 32 / 100 processes publishing distinct claims
  2. idempotency         50 processes publishing the SAME proposition (x3)
  3. claim contention    20 processes claiming one question; holder + handoff
  4. reads under write   6 processes hammering /find and /since during a run,
                         plus one cursor consumer that must lose nothing
  5. votes               20 processes voting on one claim; score recomputed
                         from the raw nodes and compared; repeated for
                         determinism

Usage
    python3 tests/concurrency_test.py                # full sweep, ~1-2 min
    python3 tests/concurrency_test.py --writers 100  # one size only
    python3 tests/concurrency_test.py --keep         # keep the throwaway root
    python3 tests/concurrency_test.py --json out.json

Exit code 0 = harness completed (anomalies are data, not a crash).
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import math
import multiprocessing as mp
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
SERVER = HERE.parent / "server.py"

# Mirrors server.WEIGHTS; the point of recomputing is that the benchmark does
# not call the server's own scoring code, it re-derives the number from nodes.
WEIGHTS = {"adversarial": 4, "replicated": 3, "sourced": 2, "derived": 1,
           "agree": 0, "refuted": -3}


# ── transport ──────────────────────────────────────────────────────────────
def http(port: int, path: str, payload=None, timeout: float = 30.0):
    """One request, one connection (what a fresh agent/curl invocation does).

    Returns (status, ms, obj, err). status 0 means no HTTP response at all
    (connection refused / reset / timeout), which is a different failure from
    a 5xx and must be counted separately.
    """
    url = f"http://127.0.0.1:{port}{path}"
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="POST" if data is not None else "GET",
        headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body, code = r.read(), r.status
    except urllib.error.HTTPError as e:
        body, code = e.read(), e.code
    except Exception as e:  # URLError, reset, timeout, ...
        return 0, (time.perf_counter() - t0) * 1000.0, None, f"{type(e).__name__}: {e}"
    dt = (time.perf_counter() - t0) * 1000.0
    try:
        return code, dt, json.loads(body), None
    except Exception as e:
        return code, dt, None, f"badjson:{type(e).__name__}: {e} body={body[:120]!r}"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def wait_health(port: int, timeout: float = 20.0) -> dict:
    end = time.time() + timeout
    last = None
    while time.time() < end:
        code, _, obj, err = http(port, "/health", timeout=1.0)
        if code == 200:
            return obj
        last = err
        time.sleep(0.05)
    raise RuntimeError(f"server on {port} never became healthy: {last}")


# ── worker processes (module level: must be picklable under spawn) ─────────
def _dump(outdir: str, tag: str, idx: int, data: dict) -> None:
    p = Path(outdir) / f"{tag}-{idx:03d}-{os.getpid()}.json"
    p.write_text(json.dumps(data))


def writer_proc(port, barrier, outdir, idx, count, run_id, timeout):
    """Publish `count` distinct claims; record latency, status and seq."""
    res = []
    try:
        barrier.wait(timeout=120)
        t0 = time.perf_counter()
        for i in range(count):
            # Lexically distinct objects: server.py §3.4.1 rejects (409) a new
            # claim whose p+o is ≥0.62 similar to an existing one, so a load
            # generator that varies only a digit measures that gate instead of
            # concurrency.
            tok = uuid.uuid4().hex
            payload = {
                "s": f"bench-{run_id}",
                "p": f"writer-{idx:03d}/claim-{i:04d}/{tok[:8]}",
                "o": f"{tok[:16]} measured {tok[16:24]} MB/s at queue depth {i}",
                "author": f"bench-w{idx}",
                "type": "claim",
                "topics": ["bench", f"run-{run_id}"],
                "note": "concurrency_test.py writer sweep",
            }
            code, ms, obj, err = http(port, "/nodes", payload, timeout=timeout)
            created = bool(obj and obj.get("created"))
            seq = (obj or {}).get("node", {}).get("seq") if isinstance(obj, dict) else None
            nid = (obj or {}).get("node", {}).get("id") if isinstance(obj, dict) else None
            res.append([code, ms, created, seq, nid, err])
        t1 = time.perf_counter()
        _dump(outdir, "writer", idx, {"res": res, "t0": t0, "t1": t1})
    except Exception:
        _dump(outdir, "writer", idx, {"res": res, "error": traceback.format_exc()})


def writer_proc_ka(port, barrier, outdir, idx, count, run_id, timeout):
    """Same load, one persistent connection per process (HTTP/1.1 keep-alive).

    Separates the store's write path from TCP accept cost: if fresh-connection
    runs are slow but this one is fast, the queue is not the bottleneck.
    """
    import http.client
    res = []
    conn = None
    try:
        barrier.wait(timeout=120)
        t0 = time.perf_counter()
        for i in range(count):
            tok = uuid.uuid4().hex
            payload = {
                "s": f"bench-ka-{run_id}",
                "p": f"writer-{idx:03d}/claim-{i:04d}/{tok[:8]}",
                "o": f"{tok[:16]} measured {tok[16:24]} MB/s keepalive",
                "author": f"bench-k{idx}",
                "type": "claim", "topics": ["bench"],
            }
            body = json.dumps(payload).encode()
            for attempt in (1, 2):
                try:
                    if conn is None:
                        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
                    t1 = time.perf_counter()
                    conn.request("POST", "/nodes", body, {"Content-Type": "application/json"})
                    r = conn.getresponse()
                    data, code = r.read(), r.status
                    ms = (time.perf_counter() - t1) * 1000.0
                    break
                except Exception as e:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                    if attempt == 2:
                        code, data, ms = 0, b"", (time.perf_counter() - t1) * 1000.0
            try:
                obj = json.loads(data)
            except Exception:
                obj = None
            seq = ((obj or {}).get("node") or {}).get("seq")
            res.append([code, ms, bool((obj or {}).get("created")), seq,
                        ((obj or {}).get("node") or {}).get("id"), None])
        t1 = time.perf_counter()
        _dump(outdir, "writer", idx, {"res": res, "t0": t0, "t1": t1})
    except Exception:
        _dump(outdir, "writer", idx, {"res": res, "error": traceback.format_exc()})
    finally:
        try:
            conn and conn.close()
        except Exception:
            pass


def burst_proc(port, barrier, outdir, idx, path, timeout):
    """One GET, no store work on /health: a pure accept-path probe."""
    try:
        barrier.wait(timeout=120)
        code, ms, obj, err = http(port, path, timeout=timeout)
        _dump(outdir, "burst", idx, {"code": code, "ms": ms, "err": err})
    except Exception:
        _dump(outdir, "burst", idx, {"code": 0, "err": traceback.format_exc()})


def same_proc(port, barrier, outdir, idx, payload, timeout):
    """Publish a byte-identical proposition; report created flag."""
    try:
        barrier.wait(timeout=120)
        code, ms, obj, err = http(port, "/nodes", payload, timeout=timeout)
        _dump(outdir, "same", idx, {
            "code": code, "ms": ms, "err": err,
            "created": bool(obj and obj.get("created")),
            "id": ((obj or {}).get("node") or {}).get("id"),
            "seq": ((obj or {}).get("node") or {}).get("seq"),
        })
    except Exception:
        _dump(outdir, "same", idx, {"code": 0, "err": traceback.format_exc()})


def claim_proc(port, barrier, outdir, idx, question, ttl, author, timeout):
    try:
        barrier.wait(timeout=120)
        code, ms, obj, err = http(
            port, "/claims", {"question": question, "author": author, "ttl": ttl},
            timeout=timeout)
        _dump(outdir, "claim", idx, {
            "code": code, "ms": ms, "err": err, "author": author,
            "acquired": bool(obj and obj.get("acquired")),
            "holder": (obj or {}).get("holder"),
            "task_key": (obj or {}).get("task_key"),
            "body": obj if code != 200 else None,
        })
    except Exception:
        _dump(outdir, "claim", idx, {"code": 0, "err": traceback.format_exc(),
                                     "author": author, "acquired": False})


def reader_proc(port, start_evt, stop_evt, outdir, idx, deadline_s, timeout):
    """Hammer /find and /since for as long as the writers are running."""
    res = []
    deadline = time.time() + deadline_s
    try:
        start_evt.wait(timeout=60)
        while not stop_evt.is_set() and time.time() < deadline:
            for path in ("/find?q=bench-writer", f"/since?cursor={idx * 7}"):
                code, ms, obj, err = http(port, path, timeout=timeout)
                ok_shape = (
                    isinstance(obj, dict) and (
                        (path.startswith("/find") and isinstance(obj.get("hits"), list))
                        or (path.startswith("/since") and isinstance(obj.get("nodes"), list))
                    )
                )
                res.append([path.split("?")[0], code, ms, ok_shape, err])
            time.sleep(0.005)  # bound the loop: a full /since dump is ~1 MB late in the run
        _dump(outdir, "reader", idx, {"res": res})
    except Exception:
        _dump(outdir, "reader", idx, {"res": res, "error": traceback.format_exc()})


def cursor_proc(port, start_evt, stop_evt, outdir, deadline_s, timeout):
    """A well-behaved agent: poll since?cursor=N, advance, never skip.

    If this consumer's id set differs from the final store, an agent that was
    reading the whole time lost a node — the failure mode §10.4 exists to
    prevent.
    """
    seen, dups, backwards, errs = [], 0, 0, []
    cursor = 0
    try:
        start_evt.wait(timeout=60)
        deadline = time.time() + deadline_s
        while not stop_evt.is_set() and time.time() < deadline:
            code, ms, obj, err = http(port, f"/since?cursor={cursor}", timeout=timeout)
            if code != 200 or not isinstance((obj or {}).get("nodes"), list):
                errs.append(f"{code} {err}")
                time.sleep(0.01)
                continue
            nodes = obj["nodes"]
            for n in nodes:
                s = n.get("seq", 0)
                if s <= cursor:
                    backwards += 1
                seen.append(n["id"])
            if nodes:
                cursor = max(n.get("seq", 0) for n in nodes)
        _dump(outdir, "cursor", 0, {
            "seen": seen, "cursor": cursor, "dups": dups,
            "backwards": backwards, "errs": errs[:20], "nerr": len(errs)})
    except Exception:
        _dump(outdir, "cursor", 0, {"seen": seen, "error": traceback.format_exc()})


def vote_proc(port, barrier, outdir, idx, claim_id, prior_id, run_id, timeout):
    try:
        barrier.wait(timeout=120)
        body = {
            "kind": "replicated",
            "text": f"voter-{idx:02d} reproduced it independently in run {run_id}",
            "author": f"bench-voter-{idx:02d}",
            "prior": prior_id,
            "artifact": f"/tmp/bench-artifact-{run_id}-{idx}.txt",
        }
        code, ms, obj, err = http(
            port, f"/nodes/{claim_id}/replies", body, timeout=timeout)
        _dump(outdir, "vote", idx, {
            "code": code, "ms": ms, "err": err,
            "created": bool(obj and obj.get("created")),
            "id": ((obj or {}).get("node") or {}).get("id"),
            "seq": ((obj or {}).get("node") or {}).get("seq"),
        })
    except Exception:
        _dump(outdir, "vote", idx, {"code": 0, "err": traceback.format_exc()})


# ── harness ────────────────────────────────────────────────────────────────
class Bench:
    def __init__(self, port: int, root: Path):
        self.port = port
        self.root = root
        self.results: dict = {}

    # -- plumbing ---------------------------------------------------------
    def collect(self, outdir: Path, tag: str) -> list[dict]:
        out = []
        for f in sorted(glob.glob(str(outdir / f"{tag}-*.json"))):
            try:
                out.append(json.loads(Path(f).read_text()))
            except Exception:
                pass
        return out

    # -- store inspection -------------------------------------------------
    def snapshot(self) -> tuple[list[dict], str | None]:
        code, _, obj, err = http(self.port, "/since?cursor=0", timeout=60)
        if code != 200:
            return [], err or f"HTTP {code}"
        return obj.get("nodes", []), None

    def files(self) -> list[str]:
        return sorted(os.path.basename(p) for p in
                      glob.glob(str(self.root / "nodes" / "*.json")))

    def integrity(self) -> dict:
        """Truth (files) vs index (db bodies): both directions, plus seq shape."""
        nodes, err = self.snapshot()
        seqs = [n.get("seq") for n in nodes]
        ids_db = {n["id"] for n in nodes}
        files = self.files()
        ids_fs = {f[:-5] for f in files}
        uniq = sorted(s for s in seqs if isinstance(s, int))
        gaps = []
        if uniq:
            have = set(uniq)
            gaps = [i for i in range(1, max(uniq) + 1) if i not in have]
        dupe_seq = len(seqs) - len(set(seqs))
        return {
            "db_rows": len(nodes), "files": len(files), "max_seq": max(uniq) if uniq else 0,
            "seq_gaps": len(gaps), "gap_examples": gaps[:10], "dupe_seq": dupe_seq,
            "files_not_in_db": len(ids_fs - ids_db), "db_not_in_files": len(ids_db - ids_fs),
            "snapshot_error": err,
        }


def pct(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = max(0, min(len(s) - 1, math.ceil(q * len(s)) - 1))
    return s[k]


def summarise_writers(chunks: list[dict], n: int, integrity: dict, expected: int) -> dict:
    rows = [r for c in chunks for r in c.get("res", [])]
    lat = [r[1] for r in rows]
    codes: dict[int, int] = {}
    for r in rows:
        codes[r[0]] = codes.get(r[0], 0) + 1
    seqs = [r[3] for r in rows if isinstance(r[3], int)]
    created = sum(1 for r in rows if r[2])
    t0 = min((c.get("t0", 0) for c in chunks), default=0)
    t1 = max((c.get("t1", 0) for c in chunks), default=0)
    wall = (t1 - t0) if t1 > t0 else 0.0
    errors = [c["error"] for c in chunks if c.get("error")]
    err_msgs = sorted({r[5] for r in rows if r[5]})
    return {
        "n": n, "sent": len(rows), "expected": expected,
        "ok": codes.get(201, 0) + codes.get(200, 0),
        "created": created,
        "fail": len(rows) - (codes.get(201, 0) + codes.get(200, 0)),
        "s5xx": sum(v for k, v in codes.items() if 500 <= k < 600),
        "s4xx": sum(v for k, v in codes.items() if 400 <= k < 500),
        "no_response": codes.get(0, 0),
        "seq_unique": len(set(seqs)), "seq_returned": len(seqs),
        "wall_s": round(wall, 2),
        "thr": round(len(rows) / wall, 1) if wall > 0 else 0.0,
        "p50": round(pct(lat, 0.50), 1), "p95": round(pct(lat, 0.95), 1),
        "p99": round(pct(lat, 0.99), 1), "max": round(max(lat), 1) if lat else 0.0,
        "errors": err_msgs[:4], "proc_errors": len(errors),
        **integrity,
    }


# ── scenario 1: writer sweep ───────────────────────────────────────────────
def scenario_writers(b: Bench, sizes: list[int], total: int, timeout: float) -> list[dict]:
    ctx = mp.get_context("fork")
    table = []
    for n in sizes:
        tag = f"w{n}"
        outdir = b.root / f"out-{tag}"
        outdir.mkdir(exist_ok=True)
        per = max(1, total // n)
        run_id = f"{tag}x{per}"
        before = b.integrity()
        barrier = ctx.Barrier(n)
        procs = [ctx.Process(target=writer_proc,
                             args=(b.port, barrier, str(outdir), i, per, run_id, timeout))
                 for i in range(n)]
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=180)
        alive = [p for p in procs if p.is_alive()]
        for p in alive:
            p.terminate()
        chunks = b.collect(outdir, "writer")
        after = b.integrity()
        row = summarise_writers(chunks, n, after, before["db_rows"] + n * per)
        row["expected"] = n * per
        row["expected_total_rows"] = before["db_rows"] + n * per
        row["rows_delta"] = after["db_rows"] - before["db_rows"]
        row["files_delta"] = after["files"] - before["files"]
        row["hung_procs"] = len(alive)
        table.append(row)
        print(f"  N={n:<4} sent={row['sent']:<5} ok={row['ok']:<5} fail={row['fail']:<3} "
              f"5xx={row['s5xx']} noresp={row['no_response']} "
              f"seq_uniq={row['seq_unique']}/{row['seq_returned']} "
              f"gaps={row['seq_gaps']} rows+{row['rows_delta']}/{n * per} "
              f"wall={row['wall_s']}s {row['thr']}/s "
              f"p50={row['p50']} p99={row['p99']}", flush=True)
    b.results["writers"] = table
    return table


# ── scenario 1b: accept path vs store path ─────────────────────────────────
def scenario_burst(b: Bench, sizes=(8, 20, 50, 100), path="/health", timeout=30.0):
    """N simultaneous NEW connections against an endpoint with no store work.

    /health only reads max_seq, so any latency spike here is the HTTP accept
    path, not the writer queue. Same shape as the claim race's tail.
    """
    ctx = mp.get_context("fork")
    rows = []
    for n in sizes:
        tag = f"burst{n}-{int(time.time() * 1000) % 10 ** 6}"
        outdir = b.root / f"out-{tag}"
        outdir.mkdir(exist_ok=True)
        barrier = ctx.Barrier(n)
        procs = [ctx.Process(target=burst_proc,
                             args=(b.port, barrier, str(outdir), i, path, timeout))
                 for i in range(n)]
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=120)
        res = b.collect(outdir, "burst")
        lat = [x.get("ms", 0.0) for x in res]
        row = {"n": n, "responses": len(res),
               "ok": sum(1 for x in res if x.get("code") == 200),
               "no_response": sum(1 for x in res if (x.get("code") or 0) == 0),
               "p50": round(pct(lat, 0.50), 1), "p90": round(pct(lat, 0.90), 1),
               "p99": round(pct(lat, 0.99), 1),
               "max": round(max(lat), 1) if lat else 0.0,
               "over_500ms": sum(1 for x in lat if x > 500)}
        rows.append(row)
        print(f"  {path} N={n:<4} ok={row['ok']:<4} noresp={row['no_response']} "
              f"p50={row['p50']} p90={row['p90']} p99={row['p99']} max={row['max']} "
              f">500ms={row['over_500ms']}", flush=True)
    b.results["burst"] = {"path": path, "rows": rows}
    return rows


def scenario_keepalive(b: Bench, n: int = 100, total: int = 1000, timeout=30.0):
    """100 writers, one persistent connection each: store-only cost."""
    ctx = mp.get_context("fork")
    tag = f"ka{n}"
    outdir = b.root / f"out-{tag}"
    outdir.mkdir(exist_ok=True)
    per = max(1, total // n)
    run_id = f"{tag}-{int(time.time() * 1000) % 10 ** 6}"
    before = b.integrity()
    barrier = ctx.Barrier(n)
    procs = [ctx.Process(target=writer_proc_ka,
                         args=(b.port, barrier, str(outdir), i, per, run_id, timeout))
             for i in range(n)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=240)
    hung = [p for p in procs if p.is_alive()]
    for p in hung:
        p.terminate()
    chunks = b.collect(outdir, "writer")
    after = b.integrity()
    row = summarise_writers(chunks, n, after, n * per)
    row["rows_delta"] = after["db_rows"] - before["db_rows"]
    row["hung_procs"] = len(hung)
    row["mode"] = "keepalive"
    b.results["keepalive"] = row
    print(f"  keepalive N={n} sent={row['sent']} ok={row['ok']} 5xx={row['s5xx']} "
          f"noresp={row['no_response']} wall={row['wall_s']}s {row['thr']}/s "
          f"p50={row['p50']} p99={row['p99']} rows+{row['rows_delta']}/{n * per}",
          flush=True)
    return row


def probe_reads(b: Bench, label: str, reps: int = 20) -> dict:
    """Sequential read latency at the current store size (no concurrent load).

    /find runs annotate() → every node body is loaded per query, so this curve
    should grow with N even though the FTS match is indexed.
    """
    nodes, _ = b.snapshot()
    mx = max((n.get("seq", 0) for n in nodes), default=0)
    find_lat, since_lat = [], []
    for _ in range(reps):
        _, ms, _, _ = http(b.port, "/find?q=measured", timeout=60)
        find_lat.append(ms)
        _, ms, _, _ = http(b.port, f"/since?cursor={max(0, mx - 200)}", timeout=60)
        since_lat.append(ms)
    row = {"label": label, "rows": len(nodes),
           "find_p50": round(pct(find_lat, 0.50), 1),
           "find_p99": round(pct(find_lat, 0.99), 1),
           "since_p50": round(pct(since_lat, 0.50), 1),
           "since_p99": round(pct(since_lat, 0.99), 1)}
    print(f"  reads @ {row['rows']:<5} rows: find p50={row['find_p50']}ms "
          f"p99={row['find_p99']}ms | since(200) p50={row['since_p50']}ms", flush=True)
    return row


# ── scenario 2: idempotency ────────────────────────────────────────────────
def scenario_idempotency(b: Bench, rounds: int = 3, n: int = 50, timeout: float = 60.0):
    ctx = mp.get_context("fork")
    out = []
    for r in range(rounds):
        tag = f"idem{r}"
        outdir = b.root / f"out-{tag}"
        outdir.mkdir(exist_ok=True)
        payload = {"s": "bench-idem",
                   "p": f"same-proposition-round-{uuid.uuid4().hex[:12]}",
                   "o": f"{uuid.uuid4().hex} the store must collapse this to one node",
                   "author": "bench-idem",
                   "type": "claim", "topics": ["bench"]}
        # Predict the id the way server.add_node does (rel=root, no parents), so
        # the race starts with the node genuinely absent: a probe POST would
        # make every worker's answer a trivial created:false.
        blob = json.dumps([payload["s"], payload["p"], payload["o"], "root", []],
                          ensure_ascii=False)
        nid = hashlib.sha256(blob.encode()).hexdigest()[:12]
        barrier = ctx.Barrier(n)
        procs = [ctx.Process(target=same_proc,
                             args=(b.port, barrier, str(outdir), i, payload, timeout))
                 for i in range(n)]
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=180)
        rows = b.collect(outdir, "same")
        created = sum(1 for x in rows if x.get("created"))
        received_false = sum(1 for x in rows
                             if x.get("code") == 200 and not x.get("created"))
        no_resp = sum(1 for x in rows if (x.get("code") or 0) == 0)
        ids = {x.get("id") for x in rows if x.get("id")}
        files_with_id = len(glob.glob(str(b.root / "nodes" / f"{nid}.json")))
        out.append({"round": r, "workers": n, "responses": len(rows),
                    "created_true": created,
                    "created_false_received": received_false,
                    "no_response": no_resp,
                    "created_false_total": len(rows) - created,
                    "distinct_ids": len(ids), "files_for_id": files_with_id,
                    "id_matches_prediction": ids == {nid},
                    "codes": sorted({x.get("code") for x in rows})})
        print(f"  round {r}: 201={created} 200(created:false)={received_false} "
              f"no_response={no_resp} distinct_ids={len(ids)} files={files_with_id}",
              flush=True)
    b.results["idempotency"] = out
    return out


# ── scenario 3: claim contention ───────────────────────────────────────────
def _claim_race(b: Bench, n: int, question: str, ttl: int, tag: str, timeout=30.0):
    ctx = mp.get_context("fork")
    outdir = b.root / f"out-{tag}"
    outdir.mkdir(exist_ok=True)
    barrier = ctx.Barrier(n)
    procs = [ctx.Process(target=claim_proc,
                         args=(b.port, barrier, str(outdir), i, question, ttl,
                               f"bench-c{i:02d}", timeout))
             for i in range(n)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=120)
    return b.collect(outdir, "claim")


def scenario_claims(b: Bench, n: int = 20, timeout: float = 30.0):
    out = {}
    stamp = int(time.time() * 1000)
    q = f"bench: who owns the concurrency measurement {stamp}"
    rows = _claim_race(b, n, q, 30, f"contend{stamp}")
    winners = [r for r in rows if r.get("acquired")]
    losers = [r for r in rows if not r.get("acquired")]
    holders = sorted({r.get("holder") for r in losers if r.get("holder")})
    lat = [r.get("ms", 0.0) for r in rows]
    out["contention"] = {
        "workers": n, "responses": len(rows), "winners": len(winners),
        "winner": winners[0]["author"] if winners else None,
        "losers": len(losers), "holder_values": holders,
        "holder_matches_winner": holders == ([winners[0]["author"]] if winners else []),
        "codes": sorted({r.get("code") for r in rows}),
        "no_response": sum(1 for r in rows if (r.get("code") or 0) == 0),
        "p50": round(pct(lat, 0.50), 1), "p99": round(pct(lat, 0.99), 1),
        "max": round(max(lat), 1) if lat else 0.0,
        "errors": sorted({r.get("err") for r in rows if r.get("err")})[:4],
    }
    print(f"  contention: winners={len(winners)}/{len(rows)} "
          f"holder={out['contention']['winner']} "
          f"losers_see={holders} p50={out['contention']['p50']}ms "
          f"p99={out['contention']['p99']}ms resets={out['contention']['no_response']}",
          flush=True)

    # Same author re-entering its own live lease: lease renewal, by design.
    win = winners[0]["author"] if winners else "bench-c00"
    _, _, own, _ = http(b.port, "/claims", {"question": q, "author": win, "ttl": 30})
    other = "bench-outside"
    _, _, again, _ = http(b.port, "/claims", {"question": q, "author": other, "ttl": 30})
    out["reentry"] = {"same_author_acquired": bool((own or {}).get("acquired")),
                      "other_author_acquired": bool((again or {}).get("acquired")),
                      "other_holder": (again or {}).get("holder")}

    # Handoff. There is no release endpoint; the only server-side release is
    # TTL expiry. ttl=0 makes the lease expire immediately, which is how a
    # handoff can be observed without waiting a full minute.
    q2 = f"bench: handoff probe {stamp}"
    _, _, a1, _ = http(b.port, "/claims", {"question": q2, "author": "bench-h1", "ttl": 0})
    key2 = (a1 or {}).get("task_key") or ""
    _, _, a2, _ = http(b.port, "/claims", {"question": q2, "author": "bench-h2", "ttl": 0})
    out["handoff"] = {"first": bool((a1 or {}).get("acquired")),
                      "second_after_expiry": bool((a2 or {}).get("acquired")),
                      "second_author": "bench-h2",
                      "task_key": key2}
    # Is there any release path over HTTP? Probe the shapes a client would try.
    probes = {}
    for path, body in ((f"/claims/{key2}/release", {"author": "bench-h2"}),
                       (f"/claims/{key2}", {"author": "bench-h2", "status": "done"}),
                       ("/claims/release", {"question": q2, "author": "bench-h2"})):
        code, _, obj, _ = http(b.port, path, body)
        probes[path] = {"code": code, "body": obj}
    out["release_probe"] = probes

    # Degenerate ttl=0 race: if "release" means lease_until=now, every contender
    # may win. Measure rather than assume.
    q3 = f"bench: ttl0 race {stamp}"
    rows0 = _claim_race(b, n, q3, 0, f"ttl0-{stamp}")
    out["ttl0_race"] = {"workers": n, "winners": sum(1 for r in rows0 if r.get("acquired")),
                        "responses": len(rows0)}

    # Distinct questions in one burst: every claim writes claims.json outside
    # the writer queue, so this is the real shared-state collision.
    qs_tag = f"distinct{stamp}"
    outdir = b.root / f"out-{qs_tag}"
    outdir.mkdir(exist_ok=True)
    ctx = mp.get_context("fork")
    barrier = ctx.Barrier(n)
    procs = [ctx.Process(target=claim_proc,
                         args=(b.port, barrier, str(outdir), i,
                               f"bench: distinct question {qs_tag} #{i}", 30,
                               f"bench-d{i:02d}", timeout))
             for i in range(n)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=120)
    drows = b.collect(outdir, "claim")
    out["distinct_burst"] = {
        "workers": n, "responses": len(drows),
        "acquired": sum(1 for r in drows if r.get("acquired")),
        "s5xx": sum(1 for r in drows if 500 <= (r.get("code") or 0) < 600),
        "no_response": sum(1 for r in drows if (r.get("code") or 0) == 0),
        "s5xx_messages": sorted({str((r.get("body") or {}).get("error"))
                                 for r in drows if 500 <= (r.get("code") or 0) < 600})[:4],
        "errors": sorted({r.get("err") for r in drows if r.get("err")})[:4],
    }
    claims_file = b.root / "claims.json"
    try:
        parsed = json.loads(claims_file.read_text())
        out["claims_json"] = {"parses": True, "entries": len(parsed)}
    except Exception as e:
        out["claims_json"] = {"parses": False, "error": f"{type(e).__name__}: {e}"}
    # Did a claim that returned 500 nevertheless take the lease? Re-claim each
    # question as an outsider: refused means someone holds it.
    by_author = {r.get("author"): r for r in drows}
    silently_held = 0
    held_by = {}
    for i in range(n):
        q_i = f"bench: distinct question {qs_tag} #{i}"
        _, _, pr, _ = http(b.port, "/claims",
                           {"question": q_i, "author": "bench-probe", "ttl": 30})
        holder = (pr or {}).get("holder")
        held_by[i] = holder
        row = by_author.get(f"bench-d{i:02d}") or {}
        if (row.get("code") or 0) >= 500 and holder == row.get("author"):
            silently_held += 1
    out["distinct_burst"]["locked_after_500"] = silently_held
    out["distinct_burst"]["held_now"] = sum(1 for v in held_by.values() if v)
    out["claims_json_after"] = {}
    try:
        again = json.loads(claims_file.read_text())
        out["claims_json_after"] = {"parses": True, "entries": len(again)}
    except Exception as e:
        out["claims_json_after"] = {"parses": False, "error": str(e)}
    print(f"  distinct-question burst: acquired={out['distinct_burst']['acquired']}/{n} "
          f"5xx={out['distinct_burst']['s5xx']} "
          f"locked_after_500={silently_held} "
          f"claims.json_parses={out['claims_json']['parses']}", flush=True)
    b.results["claims"] = out
    return out


# ── scenario 4: reads during writes ────────────────────────────────────────
def scenario_reads(b: Bench, writers: int = 32, per: int = 40, readers: int = 6,
                   timeout: float = 60.0):
    ctx = mp.get_context("fork")
    stamp = int(time.time() * 1000)
    wdir = b.root / f"out-rd-w{stamp}"
    rdir = b.root / f"out-rd-r{stamp}"
    cdir = b.root / f"out-rd-c{stamp}"
    for d in (wdir, rdir, cdir):
        d.mkdir(exist_ok=True)
    start = ctx.Event()
    stop = ctx.Event()
    rprocs = [ctx.Process(target=reader_proc,
                          args=(b.port, start, stop, str(rdir), i, 120.0, timeout))
              for i in range(readers)]
    cproc = ctx.Process(target=cursor_proc,
                        args=(b.port, start, stop, str(cdir), 120.0, timeout))
    for p in rprocs + [cproc]:
        p.start()
    barrier = ctx.Barrier(writers)
    wprocs = [ctx.Process(target=writer_proc,
                          args=(b.port, barrier, str(wdir), i, per, f"rd{stamp}", timeout))
              for i in range(writers)]
    for p in wprocs:
        p.start()
    start.set()
    t0 = time.perf_counter()
    for p in wprocs:
        p.join(timeout=240)
    hung = [p for p in wprocs if p.is_alive()]
    for p in hung:
        p.terminate()
    wall = time.perf_counter() - t0
    stop.set()
    for p in rprocs + [cproc]:
        p.join(timeout=30)
    for p in [q for q in rprocs + [cproc] if q.is_alive()]:
        p.terminate()

    wchunks = b.collect(wdir, "writer")
    wrow = summarise_writers(wchunks, writers, b.integrity(), writers * per)
    rrows = [x for c in b.collect(rdir, "reader") for x in c.get("res", [])]
    find_lat = [x[2] for x in rrows if x[0] == "/find"]
    since_lat = [x[2] for x in rrows if x[0] == "/since"]
    read_out = {
        "readers": readers, "reads": len(rrows),
        "ok": sum(1 for x in rrows if x[1] == 200),
        "s5xx": sum(1 for x in rrows if 500 <= (x[1] or 0) < 600),
        "no_response": sum(1 for x in rrows if (x[1] or 0) == 0),
        "bad_shape": sum(1 for x in rrows if not x[3]),
        "transport_errors": sorted({x[4] for x in rrows if x[4]})[:4],
        "find_p50": round(pct(find_lat, 0.50), 1), "find_p99": round(pct(find_lat, 0.99), 1),
        "since_p50": round(pct(since_lat, 0.50), 1), "since_p99": round(pct(since_lat, 0.99), 1),
        "read_thr": round(len(rrows) / wall, 1) if wall else 0,
        "wall_s": round(wall, 2),
    }
    c = (b.collect(cdir, "cursor") or [{}])[0]
    nodes, _ = b.snapshot()
    final_ids = {n["id"] for n in nodes}
    seen = c.get("seen", [])
    read_out["cursor"] = {
        "polls_nodes": len(seen), "cursor_reached": c.get("cursor"),
        "max_seq": max((n.get("seq", 0) for n in nodes), default=0),
        "unique_seen": len(set(seen)), "duplicates": len(seen) - len(set(seen)),
        "backwards": c.get("backwards", 0),
        "lost_ids": len(final_ids - set(seen)),
        "unknown_ids": len(set(seen) - final_ids),
        "errors": c.get("nerr", 0),
    }
    wrow["_read"] = read_out
    b.results["reads"] = {"writers": wrow, "reads": read_out}
    print(f"  writers ok={wrow['ok']}/{wrow['sent']} wall={wrow['wall_s']}s "
          f"p50={wrow['p50']} p99={wrow['p99']}", flush=True)
    print(f"  {len(rrows)} reads: 5xx={read_out['s5xx']} noresp={read_out['no_response']} "
          f"bad_shape={read_out['bad_shape']} find_p99={read_out['find_p99']}ms "
          f"lost={read_out['cursor']['lost_ids']} dup={read_out['cursor']['duplicates']}",
          flush=True)
    return b.results["reads"]


# ── scenario 5: votes ──────────────────────────────────────────────────────
def _target(b: Bench, tag: str) -> tuple[str, str]:
    stamp = f"{tag}-{uuid.uuid4().hex[:12]}"
    _, _, prior, _ = http(b.port, "/nodes", {
        "s": "bench-vote", "p": f"prior-{stamp}", "o": f"prior result {uuid.uuid4().hex}",
        "author": "bench-prior", "type": "claim", "topics": ["bench"]})
    _, _, tgt, _ = http(b.port, "/nodes", {
        "s": "bench-vote", "p": f"target-{stamp}", "o": f"target claim {uuid.uuid4().hex}",
        "author": "bench-author", "type": "claim", "topics": ["bench"],
        "note": "vote determinism target"})
    if "node" not in (prior or {}) or "node" not in (tgt or {}):
        raise RuntimeError(f"could not create vote target: prior={prior} tgt={tgt}")
    return tgt["node"]["id"], prior["node"]["id"]


def _vote_race(b: Bench, n: int, claim_id: str, prior_id: str, tag: str, timeout=30.0):
    ctx = mp.get_context("fork")
    outdir = b.root / f"out-{tag}"
    outdir.mkdir(exist_ok=True)
    barrier = ctx.Barrier(n)
    procs = [ctx.Process(target=vote_proc,
                         args=(b.port, barrier, str(outdir), i, claim_id, prior_id,
                               tag, timeout))
             for i in range(n)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=120)
    return b.collect(outdir, "vote")


def recompute_score(nodes: list[dict], target_id: str, target_author: str) -> dict:
    """Independent implementation of PROTOCOL §4.2 from raw nodes."""
    votes = [n for n in nodes if n.get("type") == "review" and n.get("claim") == target_id]
    total, counted, caps = 0.0, 0, []
    for v in votes:
        kind = v.get("kind") or v.get("p", "agree")
        if v.get("author") == target_author:
            continue
        w = WEIGHTS.get(kind, 0)
        c = []
        if kind in ("replicated", "adversarial", "refuted"):
            if kind != "refuted" and not v.get("prior"):
                c.append("no-prior")
            if not (v.get("artifact") or v.get("method")):
                c.append("not-checkable")
            if c:
                w = 1 if w > 0 else -1
        total += w
        counted += 1
        caps.append(c)
    return {"score": round(total, 2), "votes": len(votes), "counted": counted,
            "capped": sum(1 for c in caps if c)}


def scenario_votes(b: Bench, n: int = 20, timeout: float = 30.0):
    out = {"runs": []}
    for r in range(2):
        cid, pid = _target(b, f"vote{r}")
        rows = _vote_race(b, n, cid, pid, f"vote{r}")
        created = sum(1 for x in rows if x.get("created"))
        code, _, rev, err = http(b.port, f"/reviews?claim={cid}", timeout=timeout)
        code2, _, rev2, _ = http(b.port, f"/reviews?claim={cid}", timeout=timeout)
        nodes, _ = b.snapshot()
        mine = recompute_score(nodes, cid, "bench-author")
        out["runs"].append({
            "round": r, "voters": n, "created": created,
            "no_response": sum(1 for x in rows if (x.get("code") or 0) == 0),
            "http": code, "http_ok": sum(1 for x in rows if x.get("code") == 201),
            "server_score": (rev or {}).get("score"),
            "server_votes": (rev or {}).get("votes") and len(rev["votes"]),
            "recomputed": mine["score"], "recomputed_counted": mine["counted"],
            "capped": mine["capped"],
            "repeat_identical": (rev or {}).get("score") == (rev2 or {}).get("score"),
        })
        print(f"  round {r}: votes_created={created}/{n} server_score="
              f"{(rev or {}).get('score')} recomputed={mine['score']} "
              f"caps={mine['capped']}", flush=True)

    # Probe: identity is a hash of (s,p,o,rel,parents) and does NOT include the
    # author. Two agents saying the same words are one vote.
    cid, pid = _target(b, "votecollapse")
    same_text = "identical wording, different agents"
    a = {"kind": "replicated", "text": same_text, "author": "bench-twin-a",
         "prior": pid, "artifact": "/tmp/x"}
    bb = dict(a, author="bench-twin-b")
    _, _, r1, _ = http(b.port, f"/nodes/{cid}/replies", a)
    _, _, r2, _ = http(b.port, f"/nodes/{cid}/replies", bb)
    _, _, rev, _ = http(b.port, f"/reviews?claim={cid}")
    out["collapse_probe"] = {
        "first_created": bool((r1 or {}).get("created")),
        "second_created": bool((r2 or {}).get("created")),
        "second_author_reported": ((r2 or {}).get("node") or {}).get("author"),
        "votes_counted": rev.get("votes") and len(rev["votes"]),
        "score": rev.get("score"),
    }
    print(f"  collapse probe: second_created={out['collapse_probe']['second_created']} "
          f"votes={out['collapse_probe']['votes_counted']} "
          f"score={out['collapse_probe']['score']}", flush=True)
    b.results["votes"] = out
    return out


# ── report ─────────────────────────────────────────────────────────────────
def print_report(res: dict) -> None:
    """The table, from a live run or from a --json file."""
    w = res.get("writers") or []
    print("\n== 1. writers (distinct claims, one process per agent) ==")
    print(f"{'N':>4} {'sent':>6} {'2xx':>6} {'lost':>6} {'5xx':>4} "
          f"{'seqgap':>7} {'file=row':>9} {'wall':>7} {'w/s':>7} "
          f"{'p50':>7} {'p95':>8} {'p99':>8}")
    for r in w:
        same = "yes" if (r["files"] == r["db_rows"]
                         and not r["files_not_in_db"] and not r["db_not_in_files"]) else "NO"
        print(f"{r['n']:>4} {r['sent']:>6} {r['ok']:>6} {r['no_response']:>6} "
              f"{r['s5xx']:>4} {r['seq_gaps']:>7} {same:>9} "
              f"{r['wall_s']:>6}s {r['thr']:>7} {r['p50']:>7} {r['p95']:>8} {r['p99']:>8}")
    ka = res.get("keepalive")
    if ka:
        same = "yes" if ka["files"] == ka["db_rows"] else "NO"
        print(f"{'100ka':>4} {ka['sent']:>6} {ka['ok']:>6} {ka['no_response']:>6} "
              f"{ka['s5xx']:>4} {ka['seq_gaps']:>7} {same:>9} "
              f"{ka['wall_s']:>6}s {ka['thr']:>7} {ka['p50']:>7} {ka['p95']:>8} {ka['p99']:>8}")

    b = (res.get("burst") or {}).get("rows") or []
    if b:
        print("\n== 2. accept path: GET /health, no store work ==")
        print(f"{'N':>4} {'ok':>4} {'lost':>5} {'p50':>8} {'p90':>8} {'p99':>9} {'>500ms':>7}")
        for r in b:
            print(f"{r['n']:>4} {r['ok']:>4} {r['no_response']:>5} {r['p50']:>8} "
                  f"{r['p90']:>8} {r['p99']:>9} {r['over_500ms']:>7}")

    idem = res.get("idempotency") or []
    if idem:
        print("\n== 3. idempotency: 50 processes, one proposition ==")
        print(f"{'round':>5} {'201':>4} {'200 false':>9} {'no response':>11} "
              f"{'ids':>4} {'files':>5}")
        for r in idem:
            print(f"{r['round']:>5} {r['created_true']:>4} "
                  f"{r['created_false_received']:>9} {r['no_response']:>11} "
                  f"{r['distinct_ids']:>4} {r['files_for_id']:>5}")

    c = res.get("claims") or {}
    if c:
        ct, db = c.get("contention", {}), c.get("distinct_burst", {})
        print("\n== 4. claims ==")
        print(f"  one question, {ct.get('workers')} processes: "
              f"winners={ct.get('winners')} holder={ct.get('winner')} "
              f"p50={ct.get('p50')}ms p99={ct.get('p99')}ms resets={ct.get('no_response')}")
        print(f"  distinct questions, {db.get('workers')} processes: "
              f"acquired={db.get('acquired')} 500s={db.get('s5xx')} "
              f"no_response={db.get('no_response')} "
              f"locked_after_500={db.get('locked_after_500')}")
        print(f"  ttl=0 race: winners={c.get('ttl0_race', {}).get('winners')}"
              f"/{c.get('ttl0_race', {}).get('workers')}   "
              f"release endpoint: {c.get('release_probe')}")

    rd = res.get("reads") or {}
    if rd:
        r_, cur = rd.get("reads", {}), (rd.get("reads", {}).get("cursor") or {})
        wrow = rd.get("writers", {})
        print("\n== 5. reads during writes ==")
        print(f"  writers: {wrow.get('ok')}/{wrow.get('sent')} 2xx, "
              f"{wrow.get('no_response')} lost, p50={wrow.get('p50')}ms p99={wrow.get('p99')}ms")
        print(f"  reads: {r_.get('reads')} total, 5xx={r_.get('s5xx')} "
              f"bad_shape={r_.get('bad_shape')} find p50/p99={r_.get('find_p50')}/"
              f"{r_.get('find_p99')}ms since p50/p99={r_.get('since_p50')}/"
              f"{r_.get('since_p99')}ms")
        print(f"  cursor consumer: {cur.get('unique_seen')} nodes, "
              f"lost={cur.get('lost_ids')} dup={cur.get('duplicates')} "
              f"backwards={cur.get('backwards')}")

    v = res.get("votes") or {}
    if v.get("runs"):
        print("\n== 6. votes: 20 processes per round ==")
        print(f"{'round':>5} {'created':>8} {'lost':>5} {'server':>7} "
              f"{'recomputed':>10} {'repeat':>7}")
        for r in v["runs"]:
            print(f"{r['round']:>5} {r['created']:>4}/{r['voters']:<3} {r['no_response']:>5} "
                  f"{r['server_score']:>7} {r['recomputed']:>10} "
                  f"{str(r['repeat_identical']):>7}")
        cp = v.get("collapse_probe", {})
        print(f"  identical text from 2 authors: created={cp.get('first_created')}/"
              f"{cp.get('second_created')} votes={cp.get('votes_counted')} "
              f"score={cp.get('score')}")

    for s in res.get("read_scaling") or []:
        print(f"  reads @ {s['rows']} rows: find p50={s['find_p50']}ms p99={s['find_p99']}ms "
              f"since(200) p50={s['since_p50']}ms")
    f = res.get("final")
    if f:
        print(f"\n== 7. integrity == rows={f['db_rows']} files={f['files']} "
              f"max_seq={f['max_seq']} gaps={f['seq_gaps']} dupe_seq={f['dupe_seq']} "
              f"files-not-in-db={f['files_not_in_db']} db-not-in-files={f['db_not_in_files']} "
              f"server_tracebacks={res.get('server_tracebacks')}")


# ── main ───────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-from", default="",
                    help="print the table for a previous --json file and exit")
    ap.add_argument("--writers", type=int, default=0,
                    help="run only this writer count (default: 8,32,100)")
    ap.add_argument("--total", type=int, default=1000, help="writes per writer run")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--root", default="", help="throwaway root (default: mktemp)")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--json", default="", help="write raw results here")
    a = ap.parse_args()

    if a.report_from:
        print_report(json.loads(Path(a.report_from).read_text()))
        return 0

    port = a.port or free_port()
    root = Path(a.root or tempfile.mkdtemp(prefix="corallium-bench-"))
    root.mkdir(parents=True, exist_ok=True)
    log = root / "server.log"

    print("== Corallium concurrency benchmark ==")
    print(f"server {SERVER}\nroot   {root}\nport   {port}  "
          f"python {sys.version.split()[0]}  cores {os.cpu_count()}", flush=True)

    init = subprocess.run([sys.executable, str(SERVER), "--init", str(root),
                           "--init-name", "bench"],
                          capture_output=True, text=True)
    if init.returncode != 0:
        print("init failed:", init.stdout, init.stderr)
        return 2
    fh = open(log, "w")
    srv = subprocess.Popen([sys.executable, str(SERVER), "--root", str(root),
                            "--port", str(port), "--bind", "127.0.0.1"],
                           stdout=fh, stderr=subprocess.STDOUT)
    b = Bench(port, root)
    rc = 0
    try:
        wait_health(port)
        sizes = [a.writers] if a.writers else [8, 32, 100]

        print("\n[1] writer sweep (distinct claims, processes share a barrier)", flush=True)
        scenario_writers(b, sizes, a.total, timeout=60.0)

        print("\n[1b] 100 writers, persistent connection each (store cost only)",
              flush=True)
        if max(sizes) >= 100:
            scenario_keepalive(b, n=100, total=a.total)

        print("\n[1c] accept path: N simultaneous new connections to /health", flush=True)
        scenario_burst(b, sizes=[s for s in (8, 20, 50, 100) if s <= max(sizes)],
                       path="/health")

        print("\n[1d] sequential read latency vs store size (no writers running)",
              flush=True)
        b.results.setdefault("read_scaling", []).append(probe_reads(b, "after-writers"))

        print("\n[2] idempotency: 50 processes, one proposition, 3 rounds", flush=True)
        scenario_idempotency(b)

        print("\n[3] claim contention: 20 processes, one question", flush=True)
        scenario_claims(b)

        print("\n[4] reads during writes: 32 writers, 6 readers, 1 cursor consumer", flush=True)
        scenario_reads(b)

        print("\n[5] votes: 20 processes, one claim, twice", flush=True)
        scenario_votes(b)

        print("\n[6] final integrity", flush=True)
        fin = b.integrity()
        b.results["final"] = fin
        print(f"  rows={fin['db_rows']} files={fin['files']} max_seq={fin['max_seq']} "
              f"gaps={fin['seq_gaps']} dupe_seq={fin['dupe_seq']} "
              f"files_not_in_db={fin['files_not_in_db']} db_not_in_files={fin['db_not_in_files']}",
              flush=True)
        tail = log.read_text(errors="replace")
        tbs = tail.count("Traceback (most recent call last)")
        b.results["server_tracebacks"] = tbs
        b.results.setdefault("read_scaling", []).append(probe_reads(b, "final"))
        print(f"  server tracebacks in log: {tbs}", flush=True)
        if tbs:
            print("\n  ".join(tail.splitlines()[-25:]))
        print_report(b.results)
    except Exception:
        traceback.print_exc()
        rc = 2
    finally:
        srv.terminate()
        try:
            srv.wait(5)
        except subprocess.TimeoutExpired:
            srv.kill()
        fh.close()

    if a.json:
        Path(a.json).write_text(json.dumps(b.results, indent=2))
        print(f"\nraw results -> {a.json}")
    if not a.keep:
        shutil.rmtree(root, ignore_errors=True)
    else:
        print(f"\nkept {root}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
