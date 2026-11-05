#!/usr/bin/env python3
"""Purge/tombstone test (PROTOCOL §14.3, §19).

A purge is only real if all four of these hold — each one has silently failed
somewhere before, which is why each gets its own assertion:

  1. the payload is gone from the node file (rewritten, not shadowed)
  2. the payload is gone from the search index (FTS rows deleted, not duplicated)
  3. the id still resolves and children become `suspect` instead of dangling
  4. deleting index.db and restarting does not resurrect any of it

Usage:  python3 tests/test_purge.py [--server ~/corallium/server.py] [--port 0]
Exit code 0 = all passed.
"""
import argparse, glob, json, os, shutil, socket, subprocess, sys, tempfile
import time, urllib.error, urllib.request
from urllib.parse import quote

SECRET = "sk-live-TOPSECRET-9f3a2b"
NOTE = "prod deploy key, do not share"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def wait_health(port, timeout=15.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as r:
                return json.loads(r.read())
        except Exception:
            time.sleep(0.2)
    raise RuntimeError(f"server on {port} never became healthy")


def start(server, root, port):
    p = subprocess.Popen(
        [sys.executable, server, "--root", root, "--port", str(port), "--bind", "127.0.0.1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    wait_health(port)
    return p


def stop(p):
    if p is None:
        return
    p.terminate()
    try:
        p.wait(10)
    except subprocess.TimeoutExpired:
        p.kill()


def req(port, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data,
                               method="POST" if data else "GET",
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            return resp.code, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def hits(port, q):
    return req(port, "/find?q=" + quote(q))[1].get("hits", [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=os.path.expanduser("~/corallium/server.py"))
    ap.add_argument("--port", type=int, default=0, help="0 = pick a free port")
    a = ap.parse_args()
    a.port = a.port or free_port()
    root = tempfile.mkdtemp(prefix="coral-purge-")
    fails, srv = [], None

    def check(n, label, ok, detail=""):
        print(f"[{n}] {label}: {detail} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(n)

    try:
        srv = start(a.server, root, a.port)

        # ── fixture: a secret, its child, its grandchild ───────────────────
        secret = req(a.port, "/nodes", {
            "s": "deploy", "p": "prod_api_key", "o": SECRET, "author": "alice",
            "topics": ["secret"], "note": NOTE, "method": "pasted from vault",
        })[1]["node"]
        child = req(a.port, "/nodes", {
            "s": "deploy", "p": "uses_prod_key", "o": "deploy script reads the key",
            "author": "bob", "rel": "extends", "parents": [secret["id"]],
            "topics": ["secret"],
        })[1]["node"]
        grand = req(a.port, "/nodes", {
            "s": "deploy", "p": "rollout", "o": "rollout needs the deploy script",
            "author": "carol", "rel": "extends", "parents": [child["id"]],
            "topics": ["secret"],
        })[1]["node"]
        path = os.path.join(root, "nodes", secret["id"] + ".json")

        # ── 0. precondition: the secret is published, indexed, and load-bearing
        ok = (len(hits(a.port, SECRET)) == 1 and len(hits(a.port, "prod_api_key")) == 1
              and hits(a.port, "rollout")[0]["derived_status"] == "open")
        check(0, "precondition searchable", ok,
              f"secret={len(hits(a.port, SECRET))} p={len(hits(a.port, 'prod_api_key'))} "
              f"grand={hits(a.port, 'rollout')[0]['derived_status']}")

        # ── 1. purge succeeds and rewrites the file ─────────────────────────
        code, r = req(a.port, "/purge", {"id": secret["id"], "reason": "contained a live API key",
                                         "author": "agent-store"})
        check(1, "purge status", code == 200 and r.get("purged") is True, f"code={code}")

        raw = open(path, encoding="utf-8").read()
        tomb = json.loads(raw)
        want = {"id", "seq", "ts", "type", "status", "purged_at", "reason", "author"}
        ok = (set(tomb) == want and tomb["type"] == "tombstone"
              and tomb["status"] == "withdrawn" and tomb["id"] == secret["id"]
              and tomb["seq"] == secret["seq"])
        check(2, "tombstone shape", ok, f"keys={sorted(tomb)}")

        # ── 3. the payload is not in the file, in any encoding of it ───────
        leaked = [w for w in (SECRET, "prod_api_key", NOTE, "pasted from vault",
                              secret["author"]) if w in raw]
        ok = leaked == []
        check(3, "no payload in file", ok, f"leaked={leaked}")

        # ── 4. the index forgot it: 0 hits, not "one tombstone with the text" ─
        n_secret, n_key, n_note = len(hits(a.port, SECRET)), len(hits(a.port, "prod_api_key")), len(hits(a.port, NOTE))
        ok = n_secret == 0 and n_key == 0 and n_note == 0
        check(4, "find returns 0 hits", ok, f"secret={n_secret} p={n_key} note={n_note}")

        # ── 5. the id still resolves: no dangling parent, cascade is suspect ─
        s_child = hits(a.port, "uses_prod_key")
        s_grand = hits(a.port, "rollout")
        since = req(a.port, "/since?cursor=%d" % (secret["seq"] - 1))[1]["nodes"]
        ids = [n["id"] for n in since]
        ok = (len(s_child) == 1 and s_child[0]["derived_status"] == "suspect"
              and len(s_grand) == 1 and s_grand[0]["derived_status"] == "suspect"
              and secret["id"] in ids)
        check(5, "graph holds / child suspect", ok,
              f"child={s_child[0]['derived_status'] if s_child else 'missing'} "
              f"grand={s_grand[0]['derived_status'] if s_grand else 'missing'} "
              f"id_in_since={secret['id'] in ids}")

        # ── 6. a retired node must not be a tip either ──────────────────────
        tip_ids = {t["id"] for t in req(a.port, "/tip?topic=secret")[1]["tips"]}
        ok = secret["id"] not in tip_ids
        check(6, "tombstone is not a tip", ok, f"tips={len(tip_ids)}")

        # ── 7. second purge: 409, and the first reason survives ────────────
        code2, r2 = req(a.port, "/purge", {"id": secret["id"], "reason": "again",
                                           "author": "agent-store"})
        again = json.loads(open(path, encoding="utf-8").read())
        ok = code2 == 409 and again["reason"] == "contained a live API key"
        check(7, "second purge = 409", ok, f"code={code2} reason={again['reason']!r}")

        # ── 8. refusals: no reason (400, node untouched), unknown id (404) ──
        other = req(a.port, "/nodes", {"s": "deploy", "p": "other", "o": "keep me",
                                       "author": "alice"})[1]["node"]
        c_noreason = req(a.port, "/purge", {"id": other["id"], "reason": "   "})[0]
        c_unknown = req(a.port, "/purge", {"id": "deadbeef0000", "reason": "x"})[0]
        still = json.loads(open(os.path.join(root, "nodes", other["id"] + ".json")).read())
        ok = (c_noreason == 400 and c_unknown == 404 and still.get("o") == "keep me")
        check(8, "400 no reason / 404 unknown", ok,
              f"noreason={c_noreason} unknown={c_unknown} survivor={still.get('o')!r}")

        # ── 9. rebuild: delete the index, restart, the past stays deleted ───
        stop(srv)
        for f in glob.glob(os.path.join(root, "index.db*")):
            os.remove(f)
        srv = start(a.server, root, a.port)
        tomb2 = json.loads(open(path, encoding="utf-8").read())
        n_secret = len(hits(a.port, SECRET))
        n_key = len(hits(a.port, "prod_api_key"))
        s_child = hits(a.port, "uses_prod_key")
        s_grand = hits(a.port, "rollout")
        ok = (tomb2["type"] == "tombstone" and n_secret == 0 and n_key == 0
              and len(s_child) == 1 and s_child[0]["derived_status"] == "suspect"
              and len(s_grand) == 1 and s_grand[0]["derived_status"] == "suspect")
        check(9, "survives index rebuild", ok,
              f"type={tomb2['type']} secret={n_secret} p={n_key} "
              f"child={s_child[0]['derived_status'] if s_child else 'missing'} "
              f"grand={s_grand[0]['derived_status'] if s_grand else 'missing'}")

        # ── 10. the restart must not have re-indexed the secret from anywhere
        code3 = req(a.port, "/purge", {"id": secret["id"], "reason": "x"})[0]
        check(10, "still a tombstone after rebuild", code3 == 409, f"code={code3}")
    finally:
        stop(srv)
        shutil.rmtree(root, ignore_errors=True)

    print("ALL PASS" if not fails else f"FAILED: {fails}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
