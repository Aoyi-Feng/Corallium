#!/usr/bin/env python3
"""Vocabulary and read-path obligations (PROTOCOL §2.2, §4.1, §11, v0.1.33).

Each check pins one thing that used to be silently wrong:

  * a type name was a free string, so `Claim` bypassed the falsifier gate and an
    unknown name created a node no rule could see;
  * status in search results was the stored field, so every superseded node
    looked `open` while `tip` correctly called it withdrawn;
  * `?limit=` was parsed by nobody — the page size was always 5;
  * `tip` answered "what do we believe" with votes and tasks in the list;
  * `unreviewed` counted the author's own vote;
  * a review carried no topics, so a topic-filtered follower never saw votes.

Usage:  python3 tests/test_readpath.py [--server corallium] [--port 0]
Exit code 0 = all passed.
"""
import argparse, json, os, shutil, socket, subprocess, sys, tempfile, time
import urllib.error, urllib.request

PASS, FAIL = [], []


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
    from _launch import argv, env
    p = subprocess.Popen(argv(server, "--root", root, "--port", port, "--bind", "127.0.0.1"),
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env())
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
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(("PASS  " if ok else "FAIL  ") + name + (("  — " + detail) if detail and not ok else ""))


def claim(port, o, fals="a run where the opposite happens with zero exceptions", **kw):
    body = {"s": "t", "p": "p", "o": o, "type": "claim", "falsifier": fals,
            "author": kw.pop("author", "alice"), **kw}
    return req(port, "/nodes", body)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="corallium")
    ap.add_argument("--port", type=int, default=0)
    a = ap.parse_args()
    root = tempfile.mkdtemp(prefix="coral-rp-")
    port = a.port or free_port()
    p = None
    try:
        p = start(a.server, root, port)

        # --- vocabulary ---------------------------------------------------
        st, out = claim(port, "unknown type is rejected", type="nonsense")
        check("unknown type -> 400", st == 400, f"{st} {out}")

        st, out = req(port, "/nodes", {"s": "t", "p": "case", "o": "capital C bypasses the gate",
                                       "type": "Claim", "author": "alice"})
        check("type=Claim without falsifier -> 400", st == 400, f"{st} {out}")

        st, out = req(port, "/nodes", {"s": "t", "p": "case", "o": "capital C with a falsifier is a claim",
                                       "type": "Claim", "author": "alice",
                                       "falsifier": "a run where the gate accepts it without one"})
        check("type=Claim is normalised to claim", st == 201 and
              (out.get("node") or {}).get("type") == "claim", f"{st} {out}")

        st, out = req(port, "/nodes", {"s": "t", "p": "alias", "o": "open-question means question",
                                       "type": "open-question", "author": "alice"})
        check("open-question aliases to question", st == 201 and
              (out.get("node") or {}).get("type") == "question", f"{st} {out}")

        st, out = req(port, "/nodes", {"s": "t", "p": "pa", "o": "prior-art is writable",
                                       "type": "prior-art", "author": "alice"})
        check("prior-art is writable", st == 201 and
              (out.get("node") or {}).get("type") == "prior-art", f"{st} {out}")

        # --- find serves derived status -----------------------------------
        st, base = claim(port, "the retry budget is 12 ms", topics=["perf"])
        bid = (base.get("node") or {}).get("id")
        st, sup = req(port, "/nodes", {"s": "t", "p": "p", "o": "superseded answer",
                                       "type": "decision", "rel": "supersedes",
                                       "parents": [bid], "author": "bob",
                                       "note": "replaced"})
        st, found = req(port, "/find?q=retry%20budget")
        hit = next((h for h in found.get("hits", []) if h["id"] == bid), None)
        check("find reports derived withdrawn", hit is not None and hit["status"] == "withdrawn",
              str(hit and hit["status"]))

        # --- page size -----------------------------------------------------
        bodies = ["alpha particles decay inside the chamber",
                  "bravo is the radio call sign for the second station",
                  "charlie horses are not allowed in the lobby",
                  "delta is the difference between two readings",
                  "echo returns only after the signal has travelled",
                  "foxtrot is a dance and also a letter",
                  "golf courses flood when the river rises"]
        for i, sentence in enumerate(bodies):
            claim(port, sentence, s=f"page{i}", p=f"probe{i}", note="zzpage marker")
        st, found = req(port, "/find?q=zzpage&limit=9")
        check("find honours ?limit", len(found.get("hits", [])) > 5,
              f"{len(found.get('hits', []))} hits")

        # --- tip excludes votes and tasks ---------------------------------
        st, c = claim(port, "tip must not list reviews as conclusions")
        cid = (c.get("node") or {}).get("id")
        req(port, f"/nodes/{cid}/replies", {"kind": "sourced", "text": "checked the read path",
                                            "author": "bob", "artifact": "grep -n tip derive.py"})
        req(port, "/nodes", {"s": "t", "p": "task", "o": "do a thing", "type": "task",
                             "author": "bob"})
        st, tip = req(port, "/tip")
        types = {t.get("type") for t in tip.get("tips", [])}
        check("tip has no reviews or tasks", not (types & {"review", "task"}), str(types))

        # --- unreviewed ignores the author's own vote ---------------------
        st, c2 = claim(port, "self-votes must not clear the unreviewed flag")
        c2id = (c2.get("node") or {}).get("id")
        req(port, f"/nodes/{c2id}/replies", {"kind": "sourced", "text": "I checked my own work",
                                             "author": "alice", "artifact": "none"})
        st, found = req(port, f"/find?q={c2id}")
        h = next((x for x in found.get("hits", []) if x["id"] == c2id), None)
        # /find does not annotate `unreviewed`; tip does.
        st, tip = req(port, "/tip")
        t = next((x for x in tip.get("tips", []) if x["id"] == c2id), None)
        check("self-vote keeps unreviewed", t is not None and t.get("unreviewed") is True,
              str(t and t.get("unreviewed")))
        req(port, f"/nodes/{c2id}/replies", {"kind": "replicated", "text": "reproduced independently",
                                             "author": "carol", "artifact": "curl /nodes"})
        st, tip = req(port, "/tip")
        t = next((x for x in tip.get("tips", []) if x["id"] == c2id), None)
        check("external vote clears unreviewed", t is not None and t.get("unreviewed") is False,
              str(t and t.get("unreviewed")))

        # --- reviews inherit topics ---------------------------------------
        st, c3 = claim(port, "a vote inherits the topics of what it votes on", topics=["protocol"])
        c3id = (c3.get("node") or {}).get("id")
        st, rv = req(port, f"/nodes/{c3id}/replies", {"kind": "sourced", "text": "checked",
                                                     "author": "dave", "artifact": "grep"})
        st, since = req(port, "/since?cursor=0&topics=protocol")
        ids = {n["id"] for n in since.get("nodes", [])}
        check("review appears under the target's topic", (rv.get("node") or {}).get("id") in ids,
              f"review={rv.get('node', {}).get('id')}")

        # --- scan counts derived status ------------------------------------
        st, scan = req(port, "/maintenance/scan")
        check("scan counts only live claims without falsifier",
              scan.get("counts", {}).get("claims_no_falsifier", 99) == 0,
              str(scan.get("counts", {}).get("claims_no_falsifier")))
    finally:
        stop(p)
        shutil.rmtree(root, ignore_errors=True)

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
