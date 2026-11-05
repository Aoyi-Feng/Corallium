#!/usr/bin/env python3
"""Smoke test for the Corallium server (PROTOCOL §13).

Proves the four properties whose failure silently corrupts the record:
  1. concurrent identical publishes collapse into one node
  2. concurrent claims: exactly one winner
  3. the SQLite index is disposable (rebuild from nodes/, seq never reused)
  4. a read token cannot write

Usage:  python3 server_smoke_test.py [--server ~/corallium/server.py] [--port 8799]
Exit code 0 = all passed.
"""
import argparse, concurrent.futures as cf, glob, json, os, shutil, socket
import subprocess, sys, tempfile, time, urllib.error, urllib.request
from pathlib import Path


def free_port():
    import socket
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


def start(server, root, port, env_extra=None, extra=None):
    env = dict(os.environ)
    env.update(env_extra or {})
    args = [sys.executable, server] + (extra if extra is not None
                                       else ["--root", root])
    p = subprocess.Popen(
        args + ["--port", str(port), "--bind", "127.0.0.1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env)
    wait_health(port)
    return p


def stop(p):
    p.terminate()
    try:
        p.wait(10)
    except subprocess.TimeoutExpired:
        p.kill()


def req(port, path, payload=None, token=None, method=None):
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(url, data=data, method=method or ("POST" if data else "GET"),
                               headers={"Content-Type": "application/json"} |
                                       ({"Authorization": f"Bearer {token}"} if token else {}))
    try:
        with urllib.request.urlopen(r, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=os.path.expanduser("~/corallium/server.py"))
    ap.add_argument("--port", type=int, default=0, help="0 = pick a free port")
    a = ap.parse_args()
    a.port = a.port or free_port()  # a fixed port collides with a stale child
    root = tempfile.mkdtemp(prefix="coral-smoke-")
    fails = []
    srv = None

    try:
        srv = start(a.server, root, a.port)

        # 1 — concurrent identical publishes
        payload = {"s": "T", "p": "same", "o": "v", "author": "a"}
        with cf.ThreadPoolExecutor(10) as ex:
            results = list(ex.map(lambda _: req(a.port, "/nodes", payload)[1], range(10)))
        created = sum(1 for r in results if r.get("created"))
        files = len(glob.glob(os.path.join(root, "nodes", "*.json")))
        ok = created == 1 and files == 1
        print(f"[1] idempotent publish: created={created} files={files} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(1)

        # 2 — atomic claim
        q = "why is write throughput 571"
        _, r1 = req(a.port, "/claims", {"question": q, "author": "alice", "ttl": 30})
        _, r2 = req(a.port, "/claims", {"question": q, "author": "bob", "ttl": 30})
        ok = r1.get("acquired") is True and r2.get("acquired") is False
        print(f"[2] atomic claim: alice={r1.get('acquired')} bob={r2.get('acquired')} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(2)

        # 3 — index is disposable, seq never reused
        before = req(a.port, "/health")[1]["max_seq"]
        stop(srv)
        for f in glob.glob(os.path.join(root, "index.db*")):
            os.remove(f)
        srv = start(a.server, root, a.port)
        after = req(a.port, "/health")[1]["max_seq"]
        _, new = req(a.port, "/nodes", {"s": "T", "p": "after_restart", "o": "x", "author": "a"})
        new_seq = new.get("node", {}).get("seq")
        ok = after == before and new_seq == before + 1
        print(f"[3] rebuild+seq: before={before} rebuilt={after} next={new_seq} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(3)
        stop(srv)

        # 4 — token separation
        srv = start(a.server, root, a.port,
                    {"CORALLIUM_READ_TOKEN": "r", "CORALLIUM_WRITE_TOKEN": "w"})
        code_no = req(a.port, "/find?q=x")[0]   # data endpoints need the token
        code_read_write = req(a.port, "/nodes", {"s": "T", "p": "tok", "o": "x"}, token="r")[0]
        code_write = req(a.port, "/nodes", {"s": "T", "p": "tok", "o": "x"}, token="w")[0]
        ok = code_no == 401 and code_read_write == 401 and code_write == 201
        print(f"[4] tokens: none={code_no} read_write={code_read_write} write={code_write} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(4)
        stop(srv)
        # 5 — several corals in one process, isolated; leases survive an index wipe
        multi = os.path.join(root, "multi")
        os.makedirs(multi, exist_ok=True)
        reg = os.path.join(multi, "registry.yaml")
        with open(reg, "w") as fh:
            fh.write("alpha: first domain\nbeta: second domain\n")
        srv = start(a.server, root, a.port, extra=["--registry", reg])
        req(a.port, "/c/alpha/nodes", {"s": "A", "p": "only_alpha", "o": "1", "author": "x",
                                       "topics": ["alpha"]})
        req(a.port, "/c/beta/nodes", {"s": "B", "p": "only_beta", "o": "2", "author": "x",
                                      "topics": ["beta"]})
        _, in_a = req(a.port, "/c/alpha/find?q=only_alpha")
        _, cross = req(a.port, "/c/beta/find?q=only_alpha")
        code_unknown, _ = req(a.port, "/c/gamma/find?q=x")
        ok = len(in_a.get("hits", [])) == 1 and cross.get("hits") == [] and code_unknown == 404
        print(f"[5] multi-coral isolation: alpha={len(in_a.get('hits', []))} "
              f"beta_cross={len(cross.get('hits', []))} unknown={code_unknown} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(5)

        req(a.port, "/c/alpha/claims", {"question": "who owns this", "author": "alice", "ttl": 60})
        stop(srv)
        for f in glob.glob(os.path.join(multi, "alpha", "index.db*")):
            os.remove(f)
        srv = start(a.server, root, a.port, extra=["--registry", reg])
        _, again = req(a.port, "/c/alpha/claims", {"question": "who owns this", "author": "bob", "ttl": 60})
        ok = again.get("acquired") is False and again.get("holder") == "alice"
        print(f"[6] lease survives index wipe: bob={again.get('acquired')} "
              f"holder={again.get('holder')} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(6)
        stop(srv)

        # 7-9 — semantics: attempts gate, suspect cascade, tip follows edges
        srv = start(a.server, root, a.port)
        pr = req(a.port, "/nodes", {"s": "T", "p": "probe", "o": "x", "author": "z"})[1]["node"]["id"]
        g = req(a.port, "/nodes", {"s": "T", "p": "many_tries", "o": "1.6", "author": "a",
                                   "attempts": 9, "topics": ["T"]})[1]["node"]
        req(a.port, f"/nodes/{g['id']}/replies",
            {"kind": "replicated", "author": "b", "prior": pr,
             "artifact": "cmd", "text": "re-ran, same number"})
        st1 = req(a.port, "/find?q=many_tries")[1]["hits"][0]["derived_status"]
        req(a.port, f"/nodes/{g['id']}/replies",
            {"kind": "adversarial", "author": "c", "prior": pr, "artifact": "attack.py",
             "text": "tried 5 configs, all failed"})
        st2 = req(a.port, "/find?q=many_tries")[1]["hits"][0]["derived_status"]
        ok = st1 == "open" and st2 == "confirmed"
        print(f"[7] attempts>5 gate: without adversarial={st1} with={st2} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(7)

        base = req(a.port, "/nodes", {"s": "T", "p": "base", "o": "1", "author": "a", "topics": ["T"]})[1]["node"]
        mid = req(a.port, "/nodes", {"s": "T", "p": "mid", "o": "2", "author": "a",
                                     "rel": "extends", "parents": [base["id"]], "topics": ["T"]})[1]["node"]
        req(a.port, "/nodes", {"s": "T", "p": "base", "o": "1", "author": "a", "rel": "supersedes",
                               "parents": [base["id"]], "status": "withdrawn", "note": "made up"})
        stb = req(a.port, "/find?q=mid")[1]["hits"][0]["derived_status"]
        ok = stb == "suspect"
        print(f"[8] retraction cascade: child of withdrawn = {stb} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(8)

        tips = req(a.port, "/tip?topic=T")[1]["tips"]
        ids = {t["id"] for t in tips}
        ok = base["id"] not in ids and mid["id"] in ids
        print(f"[9] tip = graph leaves: superseded absent, leaf present -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(9)
        stop(srv)

        # 10 — file references: pinned at write, live-checked, missing -> suspect
        srv = start(a.server, root, a.port)
        f = Path(root) / "evidence.csv"
        f.write_text("run,iops\n1,22558\n")
        ref = req(a.port, "/nodes", {"s": "T", "p": "ref", "o": "22,558",
                                     "author": "a", "topics": ["T"],
                                     "refs": [{"path": "evidence.csv"}]})[1]["node"]
        ok_pin = bool(ref["refs"][0].get("sha256"))
        st_ok = req(a.port, "/find?q=22,558")[1]["hits"][0]["derived_status"]
        f.write_text("run,iops\n1,22558\n2,900\n")
        v = req(a.port, "/verify?node=" + ref["id"])[1]
        st_chg = req(a.port, "/find?q=22,558")[1]["hits"][0]["derived_status"]
        f.unlink()
        v2 = req(a.port, "/verify?node=" + ref["id"])[1]
        st_gone = req(a.port, "/find?q=22,558")[1]["hits"][0]["derived_status"]
        ok = (ok_pin and st_ok == "open" and v["changed"] == 1 and st_chg == "open"
              and v2["missing"] == 1 and st_gone == "suspect")
        print(f"[10] refs: pinned={ok_pin} changed={v['changed']}/{st_chg} "
              f"missing={v2['missing']}/{st_gone} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(10)
        stop(srv)
    finally:
        if srv is not None:
            stop(srv)
        shutil.rmtree(root, ignore_errors=True)

    print("ALL PASS" if not fails else f"FAILED: {fails}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
