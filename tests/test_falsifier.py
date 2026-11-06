#!/usr/bin/env python3
"""Falsifier gate (PROTOCOL §2.1, v0.1.29).

A claim must say what would make it false. The server enforces three things and
nothing more: presence, a minimum length, and that the sentence is not the claim
restated. Whether a falsifier is *good* is a reader's judgement — this test only
pins the mechanical part, because that is the part that was missing for 103
claims written before v0.1.29.

Usage:  python3 tests/test_falsifier.py [--server corallium] [--port 0]
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
            return resp.code, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def check(n, name, ok, detail=""):
    (PASS if ok else FAIL).append(n)
    print(f"[{n}] {name}: {detail} -> {'PASS' if ok else 'FAIL'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="corallium")
    ap.add_argument("--port", type=int, default=0)
    a = ap.parse_args()
    port = a.port or free_port()
    root = tempfile.mkdtemp(prefix="coral-fals-")
    srv = None
    try:
        srv = start(a.server, root, port)
        base = {"s": "cache", "p": "eviction", "o": "LRU evicts 40 percent under 2GB",
                "author": "tester"}

        code, body = req(port, "/nodes", dict(base))
        check(1, "claim without falsifier", code == 400 and "falsifier" in json.dumps(body),
              f"code={code}")

        code, body = req(port, "/nodes", dict(base, falsifier="maybe wrong"))
        check(2, "falsifier too short", code == 400 and "too short" in json.dumps(body),
              f"code={code}")

        code, body = req(port, "/nodes", dict(base, falsifier=base["o"] + " of memory"))
        check(3, "falsifier restates the claim", code == 400 and "restates" in json.dumps(body),
              f"code={code}")

        code, body = req(port, "/nodes", dict(base, falsifier=base["o"]))
        check(4, "verbatim copy rejected", code == 400, f"code={code}")

        good = "a 10-minute trace where evictions stay under 10 percent at 2GB"
        code, body = req(port, "/nodes", dict(base, falsifier=good))
        stored = (body.get("node") or {}).get("falsifier")
        check(5, "good falsifier accepted and stored", code == 201 and stored == good,
              f"code={code} stored={stored!r}")

        code, body = req(port, "/nodes", {"s": "log", "p": "odd", "o": "two timestamps one second apart",
                                          "type": "anomaly", "author": "tester"})
        check(6, "anomaly needs none", code == 201, f"code={code}")

        code, body = req(port, "/nodes", {"s": "corallium", "p": "design", "o": "gates are scoped by node class",
                                          "type": "decision", "author": "tester"})
        check(7, "decision needs none", code == 201, f"code={code}")

        code, body = req(port, "/find?q=eviction")
        hit = [h for h in (body.get("hits") or []) if h.get("id") == (body.get("hits") or [{}])[0].get("id")]
        check(8, "falsifier is readable back", bool(hit), f"hits={len(body.get('hits') or [])}")
    finally:
        stop(srv)
        shutil.rmtree(root, ignore_errors=True)

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    return 0 if not FAIL else 1


if __name__ == "__main__":
    sys.exit(main())
