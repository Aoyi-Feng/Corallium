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
import subprocess, sys, tempfile, time, urllib.error, urllib.parse, urllib.request
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

        # 11 — a parent id must resolve: an unknown one is no edge at all, and it
        #      used to buy a free pass around the §3.4 similarity gate.
        srv = start(a.server, root, a.port)
        code_bad, bad = req(a.port, "/nodes", {"s": "P", "p": "child", "o": "x",
                                               "author": "a", "rel": "extends",
                                               "parents": ["000000000000"]})
        rootn = req(a.port, "/nodes", {"s": "P", "p": "anchor", "o": "anchor value",
                                       "author": "a", "topics": ["P"]})[1]["node"]
        code_ok, okbody = req(a.port, "/nodes", {"s": "P", "p": "child", "o": "x", "author": "a",
                                                 "rel": "extends", "parents": [rootn["id"]]})
        ok = code_bad == 400 and code_ok == 201 and "linked_to" in okbody
        print(f"[11] parent must exist: unknown={code_bad} real={code_ok} "
              f"linked_to={'linked_to' in okbody} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(11)

        # 12 — leases: holder-only release, and GET /claims can be read at all
        q2 = "who owns the write cache"
        req(a.port, "/claims", {"question": q2, "author": "alice", "ttl": 30})
        _, steal = req(a.port, "/claims", {"question": q2, "author": "bob", "ttl": -1})
        _, give = req(a.port, "/claims", {"question": q2, "author": "alice", "ttl": -1})
        _, again = req(a.port, "/claims", {"question": q2, "author": "bob", "ttl": 30})
        _, live = req(a.port, "/claims")
        held = [c for c in live.get("claims", []) if c.get("question") == q2]
        ok = (steal.get("released") is False and steal.get("holder") == "alice"
              and give.get("released") is True and again.get("acquired") is True
              and len(held) == 1 and held[0]["author"] == "bob")
        print(f"[12] leases: steal={steal.get('released')} owner_release={give.get('released')} "
              f"next={again.get('acquired')} listed={len(held)} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(12)
        stop(srv)

        # 13 — read-only deployment: a read token alone must not leave writes open
        srv = start(a.server, root, a.port, {"CORALLIUM_READ_TOKEN": "r"})
        code_ro = req(a.port, "/nodes", {"s": "T", "p": "ro", "o": "x"}, token="r")[0]
        code_ro_none = req(a.port, "/nodes", {"s": "T", "p": "ro", "o": "x"})[0]
        ok = code_ro == 401 and code_ro_none == 401
        print(f"[13] read-only: write_with_read_token={code_ro} write_no_token={code_ro_none} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(13)
        stop(srv)

        # 14 — the gate works on Chinese, where the old prefilter compared against
        #      an arbitrary LIMIT N rows instead of the trigram index.
        srv = start(a.server, root, a.port)
        zh = {"s": "MQSim", "p": "写缓存", "author": "a",
              "o": "断电时已确认的写请求会丢失，因为写缓存没有刷盘"}
        first = req(a.port, "/nodes", zh)[0]
        near = dict(zh, o="断电时已经确认的写请求会丢失，因为写缓存没有刷盘")
        again_zh = req(a.port, "/nodes", near)[0]
        forced = req(a.port, "/nodes", dict(near, force=True,
                                            why_new="讲的是掉电后的恢复路径，不是缓存本身"))[0]
        ok = first == 201 and again_zh == 409 and forced == 201
        print(f"[14] CJK gate: first={first} near_dup={again_zh} forced={forced} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(14)

        # 15 — refs stay a list in /find (they were replaced by a count), and one
        #      topic-scoped query reports the exact number outside the scope.
        ev = Path(root) / "ev.txt"
        ev.write_text("x\n")
        req(a.port, "/nodes", {"s": "R", "p": "pinned", "o": "refs stay a list",
                               "author": "a", "topics": ["inscope"],
                               "refs": [{"path": "ev.txt"}]})
        for i, tail in enumerate(("cache line is 64 bytes", "DRAM refresh is 32ms",
                                  "PCIe lane is 8GT/s")):
            code = req(a.port, "/nodes", {"s": "R", "p": "other_%d" % i,
                                          "o": "outside the scope: " + tail, "author": "a",
                                          "topics": ["outscope"]})[0]
            if code != 201:
                print(f"    warn: scoped fixture {i} not written ({code})")
        _, f = req(a.port, "/find?q=refs%20stay%20a%20list")
        hit = (f.get("hits") or [{}])[0]
        ok_list = isinstance(hit.get("refs"), list) and hit.get("refs_count") == 1
        _, scoped = req(a.port, "/find?q=outside%20the%20scope&root=inscope")
        ok_scope = scoped.get("hits") == [] and scoped.get("outside") == 3
        ok = ok_list and ok_scope
        print(f"[15] find shape: refs_is_list={ok_list} outside={scoped.get('outside')} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(15)
        stop(srv)

        # 16 — a raw UTF-8 query (?q=中文, what curl sends when pasted) used to
        #      arrive as latin-1 mojibake and match nothing, silently.
        srv = start(a.server, root, a.port)
        req(a.port, "/nodes", {"s": "Z", "p": "raw", "o": "原始中文查询命中", "author": "a"})

        def raw_get(target):
            sk = socket.create_connection(("127.0.0.1", a.port), timeout=5)
            sk.sendall(("GET " + target + " HTTP/1.1\r\nHost: x\r\n"
                        "Connection: close\r\n\r\n").encode("utf-8"))
            buf = b""
            while True:
                ch = sk.recv(65536)
                if not ch:
                    break
                buf += ch
            sk.close()
            return json.loads(buf.split(b"\r\n\r\n", 1)[1])

        n_raw = len(raw_get("/find?q=原始中文查询").get("hits") or [])
        n_enc = len(raw_get("/find?q=" + urllib.parse.quote("原始中文查询")).get("hits") or [])
        ok = n_raw == 1 and n_enc == 1
        print(f"[16] raw UTF-8 query: raw={n_raw} encoded={n_enc} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(16)
        stop(srv)

        # 17 — tip scopes by topic label, not by any occurrence of the word in the
        #      body; a vote on a node that does not exist is a 404, not a lost vote.
        srv = start(a.server, root, a.port)
        in_topic = req(a.port, "/nodes", {"s": "TP", "p": "labelled", "o": "a labelled node",
                                          "author": "a", "topics": ["labelfix"]})[1]["node"]
        req(a.port, "/nodes", {"s": "TP", "p": "mentions", "author": "a", "topics": ["other"],
                               "o": "this body mentions labelfix but carries another label"})
        tips = req(a.port, "/tip?topic=labelfix")[1].get("tips") or []
        code_vote = req(a.port, "/nodes/ffffffffffff/replies",
                        {"kind": "refuted", "text": "typo target", "author": "b",
                         "artifact": "cmd"})[0]
        code_q = req(a.port, "/claims", {"question": "   ", "author": "b", "ttl": 30})[0]
        ok = (len(tips) == 1 and tips[0]["id"] == in_topic["id"]
              and code_vote == 404 and code_q == 400)
        print(f"[17] tip/vote guards: tips={len(tips)} vote={code_vote} empty_q={code_q} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(17)
        stop(srv)

        # 18 — a `merges` decision settles the rivalry it names (§6.2). Two live
        #      answers stayed `contested` forever after the adjudication, so the
        #      graph recorded a resolution the status never noticed.
        srv = start(a.server, root, a.port)
        A = req(a.port, "/nodes", {"s": "M", "p": "answer", "o": "571 MB/s",
                                   "author": "a", "topics": ["M"]})[1]["node"]["id"]
        B = req(a.port, "/nodes", {"s": "M", "p": "answer", "o": "903 MB/s",
                                   "author": "b", "topics": ["M"]})[1]["node"]["id"]

        def status_of(q, nid):
            for h in req(a.port, "/find?q=" + q)[1].get("hits", []):
                if h.get("id") == nid:
                    return h.get("derived_status")
            return None

        before = (status_of("571", A), status_of("903", B))
        code_merge = req(a.port, "/maintenance/merge",
                         {"keep": A, "drop": B, "reason": "geometry 70%, cache 30%",
                          "author": "curator"})[0]
        after = (status_of("571", A), status_of("903", B))
        ok = (before == ("contested", "contested") and code_merge == 201
              and "contested" not in after)
        print(f"[18] merge resolves: before={before} merge={code_merge} after={after} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(18)

        # 19 — §4.4 `confirmed` has two levels, and two `sourced` votes confirm
        pr = req(a.port, "/nodes", {"s": "V", "p": "prior", "o": "pre-registered baseline",
                                    "author": "w"})[1]["node"]["id"]
        c1 = req(a.port, "/nodes", {"s": "V", "p": "rep", "o": "0.931", "author": "w",
                                    "topics": ["V"]})[1]["node"]["id"]
        req(a.port, f"/nodes/{c1}/replies",
            {"kind": "replicated", "author": "x", "prior": pr,
             "artifact": "cmd --same", "text": "re-ran"})
        c2 = req(a.port, "/nodes", {"s": "V", "p": "src", "o": "0.742", "author": "w",
                                    "topics": ["V"]})[1]["node"]["id"]
        for who in ("y", "z"):
            req(a.port, f"/nodes/{c2}/replies",
                {"kind": "sourced", "author": who, "artifact": "doi:" + who,
                 "text": "independent paper"})
        c3 = req(a.port, "/nodes", {"s": "V", "p": "adv", "o": "0.615", "author": "w",
                                    "topics": ["V"]})[1]["node"]["id"]
        req(a.port, f"/nodes/{c3}/replies",
            {"kind": "adversarial", "author": "x", "prior": pr,
             "artifact": "attack.py", "text": "tried 5 configs"})

        def level_of(q, nid):
            for h in req(a.port, "/find?q=" + q)[1].get("hits", []):
                if h.get("id") == nid:
                    return h.get("derived_status"), h.get("confirmed_level")
            return None, None

        l1, l2, l3 = level_of("0.931", c1), level_of("0.742", c2), level_of("0.615", c3)
        ok = (l1 == ("confirmed", "unchallenged") and l2 == ("confirmed", "unchallenged")
              and l3 == ("confirmed", "challenged"))
        print(f"[19] confirmed_level: replicated={l1} 2xSourced={l2} adversarial={l3} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(19)
        stop(srv)

        # 20 — a node reference is checkable: a ref to an id that is not here is
        #      `missing` in /verify and makes the node `suspect`, not "ok".
        srv = start(a.server, root, a.port)
        tgt = req(a.port, "/nodes", {"s": "N", "p": "target", "o": "a real node",
                                     "author": "a", "topics": ["N"]})[1]["node"]
        good = req(a.port, "/nodes", {"s": "N", "p": "refgood", "o": "points at a node",
                                      "author": "a", "topics": ["N"],
                                      "refs": [{"node": tgt["id"]}]})[1]["node"]
        bad = req(a.port, "/nodes", {"s": "N", "p": "refbad", "o": "points nowhere",
                                     "author": "a", "topics": ["N"],
                                     "refs": [{"node": "ffffffffffff"}]})[1]["node"]
        vg = req(a.port, "/verify?node=" + good["id"])[1]
        vb = req(a.port, "/verify?node=" + bad["id"])[1]
        sg = status_of("refgood", good["id"])
        sb = status_of("refbad", bad["id"])
        ok = vg["ok"] == 1 and vb["missing"] == 1 and sg == "open" and sb == "suspect"
        print(f"[20] node refs: good={vg['ok']}/{sg} bad={vb['missing']}/{sb} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(20)

        # 21 — every proposition carries its score, not only `claim` (§4.2).
        #      A sourced vote on a `question` used to come back score=None, so the
        #      vote was in the graph and invisible in every read.
        qn = req(a.port, "/nodes", {"s": "S", "p": "q:score", "o": "does a question keep its score",
                                    "type": "question", "author": "a", "topics": ["S"]})[1]["node"]
        req(a.port, f"/nodes/{qn['id']}/replies",
            {"kind": "sourced", "author": "b", "artifact": "doc:b", "text": "yes"})
        hit = next((h for h in req(a.port, "/find?q=question")[1]["hits"]
                    if h.get("id") == qn["id"]), {})
        ok = hit.get("score") == 2 and hit.get("votes") == 1
        print(f"[21] non-claim score: score={hit.get('score')} votes={hit.get('votes')} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(21)

        # 22 — a malformed query parameter is 400, not 500. A 500 says "the
        #      server broke"; the caller only has to fix their cursor.
        code_bad = req(a.port, "/since?cursor=abc")[0]
        ok = code_bad == 400
        print(f"[22] bad cursor: {code_bad} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(22)
        # 23 — the maintenance report must not flag evidence of a retracted node:
        #      its missing ref is nobody's job, and re-listing it every scan is
        #      noise that hides real ones. `withdrawn` must be the derived count.
        dead = req(a.port, "/nodes", {"s": "D", "p": "gone", "o": "a retracted claim with bad evidence",
                                      "author": "a", "topics": ["D"],
                                      "refs": [{"path": "/no/such/file"}]})[1]["node"]
        req(a.port, "/nodes", {"s": "D", "p": "gone", "o": "the correction", "author": "a",
                               "topics": ["D"], "rel": "supersedes", "parents": [dead["id"]]})
        sc = req(a.port, "/maintenance/scan")[1]
        dead_listed = any(b.get("node") == dead["id"] for b in sc.get("broken_refs", []))
        dead_as_dup = any(dead["id"] in (d.get("a"), d.get("b")) for d in sc.get("duplicates", []))
        ok = (not dead_listed and not dead_as_dup
              and sc["counts"].get("withdrawn", 0) >= 1
              and isinstance(sc["counts"].get("by_status"), dict))
        print(f"[23] scan ignores retracted refs: listed={dead_listed} as_dup={dead_as_dup} "
              f"withdrawn={sc['counts'].get('withdrawn')} by_status={sc['counts'].get('by_status')} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(23)
        stop(srv)
    finally:
        if srv is not None:
            stop(srv)
        shutil.rmtree(root, ignore_errors=True)

    print("ALL PASS" if not fails else f"FAILED: {fails}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
