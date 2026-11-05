#!/usr/bin/env python3
"""Run the Hermes plugin's seven handlers against a throwaway coral (PROTOCOL §13.5).

Import success, "enabled in config" and seven registered names prove only that the
module loads. This exercises the handlers and the HTTP client together — the part
that can rot silently, because a plugin is only ever tested by being used.

    python3 tests/plugin_probe.py [--plugin ~/.hermes/plugins/corallium]

Exit code 0 = all passed. Never touches ~/corals.
"""
import argparse
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import types
import urllib.request
from pathlib import Path

TOOLS = ["coral_find", "coral_publish", "coral_tip", "coral_vote", "coral_state",
         "coral_claim", "coral_release"]


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


def load_plugin(path: Path):
    """A plugin package imports itself relatively (`from . import client`), so the
    file cannot be loaded alone — register a stub parent with a __path__ first."""
    pkg = types.ModuleType("coralplug")
    pkg.__path__ = [str(path)]
    sys.modules["coralplug"] = pkg
    spec = importlib.util.spec_from_file_location("coralplug.__init__", str(path / "__init__.py"))
    assert spec and spec.loader, "cannot load the plugin package from %s" % path
    mod = importlib.util.module_from_spec(spec)
    sys.modules["coralplug.__init__"] = mod
    spec.loader.exec_module(mod)
    return mod


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plugin", default=os.path.expanduser("~/.hermes/plugins/corallium"))
    ap.add_argument("--server", default=os.path.expanduser("~/corallium/server.py"))
    a = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="coral-plugin-"))
    root = tmp / "probe"
    subprocess.run([sys.executable, a.server, "--init", str(root), "--init-name", "probe",
                    "--desc", "plugin probe", "--owner", "test"],
                   check=True, stdout=subprocess.DEVNULL)
    port = free_port()
    srv = subprocess.Popen([sys.executable, a.server, "--root", str(root),
                            "--port", str(port), "--bind", "127.0.0.1"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    fails = []
    try:
        wait_health(port)
        os.environ["CORALLIUM_URL"] = f"http://127.0.0.1:{port}"
        os.environ["CORALLIUM_CORAL"] = "default"
        os.environ["CORALLIUM_CORALS_DIR"] = str(tmp)
        os.environ.pop("CORALLIUM_AUTHOR", None)

        m = load_plugin(Path(a.plugin).expanduser())

        class Ctx:
            names = []

            def register_tool(self, **kw):
                self.names.append(kw["name"])

        ctx = Ctx()
        m.register(ctx)
        ok = ctx.names == TOOLS
        print(f"[1] register: {len(ctx.names)} tools -> {'PASS' if ok else 'FAIL ' + str(ctx.names)}")
        if not ok:
            fails.append(1)

        print("[2] publish twice")
        one = m._h_publish({"s": "Corallium", "p": "plugin:publish", "o": "插件与 CLI 走同一个写入闸",
                            "topics": ["plugin"], "method": '{"cmd": "plugin_probe.py"}'})
        two = m._h_publish({"s": "Corallium", "p": "plugin:publish", "o": "插件与 CLI 走同一个写入闸",
                            "topics": ["plugin"]})
        ok = one.startswith("created") and two.startswith("already existed")
        print(f"    {one.splitlines()[0]} / {two.splitlines()[0]} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(2)
        nid = one.split()[1]

        print("[3] 409 similar shows candidates and the three exits")
        c = m._h_publish({"s": "Corallium", "p": "plugin:gate", "o": "插件与 CLI 走同一个写入闸。",
                          "topics": ["plugin"]})
        ok = ("409 similar" in c and nid in c and "force=true" in c
              and "nothing was recorded" in c)
        print("    " + c.replace("\n", "\n    ") + f"\n    -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(3)

        print("[4] force + why_new is the third exit")
        d = m._h_publish({"s": "Corallium", "p": "plugin:gate", "o": "插件与 CLI 走同一个写入闸。",
                          "topics": ["plugin"], "force": True,
                          "why_new": "这条讲绕过闸门，候选讲正常写入"})
        e = m._h_publish({"s": "x", "p": "y", "o": "z", "force": True})
        ok = d.startswith("created") and "needs why_new" in e
        print(f"    forced={d.splitlines()[0]} bare_force_refused={'needs why_new' in e} "
              f"-> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(4)

        print("[5] vote: gate caveat only where the gate applies")
        tgt = m._h_publish({"s": "T", "p": "target", "o": "a claim to vote on"}).split()[1]
        src = m._h_vote({"node": tgt, "kind": "sourced", "text": "paper", "artifact": "doi:1"})
        rep = m._h_vote({"node": tgt, "kind": "replicated", "text": "x"})
        ok = "capped" not in src and "capped at 1" in rep
        print(f"    sourced={src!r}\n    replicated={rep!r} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(5)

        print("[6] claim contention: one winner, the loser gets a name")
        os.environ["CORALLIUM_AUTHOR"] = "alice"
        a1 = m._h_claim({"question": "why is 128K seq write only 571", "ttl": 5})
        os.environ["CORALLIUM_AUTHOR"] = "bob"
        b1 = m._h_claim({"question": "why is 128K seq write only 571", "ttl": 5})
        steal = m._h_release({"question": "why is 128K seq write only 571"})
        os.environ["CORALLIUM_AUTHOR"] = "alice"
        rel = m._h_release({"question": "why is 128K seq write only 571"})
        os.environ["CORALLIUM_AUTHOR"] = "bob"
        b2 = m._h_claim({"question": "why is 128K seq write only 571", "ttl": 5})
        ok = (a1.startswith("claimed") and b1.startswith("busy: alice")
              and steal.startswith("cannot release: alice")
              and rel.startswith("released") and b2.startswith("claimed"))
        print(f"    alice={'claimed' if a1.startswith('claimed') else a1}\n"
              f"    bob={'busy' if b1.startswith('busy') else b1}\n"
              f"    steal_refused={'cannot release' in steal} release={'released' if rel.startswith('released') else rel}\n"
              f"    bob_after={'claimed' if b2.startswith('claimed') else b2} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(6)

        print("[7] read side: find / tip / state see the writes")
        f = m._h_find({"q": "plugin:publish"})
        tip = m._h_tip({"topic": "plugin"})
        st = m._h_state({"cursor": 0})
        ok = nid in f and "plugin:publish" in f and "plugin:publish" in tip and "new since" in st
        print(f"    find={'hit' if nid in f else f!r} tip={'hit' if 'plugin:publish' in tip else tip!r} "
              f"state={'ok' if 'new since' in st else st!r} -> {'PASS' if ok else 'FAIL'}")
        if not ok:
            fails.append(7)
    finally:
        srv.terminate()
        try:
            srv.wait(10)
        except subprocess.TimeoutExpired:
            srv.kill()
        shutil.rmtree(tmp, ignore_errors=True)

    print("ALL PASS" if not fails else f"FAILED: {fails}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
