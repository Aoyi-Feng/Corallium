"""HTTP surface: one process, many corals (PROTOCOL §13.9)."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .errors import ApiError
from .registry import coral_meta, server_version

def make_handler(corals: dict, read_token: str, write_token: str, default: str | None):
    """One process, many corals. `/c/<name>/<endpoint>` addresses them explicitly;
    bare `/<endpoint>` goes to the default coral so a single-coral install needs
    no prefix."""
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):  # quiet
            pass

        def _path(self) -> str:
            """The request target, decoded as UTF-8 when it is not ASCII.

            http.server decodes the request line as latin-1, so `?q=写缓存`
            (what `curl` sends when the query is pasted raw) arrives as mojibake
            and matches nothing — a silent zero-hit answer for a library whose
            contents are mostly Chinese. Percent-encoded clients are unaffected.
            """
            p = self.path
            if not p.isascii():
                try:
                    p = p.encode("latin-1").decode("utf-8")
                except (UnicodeEncodeError, UnicodeDecodeError):
                    pass
            return p

        def _send(self, code: int, obj) -> None:
            body = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _auth(self, write: bool) -> bool:
            got = (self.headers.get("Authorization") or "").replace("Bearer ", "").strip()
            want = write_token if write else read_token
            if write and not write_token and read_token:
                # A deployment that set only a read token is declaring "read only".
                # Treating the missing write token as "no auth needed" turned that
                # declaration into the opposite: the one credential handed out was
                # a read token, and every write was open to anyone.
                self._send(401, {"error": "read-only deployment: no write token is configured"})
                return False
            if not want:  # nothing configured → local trust
                return True
            if got != want:
                self._send(401, {"error": "unauthorized"})
                return False
            return True

        def _resolve(self, path: str):
            """-> (store, endpoint, coral_name) or (None, None, None) if unknown."""
            parts = [p for p in path.split("/") if p]
            if parts and parts[0] == "c":
                if len(parts) < 2:
                    return None, None, None
                name = parts[1]
                if name not in corals:
                    self._send(404, {"error": f"unknown coral '{name}'",
                                     "corals": sorted(corals)})
                    return None, None, None
                return corals[name], "/" + "/".join(parts[2:]), name
            if default is None:
                self._send(400, {"error": "no default coral; use /c/<name>/...",
                                 "corals": sorted(corals)})
                return None, None, None
            return corals[default], path, default

        def do_GET(self):
            u = urlparse(self._path())
            q = parse_qs(u.query)
            # /health stays unauthenticated: systemd and monitors probe it, and
            # it leaks nothing but coral names and sequence numbers.
            if u.path == "/health":
                # Unauthenticated on purpose (systemd and monitors probe it), so it
                # may say which corals exist and how far they are — never where
                # they live on disk. The path is served by the authenticated
                # /corals endpoint instead.
                meta = {n: {"max_seq": s.max_seq(),
                            **{k: v for k, v in coral_meta(s.root).items()
                               if k in ("description", "owner", "protocol")}}
                        for n, s in corals.items()}
                return self._send(200, {
                    "ok": True, "default": default,
                    "protocol": server_version(),
                    "max_seq": meta[default]["max_seq"] if default else 0,
                    "corals": meta,
                })
            if not self._auth(write=False):
                return
            try:
                if u.path == "/corals":
                    meta = {n: {"path": str(s.root), "max_seq": s.max_seq(),
                                **{k: v for k, v in coral_meta(s.root).items()
                                   if k in ("description", "owner", "protocol")}}
                            for n, s in corals.items()}
                    return self._send(200, {
                        "ok": True,
                        "default": default,
                        "protocol": server_version(),
                        # top-level max_seq keeps single-coral clients working
                        "max_seq": meta[default]["max_seq"] if default else 0,
                        "corals": meta,
                    })
                store, ep, name = self._resolve(u.path)
                if store is None:
                    return
                if ep == "/health":
                    return self._send(200, {"ok": True, "coral": name,
                                            "max_seq": store.max_seq()})
                if ep == "/since":
                    cursor = int(q.get("cursor", ["0"])[0] or 0)
                    topics = [t for t in (q.get("topics", [""])[0] or "").split(",") if t]
                    return self._send(200, {"nodes": store.since(cursor, topics)})
                if ep == "/tip":
                    return self._send(200, {"tips": store.tip(q.get("topic", [""])[0])})
                if ep == "/verify":
                    return self._send(200, store.verify_refs(q.get("node", [None])[0]))
                if ep == "/maintenance/scan":
                    return self._send(200, store.scan())
                if ep == "/claims":
                    return self._send(200, {"claims": store.claims(
                        live_only=(q.get("all", ["0"])[0] != "1"))})
                if ep == "/tasks":
                    return self._send(200, {"tasks": store.tasks(
                        include_closed=(q.get("all", ["0"])[0] == "1"))})
                if ep == "/reviews":
                    r = store.reviews(q.get("claim", [""])[0])
                    return self._send(404 if "error" in r else 200, r)
                if ep == "/find":
                    hits, outside = store.find(q.get("q", [""])[0],
                                               root=(q.get("root", [""])[0] or ""),
                                               order=(q.get("sort", ["recent"])[0] or "recent"))
                    return self._send(200, {"hits": hits, "outside": outside})
                return self._send(404, {"error": "not found"})
            except ValueError as e:  # ?cursor=abc is the caller's fault, not a crash
                return self._send(400, {"error": str(e)})
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

        def do_POST(self):
            if not self._auth(write=True):
                return
            u = urlparse(self._path())
            try:
                ln = int(self.headers.get("Content-Length") or 0)
                payload = json.loads(self.rfile.read(ln) or b"{}")
                store, ep, name = self._resolve(u.path)
                if store is None:
                    return
                if ep == "/nodes":
                    r = store.add_node(payload)
                    if r.get("conflict") == "similar":
                        # 409, not 201: the write did not happen and the caller
                        # has to decide what this node is *in relation to*.
                        return self._send(409, r)
                    return self._send(201 if r["created"] else 200, r)
                if ep == "/claims":
                    return self._send(200, store.claim(
                        payload.get("question", ""), payload.get("author", "unknown"),
                        int(payload.get("ttl", 30))))
                if ep.startswith("/nodes/") and ep.endswith("/replies"):
                    nid = ep[len("/nodes/"):-len("/replies")]
                    r = store.reply(nid, payload)
                    return self._send(201 if r.get("created") else 200, r)
                if ep == "/maintenance/merge":
                    return self._send(201, store.merge(payload.get("keep", ""),
                                                       payload.get("drop", ""),
                                                       payload.get("reason", ""),
                                                       payload.get("author", "curator")))
                if ep == "/maintenance/retire":
                    return self._send(200, store.retire(payload.get("id", ""),
                                                        payload.get("reason", ""),
                                                        payload.get("author", "curator")))
                if ep == "/purge":
                    r = store.purge(payload.get("id", ""), payload.get("reason", ""),
                                    payload.get("author", "unknown"))
                    return self._send(200, r)
                return self._send(404, {"error": "not found"})
            except ApiError as e:  # carries its own status: 400 / 404 / 409
                return self._send(e.code, {"error": str(e)})
            except ValueError as e:  # bad input is the caller's fault
                return self._send(400, {"error": str(e)})
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    return H


