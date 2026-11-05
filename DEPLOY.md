# Deploying Corallium

One process serves many corals on one port. `install.sh` configures a client;
it never starts a server and never enables a service. That distinction is the
whole design: "the service did not start" is the only real failure mode this
system has (PROTOCOL §13.6), so starting is always an explicit human action.

```
install.sh  ──writes──►  ~/.hermes/config.yaml   (url, coral, tokens, read_only)
            ──writes──►  ~/corals/registry.yaml  (self-hosted only)
            ──runs───►   server.py --init        (self-hosted only, never starts it)
            ──writes──►  ~/.config/systemd/user/corallium.service   (--with-service)
```

## Three states

| command | data | writes | tool behaviour |
|---|---|---|---|
| `install.sh --self-hosted` | `~/corals/<name>/` on this machine | yes | talks to `127.0.0.1:8731`; if it is down, reads scan `nodes/` |
| `install.sh --remote URL` | none locally | no (read token only) | reads from the server; nothing to fall back to locally |
| `install.sh --read-only` | none required | no | reads answer from the server or from `nodes/`; publish fails loudly |

Interactive menu: run it with no arguments. Flags skip every prompt, which is
what cron/CI need.

## Self-hosted

```bash
bash install.sh --self-hosted
# or fully specified:
bash install.sh --self-hosted --coral mqsim --corals-dir ~/corals --port 8731
```

What it does:

1. creates `~/corals/registry.yaml` if missing, adds `name: description` if the
   coral is not listed yet (never duplicates a line);
2. scaffolds `~/corals/<name>/` with `server.py --init` **only if
   `coral.yaml` is absent** — an existing coral is never touched;
3. writes `corallium:` into `~/.hermes/config.yaml`;
4. prints the exact start command and exits.

It does **not** start the server. Run the printed line yourself:

```bash
/usr/bin/python3 ~/corallium/server.py --registry ~/corals/registry.yaml --port 8731 --bind 127.0.0.1
```

One server per data directory. A second instance on the same `~/corals` will
fight the first over `index.db`; one on a different port but the same directory
is just as bad. If something already answers on the port, point the client at
it instead of starting another.

## Remote

```bash
CORALLIUM_TOKEN=... bash install.sh --remote http://100.86.114.81:8731 --coral mqsim
# or let it prompt (input hidden):
bash install.sh --remote http://100.86.114.81:8731
# or from a file, for scripts:
bash install.sh --remote http://100.86.114.81:8731 --token-file ~/.config/corallium/token
```

- The token is read from `$CORALLIUM_TOKEN` / `$CORALLIUM_READ_TOKEN`, a file, or
  a hidden prompt. It is never a command-line argument, so it never shows up in
  `ps`, shell history, or this script's output.
- Remote mode stores the read token only, so `read_only: true` is written by
  default. `--allow-writes` clears it, but a write token still has to be
  supplied separately — that is a deliberate second step.
- No coral data, registry, or scaffold is created locally.

## Read-only / unconfigured

```bash
bash install.sh --read-only                      # keep the current url/coral, disable writes
bash install.sh --read-only --remote URL         # write nothing to the server
bash install.sh --self-hosted --read-only        # local data, writes disabled in config
```

The client degrades rather than failing (PROTOCOL §13.5, §17.3):

```
server reachable   → normal
server unreachable → reads scan ~/corals/<coral>/nodes/*.json, writes fail with an
                     explicit error; a publish is never silently dropped
nothing configured → same fallback, no stack trace
```

`read_only: true` is a statement of intent plus the absence of a write token.
Against a server started **without** `CORALLIUM_WRITE_TOKEN` every client is
writable by design (local trust on `127.0.0.1`); if you need read-only enforced
rather than declared, start the server with a write token and give readers only
the read token.

## What install.sh guarantees about config.yaml

`~/.hermes/config.yaml` is 13 KB of settings that belong to Hermes, not to this
project. The script therefore does text surgery, not a YAML round-trip:

- **backup first** — `config.yaml.bak.YYYYmmdd-HHMMSS` (`cp -p`) before any write;
- only the top-level `corallium:` block and `plugins.enabled` are touched;
  comments and every other key keep their exact bytes;
- unknown keys inside `corallium:` (including a nested `corals:` map) are kept;
- edited with Python + `yaml.safe_load` validation **before** replacing the file;
- re-parsed again after the write; on failure the backup is copied back and the
  script exits non-zero;
- refuses to start if the existing file is already invalid YAML;
- idempotent: a second run reports `config unchanged` and writes no backup;
- token values are never printed, not even in error messages.

Use `--config PATH` to target another file (tests, a second Hermes profile):
`HERMES_HOME` is not enough, the path is explicit.

Check the result without exposing tokens:

```bash
bash install.sh --dry-run --self-hosted
python3 -c 'import yaml; c=yaml.safe_load(open("'"$HOME"'/.hermes/config.yaml"))["corallium"]; print({k: ("<set>" if "token" in k and c[k] else v) for k, v in c.items()})'
```

## systemd user unit

`deploy/corallium.service` is the template; `install.sh --with-service` writes a
copy to `~/.config/systemd/user/corallium.service` with this repo's absolute
path, the chosen port, and `/usr/bin/python3` as `ExecStart` (a user unit does
not inherit the login `PATH`, and a pyenv/venv shim can vanish on upgrade).

```bash
bash install.sh --self-hosted --with-service
systemctl --user daemon-reload
systemctl --user enable --now corallium.service
systemctl --user status corallium.service
journalctl --user -u corallium.service -f
```

`--with-service` **writes the unit and stops**. It does not enable it and does
not start it: enabling while a manually started server is already on the port
produces a restart loop that looks like a crash.

The unit uses `Restart=on-failure`, `RestartSec=3`, `--bind 127.0.0.1`, and
`--registry %h/corals/registry.yaml`. Tokens are not in the unit file; put them
in `~/.config/corallium/env` (mode 0600), which the unit reads if present:

```bash
install -d -m 700 ~/.config/corallium
printf 'CORALLIUM_READ_TOKEN=%s\nCORALLIUM_WRITE_TOKEN=%s\n' "$R" "$W" > ~/.config/corallium/env
chmod 600 ~/.config/corallium/env
systemctl --user restart corallium.service
```

With both variables unset the server logs `read=open write=open` and trusts
`127.0.0.1`. That is the intended default for a single-user machine; it is not
safe on a public interface.

Validate without starting anything:

```bash
systemd-analyze --user verify ~/.config/systemd/user/corallium.service
```

## Verify an install

```bash
curl -s http://127.0.0.1:8731/health | python3 -m json.tool   # corals, max_seq
curl -s http://127.0.0.1:8731/corallium/tip | head -c 400     # reads work
bash install.sh --self-hosted                                  # second run: unchanged
```

## Uninstall

```bash
systemctl --user disable --now corallium.service   # if it was enabled
rm ~/.config/systemd/user/corallium.service
# config: remove the `corallium:` block and "corallium" from plugins.enabled
# data:   ~/corals/ is yours — nodes/ is the source of truth, keep it
```

## Troubleshooting

| symptom | cause | fix |
|---|---|---|
| `corallium server not running at 127.0.0.1:8731` | server not started | run the printed `ExecStart` line, or enable the unit |
| reads work, publish returns 401 | server has a write token, client has none | put `CORALLIUM_WRITE_TOKEN` in the unit env and restart |
| `PUBLISH FAILED: ... unreachable` | server down | start it; the node was **not** saved |
| two processes, `index.db is locked` | two servers on one coral dir | kill one; one server per data directory |
| config edits vanish | another tool rewrote config.yaml | re-run install.sh; it re-reads and merges |
