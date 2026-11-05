#!/usr/bin/env bash
# install.sh — put Corallium into one of three states.
#
#   1) self-hosted   data on this machine (~/corals), server not started
#   2) remote        url + read token in ~/.hermes/config.yaml, no local data
#   3) read-only     config written, writes disabled; reads still answer
#
# Every path edits ~/.hermes/config.yaml the same way: back it up, change only
# the keys it owns with Python, re-parse the result with yaml.safe_load, and
# restore the backup if the result is not valid YAML. Token values are never
# passed as arguments (so they never appear in ps), never printed, never logged.
#
# Usage:
#   bash install.sh                     # interactive menu
#   bash install.sh --self-hosted [--coral NAME] [--corals-dir DIR] [--port N]
#   bash install.sh --remote URL [--token-file FILE] [--allow-writes]
#   bash install.sh --read-only [--remote URL | --self-hosted]
#   bash install.sh --config PATH       # test against another config file
#
# Deliberately does NOT: start the server, enable systemd, git init, or touch
# anything outside its own config keys (unless --with-service).
set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER="$SELF_DIR/corallium/__main__.py"
DEPLOY_UNIT="$SELF_DIR/deploy/corallium.service"

CONFIG="${HERMES_CONFIG:-$HOME/.hermes/config.yaml}"
CORAL="${CORALLIUM_CORAL:-corallium}"
DESC="Corallium 知识珊瑚"
OWNER="${USER:-$(id -un)}"
CORALS_DIR="${CORALLIUM_CORALS_DIR:-$HOME/corals}"
PORT="8731"
BIND="127.0.0.1"
REMOTE_URL=""
TOKEN="${CORALLIUM_READ_TOKEN:-${CORALLIUM_TOKEN:-}}"
TOKEN_FILE=""
MODE=""                 # self | remote | readonly
CORAL_EXPLICIT=0
READ_ONLY=""
ALLOW_WRITES=0
WITH_SERVICE=0
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
SCAFFOLD=1
ASSUME_YES=0
DRY_RUN=0

# ── output ───────────────────────────────────────────────────────────────────

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  B=$'\033[1m'; DIM=$'\033[2m'; Y=$'\033[33m'; R=$'\033[31m'; G=$'\033[32m'; N=$'\033[0m'
else
  B=""; DIM=""; Y=""; R=""; G=""; N=""
fi
say()  { printf '%s\n' "$*"; }
step() { printf '%s==>%s %s\n' "$B" "$N" "$*"; }
ok()   { printf '%s ok %s %s\n' "$G" "$N" "$*"; }
warn() { printf '%s warn %s %s\n' "$Y" "$N" "$*" >&2; }
die()  { printf '%serror%s %s\n' "$R" "$N" "$*" >&2; exit 1; }

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

need_val() { [ $# -ge 2 ] && [ -n "${2:-}" ] || die "$1 needs a value"; }

# ── arguments ────────────────────────────────────────────────────────────────

while [ $# -gt 0 ]; do
  case "$1" in
    --self-hosted) MODE="self"; shift ;;
    --remote)      MODE="remote"
                   if [ $# -ge 2 ] && [ "${2#--}" = "$2" ]; then REMOTE_URL="$2"; shift 2; else shift; fi ;;
    --remote=*)    MODE="remote"; REMOTE_URL="${1#*=}"; shift ;;
    --read-only)   READ_ONLY=1; shift ;;
    --allow-writes) ALLOW_WRITES=1; shift ;;
    --coral)       need_val "$1" "${2:-}"; CORAL="$2"; CORAL_EXPLICIT=1; shift 2 ;;
    --coral=*)     CORAL="${1#*=}"; CORAL_EXPLICIT=1; shift ;;
    --desc)        need_val "$1" "${2:-}"; DESC="$2"; shift 2 ;;
    --desc=*)      DESC="${1#*=}"; shift ;;
    --owner)       need_val "$1" "${2:-}"; OWNER="$2"; shift 2 ;;
    --owner=*)     OWNER="${1#*=}"; shift ;;
    --corals-dir)  need_val "$1" "${2:-}"; CORALS_DIR="$2"; shift 2 ;;
    --corals-dir=*) CORALS_DIR="${1#*=}"; shift ;;
    --port)        need_val "$1" "${2:-}"; PORT="$2"; shift 2 ;;
    --port=*)      PORT="${1#*=}"; shift ;;
    --bind)        need_val "$1" "${2:-}"; BIND="$2"; shift 2 ;;
    --bind=*)      BIND="${1#*=}"; shift ;;
    --config)      need_val "$1" "${2:-}"; CONFIG="$2"; shift 2 ;;
    --config=*)    CONFIG="${1#*=}"; shift ;;
    --unit-dir)    need_val "$1" "${2:-}"; UNIT_DIR="$2"; shift 2 ;;
    --unit-dir=*)  UNIT_DIR="${1#*=}"; shift ;;
    --token-file)  need_val "$1" "${2:-}"; TOKEN_FILE="$2"; shift 2 ;;
    --token-file=*) TOKEN_FILE="${1#*=}"; shift ;;
    --with-service) WITH_SERVICE=1; shift ;;
    --no-scaffold) SCAFFOLD=0; shift ;;
    --dry-run)     DRY_RUN=1; shift ;;
    -y|--yes)      ASSUME_YES=1; shift ;;
    -h|--help)     usage 0 ;;
    *)             die "unknown argument: $1 (try --help)" ;;
  esac
done

case "$PORT" in ''|*[!0-9]*) die "--port must be a number, got '$PORT'" ;; esac
[ "$PORT" -ge 1 ] && [ "$PORT" -le 65535 ] || die "--port out of range: $PORT"
[ -f "$SERVER" ] || die "corallium package not found next to install.sh (looked for $SERVER)"

# ── pick interpreter (install-time needs yaml; the unit prefers /usr/bin/python3)

PY=""
for cand in /usr/bin/python3 "$(command -v python3 2>/dev/null || true)"; do
  [ -n "$cand" ] && [ -x "$cand" ] || continue
  if "$cand" -c 'import sys, yaml' >/dev/null 2>&1; then PY="$cand"; break; fi
done
[ -n "$PY" ] || die "no python3 with PyYAML found; install python3 + python3-yaml"
UNIT_PY="/usr/bin/python3"
[ -x "$UNIT_PY" ] || UNIT_PY="$PY"

# Refuse early if the config we are about to edit is already broken: a broken
# input must not cost the user their --with-service unit or their coral scaffold.
if [ -f "$CONFIG" ]; then
  "$PY" - "$CONFIG" <<'PYCHK' || die "existing config is not valid YAML — nothing was changed"
import sys, yaml
try:
    d = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))
except yaml.YAMLError as e:
    sys.exit(str(e))
if d is not None and not isinstance(d, dict):
    sys.exit("not a YAML mapping")
PYCHK
fi

# ── scenario 0: interactive choice ───────────────────────────────────────────

if [ -z "$MODE" ]; then
  if [ "$READ_ONLY" = "1" ]; then
    MODE="readonly"
  elif [ -t 0 ]; then
    cat <<'EOF'
Choose how this machine will use Corallium:

  1) self-hosted  data lives here (~/corals); you start the server yourself
  2) remote       connect to a server that already runs elsewhere (read token)
  3) read-only    no writes; reads still answer (offline scan if server is down)

EOF
    printf 'Choice [1-3]: '
    read -r pick || true
    case "$pick" in
      1|"") MODE="self" ;;
      2)    MODE="remote" ;;
      3)    MODE="readonly" ;;
      *)    die "no such choice: $pick" ;;
    esac
  else
    die "no scenario given and stdin is not a terminal; use --self-hosted, --remote URL, or --read-only"
  fi
fi

# ── per-scenario defaults ────────────────────────────────────────────────────

if [ "$MODE" = "remote" ] && [ -z "$REMOTE_URL" ]; then
  if [ -t 0 ]; then
    printf 'Server URL (e.g. http://100.86.114.81:8731): '
    read -r REMOTE_URL || true
  fi
  [ -n "$REMOTE_URL" ] || die "--remote needs a URL"
fi

if [ -n "$REMOTE_URL" ]; then
  case "$REMOTE_URL" in
    http://*|https://*) ;;
    *) die "URL must start with http:// or https:// (got '$REMOTE_URL')" ;;
  esac
  REMOTE_URL="${REMOTE_URL%/}"
fi

case "$MODE" in
  self)
    EFF_URL="http://$BIND:$PORT"
    [ -n "$READ_ONLY" ] || READ_ONLY=0
    ;;
  remote)
    EFF_URL="$REMOTE_URL"
    if [ "$ALLOW_WRITES" = "1" ]; then
      [ -n "$READ_ONLY" ] || READ_ONLY=0
    else
      READ_ONLY=1          # only a read token is stored here
    fi
    SCAFFOLD=0
    ;;
  readonly)
    EFF_URL="${REMOTE_URL:-http://127.0.0.1:$PORT}"
    READ_ONLY=1
    SCAFFOLD=0
    ;;
  *) die "internal: bad mode '$MODE'" ;;
esac

# Read the token without ever putting it on the command line.
if [ -n "$TOKEN_FILE" ]; then
  [ -r "$TOKEN_FILE" ] || die "cannot read token file: $TOKEN_FILE"
  TOKEN="$(tr -d '\r\n' < "$TOKEN_FILE")"
fi
if [ "$MODE" = "remote" ] && [ -z "$TOKEN" ] && [ -t 0 ] && [ "$ASSUME_YES" != "1" ]; then
  printf 'Read token (leave empty if the server is open): '
  read -rs TOKEN || true
  printf '\n'
fi
if [ "$MODE" = "remote" ] && [ -z "$TOKEN" ]; then
  warn "no read token stored — only works if the remote server was started without tokens"
fi

# ── self-hosted filesystem: coral dir + registry + scaffold ──────────────────

if [ "$MODE" = "self" ]; then
  if [ "$DRY_RUN" = "1" ]; then
    say "would use corals dir: $CORALS_DIR (coral: $CORAL)"
  else
    mkdir -p "$CORALS_DIR"
    CORALS_DIR="$(cd "$CORALS_DIR" && pwd)"   # absolute: the plugin resolves it from its own cwd
  fi

  if [ "$SCAFFOLD" = "1" ]; then
    if [ -f "$CORALS_DIR/$CORAL/coral.yaml" ]; then
      ok "coral '$CORAL' already exists at $CORALS_DIR/$CORAL (left untouched)"
    elif [ "$DRY_RUN" = "1" ]; then
      say "would scaffold coral: $CORALS_DIR/$CORAL"
    else
      step "scaffolding coral '$CORAL'"
      "$PY" "$SERVER" --init "$CORALS_DIR/$CORAL" --init-name "$CORAL" \
        --desc "$DESC" --owner "$OWNER" >/dev/null \
        || die "scaffold failed: $CORALS_DIR/$CORAL"
      ok "created $CORALS_DIR/$CORAL (coral.yaml, README.md, .gitignore, nodes/)"
    fi
  fi

  if [ "$DRY_RUN" != "1" ]; then
    step "registry: $CORALS_DIR/registry.yaml"
    CI_REG="$CORALS_DIR/registry.yaml" CI_REG_NAME="$CORAL" CI_REG_DESC="$DESC" \
    "$PY" - <<'PY'
import os, sys
from pathlib import Path
import yaml

path = Path(os.environ["CI_REG"]).expanduser()
name = os.environ["CI_REG_NAME"]
desc = os.environ["CI_REG_DESC"].replace("#", "").strip() or name

old = path.read_text() if path.exists() else ""
names = []
for line in old.splitlines():
    s = line.split("#", 1)[0].strip()
    if s and ":" in s:
        names.append(s.split(":", 1)[0].strip())

if name in names:
    print("registry already lists %s" % name)
    sys.exit(0)

new = old
if new and not new.endswith("\n"):
    new += "\n"
if not new.strip():
    new = ("# Corallium registry — one line per coral: name: one-sentence description\n"
           "# Paths default to ~/corals/<name>/; agents read this table to pick a coral.\n")
new += "%-10s %s\n" % (name + ":", desc)
try:
    loaded = yaml.safe_load(new) or {}
except yaml.YAMLError as e:
    sys.exit("registry would not parse: %s" % e)
if name not in loaded:
    sys.exit("registry check failed: %s missing after write" % name)
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(new)
print("added %s to registry" % name)
PY
    ok "registry ready"
  fi
fi

# ── config.yaml: back up, edit with Python, re-parse, restore on failure ────

# Only write `coral:` when the caller named one (or in self-hosted mode, where
# the scaffold created it). On a remote server the plugin should pick the coral.
CFG_CORAL="$CORAL"
CFG_CORALS_DIR="$CORALS_DIR"
if [ "$MODE" != "self" ] && [ "$CORAL_EXPLICIT" != "1" ]; then CFG_CORAL=""; fi
if [ "$MODE" = "remote" ]; then CFG_CORALS_DIR=""; fi
case "$CFG_CORALS_DIR" in
  ""|/*) ;;
  *) CFG_CORALS_DIR="$PWD/$CFG_CORALS_DIR" ;;   # never store a cwd-relative path
esac

if [ "$DRY_RUN" = "1" ]; then
  if [ "$READ_ONLY" = "1" ]; then ro_label="yes"; else ro_label="no"; fi
  say "dry run — would write $CONFIG"
  say "  url          $EFF_URL"
  say "  coral        ${CFG_CORAL:-<leave as is>}"
  say "  corals dir   ${CFG_CORALS_DIR:-<leave as is>}"
  say "  read-only    $ro_label"
  if [ "$MODE" = "self" ]; then
    say "  scaffold     $CORALS_DIR/$CORAL"
    say "  start        $UNIT_PY $SERVER --registry $CORALS_DIR/registry.yaml --port $PORT --bind $BIND"
  fi
  say "${DIM}nothing written${N}"
  exit 0
fi

mkdir -p "$(dirname "$CONFIG")"
STAGE="$(mktemp "${CONFIG}.new.XXXXXX")"
cleanup() { rm -f "$STAGE"; }
trap cleanup EXIT

set +e
CI_URL="$EFF_URL" CI_CORAL="$CFG_CORAL" CI_CORALS_DIR="$CFG_CORALS_DIR" \
CI_READ_TOKEN="$TOKEN" CI_READ_ONLY="$READ_ONLY" CI_MODE="$MODE" \
"$PY" - "$CONFIG" "$STAGE" <<'PY'
"""Rewrite only the `corallium:` block and plugins.enabled in config.yaml.

Text surgery, not a YAML round-trip: the file is 13 KB of the user's settings and
re-dumping it would delete every comment and reorder every key. Comments inside
our block survive; unknown keys inside it survive; everything else is untouched.
"""
import os, re, sys
import yaml

cfg = sys.argv[1]
stage = sys.argv[2]

url = os.environ["CI_URL"].strip()
coral = os.environ["CI_CORAL"].strip()
corals_dir = os.environ["CI_CORALS_DIR"].strip()
token = os.environ.get("CI_READ_TOKEN", "")
read_only = os.environ.get("CI_READ_ONLY", "0") == "1"
mode = os.environ.get("CI_MODE", "")

managed = {"url": url}
if coral:
    managed["coral"] = coral
if corals_dir:
    managed["corals_dir"] = corals_dir
managed["read_only"] = read_only
if token:
    managed["read_token"] = token
if read_only:
    managed["write_token"] = ""

MODE_NOTE = {
    "self": "self-hosted: server on this machine, start it yourself",
    "remote": "remote: data lives on the server, nothing local",
    "readonly": "read-only: writes disabled, reads still answer",
}

try:
    old_text = open(cfg, encoding="utf-8").read() if os.path.exists(cfg) else ""
except OSError as e:
    sys.exit("cannot read %s: %s" % (cfg, e))

old_doc = {}
if old_text.strip():
    try:
        old_doc = yaml.safe_load(old_text)
    except yaml.YAMLError as e:
        sys.exit("existing config is not valid YAML, refusing to touch it: %s" % e)
    if not isinstance(old_doc, dict):
        sys.exit("existing config is not a YAML mapping, refusing to touch it")

lines = old_text.splitlines()
lines = [ln + "\n" for ln in lines]
if old_text and not old_text.endswith("\n"):
    lines[-1] = lines[-1].rstrip("\n")   # keep the file's no-final-newline habit

KEY = re.compile(r"^(\s+)([A-Za-z_][A-Za-z0-9_-]*)\s*:(?:\s|$)")


def block_range(lines, key):
    """(start, end, inline) for a top-level `key:` block."""
    start = None
    for i, ln in enumerate(lines):
        if re.match(r"^%s\s*:" % re.escape(key), ln):
            start = i
            break
    if start is None:
        return None
    rest = lines[start].split(":", 1)[1].strip()
    if rest and not rest.startswith("#"):
        return start, start + 1, True
    j = start + 1
    while j < len(lines):
        s = lines[j]
        if not s.strip():
            j += 1
            continue
        if s[0] in " \t":
            j += 1
            continue
        break
    return start, j, False


def render(key, value):
    if isinstance(value, bool):
        return "%s: %s" % (key, "true" if value else "false")
    out = yaml.safe_dump({key: value}, default_flow_style=False,
                         allow_unicode=True, width=4096, sort_keys=False).strip()
    return out


# ---- corallium block -------------------------------------------------------
rng = block_range(lines, "corallium")
old_block = old_doc.get("corallium")
old_block = old_block if isinstance(old_block, dict) else {}

if rng is None:
    body_indent = "  "
    new_lines = list(lines)
    if new_lines and new_lines[-1].strip():
        new_lines.append("\n")
    new_lines.append("corallium:\n")
    for k, v in managed.items():
        new_lines.append(body_indent + render(k, v) + "\n")
    lines = new_lines
else:
    s, e, inline = rng
    if inline:
        body = [body_indent + render(k, v) + "\n" for k, v in old_block.items()]
        head = "corallium:\n"
        body_indent = "  "
    else:
        head = lines[s]
        body = lines[s + 1:e]
        body_indent = None
        for ln in body:
            m = KEY.match(ln)
            if m:
                body_indent = m.group(1)
                break
        body_indent = body_indent or "  "

    out_body = []
    last_key_at = None
    for ln in body:
        m = KEY.match(ln)
        if not m or ln.lstrip().startswith("#"):
            out_body.append(ln)
            continue
        k = m.group(2)
        if k in managed:
            out_body.append(body_indent + render(k, managed[k]) + "\n")
        else:
            out_body.append(ln)          # unknown key: leave bytes alone
        last_key_at = len(out_body) - 1

    missing = [(k, v) for k, v in managed.items() if k not in old_block
               and not any(KEY.match(x) and KEY.match(x).group(2) == k for x in body)]
    if missing:
        at = (last_key_at + 1) if last_key_at is not None else len(out_body)
        ins = [body_indent + render(k, v) + "\n" for k, v in missing]
        out_body[at:at] = ins

    lines = lines[:s] + [head] + out_body + lines[e:]

# ---- plugins.enabled -------------------------------------------------------
def ensure_plugin(lines):
    rng = block_range(lines, "plugins")
    if rng is None:
        if lines and lines[-1].strip():
            lines = lines + ["\n"]
        return lines + ["plugins:\n", "  enabled:\n", "    - corallium\n"], True

    s, e, inline = rng
    if inline:
        try:
            cur = (yaml.safe_load(lines[s]) or {}).get("plugins") or {}
        except yaml.YAMLError:
            cur = {}
        enabled = list(dict.fromkeys((cur.get("enabled") or []) + ["corallium"]))
        block = yaml.safe_dump({"plugins": {**cur, "enabled": enabled}},
                               default_flow_style=False, allow_unicode=True,
                               width=4096, sort_keys=False)
        return lines[:s] + [ln + "\n" for ln in block.splitlines()] + lines[e:], True

    body = lines[s + 1:e]
    at = None
    for i, ln in enumerate(body):
        if re.match(r"^\s+enabled\s*:", ln):
            at = i
            break

    if at is None:
        indent = "  "
        for ln in body:
            m = KEY.match(ln)
            if m:
                indent = m.group(1)
                break
        ins = [indent + "enabled:\n", indent + "  - corallium\n"]
        return lines[:s + 1] + ins + lines[s + 1:], True

    head = body[at]
    m = re.match(r"^(\s+)enabled\s*:\s*(.*)$", head)
    indent, rest = m.group(1), m.group(2).strip()

    if rest.startswith("["):
        try:
            items = yaml.safe_load("enabled: " + rest)["enabled"] or []
        except yaml.YAMLError:
            items = []
        if "corallium" in items:
            return lines, False
        items = list(dict.fromkeys(list(items) + ["corallium"]))
        new = [indent + "enabled:\n"] + ["%s  - %s\n" % (indent, x) for x in items]
        return lines[:s + 1] + body[:at] + new + body[at + 1:] + lines[e:], True

    # block list (the normal shape)
    j = at + 1
    last_item = None
    has = False
    while j < len(body):
        ln = body[j]
        if not ln.strip():
            break
        if not ln.strip().startswith("-"):
            break
        if ln.strip().lstrip("-").strip().strip('"\'') == "corallium":
            has = True
        last_item = j
        j += 1
    if has:
        return lines, False
    if last_item is None:
        ins_at = at + 1
    else:
        ins_at = last_item + 1
    add = indent + "  - corallium\n"
    return lines[:s + 1] + body[:ins_at] + [add] + body[ins_at:] + lines[e:], True


lines, plugin_changed = ensure_plugin(lines)

new_text = "".join(lines)
if new_text == old_text:
    print("config already up to date")
    sys.exit(3)

# ---- validate before it can replace anything ------------------------------
try:
    doc = yaml.safe_load(new_text)
except yaml.YAMLError as e:
    sys.exit("edited config does not parse (%s); nothing written" % e)
if not isinstance(doc, dict):
    sys.exit("edited config is not a mapping; nothing written")

expected = {k: v for k, v in old_block.items() if k not in ("read_only",)}
expected.update(managed)
got = doc.get("corallium")
if not isinstance(got, dict):
    sys.exit("edited config lost the corallium block; nothing written")
for k, v in managed.items():
    if k == "read_token":
        continue                       # do not echo secrets, even on failure
    if got.get(k) != v:
        sys.exit("edited config has corallium.%s=%r, wanted %r; nothing written"
                 % (k, got.get(k), v))
if "read_token" in managed and got.get("read_token") != managed["read_token"]:
    sys.exit("edited config did not take the read token; nothing written")
plug = (doc.get("plugins") or {}).get("enabled") or []
if "corallium" not in plug:
    sys.exit("edited config is missing plugins.enabled: corallium; nothing written")

with open(stage, "w", encoding="utf-8") as f:
    f.write(new_text)
os.chmod(stage, 0o600)
print("staged config (%s)" % MODE_NOTE.get(mode, mode))
PY
rc=$?
set -e

# Only write `coral:` when the caller named one (or in self-hosted mode, where
# the scaffold created it). On a remote server the plugin should pick the coral.
CFG_CORAL="$CORAL"
if [ "$MODE" != "self" ] && [ "$CORAL_EXPLICIT" != "1" ]; then CFG_CORAL=""; fi

case "$rc" in
  0)
    BACKUP=""
    if [ -f "$CONFIG" ]; then
      BASE="$CONFIG.bak.$(date +%Y%m%d-%H%M%S)"
      BACKUP="$BASE"
      n=1
      while [ -e "$BACKUP" ]; do BACKUP="$BASE.$n"; n=$((n+1)); done
      cp -p "$CONFIG" "$BACKUP"
      say "${DIM}backup:${N} $BACKUP"
      chmod --reference="$CONFIG" "$STAGE" 2>/dev/null || true
    fi
    mv "$STAGE" "$CONFIG"
    if ! "$PY" -c 'import sys, yaml; d = yaml.safe_load(open(sys.argv[1], encoding="utf-8")); assert isinstance(d, dict)' "$CONFIG" 2>/dev/null; then
      if [ -n "$BACKUP" ] && [ -f "$BACKUP" ]; then
        cp -p "$BACKUP" "$CONFIG"
        die "config failed to re-parse after write; restored $BACKUP"
      fi
      rm -f "$CONFIG"
      die "config failed to re-parse after write; removed the broken file"
    fi
    ok "config written: $CONFIG"
    ;;
  3)
    rm -f "$STAGE"
    ok "config unchanged: $CONFIG (already configured, nothing duplicated)"
    ;;
  *)
    rm -f "$STAGE"
    die "config edit aborted; $CONFIG was not modified"
    ;;
esac
trap - EXIT

# ── optional systemd user unit (never enabled, never started) ───────────────

if [ "$WITH_SERVICE" = "1" ]; then
  [ -f "$DEPLOY_UNIT" ] || die "unit template missing: $DEPLOY_UNIT"
  [ "$MODE" = "self" ] || warn "installing a local server unit while MODE=$MODE — the unit serves ~/corals, not $EFF_URL"
  mkdir -p "$UNIT_DIR"
  "$PY" - "$DEPLOY_UNIT" "$UNIT_DIR/corallium.service" "$UNIT_PY" "$SELF_DIR" "$PORT" "$BIND" "$CORALS_DIR/registry.yaml" <<'PY'
import os, sys
src, dst, py, repo, port, bind, reg = sys.argv[1:8]
regarg = "%h/corals/registry.yaml" if reg == os.path.expanduser("~/corals/registry.yaml") else reg
out = []
for line in open(src, encoding="utf-8"):
    if line.startswith("ExecStart="):
        line = ("ExecStart=%s -m corallium --registry %s"
                " --port %s --bind %s\n" % (py, regarg, port, bind))
    elif line.startswith("WorkingDirectory="):
        line = "WorkingDirectory=%s\n" % repo
    elif line.startswith("Environment=PYTHONPATH="):
        # `-m corallium` resolves against sys.path; WorkingDirectory covers the
        # normal case, this covers an interpreter that drops cwd from sys.path.
        line = "Environment=PYTHONPATH=%s\n" % repo
    elif line.startswith("Documentation=file://"):
        line = "Documentation=file://%s/DEPLOY.md\n" % repo
    out.append(line)
open(dst, "w", encoding="utf-8").write("".join(out))
PY
  ok "unit installed: $UNIT_DIR/corallium.service"
  say "    ${DIM}not enabled, not started — a second instance would fight the one on port $PORT${N}"
  say "    ${DIM}enable with: systemctl --user daemon-reload && systemctl --user enable --now corallium.service${N}"
fi

# ── summary ─────────────────────────────────────────────────────────────────

say ""
step "Corallium configured"
say "  config       $CONFIG"
say "  mode         $MODE"
say "  url          $EFF_URL"
if [ -n "$CFG_CORAL" ]; then say "  coral        $CFG_CORAL"; fi
if [ "$MODE" = "self" ]; then say "  corals dir   $CORALS_DIR"; fi
if [ "$READ_ONLY" = "1" ]; then say "  read-only    yes"; else say "  read-only    no"; fi
if [ -n "$TOKEN" ]; then say "  read token   stored (hidden)"; else say "  read token   (not set)"; fi

if [ "$MODE" = "self" ]; then
  say "  registry     $CORALS_DIR/registry.yaml"
  say ""
  say "${B}Start the server (corallium does not start it for you):${N}"
  say "  $UNIT_PY $SERVER --registry $CORALS_DIR/registry.yaml --port $PORT --bind $BIND"
  say ""
  say "${DIM}One server per data directory. If something already listens on $BIND:$PORT,"
  say "do not start a second one — point the client at the running instance instead.${N}"
  say "${DIM}For boot/restart handling: re-run with --with-service, then enable the unit.${N}"
elif [ "$MODE" = "remote" ]; then
  say ""
  say "${DIM}No local coral data is created. Reads go to $EFF_URL."
  if [ "$READ_ONLY" = "1" ]; then
    say "No write token is stored, so writes are refused by an authenticated server."
  fi
  say "If the host is unreachable the client scans nothing locally and says so.${N}"
else
  say ""
  say "${DIM}Writes disabled in config. Reads use the server when reachable, otherwise"
  say "the local nodes/ scan under $CORALS_DIR.${N}"
fi
say ""
say "${DIM}Token values are never printed or stored in this script's output.${N}"
