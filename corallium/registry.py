"""Coral metadata and the registry that names them (PROTOCOL §16, §11.5)."""
import json
from datetime import datetime, timezone
from pathlib import Path

CORAL_GITIGNORE = """index.db
index.db-wal
index.db-shm
claims.json
__pycache__/
"""

CORAL_README = """# {name}

{desc}

## What belongs here

One falsifiable proposition per node. If you cannot say what would make it false,
it is an opinion and does not belong.

## Query

```bash
curl 'http://127.0.0.1:8731/c/{name}/find?q=<text>'
curl 'http://127.0.0.1:8731/c/{name}/since?cursor=0'
curl 'http://127.0.0.1:8731/c/{name}/tip?topic=<topic>'
```

## Layout

- `nodes/<id>.json` — immutable; the id is a hash of the proposition
- `claims.json` — live leases (not in git)
- `index.db` — derived search index; delete it freely

Protocol: see the Corallium repository, `PROTOCOL.md`.
"""




def server_version() -> str:
    """The protocol this process implements, from VERSION.

    `/health` also carries a per-coral `protocol` taken from coral.yaml, which is
    the version the coral was *scaffolded* under — reading that as the server's
    version made every old coral look like an old server. Both are now explicit.
    """
    vf = Path(__file__).resolve().parent.parent / "VERSION"
    try:
        return vf.read_text().strip()
    except OSError:
        return "unknown"


def coral_meta(root: Path) -> dict:
    """coral.yaml: the identity of one coral, readable by a human in 10 seconds."""
    f = root / "coral.yaml"
    meta = {"name": root.name, "description": ""}
    if not f.exists():
        return meta
    for line in f.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip()
    return meta


def init_coral(root: Path, name: str, desc: str, owner: str = "") -> dict:
    if (root / "coral.yaml").exists():
        return {"ok": False, "error": f"{root} is already a coral"}
    root.mkdir(parents=True, exist_ok=True)
    (root / "nodes").mkdir(exist_ok=True)
    version = "0.1.10"
    vf = Path(__file__).resolve().parent.parent / "VERSION"
    if vf.exists():
        version = vf.read_text().strip()
    (root / "coral.yaml").write_text(
        f"name: {name}\n"
        f"description: {desc}\n"
        f"owner: {owner}\n"
        f"created: {datetime.now(timezone.utc).astimezone().strftime('%Y-%m-%d')}\n"
        f"protocol: {version}\n"
    )
    (root / ".gitignore").write_text(CORAL_GITIGNORE)
    (root / "README.md").write_text(CORAL_README.format(name=name, desc=desc))
    return {"ok": True, "root": str(root), "name": name}


def load_registry(path: Path) -> dict[str, str]:
    """registry.yaml, deliberately parsed with a 10-line reader.

        mqsim: 仿真器、SSD、存储栈
        trading: 动量模型、组合、因子

    Paths resolve relative to ~/corals/. A JSON file is also accepted.
    """
    text = path.read_text()
    if text.lstrip().startswith("{") or text.lstrip().startswith("["):
        data = json.loads(text)
        if isinstance(data, list):
            return {d["name"]: d["path"] for d in data}
        return {k: (v if isinstance(v, str) else v.get("path")) for k, v in data.items()}
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        name, val = line.split(":", 1)
        val = val.strip()
        first = val.split()[0] if val else ""
        # a bare description (no path-looking token) means ~/corals/<name>
        path_str = first if ("/" in first or first.startswith("~")) else name.strip()
        base = Path(path_str).expanduser()
        if not base.is_absolute():
            base = path.parent / path_str
        out[name.strip()] = str(base)
    return out


