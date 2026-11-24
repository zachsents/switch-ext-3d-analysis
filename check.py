"""bun run check: every file compiles, and the server, as main.py makes it, serves MCP in-process (no loops run):
its tools, events and widgets listed, names unique, each widget fed by a read-only tool, the manifest whole."""
import compileall
import importlib
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("SB_HOME", os.path.join(HERE, "home"))

if not compileall.compile_dir(HERE, quiet=1, rx=re.compile(r"node_modules|\.git|home")):
    sys.exit("check: a file doesn't compile")

pkg = json.load(open(os.path.join(HERE, "package.json")))
manifest = pkg["extension"]
assert manifest.get("name") and manifest.get("command") and manifest.get("shim") == "switchboard", "check: extension manifest"

import sbmcp  # noqa: E402
import hetzner  # noqa: E402

_ms = 1795475700000  # 2026-11-23T23:15Z
assert re.fullmatch(r"[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?", hetzner.label_time(_ms)), "check: delete_by label isn't a valid Hetzner label value"
assert hetzner.label_time(_ms) == "20261123T2315Z", "check: delete_by label format"
assert hetzner.parse_label_time(hetzner.label_time(_ms)) == _ms, "check: delete_by label round trip"
assert hetzner.parse_label_time("20261123T2315+0000") == _ms, "check: old delete_by labels still read"
assert hetzner.parse_label_time("") is None and hetzner.parse_label_time(None) is None, "check: bad delete_by labels"

main = importlib.import_module("main")
app = next((v for v in vars(main).values() if isinstance(v, sbmcp.Server)), None)
assert app, "check: main.py makes no sbmcp.Server"
with sbmcp.Client(app) as mcp:
    tools = mcp.tools()
    events = mcp.events()
    widgets = mcp.request("resources/list").get("resources", []) if app._widgets else []
names = list(tools)
assert len(names) == len(set(names)) and names, "check: tools"
for w in widgets:
    fed = w["_meta"][sbmcp.WIDGET].get("tool")
    assert not fed or tools[fed]["annotations"]["readOnlyHint"], f"check: widget {w['name']} needs a read-only tool"
declared = set(manifest.get("widgets") or [])
assert declared == {w["_meta"][sbmcp.WIDGET]["name"] for w in widgets}, "check: manifest widgets"
speaks = [n for n, t in tools.items() if t["annotations"].get("speaksForPerson")]
print(f"{manifest['name']}: {len(names)} tools ({len(speaks)} speak for the user), {len(events)} events, {len(widgets)} widgets")
