"""The analysis extension's one MCP server, which each part declares its tools on: mesh.py (printability geometry and
mesh-python), fea.py (FEA on a rented Hetzner box), cfd.py (OpenFOAM on a rented Hetzner box), render.py (Blender
renders) and photo.py (a listing's photos, a size measured off a photo). Its shared pieces live here: its home, its
config, and its keys (a connection each)."""
import json
import os
import threading
import time

import sbmcp

HOME = sbmcp.home()

INSTRUCTIONS = (
    "3D analysis of parts and models. Inputs are files on this machine (paths) or https URLs.\n"
    "Mesh: report(files) answers each model's printability (size and bed fit, closed or not, bodies, walls and gaps, "
    "holes, mid-air islands, overhangs, best side down) with a sheet; section cuts planes through it (and compares "
    "versions); view renders it from any side; layers draws a sliced plate's G-code; supports and support_model read "
    "a sliced plate's supports; convert turns a 3MF, STEP or scene file (.blend, FBX, glTF, OBJ, USD) into STLs; scene "
    "says what a scene file holds; coupon cuts a fit coupon; variants builds a parametric script's variants and "
    "measures each; set_spec and spec keep a job's spec (model, features, settings) and verify checks a sliced plate "
    "against it; drawing makes a dimensioned drawing sheet. Pictures and files land in this extension's home (paths in "
    "the answer) and save_to copies them to a folder. mesh-python (written to ~/.local/bin) runs any geometry script "
    "in the same pinned environment.\n"
    "FEA (fea_*): fea_check checks parts and a load without renting; fea_run rents a Hetzner box, runs gmsh and "
    "CalculiX on each part and deletes the box when done. CFD (cfd_*): cfd_start rents a box and runs an OpenFOAM "
    "case matrix, extending unconverged cases. Renting spends the user's money: fea_run, fea_add, cfd_start, cfd_add "
    "and cfd_extend speak for the user. *_status says how a study stands; the box is deleted on its own once done or "
    "past its delete-by.\n"
    "Render: render puts a model in a standard scene (desk, table, studio, dark, cutout, wall), on a prop if asked, "
    "and renders it photoreal in Blender's Cycles; preview: true first. render_scenes lists what it offers.\n"
    "Photos: photos(listing) answers an Amazon or store listing's photos on a contact sheet with the sizes it states; "
    "measure(photo) measures a part off a photo against a known pitch or length; look zooms a region with a pixel "
    "grid. health says how each part is.")

app = sbmcp.Server("analysis", version="1", instructions=INSTRUCTIONS, connections=[
    {"provider": "hetzner", "kind": "key", "label": "Hetzner Cloud", "scopes": [], "multiple": False},
    {"provider": "ai-gateway", "kind": "key", "label": "AI Gateway", "scopes": [], "multiple": False},
], reaches={"calls": ["browser_act", "browser_run"]})
app.event("heartbeat", "How a part of it is itself", stream="vitals/analysis", kind="observation", retention="signal")


def config():
    """config.json in its home: {bed_mm, blender, results}, each optional."""
    try:
        with open(os.path.join(HOME, "config.json")) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def part_home(name):
    d = os.path.join(HOME, name)
    os.makedirs(d, mode=0o700, exist_ok=True)
    return d


_keys = {}
_keys_lock = threading.Lock()


def adopt_kept(provider):
    """A key kept for it before connections, in keys.json in its home ({provider: secret}), goes to the user's
    connections (account "default") and leaves the file: whether one did. The secret is never logged."""
    path = os.path.join(HOME, "keys.json")
    try:
        with open(path) as f:
            kept = json.load(f)
    except (OSError, ValueError):
        return False
    secret = kept.get(provider)
    if not secret:
        return False
    app.ask("connections/adopt", {"provider": provider, "account": "default", "grant": {"secret": secret}}, timeout=15)
    rest = {k: v for k, v in kept.items() if k != provider}
    if rest:
        fd = os.open(path + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(rest, f)
        os.replace(path + ".tmp", path)
    else:
        os.remove(path)
    app.log(f"{provider}: adopted the kept key")
    return True


def key(provider):
    """A key connection's secret (the user's own, through the host), or None when there's none; cached a minute."""
    with _keys_lock:
        got = _keys.get(provider)
        if got and time.time() - got[1] < 60:
            return got[0]
    try:
        accounts = app.ask("connections/list", {"provider": provider}, timeout=15).get("accounts") or []
        if not accounts and adopt_kept(provider):
            accounts = app.ask("connections/list", {"provider": provider}, timeout=15).get("accounts") or []
        secret = app.ask("connections/token", {"provider": provider, "account": accounts[0]["account"]},
                         timeout=15).get("token") if accounts else None
    except Exception as e:  # noqa: BLE001
        app.log(f"{provider}: no key: {e}")
        secret = None
    with _keys_lock:
        _keys[provider] = (secret, time.time())
    return secret
