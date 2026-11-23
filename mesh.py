"""mesh: a 3D model's printability geometry in one step, so checking a file never means throwaway scripts.

`report` reads models (STL, 3MF as its plate has them, STEP, OBJ, PLY, a zip of them, or a folder) and answers what
matters for printing each: size and bed fit, closed or not and why, separate bodies (stray specks, hidden shells), wall
thickness and narrow gaps measured by rays (where, and how much area), vertical holes with their diameters, islands that
start in mid-air, overhang by steepness and which side down prints best, and a sheet per model (walls coloured on two
views, from above, overhangs from below, four sections with the thinnest wall and narrowest gap marked). `section` cuts
any planes (z=5.4, through a point, cropped to a box) with walls, gaps and holes measured, and overlays other versions
of the model cut by the same planes (compare), against reference circles like a bore; `view` renders it from any side,
cropped or cut open; `layers` draws a sliced plate's layers from its G-code (arcs and all, by feature, with the model's
outline over them); `supports` reads where a sliced plate's supports meet the model (supports.py); `convert` turns a
3MF or STEP into STLs, on the best side, and a scene file (.blend, FBX, glTF, OBJ, USD: scenes.py, read through Blender
by blend_dump.py) into one STL per colour or material at the size asked; `scene` says what's in such a file before
converting it. `coupon` cuts a printable fit coupon from a model (coupon.py, with manifold3d). `variants` builds a
parametric model's variants from its own script (variants.py). `drawing` makes a dimensioned drawing sheet
(drawing.py). `set_spec`, `spec` and `verify` keep a job's spec in this extension's home and check sliced plates against it.

The geometry runs in mesh_worker.py, in its own environment that uv builds once (trimesh with Embree, shapely, scipy,
matplotlib); this server needs only the standard library. Pictures and files it makes are kept in its home (out/),
a week, and their paths are in each answer.
"""

import contextlib
import functools
import itertools
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
import zipfile

import procguard
import pyenv
import sbmcp
from pyenv import DEPS, uv
from server import app, config, part_home

NAME = "mesh"
HERE = os.path.dirname(os.path.abspath(__file__))
HOME = part_home(NAME)
TMP = os.path.join(HOME, "tmp")
OUT = os.path.join(HOME, "out")
"""What each call made (sheets, STLs, drawings), a folder a call, kept KEEP_DAYS."""
KEEP_DAYS = 7
WORKER = os.path.join(HERE, "mesh_worker.py")
HEARTBEAT_S = float(os.environ.get("MESH_HEARTBEAT_S") or 600)
MESH_PYTHON = os.path.expanduser(os.environ.get("MESH_PYTHON_BIN") or "~/.local/bin/mesh-python")
"""mesh-python, the command that runs anyone's script in the worker's environment (meshpython.py), where this
machine's shells find it."""
TIMEOUT_S = 900
JOBS = threading.BoundedSemaphore(2)
"""Two jobs at once at most: a big scan takes a core and a GB or two."""
# The most one worker may use (its physical footprint, GB): a verify once grew to 12.5 GB and ran a 16 GB machine out
# of memory. MESH_MAX_GB overrides it.
MAX_BYTES = procguard.limit_bytes("MESH_MAX_GB", 5)
MODEL_EXTS = (".stl", ".3mf", ".obj", ".ply", ".off", ".glb", ".gltf", ".step", ".stp", ".blend", ".fbx", ".usd", ".usda",
              ".usdc", ".usdz")
SCENE_EXTS = (".blend", ".fbx", ".usd", ".usda", ".usdc", ".usdz", ".glb", ".gltf", ".obj")
"""Files Blender reads for convert and scene: their materials, textures and colours split them into parts."""
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"

app.event("plate_check", "A sliced plate checked against its spec", stream="analysis/plate-checks", kind="observation")
app.event("spec", "A job's spec: what its plates must be", stream="analysis/specs", kind="observation")

state = {"started_at": int(time.time() * 1000), "calls": 0, "failed": 0, "last_error": None, "last_call_at": None,
         "env": None, "env_error": None, "warming": False, "mesh_python": None}


def log(msg):
    print(msg, file=sys.stderr, flush=True)


# The worker


def worker_env():
    env = {**os.environ, "MPLBACKEND": "Agg", "MPLCONFIGDIR": os.path.join(HOME, "mpl")}
    cfg = config()
    if cfg.get("bed_mm"):
        env["MESH_BED_MM"] = str(cfg["bed_mm"])
    if cfg.get("blender"):
        env["MESH_BLENDER"] = os.path.expanduser(str(cfg["blender"]))
    return env


def run_worker(job, ctx=None, timeout=TIMEOUT_S, extra=None):
    """Runs mesh_worker.py on one job in its own environment and returns what it answered (raising when it failed).
    What it writes to its progress file, a line at a time, goes to the asker as progress; extra adds packages to its
    environment (uv keeps each set it builds)."""
    with JOBS, workspace() as tmp:
        job_path, result_path = os.path.join(tmp, "job.json"), os.path.join(tmp, "result.json")
        progress_path = os.path.join(tmp, "progress.txt")
        job = {**job, "progress_file": progress_path}
        with open(job_path, "w") as f:
            json.dump(job, f)
        cmd = pyenv.command(["-I", WORKER, job_path, result_path], extra or ())
        p = subprocess.Popen(cmd, cwd=tmp, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True,
                             env=worker_env())
        started = time.monotonic()
        watch = procguard.Watch(p, MAX_BYTES)
        said = 0
        try:
            while p.poll() is None:
                if ctx is not None and ctx.cancelled.is_set():
                    raise RuntimeError("cancelled")
                if time.monotonic() - started > timeout:
                    raise RuntimeError(f"gave up after {timeout // 60} minutes")
                if watch.check():
                    raise RuntimeError(watch.message(f"the {job.get('op', 'job')}"))
                said = pass_progress(progress_path, said, ctx)
                time.sleep(0.2)
            out, err = p.communicate()
        finally:
            procguard.kill_tree(p)  # uv's Python, and anything it started, never outlives the job
        if not os.path.exists(result_path):
            raise RuntimeError(f"the worker stopped (exit {p.returncode}): {(err or out).strip()[-800:]}")
        with open(result_path) as f:
            result = json.load(f)
    if not result.get("ok"):
        if result.get("traceback"):
            log(result["traceback"])
        raise RuntimeError(result.get("error") or "the worker failed")
    return result


def pass_progress(path, said, ctx):
    """The worker's newest progress line, to the asker, when there's one it hasn't had: answers how many it has had."""
    if ctx is None:
        return said
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except OSError:
        return said
    if len(lines) > said and lines[-1].strip():
        ctx.progress(lines[-1].strip())
    return len(lines)


def warm():
    """Builds the worker's environment (the first time, a minute or so) and checks it measures a box."""
    state["warming"] = True
    try:
        res = run_worker({"op": "warm"}, timeout=1200)
        state["env"] = {"versions": res["versions"], "embree": res["embree"], "step": res["step"], "box_ok": res["box_ok"],
                        "coupon_ok": res.get("coupon_ok"), "blender": res.get("blender"),
                        "checked_at": int(time.time() * 1000)}
        state["env_error"] = (None if res["embree"] and res["box_ok"] and res.get("coupon_ok") else
                              "Embree ray casting or the box check failed" if not (res["embree"] and res["box_ok"]) else
                              "the coupon check (manifold3d cutting a box) failed")
    except Exception as e:
        state["env_error"] = str(e)
        log(f"warming the environment: {e}")
    finally:
        state["warming"] = False


# Files


@contextlib.contextmanager
def workspace():
    os.makedirs(TMP, mode=0o700, exist_ok=True)
    d = os.path.join(TMP, time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6])
    os.makedirs(d, mode=0o700)
    try:
        yield d
    finally:
        shutil.rmtree(d, ignore_errors=True)


def keep(path, tmp, name=None):
    """A file a call made, kept past the call: in out/<the call's folder>/. Answers its path."""
    folder = os.path.join(OUT, os.path.basename(tmp.rstrip("/")))
    os.makedirs(folder, exist_ok=True)
    dest = os.path.join(folder, name or os.path.basename(path))
    shutil.copyfile(path, dest)
    return dest


def save(pairs, save_to):
    """Copies (path, name) pairs into a folder (made when missing): the paths there."""
    folder = os.path.abspath(os.path.expanduser(save_to))
    os.makedirs(folder, exist_ok=True)
    out = []
    for path, name in pairs:
        if path and os.path.exists(path):
            shutil.copyfile(path, os.path.join(folder, name))
            out.append(os.path.join(folder, name))
    return out


def safe_name(text, fallback="model"):
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", str(text or "")).strip("-")
    return (s or fallback)[:120]


def is_model(name):
    return name.lower().endswith(MODEL_EXTS)


def models_in(root):
    """Every model file under a folder, skipping macOS's ._ files and what a zip of a Mac folder carries."""
    found = []
    for d, dirs, files in os.walk(root):
        dirs[:] = sorted(x for x in dirs if not x.startswith((".", "__MACOSX")))
        found += [os.path.join(d, f) for f in sorted(files) if not f.startswith("._") and is_model(f)]
    return found


def unzip(path, dest):
    """A zip, unpacked without letting a name climb out of dest (3 GB at most)."""
    total = 0
    with zipfile.ZipFile(path) as z:
        for info in z.infolist():
            name = info.filename
            if info.is_dir() or name.startswith("__MACOSX/") or os.path.basename(name).startswith("._"):
                continue
            target = os.path.realpath(os.path.join(dest, name))
            if not target.startswith(os.path.realpath(dest) + os.sep):
                continue
            total += info.file_size
            if total > 3e9:
                raise ValueError(f"{os.path.basename(path)} unpacks to more than 3 GB")
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with z.open(info) as src, open(target, "wb") as out:
                shutil.copyfileobj(src, out, 1 << 20)
    return dest


def is_url(source):
    return bool(re.match(r"https?://", str(source)))


def bring(source, dest_dir):
    """One file or folder here, from an https URL or a path on this machine. Answers its path here."""
    source = str(source).strip()
    os.makedirs(dest_dir, exist_ok=True)
    if is_url(source):
        name = safe_name(os.path.basename(urllib.parse.urlparse(source).path), "download")
        dest = os.path.join(dest_dir, name)
        req = urllib.request.Request(source, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=180) as r, open(dest, "wb") as f:
            shutil.copyfileobj(r, f)
        return dest
    name = os.path.basename(source.rstrip("/")) or "model"
    dest = os.path.join(dest_dir, safe_name(name))
    if name.lower().endswith((".obj", ".gltf")):
        dest = os.path.join(dest_dir, name.replace("/", "-"))  # its side files name it as it is
    path = os.path.expanduser(source)
    try:
        if os.path.isdir(path):
            shutil.copytree(path, dest, ignore=shutil.ignore_patterns(".*", "__MACOSX"))
        else:
            shutil.copyfile(path, dest)
    except FileNotFoundError:
        raise ValueError(f"no such file: {path}")
    except PermissionError:
        raise ValueError(f"macOS keeps {path} from this process: give it Full Disk Access, or move the file")
    if name.lower().endswith((".obj", ".gltf")):
        bring_companions(path, dest)
    return dest


def companions(path):
    """The files an OBJ or a .gltf reads beside it (its .mtl and textures; its buffers and images), relative to it."""
    low = path.lower()
    found = []
    try:
        if low.endswith(".obj"):
            with open(path, errors="replace") as f:
                mtls = [ln.split(None, 1)[1].strip() for ln in f if ln.startswith("mtllib ") and len(ln.split(None, 1)) > 1]
            found += mtls
            for m in mtls:
                mp = os.path.join(os.path.dirname(path), m)
                if os.path.exists(mp):
                    with open(mp, errors="replace") as f:
                        for ln in f:
                            bits = ln.strip().split(None, 1)
                            if len(bits) == 2 and bits[0].lower().startswith(("map_", "bump", "disp", "decal", "norm", "refl")):
                                rest = re.sub(r"^(?:-\w+(?:\s+(?:[-+]?[0-9.]+|on|off|[a-z]))*\s+)+", "", bits[1].strip())
                                found += [rest, rest.split()[-1]] if rest else []  # a name may have spaces in it
        elif low.endswith(".gltf"):
            with open(path) as f:
                g = json.load(f)
            for key in ("buffers", "images"):
                for x in g.get(key) or []:
                    uri = x.get("uri") or ""
                    if uri and not uri.startswith("data:"):
                        found.append(urllib.parse.unquote(uri))
    except (OSError, ValueError):
        return []
    out = []
    for rel in found:
        rel = rel.replace("\\", "/")
        if rel and not rel.startswith("/") and ".." not in rel.split("/") and rel not in out:
            out.append(rel)
    return out


def bring_companions(source, dest):
    """An OBJ's or .gltf's side files, beside the copy brought here (one missing is left for Blender to say)."""
    src_dir = os.path.dirname(source)
    for _ in range(2):  # an OBJ's .mtl first, then the textures it names
        for rel in companions(dest)[:64]:
            target = os.path.join(os.path.dirname(dest), rel)
            if os.path.exists(target):
                continue
            os.makedirs(os.path.dirname(target), exist_ok=True)
            try:
                shutil.copyfile(os.path.join(src_dir, rel), target)
            except OSError as e:
                log(f"couldn't bring {rel} beside {os.path.basename(dest)}: {e}")


def expand(path, name):
    """A file brought here as the models in it: itself, or what a zip or folder holds. [(path, name)]."""
    if os.path.isdir(path):
        found = models_in(path)
        return [(p, os.path.relpath(p, path)) for p in found]
    if is_model(name) or name.lower().endswith(".gcode"):
        return [(path, name)]
    sniffed = sniff(path)  # a 3MF is a zip too: it's known by its .model inside
    if sniffed:
        return [(path, name + sniffed)]
    if zipfile.is_zipfile(path):
        dest = path + ".unzipped"
        unzip(path, dest)
        return [(p, os.path.relpath(p, dest)) for p in models_in(dest)]
    raise ValueError(f"{name} isn't a model this reads (STL, 3MF, STEP, OBJ, PLY, glTF, a .blend, FBX, or a zip or folder of them)")


def sniff(path):
    """A model's kind from its bytes, for a link that came without a name."""
    with open(path, "rb") as f:
        head = f.read(512)
    if head.startswith(b"PK"):
        try:
            with zipfile.ZipFile(path) as z:
                return ".3mf" if any(n.lower().endswith(".model") for n in z.namelist()) else None
        except zipfile.BadZipFile:
            return None
    if b"ISO-10303" in head:
        return ".step"
    if head.startswith(b"BLENDER") or head.startswith(b"\x28\xb5\x2f\xfd"):  # a .blend, or one Blender saved compressed
        return ".blend"
    if head.startswith(b"glTF"):
        return ".glb"
    if head.startswith(b"Kaydara FBX Binary"):
        return ".fbx"
    if head.startswith(b"ply"):
        return ".ply"
    if head.lstrip().startswith((b"v ", b"o ", b"mtllib", b"# ")) and b"\nv " in head:
        return ".obj"
    if head.lstrip().lower().startswith(b"solid"):
        return ".stl"
    size = os.path.getsize(path)
    if len(head) >= 84 and size == 84 + 50 * int.from_bytes(head[80:84], "little"):  # binary STL: its count says its size
        return ".stl"
    return None


def gather(files, folder, tmp, ctx):
    """Everything a report names, brought here: [{path, name}]."""
    got = []
    sources = [files] if isinstance(files, str) else list(files or [])
    if folder and not sources:
        sources = [folder]
    elif folder:
        sources = [s if is_url(s) or s.startswith(("/", "~")) else folder.rstrip("/") + "/" + s for s in sources]
    for n, source in enumerate(sources):
        path = bring(source, os.path.join(tmp, "in", str(n)))
        shown = os.path.basename(str(source).rstrip("/"))
        for p, name in expand(path, shown):
            got.append({"path": p, "name": name})
    if not got:
        raise ValueError("nothing to read: give files (paths or links) or a folder")
    return got


def one(file, tmp, ctx, obj=None):
    """The single model a section, view, layers or convert works on."""
    found = gather([file], None, tmp, ctx)
    if len(found) > 1 and obj is None:
        names = ", ".join(f["name"] for f in found[:8])
        raise ValueError(f"{file} holds {len(found)} models ({names}): give one of them as file")
    return found[0]


# Answers


def calling(fn):
    """Counts the call and keeps its failure for status."""
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        state["calls"] += 1
        state["last_call_at"] = int(time.time() * 1000)
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            state["failed"] += 1
            state["last_error"] = f"{fn.__name__}: {e}"
            raise
    return wrapped


def fmt_size(s):
    return " × ".join(f"{x:g}" for x in s) + " mm"


def model_line(o):
    if o.get("error"):
        return f"{o['name']}: can't read it: {o['error']}"
    if o.get("note") and "size_mm" not in o:
        return f"{o['name']}: {o['note']}"
    bits = [fmt_size(o["size_mm"])]
    if o.get("copies", 1) > 1:
        bits.append(f"{o['copies']} copies")
    bits.append("closed solid" if o["watertight"] else "NOT closed")
    if o.get("volume_cm3") is not None:
        bits.append(f"{o['volume_cm3']:g} cm³")
    b = o["bodies"]["count"]
    bits.append(f"{b} bod{'y' if b == 1 else 'ies'}")
    w = o.get("wall") or {}
    if w.get("p1") is not None:
        bits.append(f"walls {w['p1']}–{w['median']} mm")
    if o.get("gap_min") is not None:
        bits.append(f"narrowest gap {o['gap_min']} mm")
    bits.append(("fits the bed " + o["bed"]["how"]) if o["bed"]["fits"] else o["bed"]["how"])
    name = f"{o['source']} › {o['name']}" if o.get("source") and o["source"] != o["name"] else o["name"]
    head = name + (f" ({o['label']})" if o.get("label") else "") + ": " + ", ".join(bits)
    probs = o.get("problems") or []
    return head + ("".join(f"\n  - {p}" for p in probs) if probs else "\n  nothing in the geometry stands in the way")


def slim(o):
    """A model's facts as the answer gives them: what a reader uses, short (the sheet shows the rest)."""
    if "size_mm" not in o:
        return {k: v for k, v in o.items() if k not in ("sheet", "source")}
    keep = ("name", "label", "copies", "note", "left_out", "size_mm", "min", "max", "triangles", "watertight", "open_edges",
            "nonmanifold_edges", "inside_out", "volume_cm3", "solid_grams", "bed", "units_hint", "gap_min", "hole_count",
            "floating_count", "best_down", "problems", "sheet_file", "seconds")
    out = {k: o[k] for k in keep if o.get(k) not in (None, [], {})}
    if o.get("source") and o["source"] != o["name"]:
        out["file"] = o["source"]
    b = o.get("bodies") or {}
    out["bodies"] = {"count": b.get("count"), "specks": b.get("specks"),
                     "parts": [{k: x[k] for k in ("body", "size", "min", "max", "volume_mm3", "watertight", "inside_body")}
                               for x in b.get("bodies", []) if not x.get("speck")][:6]}
    w = o.get("wall") or {}
    out["wall"] = {k: w.get(k) for k in ("min", "p1", "p5", "median", "under_mm2") if w.get(k) is not None}
    out["thin_walls"] = [{"wall": c["wall"], "at": c["at"], "area_mm2": c["area_mm2"]} for c in o.get("thin_walls", [])[:4]]
    out["narrow_gaps"] = [{"gap": c["gap"], "at": c["at"], "area_mm2": c["area_mm2"]} for c in o.get("narrow_gaps", [])[:4]]
    out["holes"] = o.get("holes", [])[:8]
    out["floating"] = o.get("floating", [])[:3]
    out["overhang_mm2"] = o.get("overhang_mm2")
    out["sides"] = {d: {"steep_mm2": round((t["overhang_mm2"]["60-75°"] or 0) + (t["overhang_mm2"]["75-90°"] or 0), 1),
                        "bed_mm2": t["bed_mm2"], "height_mm": t["height_mm"]}
                    for d, t in (o.get("orientations") or {}).items() if d in ("z-", o.get("best_down"))}
    out["sections"] = [{"z": x["z"], "islands": x["islands"],
                        "thinnest_wall": {k: (x.get("thinnest_wall") or {}).get(k) for k in ("mm", "at")} if x.get("thinnest_wall") else None,
                        "narrowest_gap": {k: (x.get("narrowest_gap") or {}).get(k) for k in ("mm", "at")} if x.get("narrowest_gap") else None}
                       for x in o.get("sections") or []]
    return out


FILE = {"type": "string", "description": "The model: a path or an https URL. STL, 3MF, STEP, OBJ, PLY."}
OBJECT = {"type": ["string", "integer"], "description": "For a 3MF with several objects: which, by number (1 is the first) "
          "or words in its name (default: all of them as placed on the plate)."}
POINT = {"type": "array", "items": {"type": "number"}, "minItems": 3, "maxItems": 3}
BOX = {"type": "array", "items": {"type": "number"}, "minItems": 6, "maxItems": 6,
       "description": "Only what's inside [x0, y0, z0, x1, y1, z1], mm."}
AROUND = dict(POINT, description="Only what's within radius of this point [x, y, z] (like a thin spot report named).")
RADIUS = {"type": "number", "description": "With around: how far, mm (default 10)."}
THIN = {"type": "number", "description": "Walls under this many mm count as thin (default 0.8, two lines of a 0.4 nozzle)."}


@app.tool(name="report", read_only=True, schema={"type": "object", "properties": {
    "files": {"type": ["array", "string"], "items": {"type": "string"},
              "description": "The models: paths (a file, a folder of them, a zip) or https URLs."},
    "folder": {"type": "string", "description": "A folder: every model in it, or the folder files' names are in."},
    "objects": {"type": "string", "enum": ["each", "merged"], "description": "A 3MF's objects: each on its own (default; "
                "identical copies counted once), or merged as placed on the plate."},
    "thin": THIN,
    "sheets": {"type": "boolean", "description": "Draw each model's sheet (default true)."},
    "show": {"type": "integer", "description": "How many sheets to show you inline (default 3; all are kept as files)."}}})
@calling
def report(ctx, files=None, folder=None, objects="each", thin=0.8, sheets=True, show=3):
    """A 3D model's printability geometry in one step, for each model given (STL, 3MF with its plate, STEP, OBJ, PLY; a folder or a zip): size and whether it fits the bed, closed solid or not (open and non-manifold edges), separate bodies (stray specks, a shell hidden inside), volume and solid grams, wall thickness by rays (thinnest, 1st percentile, median, the thin areas and where they are), narrow gaps that may fuse, vertical holes with their diameters, islands that start in mid-air, overhang by steepness and which side down prints best, all in plain words under problems; and a sheet per model: walls coloured by thickness on two views, from above, overhangs from below, and four sections at the heights that matter with the thinnest wall and narrowest gap marked. Use it instead of trimesh scripts to check a model."""
    with workspace() as tmp:
        found = gather(files, folder, tmp, ctx)
        ctx.progress(f"Measuring {len(found)} model{'s' if len(found) != 1 else ''}")
        sheet_dir = os.path.join(tmp, "sheets")
        os.makedirs(sheet_dir)
        result = run_worker({"op": "report", "files": found, "objects": objects, "thin": thin,
                             "sheet_dir": sheet_dir if sheets else None, "max_sheets": 12}, ctx)
        models, shown = result["models"], []
        for o in models:
            if o.get("sheet") and os.path.exists(o["sheet"]):
                o["sheet_file"] = keep(o["sheet"], tmp)
                if len(shown) < max(0, int(show if show is not None else 3)):
                    shown.append(sbmcp.image(o["sheet"]))
    lines = [model_line(o) for o in models]
    return sbmcp.content(sbmcp.text("\n".join(lines)), *shown, structured={"models": [slim(o) for o in models]})


CIRCLES = {"type": "array", "maxItems": 4, "items": {"type": ["object", "number"], "properties": {
    "d": {"type": "number", "description": "Its diameter, mm (0.750\" is 19.05)."},
    "at": {"type": "array", "items": {"type": "number"}, "minItems": 2, "maxItems": 2,
           "description": "Its centre in the cut's own axes, as drawn (x, y for a z cut; default: the first version's centroid there)."},
    "label": {"type": "string", "description": "What it is, drawn beside it (default Ø and its diameter)."}}, "required": ["d"]},
    "description": "Reference circles drawn dashed on every cut, like the bore a part must fit (a number is just d), and "
                   "each version measured against each: the diameter it reaches about the centre and how far past the circle "
                   "that is (under 0: inside it), the area outside it, and the diameter left clear when the centre is open."}


@app.tool(name="section", read_only=True, schema={"type": "object", "required": ["file"], "properties": {
    "file": FILE, "object": OBJECT,
    "planes": {"type": "array", "items": {"type": ["string", "object"]}, "description": "Planes to cut: \"z=5.4\", "
               "\"x=12\", or {origin: [x, y, z], normal: [x, y, z]}."},
    "axis": {"type": "string", "enum": ["x", "y", "z"], "description": "With at or count: the axis the cuts cross (default z)."},
    "at": {"type": "array", "items": {"type": "number"}, "description": "Heights along axis to cut at, mm."},
    "count": {"type": "integer", "description": "Or this many cuts spread along axis (default 4)."},
    "through": dict(POINT, description="Three cuts (z, y and x) through this point [x, y, z]."),
    "around": AROUND, "radius": RADIUS, "box": BOX, "thin": THIN,
    "label_holes_under": {"type": "number", "description": "Label holes up to this size, mm (default 5)."},
    "compare": {"type": ["string", "array"], "items": {"type": "string"}, "maxItems": 3,
                "description": "Other versions of the model (paths or URLs; up to 3), cut by the same planes and "
                               "drawn over file's cut in one frame: with one, what both have in gray and what only each has "
                               "in its colour, each outline in its own; answers each version's measures and how each differs "
                               "from file's (area only in each, how far apart the outlines get and where)."},
    "labels": {"type": "array", "items": {"type": "string"}, "description": "With compare: a name for each version, file's "
               "first, like [\"Now: 12 ribs\", \"New: 8 ribs\"] (default their file names)."},
    "align": {"type": "string", "enum": ["placed", "center"], "description": "With compare: placed keeps each where its file "
              "has it (default); center moves each so its middle (x, y) and bottom line up with file's, saying how far."},
    "apart": {"type": "boolean", "description": "With compare: also draw each version on its own beside the overlay, its "
              "thinnest wall, narrowest gap and small holes marked."},
    "circles": CIRCLES}})
@calling
def section(file, ctx, object=None, planes=None, axis=None, at=None, count=None, through=None, around=None,
            radius=None, box=None, thin=0.8, label_holes_under=None, compare=None, labels=None, align=None, apart=False,
            circles=None):
    """Cut a model with planes and see each section drawn to scale on a mm grid: horizontal (z=5.4), vertical (x=12, y=-3), any plane, three through a point, or cropped to a box or around a point. Each answers its islands, its holes (round with the diameter, or slots with width and length), its thinnest wall and narrowest gap with where they are (measured in 3D, so a slanted cut reads a wall's true thickness), and its area. For a fit check of one feature: a pocket's width, a peg's diameter, the wall around a magnet. With compare, other versions of it are cut by the same planes and drawn over it in one frame (what changed tinted in each version's colour, where the outlines are farthest apart marked), instead of a hand-written before/after figure; circles draws a bore or hole a part must fit and measures each version against it."""
    others = [compare] if isinstance(compare, str) else list(compare or [])
    if len(others) > 3:
        raise ValueError(f"{len(others)} versions to compare: give 3 at most (4 with file)")
    with workspace() as tmp:
        f = one(file, tmp, ctx, object)
        versions = [one(o, os.path.join(tmp, f"v{k}"), ctx, object) for k, o in enumerate(others)]
        sheet = os.path.join(tmp, "section.png")
        ctx.progress("Cutting them" if versions else "Cutting it")
        res = run_worker({"op": "section", "file": f["path"], "name": f["name"], "object": object, "planes": planes,
                          "axis": axis, "at": at, "count": count, "through": through, "around": around,
                          "radius": radius, "box": box, "thin": thin, "label_holes_under": label_holes_under,
                          "compare": versions, "labels": labels, "align": align, "apart": apart, "circles": circles,
                          "sheet": sheet}, ctx)
        res.pop("ok", None)
        res["sheet_file"] = keep(sheet, tmp)
        image = sbmcp.image(sheet)
    return sbmcp.content(sbmcp.text(section_words(res)), image, structured=res)


def num(x, unit=""):
    return f"{x:g}{unit}" if isinstance(x, (int, float)) else "–"


def circle_name(label, d):
    size = f"Ø{num(d)}"
    return label if label == size else f"{label} ({size})"


def circle_words(c, one_of=None):
    past = c["past"]
    where = f"{num(abs(past))} mm past it" if past > 0.005 else ("right at it" if past >= -0.005 else f"{num(-past)} mm inside it")
    out = (f"{one_of} " if one_of else "") + f"reaches Ø{num(c['reaches_d'])} about {c['center']}, {where}"
    if c.get("outside_mm2"):
        out += f" ({num(c['outside_mm2'])} mm² outside)"
    if c.get("clear_d") is not None:
        out += f", Ø{num(c['clear_d'])} clear at the centre"
    return out


def cut_words(s):
    w, g = s.get("thinnest_wall"), s.get("narrowest_gap")
    holes = ", ".join((f"Ø{h['d']}" if h.get("d") else f"{h['w']}×{h['l']} slot") + f" at {h['at']}" for h in s.get("holes", [])[:6])
    return (f"{s['islands']} island(s), {s['width']} × {s['height']} mm, {num(s.get('area_mm2'))} mm²"
            + (f", thinnest wall {w['mm']} at {w['at']}" if w else "")
            + (f", narrowest gap {g['mm']} at {g['at']}" if g else "") + (f"; holes {holes}" if holes else ""))


def section_words(res):
    """The answer in words: each cut's measures; with versions, each one's and how each differs from the first's."""
    if not res.get("versions"):
        lines = [f"{res['name']}:"]
        for s in res["sections"]:
            if s.get("empty"):
                lines.append(f"  {s['plane']}: nothing there")
                continue
            lines.append(f"  {s['plane']}: {cut_words(s)}")
            for c in s.get("circles") or []:
                lines.append(f"    {circle_name(c['label'], c['d'])}: {circle_words(c)}")
        return "\n".join(lines)
    vs = res["versions"]
    lines = [" vs ".join(v["label"] for v in vs) + ":"]
    for v in vs:
        if v.get("moved"):
            lines.append(f"  {v['label']} moved by {v['moved']} mm to line up with {vs[0]['label']}")
    for s in res["sections"]:
        lines.append(f"  {s['plane']}:")
        for v in s["versions"]:
            lines.append(f"    {v['version']}: " + ("nothing there" if v.get("empty") else cut_words(v)))
        for d in s["diffs"]:
            if d.get("same"):
                lines.append(f"    {d['version']} cuts the same as {d['against']}")
                continue
            ap = d.get("apart")
            lines.append(f"    {d['version']} against {d['against']}: area {d['area_change_mm2']:+g} mm² "
                         f"({num(d['only_in_first_mm2'])} only in {d['against']}, {num(d['only_in_this_mm2'])} only in {d['version']})"
                         + (f"; outlines up to {num(ap['mm'])} mm apart at {ap['at']}" if ap else ""))
        circ = {}
        for v in s["versions"]:
            for c in v.get("circles") or []:
                circ.setdefault((c["label"], c["d"]), []).append(circle_words(c, v["version"]))
        for (label, d), words in circ.items():
            lines.append(f"    {circle_name(label, d)}: " + "; ".join(words))
    return "\n".join(lines)


@app.tool(name="view", read_only=True, schema={"type": "object", "required": ["file"], "properties": {
    "file": FILE, "object": OBJECT,
    "views": {"type": "array", "items": {"type": ["string", "array"]}, "description": "Up to six: iso, iso-back, top, bottom, "
              "front, back, left, right, or a direction to look along [x, y, z] (default iso, iso-back, top, bottom; "
              "with cut, at the cut)."},
    "color": {"type": "string", "enum": ["thickness", "overhang", "bodies", "plain"], "description": "What the colours say "
              "(default thickness: red under 0.4 mm, orange under thin, yellow under 1.5× thin)."},
    "cut": {"type": ["string", "object"], "description": "Cut it open first: \"y=0.9\" keeps what's below that plane "
            "(or {origin, normal}: what's behind the normal); the cut face is hatched."},
    "around": AROUND, "radius": RADIUS, "box": BOX, "thin": THIN,
    "size": {"type": "integer", "description": "Each view's width in pixels (default 900, or 700 for three or more across)."}}})
@calling
def view(file, ctx, object=None, views=None, color="thickness", cut=None, around=None, radius=None, box=None,
         thin=0.8, size=None):
    """Render a model, shaded, from any side: named views or any direction, coloured by wall thickness, overhang or separate bodies, cropped to a box or around a point, or cut open along a plane with the cut face hatched (the inside of a shell shows pink). To see a feature report or section pointed at, in place."""
    with workspace() as tmp:
        f = one(file, tmp, ctx, object)
        sheet = os.path.join(tmp, "view.png")
        ctx.progress("Rendering it")
        res = run_worker({"op": "view", "file": f["path"], "name": f["name"], "object": object, "views": views,
                          "color": color, "cut": cut, "around": around, "radius": radius, "box": box, "thin": thin,
                          "size": size, "sheet": sheet}, ctx)
        res.pop("ok", None)
        res["sheet_file"] = keep(sheet, tmp)
        image = sbmcp.image(sheet)
    words = f"{res['name']}: {len(res['views'])} view(s), {res['triangles_shown']:,} triangles" + (
        f", inside {res['box']}" if res.get("box") else "") + (f", cut at {res['cut']}" if res.get("cut") else "")
    return sbmcp.content(sbmcp.text(words), image, structured=res)


@app.tool(name="layers", read_only=True, schema={"type": "object", "required": ["file"], "properties": {
    "file": {"type": "string", "description": "The sliced plate: a .gcode.3mf or .gcode, as a path or an https URL."},
    "plate": {"type": "integer", "description": "Which plate of a .gcode.3mf (default its first)."},
    "z": {"type": "array", "items": {"type": "number"}, "description": "Heights to draw the layers at, mm."},
    "layers": {"type": "array", "items": {"type": "integer"}, "description": "Or layer numbers (1 is the first)."},
    "count": {"type": "integer", "description": "Or this many layers spread up the print (default 6)."},
    "around": AROUND, "radius": RADIUS,
    "box": {"type": "array", "items": {"type": "number"}, "description": "Only moves inside [x0, y0, x1, y1] (or a 3D box)."},
    "features": {"type": "array", "items": {"type": "string"}, "description": "Only these features, as the G-code names them "
                 "(Outer wall, Inner wall, Support, Bridge, Sparse infill...)."},
    "model": {"type": "boolean", "description": "Draw the model's outline at each layer over it (default true, for a 3MF)."}}})
@calling
def layers(file, ctx, plate=None, z=None, layers=None, count=None, around=None, radius=None, box=None,
           features=None, model=True):
    """Draw a sliced plate's layers from its G-code, the way it will print: every extrusion as a bead its own width, coloured by feature (outer and inner walls, infill, bridges, supports, brim), arcs (G2/G3) drawn as arcs, at the heights or layers asked, zoomed around a point or a box, with the model's own outline at that height over it (placed where the plate has it). Answers each layer's height and how much of each feature it lays down. To see how a small feature actually prints."""
    with workspace() as tmp:
        f = one(file, tmp, ctx)
        sheet = os.path.join(tmp, "layers.png")
        ctx.progress("Reading its G-code")
        res = run_worker({"op": "layers", "file": f["path"], "name": f["name"], "plate": plate, "z": z, "layers": layers,
                          "count": count, "around": around, "radius": radius, "box": box, "features": features,
                          "model": model, "sheet": sheet}, ctx)
        res.pop("ok", None)
        res["sheet_file"] = keep(sheet, tmp)
        image = sbmcp.image(sheet)
    lines = [f"{res['name']}: {res['layer_count']} layers to z {res['top_z']}" + (f"; {res['overlay_note']}" if res.get("overlay_note") else "")]
    for L in res["layers"]:
        top = ", ".join(f"{k} {v:g} mm" for k, v in list(L["extruded_mm"].items())[:5])
        lines.append(f"  layer {L['layer']} at z {L['z']} ({L['height']} mm): {top or 'nothing extruded here'}")
    return sbmcp.content(sbmcp.text("\n".join(lines)), image, structured=res)


PLATE_FILE = re.compile(r"(?i)\.gcode(\.3mf)?$")
MAX_PLATES = 12


def wanted(name, match):
    """Whether a plate's name fits match: every word in it (any case), or a glob like *-gap03*."""
    import fnmatch
    if not match:
        return True
    base = os.path.basename(name)
    if any(ch in match for ch in "*?["):
        return fnmatch.fnmatch(base.lower(), match.lower())
    return all(w.lower() in base.lower() for w in str(match).split())


def plates_in_folder(folder, match):
    """The sliced plates in a folder (and two levels under it), by their paths."""
    root = os.path.expanduser(folder)
    if not os.path.isdir(root):
        raise ValueError(f"no such folder: {folder}")
    names = []
    for d, dirs, files in os.walk(root):
        dirs[:] = sorted(x for x in dirs if not x.startswith((".", "__MACOSX")))
        if os.path.relpath(d, root).count(os.sep) >= 2:
            dirs[:] = []
        names += [os.path.relpath(os.path.join(d, f), root) for f in sorted(files)
                  if PLATE_FILE.search(f) and not f.startswith("._")]
    names = [n for n in names if wanted(n, match)]
    if not names:
        raise ValueError(f"no sliced plates (.gcode.3mf or .gcode) in {folder}" + (f" with {match!r} in their names" if match else ""))
    return [folder.rstrip("/") + "/" + n for n in names]


def sliced_plates(files, folder, match, tmp, ctx):
    """The plates to read, brought here: [{path, name}], from paths, links or a folder."""
    sources = [files] if isinstance(files, str) else list(files or [])
    if folder:
        sources += plates_in_folder(folder, match)
    if not sources:
        raise ValueError("nothing to read: give files (paths or links) or a folder")
    if len(sources) > MAX_PLATES:
        raise ValueError(f"{len(sources)} plates is more than {MAX_PLATES} at once: narrow it with match")
    got = []
    for n, source in enumerate(sources):
        given = None
        if isinstance(source, dict):  # {file, name}: a link keeps the plate's own name
            given, source = source.get("name"), source.get("file")
        src = str(source or "").strip()
        if not src:
            raise ValueError("each file is a path or a link (or {file, name})")
        ctx.progress(f"Fetching {os.path.basename(src)}")
        path = bring(src, os.path.join(tmp, "in", str(n)))
        if os.path.isdir(path):
            raise ValueError(f"{src} is a folder: give it as folder")
        got.append({"path": path, "name": given or os.path.basename(src)})
    return got


@app.tool(name="supports", read_only=True, schema={"type": "object", "properties": {
    "files": {"type": ["array", "string"], "items": {"anyOf": [{"type": "string"}, {"type": "object", "properties": {
              "file": {"type": "string"}, "name": {"type": "string"}}, "required": ["file"]}]},
              "description": "Sliced plates (.gcode.3mf or .gcode): paths or https URLs, or {file, name} to name one. Several are read "
              "side by side, as variants."},
    "folder": {"type": "string", "description": "Or every sliced plate in this folder (and two levels under it)."},
    "match": {"type": "string", "description": "With folder: only plates whose names have these words (or match this glob, like *gap03*)."},
    "plate": {"type": "integer", "description": "Which plate of a .gcode.3mf (default its first)."},
    "reach": {"type": "number", "description": "How far under a model bead support still counts as under it, mm (default 1)."},
    "cell": {"type": "number", "description": "The grid's cell, mm (default 0.1; coarser on a big plate)."},
    "around": AROUND, "radius": RADIUS,
    "box": {"type": "array", "items": {"type": "number"}, "description": "Only the support inside [x0, y0, x1, y1] (or a 3D box)."},
    "show": {"type": "boolean", "description": "Show the sheet inline (default true; it's kept as a file either way)."}}})
@calling
def supports(ctx, files=None, folder=None, match=None, plate=None, reach=None, cell=None, around=None,
             radius=None, box=None, show=True):
    """Where a sliced plate's supports meet its model, read from the G-code as it will print, for one plate or several variants side by side: the gap actually left between the support's top and the model over it (each bead at its own height, so a tree support that rounds the 0.3 mm asked down to one 0.2 mm layer shows), against the gap asked; where the support touches the model, each place with its height, area, gap and how much of that overhang it reaches; where support stands on the model instead of the bed, and at what gap; and the layer heights the model and the support were laid at. Answers a line per plate (several: a side-by-side first), and a sheet: each plate from above and from the side with every contact coloured by its gap, and how much lies at each gap. Use it instead of unzipping plates and parsing their G-code by hand."""
    with workspace() as tmp:
        plates = sliced_plates(files, folder, match, tmp, ctx)
        sheet = os.path.join(tmp, "supports.png")
        ctx.progress(f"Reading {len(plates)} plate{'s' if len(plates) != 1 else ''}' G-code")
        res = run_worker({"op": "supports", "files": plates, "plate": plate, "reach": reach, "cell": cell,
                          "around": around, "radius": radius, "box": box, "sheet": sheet}, ctx)
        res.pop("ok", None)
        made = res.get("sheet") and os.path.exists(res["sheet"])
        res["sheet_file"] = keep(res["sheet"], tmp) if made else None
        image = sbmcp.image(res["sheet"]) if made and show else None
        res.pop("sheet", None)
    words = res.pop("words")
    blocks = [sbmcp.text("\n".join(words))] + ([image] if image else [])
    return sbmcp.content(*blocks, structured=res)


@app.tool(name="support_model", read_only=True, schema={"type": "object", "required": ["file"], "properties": {
    "file": {"type": "string", "description": "A sliced plate (.gcode.3mf or .gcode): a path or an https URL."},
    "plate": {"type": "integer", "description": "Which plate of a .gcode.3mf (default its first)."},
    "hidden": {"type": "array", "items": {"type": ["string", "integer"]}, "description": "Marks you know are covered "
               "in use (a mating face, the inside of a case that closes), by the labels an earlier call gave them "
               "(2, \"B\"): drawn and counted as hidden."},
    "show": {"type": "boolean", "description": "Show the pictures inline (default true; they're kept as files either way)."}}})
@calling
def support_model(file, ctx, plate=None, hidden=None, show=True):
    """A sliced plate's real supports to look at, read from the G-code as it will print: pictures from the two or three angles that show them and their marks best (the part in gray with the support beads in orange, interface darker; under each, the same angle with the support off and every place it meets the part painted and numbered: red on a face that shows, blue on one hidden inside a pocket, a hole or a box), and the same as a 3D file to turn around (a .usdz for Quick Look on a Mac or iPhone, and a colored .3mf), as files. Answers each mark: under the part or standing on its top, its area and place, whether it shows (judged by how open the part's surface is there: the inside of a deep case reads hidden, a shallow tray's floor shows; give hidden for faces covered in use), and how thick the part is behind it (thin under 1.2 mm: it can tear coming off). Use it instead of rendering supports by hand."""
    with workspace() as tmp:
        got = sliced_plates([file], None, None, tmp, ctx)[0]
        stem = re.sub(r"(\.gcode)?(\.3mf)?$", "", got["name"]) + "-supports"
        out = os.path.join(tmp, stem)
        ctx.progress("Building the supports from the G-code")
        res = run_worker({"op": "support_model", "file": got["path"], "plate": plate, "out": out, "sheet": out + ".png",
                          "title": got["name"], "hidden": hidden}, ctx)
        res.pop("ok", None)
        res["usdz"] = {"file": keep(res.pop("usdz"), tmp), "name": stem + ".usdz"}
        res["3mf"] = {"file": keep(res.pop("3mf"), tmp), "name": stem + ".3mf"}
        made = res.get("sheet") and os.path.exists(res["sheet"])
        image = sbmcp.image(res["sheet"]) if made and show else None
        res["picture"] = {"file": keep(res["sheet"], tmp), "name": stem + ".png"} if made else None
        res.pop("sheet", None)
    words = res.pop("words")
    return sbmcp.content(*([sbmcp.text("\n".join(words))] + ([image] if image else [])), structured=res)


SIZE = {"type": ["number", "string", "object"], "description": "For a scene file: how big it prints, uniformly scaled: its "
        "longest side in mm (30) or with a unit (\"1.2 in\", \"3cm\"), or one side as it sits on the bed after down, like "
        "{\"z\": 30} (height), {\"x\": 40} (default: the file's own units, as units reads them)."}
UNITS = {"type": "string", "enum": ["auto", "scene", "mm", "cm", "m", "in", "ft"], "description": "For a scene file: what one of "
         "its units is (default auto: the scene's unit scale when it sets one; else whichever of mm or metres gives a "
         "likely size, said in the answer). size overrides it."}
PICK = {"type": "array", "items": {"type": "string"}, "description": "For a scene file: objects by name (exactly, else words "
        "in their names). include keeps only those, hidden ones too; exclude leaves those out. Default: what Blender shows "
        "(not hidden, not a Boolean's cutter or a wire helper)."}


@app.tool(name="convert", schema={"type": "object", "required": ["file"], "properties": {
    "file": {"type": "string", "description": "The model: a path or an https URL. A .blend, FBX, "
             "glTF/GLB, OBJ (with its .mtl and textures beside it) or USD is a scene file, split into parts by colour; "
             "a 3MF, STEP, PLY or STL is turned into STL as it is."},
    "split": {"type": "string", "enum": ["auto", "material", "color", "object", "none"], "description": "For a scene file: "
              "how it splits into parts, one STL each. auto (default): by colour when a material is painted in several "
              "(a texture or colour attribute: an AI-made model, say), else by material when it has several, else one. "
              "color: the distinct colours its surfaces show, clustered (colors says how many); material: one per "
              "material; object: one per object; none: one STL."},
    "colors": {"type": "integer", "description": "With split color: how many colours (default: as many as stand apart)."},
    "size": SIZE,
    "scale": {"type": "number", "description": "Or a factor over the units read (instead of size)."},
    "units": UNITS,
    "include": PICK, "exclude": PICK,
    "bodies": {"type": "string", "enum": ["whole", "cut"], "description": "For a scene file: whole (default) puts each "
               "separate body into one part, the colour most of its lit surface is, so parts are closed where the "
               "bodies are; cut cuts along the colours instead (open surfaces: to see the paint, not to print)."},
    "objects": {"type": "string", "enum": ["merged", "each"], "description": "A 3MF's objects: one STL of all of them as placed "
                "(default), or one STL each."},
    "down": {"type": "string", "enum": ["z-", "z+", "y+", "y-", "x+", "x-", "auto"], "description": "The side to put on the bed "
             "(default z-, as it is; auto picks the one with the least steep overhang). Every part turns together."},
    "center": {"type": "boolean", "description": "Centre it on x=0, y=0 (default: where it is)."},
    "name": {"type": "string", "description": "The STLs' name before -<part>.stl (default: the file's)."},
    "save_to": {"type": "string", "description": "Also put the STLs (and the picture) in this folder (made when missing)."},
    "show": {"type": "boolean", "description": "Show the picture of the parts (default true for a scene file)."}}})
@calling
def convert(file, ctx, objects="merged", down="z-", center=False, save_to=None, split=None, colors=None, size=None,
            scale=None, units=None, include=None, exclude=None, bodies=None, name=None, show=True):
    """Turn a model into binary STL in mm, z up, on its bed, on the side asked or the best one, each as a file (and, with save_to, in a folder). A Blender file (.blend), FBX, glTF/GLB, OBJ or USD is read through Blender as it shows it (modifiers applied, instances made real, hidden objects and Boolean cutters left out) and split into one STL per colour or material, at the size asked: each separate body goes whole into one part, so the parts are closed where the bodies are; a texture's colours (an AI-made model's) are clustered, its baked shading not counted as a colour. Answers each part's colour (hex, for the slicer's color), size, whether it's closed, how the file's units were read, the slicer's parts line ready to slice, and a picture of the parts. A 3MF (as its plate places it, modifiers and negative parts left out), STEP or PLY turns into STL as it is."""
    with workspace() as tmp:
        f = one(file, tmp, ctx)
        out_dir = os.path.join(tmp, "out")
        scene = f["name"].lower().endswith(SCENE_EXTS)
        job = {"op": "convert", "file": f["path"], "name": f["name"], "objects": objects, "down": down, "center": center,
               "out_dir": out_dir}
        if isinstance(size, str) and size.strip().startswith("{"):
            size = json.loads(size)
        if scene:
            ctx.progress(f"Reading {f['name']} through Blender")
            job.update({"split": split, "colors": colors, "size": size, "scale": scale, "units": units,
                        "include": names_list(include), "exclude": names_list(exclude), "bodies": bodies, "stem": name,
                        "sheet": os.path.join(tmp, "parts.png") if show or save_to else None})
        elif any(x is not None for x in (split, colors, size, scale, units, include, exclude, bodies)):
            raise ValueError("split, colors, size, scale, units, include, exclude and bodies are for a scene file (.blend, "
                             "FBX, glTF, OBJ, USD); this is " + f["name"])
        res = run_worker(job, ctx)
        files = res["files"]
        for x in files:
            x["file"] = keep(x["path"], tmp)
        sheet = res.pop("sheet", None)
        image = None
        if sheet and os.path.exists(sheet):
            res["sheet_file"] = keep(sheet, tmp)
            image = sbmcp.image(sheet) if show else None
        if save_to:
            extra = [(sheet, f"{safe_name(name or os.path.splitext(f['name'])[0])}-parts.png")] if sheet and os.path.exists(sheet) else []
            saved = save([(x["path"], x["name"]) for x in files] + extra, save_to)
            for x in files:
                x["saved"] = {"path": os.path.join(os.path.abspath(os.path.expanduser(save_to)), x["name"])}
            res["saved"] = saved
        for x in files:
            x.pop("path", None)
    if not scene:
        lines = [f"{x['name']}: {fmt_size(x['size_mm'])}, {x['triangles']:,} triangles, "
                 f"{'closed' if x['watertight'] else 'NOT closed'}, {x['down']} down" + (f", left out {', '.join(x['left_out'])}" if x["left_out"] else "")
                 + (f", saved to {x['saved']['path']}" if x.get("saved") else "") for x in files]
        return sbmcp.content(sbmcp.text("\n".join(lines)), structured={"files": files})
    res["slicer"] = slicer_call(res, files, save_to)
    blocks = [sbmcp.text(scene_words(f["name"], res, size, scale))] + ([image] if image else [])
    res.pop("orientations", None)
    return sbmcp.content(*blocks, structured=res)


def names_list(v):
    """Objects' names as a list, however they came: a list, a JSON list in a string, or names split by commas."""
    if v is None or v == "" or v == []:
        return None
    if isinstance(v, str):
        t = v.strip()
        if t.startswith("["):
            try:
                v = json.loads(t)
            except ValueError:
                v = [x.strip(" \"'") for x in t.strip("[]").split(",")]
        else:
            v = [x.strip() for x in t.split(",")]
    if not isinstance(v, (list, tuple)):
        v = [v]
    out = [str(x).strip() for x in v if str(x).strip()]
    return out or None


def slicer_call(res, files, save_to):
    """The slicer's slice arguments for these parts: their names and the folder they're in."""
    call = {"parts": res["slicer"]["parts"], "color": res["slicer"]["color"], "machine": res["slicer"]["machine"],
            "out": res.get("stem") or safe_name(os.path.splitext(files[0]["name"])[0])}
    folder = os.path.abspath(os.path.expanduser(save_to)) if save_to else (os.path.dirname(files[0]["file"]) if files and files[0].get("file") else None)
    if folder:
        call["folder"] = folder
    return call


def describe_size(size):
    if isinstance(size, dict):
        (k, v), = size.items()
        side = {"z": "high", "height": "high", "tall": "high", "x": "wide (x)", "width": "wide (x)", "y": "deep (y)",
                "depth": "deep (y)"}.get(str(k).lower(), "on its longest side")
        return f"{v if isinstance(v, str) else f'{v:g} mm'} {side}"
    return f"{size if isinstance(size, str) else f'{size:g} mm'} on its longest side"


def scene_words(name, res, size, scale):
    """convert's answer for a scene file, in lines to read."""
    t, u = res["total"], res["units"]
    sized = (f"sized as asked: {describe_size(size)}" if size is not None else f"scaled ×{scale:g}" if scale else
             f"read {u['how']}" + (": a guess, so give size for the size it's meant to be" if u.get("guessed") else ""))
    head = (f"{name} → {len(res['files'])} part{'s' if len(res['files']) != 1 else ''}, {fmt_size(t['size_mm'])} together, "
            f"{t['down']} down ({sized}); split {res['how']}:")
    lines = [head]
    for x in res["files"]:
        bits = [f"{x['color']} #{x['hex']}", fmt_size(x["size_mm"])]
        if x["watertight"]:
            bits.append("closed")
        else:
            why = "cut along the colours" if res.get("bodies_mode") == "cut" else "open in the file itself"
            bits.append(f"NOT closed ({x['open_edges']:,} open edges, {why})")
        if x.get("bodies"):
            bits.append(f"{x['bodies']} bod{'y' if x['bodies'] == 1 else 'ies'}")
        if x.get("volume_cm3") is not None:
            bits.append(f"{x['volume_cm3']:g} cm³")
        if x.get("material") and x["material"] != x["part"]:
            bits.append(f"material {x['material']}")
        lines.append(f"- {x['name']}: " + ", ".join(bits) + (f" → {x['saved']['path']}" if x.get("saved") else ""))
    mixed = res.get("mixed") or []
    if mixed and res.get("bodies_mode") == "cut":
        lines.append(f"{len(mixed)} bod{'y' if len(mixed) == 1 else 'ies'} showed more than one colour, cut along them (open surfaces).")
    elif mixed:
        by = {}
        for m in mixed:
            by.setdefault((m["went"], "shading" in (m.get("why") or "")), []).append(m)
        for (went, shading), ms in by.items():
            first = ms[0]
            other = sorted({k for m in ms for k, v in m["reads"].items() if k != went})
            lines.append(f"{len(ms)} bod{'y' if len(ms) == 1 else 'ies'} (the {fmt_size(first['size_mm'])} one at {first['center']}"
                         + (f" and {len(ms) - 1} more" if len(ms) > 1 else "") + f") read partly {' and '.join(other) or 'another colour'}"
                         + (" from the texture's baked shading" if shading else "") + f"; went whole into {went}, "
                         + ("the colour of their lit surface." if shading else "the colour most of them is.")
                         + " (bodies: \"cut\" splits along the colours instead.)")
    asked = res.get("asked")
    big = max(t["size_mm"])
    if size is None and not scale and (big < 3 or not t["bed"]["fits"]):
        lines.append(f"At {big:g} mm it's {'too small to be what was meant' if big < 3 else 'too big for the bed'}: give size.")
    elif not t["bed"]["fits"]:
        lines.append(f"It doesn't fit the bed: {t['bed']['how']}.")
    left = [o for o in res.get("objects") or [] if not o["kept"] and o["type"] in ("mesh", "curve", "font", "surface", "meta")]
    if asked:
        lines.append(f"Kept {', '.join(res['kept'])}, as asked" + (f"; {len(left)} other object{'s' if len(left) != 1 else ''} left out." if left else "."))
    elif left:
        lines.append(f"Kept {', '.join(res['kept'])}; left out {len(left)}: " + "; ".join(
            f"{o['name']} ({', '.join(o['why_not'] or [])})" for o in left[:5]) + ("; …" if len(left) > 5 else "")
            + " (include names any of them).")
    else:
        lines.append(f"Kept {', '.join(res['kept'])}.")
    sc = res.get("slicer") or {}
    lines.append("To slice: " + json.dumps(sc))
    return "\n".join(lines)


RANGE = {"type": ["array", "string"], "items": {"type": ["number", "null"]}, "minItems": 2, "maxItems": 2}


@app.tool(name="coupon", schema={"type": "object", "required": ["file"], "properties": {
    "file": FILE, "object": OBJECT,
    "z": dict(RANGE, description="Keep this height range, mm: [lo, hi], null for the model's own end ([30.5, null] "
              "keeps everything from 30.5 up), or \"30.5..\"."),
    "x": dict(RANGE, description="Keep this x range, mm (as z)."),
    "y": dict(RANGE, description="Keep this y range, mm (as z)."),
    "box": dict(BOX, description="Or keep what's inside [x0, y0, z0, x1, y1, z1], mm."),
    "around": dict(POINT, description="Or keep a cube around this point [x, y, z]."),
    "radius": {"type": "number", "description": "With around: half the cube's side, mm (default 10)."},
    "base": {"type": "number", "description": "What was cut away under the coupon comes back as a base this thick, mm "
             "(default 0: none, the cut face sits on the bed). 2 swaps a solid foot for a 2 mm plate of its own "
             "outline, so ribs that stood on it stay joined and sit as they did."},
    "base_shape": {"type": "string", "enum": ["outline", "hull", "disc", "rect"], "description": "The base: outline "
                   "(default: the footprint of what was cut away), its hull, the disc around it, or its rectangle."},
    "margin": {"type": "number", "description": "Grow the base this much all round, mm (default 0)."},
    "shorten": {"type": ["array", "string", "object"], "description": "Take out the band between two heights and close "
                "the gap: [a, b] along z, or {\"x\": [a, b]}. Keeps both ends of a long part (a bore's mouth and floor)."},
    "snap": {"type": "number", "description": "Move each cut onto the model's flat face within this many mm, so a cut "
             "meant for a shoulder lands on it (default 0.5; 0 cuts exactly where asked)."},
    "down": {"type": "string", "enum": ["z-", "z+", "x-", "x+", "y-", "y+"], "description": "The side on the bed "
             "(default z-, as the model is); a base goes on that side."},
    "center": {"type": "boolean", "description": "Centre it on x=0, y=0 (default false: where it was)."},
    "plane": {"type": "string", "description": "The sheet's side view and section: y=0 or x=3 (default: whichever of "
              "x and y through what's kept cuts more of it)."},
    "thin": THIN,
    "name": {"type": "string", "description": "The coupon's file name, without .stl (default <model>-coupon)."},
    "save_to": {"type": "string", "description": "A folder to put the STL and its sheet in (the slicer's folder)."},
    "show": {"type": "boolean", "description": "Show the sheet inline (default true)."}}})
@calling
def coupon(file, ctx, object=None, z=None, x=None, y=None, box=None, around=None, radius=None, base=0,
           base_shape="outline", margin=0, shorten=None, snap=None, down="z-", center=False, plane=None, thin=0.8,
           name=None, save_to=None, show=True):
    """Cut a printable fit coupon from a model, instead of a hand-written manifold3d script: keep a height range (or x/y ranges, a box, a cube around a point) exactly as modelled, cut so the coupon is a closed solid; bring back what was cut away underneath as a thin base of its own footprint (a glide's solid foot swapped for a 2 mm disc under its full-height ribs), its hull, a disc or a rectangle; or take a band out and close the gap (shorten). Cuts snap onto a flat face within 0.5 mm. Lays it on the side asked and answers its size, whether it's closed and in one piece, its volume and grams, its walls and problems as report finds them, the STL as a file (and in save_to), the slicer's arguments, and a sheet: the model from the side with what's kept, the coupon's outline and section, and the coupon coloured by wall thickness."""
    if all(v is None for v in (z, x, y, box, around, shorten)):
        raise ValueError("say what to keep: z (like [30.5, null]), x, y, box, around, or shorten")
    with workspace() as tmp:
        f = one(file, tmp, ctx, object)
        ctx.progress(f"Cutting a coupon from {f['name']}")
        out_dir = os.path.join(tmp, "out")
        sheet = os.path.join(tmp, "coupon.png")
        res = run_worker({"op": "coupon", "file": f["path"], "name": f["name"], "object": object, "z": z, "x": x, "y": y,
                          "box": box, "around": around, "radius": radius, "base": base, "base_shape": base_shape,
                          "margin": margin, "shorten": shorten, "snap": snap, "down": down, "center": center,
                          "plane": plane, "thin": thin, "stem": safe_name(name) if name else None, "out_dir": out_dir,
                          "sheet": sheet}, ctx)
        res.pop("ok", None)
        res["file"] = keep(res["path"], tmp)
        res["sheet_file"] = keep(sheet, tmp) if os.path.exists(sheet) else None
        image = sbmcp.image(sheet) if show and os.path.exists(sheet) else None
        if save_to:
            stem = os.path.splitext(res["name"])[0]
            save([(res["path"], res["name"]), (sheet, f"{stem}.png")], save_to)
            res["saved"] = {"path": os.path.join(os.path.abspath(os.path.expanduser(save_to)), res["name"])}
        res.pop("path", None)
    stem = os.path.splitext(res["name"])[0]
    res["slicer"] = {"parts": [res["name"]], "out": stem,
                     "folder": os.path.abspath(os.path.expanduser(save_to)) if save_to else os.path.dirname(res["file"])}
    return sbmcp.content(sbmcp.text("\n".join(coupon_words(res))), *([image] if image else []), structured=res)


def coupon_words(res):
    """coupon's answer in lines to read."""
    g = lambda v: f"{v:g}"  # noqa: E731
    size = " × ".join(g(v) for v in res["size_mm"])
    out = [f"{res['name']}: {size} mm, {'closed' if res['watertight'] else 'NOT closed'}, "
           f"{res['bodies']} {'piece' if res['bodies'] == 1 else 'pieces'}, {g(res['volume_cm3'])} cm³ "
           f"({g(res['solid_grams']['PLA'])} g solid PLA, {g(res['solid_grams']['TPU'])} g TPU)"
           + (f", {g(round(res['of_model'] * 100, 1))}% of the model" if res.get("of_model") else "")]
    k = res["kept"]
    cut_axes = sorted({c[0] for c in res["cuts"]})
    kept = ", ".join(f"{a} {g(k[a][0])}..{g(k[a][1])}" for a in cut_axes) or "the whole model"
    out.append(f"  kept {kept} of the model ({' × '.join(g(v) for v in res['model_size_mm'])} mm), {res['down']} down")
    b = res.get("base")
    if b:
        what = f"Ø{g(b['d'])} disc" if b["shape"] == "disc" else f"{b['shape']}, {g(b['size_mm'][0])} × {g(b['size_mm'][1])} mm"
        out.append(f"  base: {g(b['mm'])} mm thick ({what}), for the {g(b['replaces_mm'])} mm cut away under it")
    s = res.get("shorten")
    if s:
        out.append(f"  shortened: {s['axis']} {g(s['from'])}..{g(s['to'])} taken out ({g(s['taken_out_mm'])} mm), the gap closed")
    w = res.get("wall") or {}
    if w.get("min") is not None:
        out.append(f"  walls {g(w['p1'])}–{g(w['median'])} mm (1st pct–median), thinnest {g(w['min'])}")
    for n in res.get("notes") or []:
        out.append(f"  note: {n}")
    if res.get("left_out_bodies"):
        out.append(f"  note: {res['left_out_bodies']} open bod{'y' if res['left_out_bodies'] == 1 else 'ies'} of the "
                   "model couldn't be cut and were left out")
    for p in res.get("problems") or []:
        out.append(f"  problem: {p}")
    if res.get("saved"):
        out.append(f"  saved to {res['saved']['path']}")
    return out


MAX_VARIANTS = 24
VALUES = {"type": "object", "additionalProperties": {}}


def as_json(v, what):
    """An argument a turn may have sent as JSON text."""
    if isinstance(v, str) and v.strip()[:1] in ("{", "["):
        try:
            return json.loads(v)
        except ValueError:
            raise ValueError(f"can't read {what}: {v[:80]!r} isn't JSON")
    return v


def label_of(params, keys):
    return " · ".join(f"{k} {fmt_value(params[k])}" for k in keys if k in params) or "as written"


def fmt_value(v):
    if isinstance(v, bool):
        return "on" if v else "off"
    if isinstance(v, float):
        return f"{v:g}"
    return str(v)


def variant_sets(vary=None, variants=None, params=None, baseline=False):
    """The variants asked for, each {params, tag, label}: every combination of vary's values, then each of variants,
    each over the fixed params; the script as written first with baseline. Tags and labels name what varies."""
    vary, variants, params = as_json(vary, "vary"), as_json(variants, "variants"), as_json(params, "params")
    if vary is not None and not isinstance(vary, dict):
        raise ValueError('vary is {name: [values]}, like {"N": [6, 8], "T": [1.0, 1.2]}')
    if variants is not None and not (isinstance(variants, list) and all(isinstance(v, dict) for v in variants)):
        raise ValueError('variants is a list of {name: value}, like [{"N": 6, "T": 1.4}, {"N": 8, "T": 1.2}]')
    if params is not None and not isinstance(params, dict):
        raise ValueError('params is {name: value}, like {"RC": 4.5}')
    fixed = dict(params or {})
    sets = []
    if vary:
        keys = list(vary)
        values = [v if isinstance(v, list) else [v] for v in vary.values()]
        if any(not v for v in values):
            raise ValueError("each name in vary needs at least one value")
        count = 1
        for v in values:
            count *= len(v)
        if count > MAX_VARIANTS:
            raise ValueError(f"vary makes {count} variants: {MAX_VARIANTS} at most in one call, so split it")
        sets += [{**fixed, **dict(zip(keys, combo))} for combo in itertools.product(*values)]
    sets += [{**fixed, **v} for v in variants or []]
    if not sets and fixed:
        sets = [fixed]
    unique = []
    for p in sets:
        if p not in unique:
            unique.append(p)
    if baseline or not unique:
        unique.insert(0, None)
    if len(unique) > MAX_VARIANTS:
        raise ValueError(f"{len(unique)} variants: {MAX_VARIANTS} at most in one call, so split it")
    keys = []
    for p in unique:
        for k in p or {}:
            if k not in keys:
                keys.append(k)
    given = [p for p in unique if p is not None]
    varied = [k for k in keys if len({json.dumps(p.get(k), sort_keys=True) for p in given}) > 1] or keys
    out = []
    for p in unique:
        if p is None:
            out.append({"params": None, "tag": "", "label": "as written"})
            continue
        tag = "-".join(f"{k}{fmt_value(p[k])}" for k in varied if k in p)
        out.append({"params": p, "tag": safe_name(tag, "variant"), "label": label_of(p, varied)})
    tags = [v["tag"] for v in out]
    for i, v in enumerate(out):
        if tags.count(v["tag"]) > 1:
            v["tag"] = f"{v['tag'] or 'variant'}-{i + 1}"
    return out


@app.tool(name="variants", schema={"type": "object", "properties": {
    "script": {"type": "string", "description": "The model script: a .py path (its folder is on its import path) "
               "or an https URL. Plain Python that builds a model and exports it (trimesh, manifold3d, numpy, "
               "shapely, scipy are there)."},
    "code": {"type": "string", "description": "Or the script's text itself."},
    "vary": dict(VALUES, description='Values to try, each name with a list: {"N": [6, 8], "T": [1.0, 1.2]} builds every '
                 f"combination (4 here; {MAX_VARIANTS} at most)."),
    "variants": {"type": ["array", "string"], "items": VALUES, "description": "Or each variant spelled out: "
                 '[{"N": 6, "T": 1.4}, {"N": 8, "T": 1.2}].'},
    "params": dict(VALUES, description='Values every variant gets: {"RC": 4.5}. Alone, builds that one variant.'),
    "baseline": {"type": "boolean", "description": "Build the script as written too, first (default false; with no "
                 "values at all it's the one built)."},
    "args": {"type": "array", "items": {"type": "string"}, "description": 'sys.argv after the script\'s name, for one '
             'that reads argparse: ["{out}", "--n", "{N}"] ({out} is the variant\'s folder).'},
    "read": {"type": "array", "items": {"type": "string"}, "description": "Top-level names to read after each run and "
             'answer per variant, like ["L", "zt"] (numbers the script works out: a rib length, a force ratio).'},
    "name": {"type": "string", "description": 'Each model\'s name, without its extension: "glide-n{N}-t{T}" '
             "(Python's format: {T:.1f} works; {stem} is the name the script gave it, {tag} what varies). A variant's "
             "other models add what sets their own name apart (-coupon). Default: <its name>-<what varies>, like "
             "glide-N6-T1.2."},
    "outputs": {"type": ["array", "string"], "items": {"type": "string"}, "description": "Only the models whose names "
                'match: words or globs, like ["*coupon*"] (default: every model each variant writes).'},
    "draw": {"type": "string", "description": "Which model of each variant the sheet draws, by words in its name "
             "(default: the first it wrote)."},
    "plane": {"type": "string", "description": 'A section through every variant at the same plane, drawn at one scale '
              'under each and measured: "z=42".'},
    "material": {"type": "string", "enum": ["PLA", "PETG", "ASA", "ABS", "TPU"], "description": "Grams are solid grams "
                 "of this (default PLA)."},
    "thin": THIN,
    "timeout": {"type": "number", "description": "Seconds one variant's script may run (default 120)."},
    "packages": {"type": "array", "items": {"type": "string"}, "description": "Packages the script needs beyond the "
                 'usual, as pip names them: ["build123d"] (the first run with a new set builds it).'},
    "save_to": {"type": "string", "description": "A folder to put every model and the sheet in (the slicer's "
                "folder)."},
    "show": {"type": "boolean", "description": "Show the sheet inline (default true)."}}})
@calling
def variants_tool(ctx, script=None, code=None, vary=None, variants=None, params=None, baseline=False, args=None,
                  read=None, name=None, outputs=None, draw=None, plane=None, material="PLA", thin=0.8, timeout=120,
                  packages=None, save_to=None, show=True):
    """Build a parametric model's variants from its own script and measure each, in one step, instead of sed on the script and a loop of uv runs: vary takes values to try ({"N": [6, 8], "T": [1.0, 1.2]} builds all four), and each reaches the script however it reads them: a top-level assignment (N, T = 8, 1.2) is pinned to the variant's value, every value is in the environment and PARAMS too, and args fills an argparse script's argv. A name the script never reads fails before anything runs, and variants that come out byte-for-byte the same are called out. Each variant runs in its own process and folder (its cwd and OUT) in the worker's Python (trimesh, manifold3d, numpy, shapely, scipy), and every model it writes is renamed by what it varied and measured as report measures: size and bed fit, closed, bodies, volume and grams, walls, gaps, mid-air islands, steep overhangs, problems. Answers a line per model, what it printed and the values asked for in read, each model as a file (and in save_to), the slicer's plates for them all, and a sheet with each variant side by side coloured by wall thickness (and a section at plane through each). With no values it builds the script as written and lists the top-level values it could vary."""
    sets = variant_sets(vary, variants, params, baseline)
    read = names_list(read) if read else None
    outputs = names_list(outputs) if outputs else None
    args = as_json(args, "args")
    if args is not None and not isinstance(args, list):
        args = [args]
    if packages:
        packages = names_list(packages)
    if bool(script) == bool(code):
        raise ValueError("give the script: a path or URL in script, or its text in code")
    with workspace() as tmp:
        script_dir = None
        if code:
            script_name = "script.py"
            path = os.path.join(tmp, "script", script_name)
            os.makedirs(os.path.dirname(path))
            with open(path, "w") as f:
                f.write(code)
        else:
            src = str(script).strip()
            path = bring(src, os.path.join(tmp, "script"))
            if os.path.isdir(path):
                raise ValueError(f"{src} is a folder: give the script in it")
            script_name = os.path.basename(src.rstrip("/")) or "model.py"
            if not script_name.endswith(".py"):
                script_name += ".py"
            if not is_url(src):
                script_dir = os.path.dirname(os.path.abspath(os.path.expanduser(src)))
        ctx.progress(f"Building {len(sets)} variant{'s' if len(sets) != 1 else ''} of {script_name}")
        out_dir = os.path.join(tmp, "out")
        sheet = os.path.join(tmp, "variants.png")
        timeout = float(timeout or 120)
        res = run_worker({"op": "variants", "script": path, "script_name": script_name, "script_dir": script_dir,
                          "variants": sets, "args": args, "read": read, "name": name, "outputs": outputs, "draw": draw,
                          "plane": plane, "material": material, "thin": thin, "timeout": timeout,
                          "work_dir": os.path.join(tmp, "work"), "out_dir": out_dir, "sheet": sheet,
                          "deadline": time.time() + 520}, ctx, timeout=570, extra=packages)
        res.pop("ok", None)
        for v in res["variants"]:
            for f in v["files"]:
                f["file"] = keep(f["path"], tmp)
        res["sheet_file"] = keep(sheet, tmp) if res.get("sheet") and os.path.exists(sheet) else None
        image = sbmcp.image(sheet) if show and res.get("sheet") and os.path.exists(sheet) else None
        if save_to:
            pairs = [(f["path"], f["name"]) for v in res["variants"] for f in v["files"]]
            if res.get("sheet") and os.path.exists(sheet):
                pairs.append((sheet, f"{os.path.splitext(script_name)[0]}-variants.png"))
            save(pairs, save_to)
            res["saved"] = {"folder": os.path.abspath(os.path.expanduser(save_to))}
        for v in res["variants"]:
            for f in v["files"]:
                f.pop("path", None)
        res.pop("sheet", None)
    plates = [{"out": os.path.splitext(f["name"])[0], "parts": [f["name"]]}
              for v in res["variants"] for f in v["files"] if not f.get("error")]
    if plates:
        kept = next((f["file"] for v in res["variants"] for f in v["files"] if f.get("file")), None)
        res["slicer"] = {"plates": plates, "folder": res["saved"]["folder"] if save_to else os.path.dirname(kept) if kept else None}
    return sbmcp.content(sbmcp.text("\n".join(variants_words(res))), *([image] if image else []), structured=res)


def variants_words(res):
    """variants' answer in lines to read: how the values reached the script, then a line per model."""
    g = lambda v: f"{v:g}" if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)  # noqa: E731
    vs = res["variants"]
    out = [f"{res['script']}: {len(vs)} variant{'s' if len(vs) != 1 else ''} built and measured"]
    how = []
    for k, h in (res.get("how") or {}).items():
        if h.get("pinned"):
            lines = ", ".join(str(x) for x in h["pinned"])
            how.append(f"{k} pinned at line{'s' if len(h['pinned']) > 1 else ''} {lines}"
                       + (f" (written {g(h['as_written'])})" if "as_written" in h else ""))
        elif h.get("env"):
            how.append(f"{k} from the environment")
        elif h.get("argv"):
            how.append(f"{k} in argv")
    if how:
        out.append("  " + "; ".join(how))
    elif res.get("knobs"):
        out.append("  its top-level values: " + ", ".join(f"{k['name']} {g(k['value'])}" for k in res["knobs"][:30]))
    mat = res.get("material") or "PLA"
    for v in vs:
        head = v["label"]
        if v.get("error"):
            out.append(f"{head}: FAILED: {v['error']}")
            if v.get("printed"):
                out.append("  printed: " + v["printed"].splitlines()[-1][:300])
        for f in v["files"]:
            if f.get("error"):
                out.append(f"{head} › {f['name']}: {f['error']}")
                continue
            if "size_mm" not in f:
                out.append(f"{head} › {f['name']}: not measured")
                continue
            bits = [" × ".join(g(x) for x in f["size_mm"]) + " mm", "closed" if f["watertight"] else "NOT closed",
                    f"{f['bodies']} bod{'y' if f['bodies'] == 1 else 'ies'}"]
            if f.get("volume_cm3") is not None:
                bits.append(f"{g(f['volume_cm3'])} cm³" + (f" ({g(f['grams'])} g {mat})" if f.get("grams") is not None else ""))
            w = f.get("wall") or {}
            if w.get("min") is not None:
                bits.append(f"thinnest wall {g(w['min'])} mm")
            if f.get("steep_overhang_mm2"):
                bits.append(f"{g(f['steep_overhang_mm2'])} mm² past 60°")
            bed = f.get("bed") or {}
            bits.append(("fits the bed " + bed.get("how", "")) if bed.get("fits") else bed.get("how", "doesn't fit the bed"))
            out.append(f"{head} › {f['name']}: " + ", ".join(b for b in bits if b))
            s = f.get("section")
            if s:
                tw = s.get("thinnest_wall")
                out.append(f"  at {s['plane']}: {s.get('islands')} island{'s' if s.get('islands') != 1 else ''}, "
                           f"{g(s.get('area_mm2'))} mm²" + (f", thinnest wall {g(tw['mm'])} mm" if tw else ""))
            for p in f.get("problems") or []:
                out.append(f"  - {p}")
        if v.get("printed") and not v.get("error"):
            out.append("  printed: " + v["printed"].strip().splitlines()[-1][:240])
        if v.get("read"):
            out.append("  read: " + ", ".join(f"{k} {json.dumps(x) if not isinstance(x, (int, float)) else g(x)}"
                                             for k, x in v["read"].items()))
        if v.get("unread"):
            out.append(f"  never set: {', '.join(v['unread'])}")
    for w in res.get("warnings") or []:
        out.append(f"note: {w}")
    if res.get("saved"):
        out.append(f"saved to {res['saved']['folder']}")
    return out



@app.tool(name="scene", read_only=True, schema={"type": "object", "required": ["file"], "properties": {
    "file": {"type": "string", "description": "A .blend, FBX, glTF/GLB, OBJ (with its .mtl and textures beside it) or USD: a "
             "path or an https URL."},
    "units": UNITS, "include": PICK, "exclude": PICK,
    "show": {"type": "boolean", "description": "Show it as Blender shows it, in its own colours (default true)."}}})
@calling
def scene(file, ctx, units=None, include=None, exclude=None, show=True):
    """What's in a Blender file (or FBX, glTF, OBJ, USD), read through Blender as it shows it, before converting it: every object (kept, or why not: hidden, a Boolean's cutter, a helper, no faces), each kept one's size and triangles, how the file's units read and its size in mm, each material and where its colour comes from (a plain colour, an image texture with the colours it holds, a colour attribute, nodes), how many separate bodies, and how convert would split it; with a picture in its own colours. Use it to pick include, size and split before convert."""
    with workspace() as tmp:
        f = one(file, tmp, ctx)
        if not f["name"].lower().endswith(SCENE_EXTS):
            raise ValueError(f"{f['name']} isn't a scene file (.blend, FBX, glTF, OBJ, USD): report or view reads it")
        ctx.progress(f"Reading {f['name']} through Blender")
        sheet = os.path.join(tmp, "scene.png")
        res = run_worker({"op": "scene", "file": f["path"], "name": f["name"], "units": units, "include": names_list(include),
                          "exclude": names_list(exclude), "sheet": sheet if show else None}, ctx)
        res.pop("ok", None)
        image = None
        if res.get("sheet") and os.path.exists(sheet):
            res["sheet_file"] = keep(sheet, tmp)
            image = sbmcp.image(sheet)
        res.pop("sheet", None)
    u = res["units"]
    lines = [f"{f['name']} (Blender {res.get('blender')}): {fmt_size(res['size_mm'])} as read, {u['how']}"
             + (" (a guess: convert's size sets it)" if u.get("guessed") else "") + f"; {res['bodies']} separate bodies, "
             f"{res['triangles']:,} triangles."]
    for k in res["kept"]:
        o = next((x for x in res["objects"] if k["object"].startswith(x["name"])), {})
        mats = ", ".join(o.get("materials") or []) or "no material"
        lines.append(f"- {k['object']}: {fmt_size(k['size_mm'])}, {k['triangles']:,} triangles, {k['area_share']:.0%} of the "
                     f"surface, {mats}" + (f"; {o['note']}" if o.get("note") else ""))
    for m in res["materials"]:
        src = {"plain": "a plain colour", "texture": f"its texture {m.get('image')}", "attribute": "a colour attribute",
               "procedural": "nodes", "none": "nothing"}.get(m["source"], m["source"])
        cols = (": " + ", ".join(f"{c['name']} #{c['hex']} {c['share']:.0%}" for c in m["colors"])) if m.get("colors") else f" #{m['hex']}"
        lines.append(f"Material {m['name'] or '(none)'}: colour from {src}{cols}" + (f" ({m['note']})" if m.get("note") else ""))
    w = res["would_split"]
    lines.append(f"convert would split it {w['how']}: " + ", ".join(f"{p['part']} #{p['hex']} {p['area_share']:.0%}" for p in w["parts"])
                 + (f"; {len(w['mixed'])} bodies show more than one colour, each going whole into one" if w.get("mixed") else ""))
    left = [o for o in res["objects"] if not o["kept"] and o["type"] in ("mesh", "curve", "font", "surface", "meta")]
    if left:
        by = {}
        for o in left:
            by.setdefault("; ".join(o["why_not"] or ["left out"]), []).append(o["name"])
        lines.append(f"Left out {len(left)}: " + " · ".join(f"{', '.join(n[:6])}{' …' if len(n) > 6 else ''} ({why})" for why, n in by.items()))
    blocks = [sbmcp.text("\n".join(lines))] + ([image] if image else [])
    return sbmcp.content(*blocks, structured=res)


# Specs and verify: what a job's plates must be, kept in this extension's home, and each sliced plate checked
# against it


FEATURE_KINDS = ("gap", "wall", "empty", "solid", "hole", "size", "volume")
EXPECT_KEYS = ("filament", "color", "printer", "layer", "walls", "infill", "brim", "brim_type", "supports", "nozzle",
               "plate_type", "nozzle_c", "seam", "top_layers", "bottom_layers", "infill_pattern", "ironing", "copies",
               "settings")
SPECS = os.path.join(HOME, "specs")


def job_key(job, required=True):
    """A job's key: its name, lower case, in letters, digits and dashes."""
    k = re.sub(r"[^a-z0-9]+", "-", str(job or "").lower()).strip("-")[:60]
    if not k and required:
        raise ValueError("give the job: a short name its plates' names start with (like bracket-v2)")
    return k or None


def plate_job(name):
    """The job a plate is for: the longest job with a spec that its name starts with."""
    stem = job_key(os.path.basename(str(name or "")), required=False) or ""
    have = sorted((f[:-5] for f in os.listdir(SPECS) if f.endswith(".json")) if os.path.isdir(SPECS) else [], key=len,
                  reverse=True)
    return next((k for k in have if stem == k or stem.startswith(k + "-")), None)


def spec_of(key):
    """A job's spec as kept in its home, or None."""
    try:
        with open(os.path.join(SPECS, key + ".json")) as f:
            spec = json.load(f)
    except (OSError, ValueError):
        return None
    return spec if not spec.get("retired") else None


def store_spec(spec):
    os.makedirs(SPECS, exist_ok=True)
    tmp = os.path.join(SPECS, spec["job"] + ".json.tmp")
    with open(tmp, "w") as f:
        json.dump(spec, f, indent=1)
    os.replace(tmp, os.path.join(SPECS, spec["job"] + ".json"))


def sha256_of(path):
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_feature(f, n):
    """A feature as a spec keeps it, checked for what each kind needs (the worker measures it)."""
    if not isinstance(f, dict):
        raise ValueError(f"feature {n} isn't an object: give {{name, kind, ...}}")
    f = {k: v for k, v in f.items() if v is not None}
    kind = str(f.get("kind") or "").lower()
    if kind not in FEATURE_KINDS:
        raise ValueError(f"feature {f.get('name') or n}: kind is one of {', '.join(FEATURE_KINDS)}")
    f["kind"] = kind
    f["name"] = str(f.get("name") or f"{kind} {n}").strip()

    def point(key, size=3):
        v = f.get(key)
        if not (isinstance(v, list) and len(v) == size and all(isinstance(x, (int, float)) for x in v)):
            raise ValueError(f"feature {f['name']}: {key} is a point, [{', '.join('xyz'[:size])}] in mm")
    if kind in ("gap", "wall"):
        point("from")
        point("to")
    if kind in ("empty", "solid"):
        point("at")
    if kind == "hole":
        if f.get("at") is not None:
            point("at")
        elif f.get("z") is None:
            raise ValueError(f"feature {f['name']}: a hole is found in a section: give z and near [x, y], or at [x, y, z]")
        if f.get("near") is not None:
            point("near", 2)
    if kind == "size" and f.get("mm") is not None and not (isinstance(f["mm"], list) and len(f["mm"]) == 3):
        raise ValueError(f"feature {f['name']}: a size's mm is [x, y, z] (null for an axis that doesn't matter)")
    if kind in ("gap", "wall", "hole") and all(f.get(k) is None for k in ("mm", "min", "max", "d", "w")):
        raise ValueError(f"feature {f['name']}: say what it must measure: mm (with tol), min or max")
    return f


def keep_file(source, key, tmp, name=None):
    """A model copied into the job's spec folder, so the spec finds it later wherever the file went."""
    path = bring(source, os.path.join(tmp, "keep", uuid.uuid4().hex[:8]))
    if os.path.isdir(path):
        raise ValueError(f"{source} is a folder: give the model file itself")
    shown = name or os.path.basename(str(source).rstrip("/"))
    sha = sha256_of(path)
    folder = os.path.join(SPECS, key)
    os.makedirs(folder, exist_ok=True)
    stored = os.path.join(folder, f"{sha[:12]}-{safe_name(shown)}")
    shutil.copyfile(path, stored)
    return {"name": shown, "source": None if is_url(source) else str(source), "sha256": sha, "file": stored}


def public_spec(spec):
    return {k: v for k, v in spec.items() if k != "history"} | {"changes": len(spec.get("history") or [])}


def spec_lines(spec):
    m = spec.get("model")
    lines = [f"{spec['job']}'s spec" + (f" (changed {len(spec['history'])} times; last: {spec['history'][-1]['why']})"
                                           if spec.get("history") and spec["history"][-1].get("why") else "") + ":"]
    if m:
        lines.append(f"  model: {m['name']} (sha256 {m['sha256'][:12]}" + (f", from {m['source']}" if m.get("source") else "") + ")")
    for o in spec.get("others") or []:
        lines.append(f"  not this earlier version: {o['name']} (sha256 {o['sha256'][:12]})")
    for f in spec.get("features") or []:
        lines.append(f"  feature: {f['name']} ({f['kind']})")
    if spec.get("expect"):
        lines.append("  settings: " + ", ".join(f"{k} {v}" for k, v in spec["expect"].items()))
    return lines


def emit_spec(spec):
    m = spec.get("model")
    store_spec(spec)
    app.emit("spec", {"entity": f"print-spec:{spec['job']}", "kind": "print-spec",
                      "summary": f"{spec['job']}: {m['name'] if m else 'no model'}; {len(spec.get('features') or [])} features, "
                                 f"{len(spec.get('expect') or {})} settings, {len(spec.get('others') or [])} earlier versions",
                      "facts": public_spec(spec)})


JOB = {"type": "string", "description": "The job: a short name (bracket-v2) its plates' names start with."}


@app.tool(name="spec", read_only=True, schema={"type": "object", "required": ["job"], "properties": {"job": JOB}})
@calling
def spec(job):
    """A job's spec as it stands: the model its plates must be made of, the earlier versions they must not be, the features asked for and the settings, as set_spec keeps them."""
    key = job_key(job)
    current = spec_of(key)
    if not current:
        return sbmcp.content(sbmcp.text(f"{key} has no spec yet: set_spec sets one (a model, features, settings)."),
                             structured={"job": key, "spec": None})
    return sbmcp.content(sbmcp.text("\n".join(spec_lines(current))), structured={"job": key, "spec": public_spec(current)})


@app.tool(name="set_spec", schema={"type": "object", "required": ["job"], "properties": {
    "job": JOB,
    "model": {"type": "string", "description": "The model its plates must be made of now (the newest version asked for): a path "
              "or a URL. The one it replaces becomes an earlier version that plates must not be."},
    "others": {"type": "array", "items": {"type": "string"}, "description": "Earlier versions to rule out besides (paths)."},
    "features": {"type": "array", "items": {"type": "object"}, "description": (
        "What the part must have, in the model's own coordinates (its STL's), each {name, kind, ...}; a name already there "
        "is replaced. kind gap or wall: measured along a line, from [x,y,z] to [x,y,z], the space (or material) the line's "
        "middle is in, like calipers: a slot's width, a chamfer's opening at a face, a wall. empty or solid: at [x,y,z] "
        "(a cut that must be there, or must not). hole: in the section at z, the hole nearest near [x,y]: its diameter d, "
        "or a slot's width w. size: mm [x,y,z] (null for an axis that doesn't matter). volume: cm3. Each gap, wall and "
        "hole says what it must measure: mm with tol (default 0.1), or min and/or max. part: words of the model's name, "
        "when the plate has several parts. gcode: false skips looking for it in the G-code.")},
    "drop": {"type": "array", "items": {"type": "string"}, "description": "Names of features to take out."},
    "expect": {"type": "object", "description": (
        "Settings every plate must have (a key set to null is taken out): filament (ASA), color (#000000), printer (P1S), "
        "layer (0.2), walls, infill (15), brim (mm), brim_type, supports (true/false), nozzle, plate_type, nozzle_c, seam, "
        "top_layers, bottom_layers, infill_pattern, ironing; settings {raw Bambu key: value} for any other.")},
    "why": {"type": "string", "description": "What was asked for that this change carries, in a few words (kept in its history)."},
    "retire": {"type": "boolean", "description": "The job is done, or its spec no longer holds: plates are checked against nothing."}}})
@calling
def set_spec(job, ctx, model=None, others=None, features=None, drop=None, expect=None, why=None, retire=False):
    """Set or change a job's spec: what its plates must be, kept in this extension's home so it outlives the task that heard it. The model its plates must be made of (the newest version; the one it replaces is kept as an earlier version a plate must not be), the features asked for as checkable measures in the model's coordinates (a chamfer's opening at each face, a slot's width, a hole's diameter, a cut that must be empty), and the settings (filament, color, printer, walls...). Set it the moment a change is asked for, and give the new model as soon as it's built, before any slicing; verify then checks every plate of the job against it. Answers too how the model measures on each feature, so a feature whose coordinates are wrong shows at once."""
    key = job_key(job)
    current = spec_of(key)
    if not any(x is not None for x in (model, others, features, drop, expect)) and not retire:
        raise ValueError("nothing to change: give a model, others, features, drop or expect (spec reads it as it stands)")
    spec = json.loads(json.dumps(current)) if current else {"job": key, "model": None, "others": [], "features": [],
                                                              "expect": {}, "history": []}
    spec.pop("retired", None)
    now = int(time.time() * 1000)
    changed = []
    with workspace() as tmp:
        if retire:
            spec["retired"] = True
            changed.append("retired")
        if model is not None:
            kept = keep_file(model, key, tmp)
            old = spec.get("model")
            if not old or old.get("sha256") != kept["sha256"]:
                if old and all(o.get("sha256") != old.get("sha256") for o in spec["others"]):
                    spec["others"].append({**old, "superseded_at": now})
                spec["others"] = [o for o in spec["others"] if o.get("sha256") != kept["sha256"]]
                spec["model"] = {**kept, "set_at": now}
                changed.append(f"model {kept['name']}")
        for o in others or []:
            kept = keep_file(o, key, tmp)
            if kept["sha256"] != (spec.get("model") or {}).get("sha256") and all(x.get("sha256") != kept["sha256"] for x in spec["others"]):
                spec["others"].append({**kept, "superseded_at": now})
                changed.append(f"earlier version {kept['name']}")
        names = {str(d).strip().lower() for d in drop or []}
        if names:
            before = len(spec["features"])
            spec["features"] = [f for f in spec["features"] if f["name"].lower() not in names]
            if len(spec["features"]) < before:
                changed.append(f"dropped {before - len(spec['features'])} feature(s)")
        for n, f in enumerate(features or []):
            f = check_feature(f, len(spec["features"]) + n + 1)
            spec["features"] = [x for x in spec["features"] if x["name"].lower() != f["name"].lower()] + [f]
            changed.append(f"feature {f['name']}")
        for k, v in (expect or {}).items():
            if k not in EXPECT_KEYS:
                raise ValueError(f"expect has no {k!r}: use {', '.join(EXPECT_KEYS)} (settings for a raw Bambu key)")
            if v is None:
                spec["expect"].pop(k, None)
            else:
                spec["expect"][k] = v
            changed.append(f"{k} {v}")
        measured = None
        if spec.get("model") and spec["features"] and not retire:
            ctx.progress("Measuring the model's features")
            path = spec["model"]["file"]
            measured = run_worker({"op": "features", "models": [{"path": path, "name": spec["model"]["name"]}],
                                   "features": spec["features"]}, ctx)["features"]
    spec["history"] = (spec.get("history") or [])[-29:] + [{"at": now, "why": why, "changed": changed}]
    emit_spec(spec)
    lines = spec_lines(spec)
    if measured:
        lines.append("On the model:")
        lines += [f"  {'✓' if x['ok'] else '✗'} {x['name']}: {x['words']} (want {x['want']})" for x in measured]
        bad = [x for x in measured if not x["ok"]]
        if bad:
            lines.append(f"The model itself fails {len(bad)} feature(s): it lacks what was asked for, or the feature's coordinates "
                         "are off. Fix whichever it is before slicing.")
    return sbmcp.content(sbmcp.text("\n".join(lines)), structured={"job": key, "spec": public_spec(spec),
                                                                  "model_features": measured})


def verdict_lines(res, plate_name, code):
    checks = res["checks"]
    bad = [c for c in checks if not c["ok"]]
    s = res.get("summary") or {}
    fil = ", ".join(f"{f.get('type')} {f.get('color')}" for f in s.get("filaments") or [])
    facts = ", ".join(x for x in (f"{s.get('copies')} cop{'y' if s.get('copies') == 1 else 'ies'}", fil,
                                  s.get("printer"), f"{s['seconds'] / 3600:.2f} h" if s.get("seconds") else None,
                                  f"{s['grams']:g} g" if s.get("grams") else None) if x)
    against = f"{code}'s spec" if res.get("spec") else "what was given"
    if res["pass"] is None:
        head = f"NOTHING TO CHECK: {plate_name} ({facts}): no spec for {code}, and no model, features or settings given." if code else \
            f"NOTHING TO CHECK: {plate_name} ({facts}): give a model, features or settings, or a job with a spec."
    elif res["pass"]:
        head = f"PASS: {plate_name} matches {against}: all {len(checks)} checks ({facts})."
    else:
        head = f"FAIL: {plate_name} doesn't match {against}: {len(bad)} of {len(checks)} checks failed ({facts})."
    return [head] + [f"✗ {c['words']}" for c in bad] + [f"✓ {c['words']}" for c in checks if c["ok"]]


@app.tool(name="verify", read_only=True, schema={"type": "object", "required": ["file"], "properties": {
    "file": {"type": "string", "description": "The sliced plate: a .gcode.3mf as a path or a URL."},
    "job": {"type": "string", "description": "The job whose spec it's checked against (default: the job its name starts with)."},
    "model": {"type": ["string", "array"], "items": {"type": "string"}, "description": "The model(s) it must be made of, over the spec's: paths."},
    "others": {"type": "array", "items": {"type": "string"}, "description": "Earlier versions it must not be, besides the spec's."},
    "features": {"type": "array", "items": {"type": "object"}, "description": "Features to check besides the spec's (as spec takes them; the same name replaces the spec's)."},
    "expect": {"type": "object", "description": "Settings to check besides the spec's (as spec takes them), like {\"copies\": 6}."},
    "compare": {"type": "string", "description": "Another sliced plate (a path) whose settings it must "
                "have: every setting that differs is a failed check (like a plate approved before)."},
    "name": {"type": "string", "description": "Its name, for a link (like bracket-v2-test.gcode.3mf): what it's called "
             "and recorded as, and the job its name starts with."},
    "plate": {"type": "integer", "description": "Which plate of the .gcode.3mf (default its first)."},
    "tol": {"type": "number", "description": "How far its parts may be from the model, mm (default 0.05)."},
    "record": {"type": "boolean", "description": "Record the outcome as a plate_check event (default true)."},
    "show": {"type": "boolean", "description": "Show the sheet inline (default true; it's kept as a file either way)."}}})
@calling
def verify(file, ctx, job=None, model=None, others=None, features=None, expect=None, compare=None, name=None,
           plate=None, tol=None, record=True, show=True):
    """Whether a sliced plate is what its job asked for, checked rather than eyeballed. Against the job's spec (spec keeps it) and anything given here: each object on the plate laid on the model and measured surface to surface (a plate sliced from an earlier version fails, saying which version it is and where it differs, like a chamfer that isn't there), every feature measured on the plate's own part and found open in the G-code of every copy at its height (a slot the brim runs into, a hole the slicer closed), the settings (filament, color, printer, walls, layer, brim, supports...), the number of copies, and nothing off the bed; or every setting against an approved plate (compare). Answers PASS or FAIL with each check in a line, failures first, and a sheet: each feature as the G-code lays it down with the model's outline and what was measured, and where a part differs from its model."""
    with workspace() as tmp:
        found = gather([str(file).strip()], None, tmp, ctx)
        path = found[0]["path"]
        name = os.path.basename(str(name)) if name else found[0]["name"]
        if not name.lower().endswith(".3mf"):
            name += ".gcode.3mf" if not name.lower().endswith(".gcode") else ""
        code = job_key(job) if job else plate_job(name)
        sp = spec_of(code) if code else None
        models, olds = [], []
        if sp and sp.get("model") and model is None:
            models.append({"path": sp["model"]["file"], "name": sp["model"]["name"]})
        for n, src in enumerate([model] if isinstance(model, str) else list(model or [])):
            got = bring(src, os.path.join(tmp, "model", str(n)))
            models.append({"path": got, "name": os.path.basename(str(src).rstrip("/"))})
        for o in (sp or {}).get("others") or []:
            if o.get("file") and os.path.exists(o["file"]):
                olds.append({"path": o["file"], "name": o["name"]})
        for n, src in enumerate(others or []):
            got = bring(src, os.path.join(tmp, "others", str(n)))
            olds.append({"path": got, "name": os.path.basename(str(src).rstrip("/"))})
        feats = list((sp or {}).get("features") or [])
        for n, f in enumerate(features or []):
            f = check_feature(f, len(feats) + n + 1)
            feats = [x for x in feats if x["name"].lower() != f["name"].lower()] + [f]
        exp = dict((sp or {}).get("expect") or {})
        exp.update({k: v for k, v in (expect or {}).items()})
        exp = {k: v for k, v in exp.items() if v is not None}
        cmp = None
        if compare:
            got = gather([str(compare).strip()], None, tmp, ctx)[0]
            cmp = {"path": got["path"], "name": got["name"]}
        sheet = os.path.join(tmp, "verify.png")
        ctx.progress(f"Checking {name}" + (f" against {code}'s spec" if sp else ""))
        res = run_worker({"op": "verify", "file": path, "name": name, "plate": plate, "models": models, "others": olds,
                          "features": feats, "expect": exp, "compare": cmp, "tol": tol, "sheet": sheet}, ctx)
        res.pop("ok", None)
        res["job"], res["spec"] = code, bool(sp)
        res["plate_sha256"] = sha256_of(path)
        res["sheet_file"] = keep(res["sheet"], tmp) if res.get("sheet") and os.path.exists(res["sheet"]) else None
        image = sbmcp.image(res["sheet"]) if show and res.get("sheet") and os.path.exists(res["sheet"]) else None
        res.pop("sheet", None)
    lines = verdict_lines(res, name, code)
    if record and res["pass"] is not None:
        stem = re.sub(r"(?i)\.gcode\.3mf$|\.3mf$|\.gcode$", "", os.path.basename(name))
        bad = [c["words"] for c in res["checks"] if not c["ok"]]
        app.emit("plate_check", {"entity": f"plate-check:{safe_name(stem).lower()}", "kind": "plate-check",
                           "summary": lines[0],
                           "facts": {"pass": res["pass"], "job": code, "plate": stem, "file": name,
                                     "plate_sha256": res["plate_sha256"], "spec": bool(sp),
                                     "model": (sp or {}).get("model", {}).get("name") if sp and model is None else None,
                                     "checks": len(res["checks"]), "failed": bad[:12], "sheet_file": res["sheet_file"],
                                     "checked_at": int(time.time() * 1000)}})
    blocks = [sbmcp.text("\n".join(lines))] + ([image] if image else [])
    return sbmcp.content(*blocks, structured=res)

PLANE = {"type": ["string", "object"], "description": "\"z=7.5\" (x=, y= too: an axis cut, in the model's own "
         "coordinates), or {origin: [x, y, z], normal: [x, y, z], up?, right?} (its coordinates then run from origin, "
         "right being the view's horizontal)."}
VPOINT = {"type": "array", "items": {"type": "number"}, "minItems": 2, "maxItems": 3,
          "description": "[h, v] in the view's own coordinates, or [x, y, z] in the model's (projected into the view)."}
DIM = {"type": "object", "properties": {
    "across": {"type": "array", "items": VPOINT, "minItems": 2, "maxItems": 2, "description": "Calipers: a probe "
               "line [a, b] whose middle is in the feature; it measures the material or gap there, between where the "
               "probe crosses the outline nearest its middle on each side (a wall's thickness, a slot's width, a "
               "pocket's depth)."},
    "from": VPOINT, "to": VPOINT,
    "hole": dict(VPOINT, description="The hole around (or within reach of) this point: its diameter (Ø) or a slot's width × length."),
    "diameter": dict(VPOINT, description="The outline (a boss, a peg) or hole nearest this point: its diameter."),
    "text": {"type": "string", "description": "Its words, \"{}\" standing for the measured value (\"{} window\", "
             "\"2× {}\"); words without {} follow the value. Default: the value alone."},
    "along": {"type": "string", "enum": ["h", "v", "aligned"], "description": "What it measures (default: h or v when "
              "the points line up that way, else the straight distance)."},
    "side": {"type": "string", "enum": ["above", "below", "left", "right", "inside"], "description": "Where it goes "
             "(default: outside, on the side nearer its points, stacked in tiers); inside draws it at the points."},
    "at": {"type": "number", "description": "Or exactly where its line goes: a v (for h) or h (for v) in the view's "
           "coordinates; for aligned, its offset from the points on paper, mm."},
    "part": {"type": "string", "description": "Measure only this part's cut (default: every part's in the view)."},
    "cut": dict(PLANE, description="Measure at another cut, seen in this view: a hidden feature's size."),
    "expect": {"type": "number", "description": "What it should measure: the answer warns when it's off by more than tol."},
    "tol": {"type": "number", "description": "With expect, mm (default 0.05)."},
    "decimals": {"type": "integer", "description": "Default 2."},
    "free": {"type": "boolean", "description": "from/to points that are meant to be off the outline (a centre, a pitch): no warning."},
    "reach": {"type": "number", "description": "hole/diameter: how far from the point it may be when nothing is around it, mm (default 2)."}}}
VIEW = {"type": "object", "properties": {
    "cut": dict(PLANE, description="A section: this plane cuts every part in the view, each cut face filled in its "
                "part's colour. " + PLANE["description"]),
    "look": {"type": ["string", "array"], "items": {"type": "number"}, "description": "Or an outside view: top, bottom, "
             "front, back, left, right, or a direction to look along [x, y, z]: each part's silhouette with the "
             "sharp edges that show."},
    "title": {"type": "string", "description": "Words after VIEW A (default: what it cuts or looks at)."},
    "caption": {"type": "string", "description": "A line or two under the title."},
    "label": {"type": "string", "description": "Its letter (default A, B, C... in order)."},
    "parts": {"type": "array", "items": {"type": "string"}, "description": "Which parts it shows, by name (default all)."},
    "window": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4, "description": "Only "
               "[h0, v0, h1, v1] of it, in its own coordinates (a detail; default: everything it cuts or sees)."},
    "scale": {"type": "number", "description": "Its own scale, like 6 for 6:1 (a detail); default the sheet's."},
    "flip": {"type": "boolean", "description": "Mirror its horizontal."},
    "new_row": {"type": "boolean", "description": "Start a new row of views with it."},
    "hidden": {"type": "array", "items": {"type": ["string", "object"]}, "description": "Hidden outlines, dashed: other "
               "cuts' outlines seen in this view, each a plane or {cut, within: [h0, v0, h1, v1], parts}."},
    "dims": {"type": "array", "items": DIM, "description": "Dimensions, each measured off the geometry."},
    "labels": {"type": "array", "items": {"type": "object", "required": ["at", "text"], "properties": {
        "at": VPOINT, "text": {"type": "string"}, "color": {"type": "string"}, "bold": {"type": "boolean"},
        "leader": {"type": "boolean", "description": "A leader to the point, its words placed where they hit nothing "
                   "(default true); false sets the words at the point."}}}},
    "lines": {"type": "array", "items": {"type": "object", "properties": {
        "horizontal": {"type": "number", "description": "A reference line across the view at this v (a layer height, a face)."},
        "vertical": {"type": "number", "description": "Or down it at this h."},
        "text": {"type": "string"}, "color": {"type": "string"},
        "style": {"type": "string", "description": "Matplotlib's: \":\" (default), \"-\", \"--\"."}}}}}}
PART = {"type": "object", "required": ["file"], "properties": {
    "file": FILE, "name": {"type": "string", "description": "What views and dims call it (default: its file's name)."},
    "object": OBJECT,
    "color": {"type": "string", "description": "Its cut face: #rrggbb or cut, blue, gold, nickel, green, purple, "
              "yellow, teal, white (default: salmon for the first part, then blue, gold, nickel...)."},
    "hatch": {"type": "boolean", "description": "Hatch its cut face."},
    "offset": {"type": "array", "items": {"type": "number"}, "description": "Move it [x, y, z] mm (after rotate)."},
    "rotate": {"type": "array", "items": {"type": "number"}, "description": "Turn it [rx, ry, rz] degrees about the "
               "origin, in that order."},
    "transform": {"type": "array", "items": {"type": "array", "items": {"type": "number"}}, "description": "Or a 4×4 "
                  "matrix placing it (applied first): a connector into its pocket's frame."}}}


@app.tool(name="drawing", schema={"type": "object", "required": ["parts", "views"], "properties": {
    "parts": {"type": "array", "items": {"anyOf": [{"type": "string"}, PART]}, "description": "The models, in one "
              "space: paths or URLs (STL, 3MF, STEP, OBJ, PLY), or {file, name, color, offset, rotate, "
              "transform...} to place one (a connector in its pocket)."},
    "views": {"type": "array", "items": VIEW, "description": "Up to 9: each a cut (section) or a look (outside view), "
              "with its dimensions, labels, reference lines and hidden outlines."},
    "title": {"type": "string", "description": "The title block's title."},
    "lines": {"type": "array", "items": {"type": "string"}, "description": "The title block's other lines (who it's "
              "for, the revision, material); the units, scale and date are added."},
    "header": {"type": "string", "description": "A line or two across the top of the sheet."},
    "notes": {"type": "array", "items": {"type": "string"}, "description": "Notes, bottom left, each wrapped."},
    "size": {"type": "string", "enum": ["A4", "A3", "A2", "letter", "tabloid"], "description": "The sheet, landscape (default A3)."},
    "portrait": {"type": "boolean"},
    "scale": {"type": "number", "description": "The sheet's scale, like 4 for 4:1 (default: the largest standard one "
              "at which every view fits)."},
    "date": {"type": "string", "description": "The title block's date (default today)."},
    "name": {"type": "string", "description": "The files' name (default: from the title)."},
    "save_to": {"type": "string", "description": "Also put the PDF and PNG in this folder."},
    "show": {"type": "boolean", "description": "Show the sheet (default true)."}}})
@calling
def drawing(ctx, parts, views, title=None, lines=None, header=None, notes=None, size="A3", portrait=False, scale=None,
            date=None, name=None, save_to=None, show=True):
    """A dimensioned drawing sheet from models, in one step, instead of a hand-written trimesh and matplotlib script: plan, section and detail views (a plane cut through every part, each cut face in its part's colour, or an outside view's silhouette and edges), with dimensions measured off the geometry itself (calipers across a wall, slot or pocket; between points; a hole's diameter; at another cut for a hidden feature), labels with leaders, reference lines and hidden outlines; laid out on a titled sheet (A3 by default) at the largest standard scale that fits, dimensions stacked in tiers outside each view, every label placed where it hits nothing, and the finished sheet checked with the renderer's own extents for words on words, lines or cut faces. Answers the PDF and a PNG (files; save_to puts them in a folder), what each dimension measured, what still overlaps (if anything) and what didn't measure (a probe that missed, an expect that's off)."""
    if isinstance(parts, str):
        parts = json.loads(parts) if parts.strip().startswith("[") else [parts]
    if isinstance(views, str):
        views = json.loads(views)
    if not parts:
        raise ValueError("a drawing needs parts: the models' files")
    with workspace() as tmp:
        got = []
        for k, p in enumerate(parts):
            p = {"file": p} if isinstance(p, str) else dict(p)
            if not p.get("file"):
                raise ValueError(f"part {k + 1} has no file")
            ctx.progress(f"Bringing {os.path.basename(str(p['file']))[:60]}")
            f = one(p["file"], os.path.join(tmp, f"part{k}"), ctx, p.get("object"))
            got.append({**{x: p[x] for x in ("color", "object", "offset", "rotate", "transform", "hatch") if p.get(x) is not None},
                        "path": f["path"], "name": p.get("name") or os.path.splitext(f["name"])[0]})
        stem = safe_name(name or title or "drawing", "drawing")
        pdf, png = os.path.join(tmp, stem + ".pdf"), os.path.join(tmp, stem + ".png")
        ctx.progress(f"Cutting and laying out {len(views)} view{'s' if len(views) != 1 else ''}")
        res = run_worker({"op": "drawing", "parts": got, "views": views, "title": title, "lines": lines, "header": header,
                          "notes": notes, "size": size, "portrait": portrait, "scale": scale, "date": date,
                          "pdf": pdf, "png": png}, ctx)
        res.pop("ok", None)
        files = [{"name": stem + ".pdf", "path": pdf, "type": "application/pdf"},
                 {"name": stem + ".png", "path": png, "type": "image/png"}]
        for x in files:
            x["file"] = keep(x["path"], tmp)
        image = sbmcp.image(png) if show else None
        if save_to:
            save([(x["path"], x["name"]) for x in files], save_to)
            for x in files:
                x["saved"] = {"path": os.path.join(os.path.abspath(os.path.expanduser(save_to)), x["name"])}
        for x in files:
            x.pop("path", None)
        res["pdf"], res["png"] = files
    lines_out = [f"{res['sheet']} at {res['scale']}: {len(res['views'])} view{'s' if len(res['views']) != 1 else ''}"
                 + (f", saved to {files[0]['saved']['path']}" if files[0].get("saved") else "")
                 + "."]
    for v in res["views"]:
        dims = "; ".join(d["text"] for d in v["dims"]) or "no dimensions"
        lines_out.append(f"VIEW {v['label']} ({v['scale']}, {v['title']}): {dims}")
    lines_out.append("Overlaps: none." if not res["overlaps"] else "Overlaps: " + "; ".join(res["overlaps"]))
    if res["warnings"]:
        lines_out.append("Warnings: " + "; ".join(res["warnings"]))
    blocks = [sbmcp.text("\n".join(lines_out))] + ([image] if image else [])
    return sbmcp.content(*blocks, structured=res)


# Health


def mesh_python_text():
    return ("#!/usr/bin/env python3\n"
            "# mesh-python: Python in mesh's environment, for geometry scripts. The analysis extension writes this file and\n"
            "# writes it again whenever it differs: change meshpython.py there instead.\n"
            "import sys\n"
            f"sys.path.insert(0, {HERE!r})\n"
            "import meshpython\n"
            "sys.exit(meshpython.main())\n")


def keep_mesh_python():
    """Puts mesh-python where this machine's shells find it, again whenever it's gone or differs, and says how it stands."""
    text, out = mesh_python_text(), {"path": MESH_PYTHON, "ok": False}
    try:
        try:
            with open(MESH_PYTHON) as f:
                same = f.read() == text
        except OSError:
            same = False
        if not same:
            os.makedirs(os.path.dirname(MESH_PYTHON), exist_ok=True)
            tmp = f"{MESH_PYTHON}.{os.getpid()}.tmp"
            with open(tmp, "w") as f:
                f.write(text)
            os.chmod(tmp, 0o755)
            os.replace(tmp, MESH_PYTHON)
            out["written_at"] = int(time.time() * 1000)
            log(f"wrote {MESH_PYTHON}")
        elif not os.access(MESH_PYTHON, os.X_OK):
            os.chmod(MESH_PYTHON, 0o755)
        found = shutil.which(os.path.basename(MESH_PYTHON))
        if found and os.path.realpath(found) == os.path.realpath(MESH_PYTHON):
            out["ok"] = True
        else:
            out["problem"] = (f"{found} comes first on PATH" if found else
                              f"{os.path.dirname(MESH_PYTHON)} isn't on PATH, so shells can't find it")
    except OSError as e:
        out["problem"] = f"can't write {MESH_PYTHON}: {e}"
    state["mesh_python"] = out
    return out


STARTING = ("its environment is being built", "its environment isn't checked yet")
"""What health_facts says while mesh starts: no failure."""


def health_facts():
    """How mesh itself is: its worker's environment (package versions, Embree, STEP), uv, the calls it answered and
    failed, its last error, disk."""
    problems = []
    try:
        uv_path = uv()
    except RuntimeError as e:
        uv_path = None
        problems.append(str(e))
    if state["env_error"]:
        problems.append(f"its environment: {state['env_error']}")
    if state["mesh_python"] and not state["mesh_python"]["ok"]:
        problems.append(f"mesh-python: {state['mesh_python'].get('problem')}")
    blender = (state["env"] or {}).get("blender") or {}
    if state["env"] and not blender.get("path"):
        problems.append("Blender isn't here, so .blend, FBX and USD files can't be read (brew install --cask blender)")
    elif not state["env"]:
        problems.append("its environment is being built" if state["warming"] else "its environment isn't checked yet")
    os.makedirs(HOME, exist_ok=True)
    free = shutil.disk_usage(HOME).free
    if free < 2e9:
        problems.append("under 2 GB free on the disk")
    return {"ok": not problems, "problems": problems, "uv": uv_path, "env": state["env"], "deps": list(DEPS),
            "mesh_python": state["mesh_python"],
            "calls": state["calls"], "failed": state["failed"], "last_error": state["last_error"],
            "last_call_at": state["last_call_at"], "started_at": state["started_at"], "disk_free": free,
            "checked_at": int(time.time() * 1000)}


def heartbeat():
    """Says how it is."""
    f = health_facts()
    env = f["env"] or {}
    v = env.get("versions") or {}
    starting = state["warming"] and not state["env_error"] and not [p for p in f["problems"] if "environment" not in p]
    # What's wrong now, for Switch to send its agent to fix: an environment still being built (or not yet
    # checked) is starting, not failing.
    f["failing"] = "; ".join(p for p in f["problems"] if p not in STARTING) or None
    summary = ("Ready" if f["ok"] else "Starting: building its environment" if starting else
               "Can't measure: " + "; ".join(f["problems"])) + (
        f": trimesh {v.get('trimesh')}, Embree {'on' if env.get('embree') else 'off'}, "
        f"Blender {(env.get('blender') or {}).get('version') or 'missing'}, {f['calls']} calls, "
        f"{f['failed']} failed, {f['disk_free'] / 1e9:.0f} GB free" if env else "")
    app.emit("heartbeat", {"entity": f"sense:{NAME}", "summary": summary, "facts": f}, about=[f"sense:{NAME}"])


def sweep():
    """Leaves nothing behind from calls a restart cut off, and what calls made once it's KEEP_DAYS old."""
    for root, age in ((TMP, 6 * 3600), (OUT, KEEP_DAYS * 86400)):
        for d in os.listdir(root) if os.path.isdir(root) else []:
            p = os.path.join(root, d)
            with contextlib.suppress(OSError):
                if time.time() - os.path.getmtime(p) > age:
                    shutil.rmtree(p, ignore_errors=True)


@app.background
def beat():
    warming = not os.environ.get("MESH_NO_WARM")
    state["warming"] = warming
    keep_mesh_python()
    heartbeat()
    if warming:
        warm()
        heartbeat()
    while True:
        time.sleep(HEARTBEAT_S)
        try:
            sweep()
            keep_mesh_python()
            heartbeat()
        except Exception as e:
            log(f"heartbeat failed: {e}")
