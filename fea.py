"""fea: how stiff a 3D-printed part is, or each of its redesign variants, by finite elements on a temporary
Hetzner box it rents, so the solver never runs here.

fea_run checks the parts and the load here (fea_spec.py) before renting anything, rents a box (labelled temp with a
delete-by), puts the engine (fea_box/engine.py: gmsh meshing sized to the walls, a CalculiX deck with the supports, the
load and any rigid contact, the solve, the numbers and pictures) and its scheduler (fea_box/runner.py) on it, watches
status.json over ssh, fetches the results here, records the study and each part (fea_study and fea_case events), and
deletes the box once it's idle, checking Hetzner holds nothing of it. Renting is the user's money: fea_run and fea_add
speak for the user; the loop only watches, fetches and deletes.
"""

import contextlib
import json
import math
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import threading
import time

import fea_spec as spec
import hetzner
from server import app, config, part_home

NAME = "fea"
HERE = os.path.dirname(os.path.abspath(__file__))
HOME = part_home(NAME)
POLL_S = float(os.environ.get("FEA_POLL_S") or 20)
HEARTBEAT_S = float(os.environ.get("FEA_HEARTBEAT_S") or 600)
REAP_S = float(os.environ.get("FEA_REAP_S") or 600)
BOX = (os.environ.get("FEA_BOX_ROOT") or "/root").rstrip("/")
"""The box's root: /root on a real box; a temp folder in tests, where its commands run here."""
BOX_FEA = f"{BOX}/fea"
BOX_PYTHON = os.environ.get("FEA_BOX_PYTHON") or f"{BOX}/venv/bin/python"
RESULTS_ROOT = os.path.join(os.path.expanduser(config()["results"]), "fea") if config().get("results") else os.path.join(HOME, "results")
"""Where each study's results land: config.json's results/fea, else results/ in its home."""
MAX_HOURS = 24
DELETE_WAIT_S = float(os.environ.get("FEA_DELETE_WAIT_S") or 5)
# A running part whose stage, fraction and increments haven't moved this long has stalled: the study stops, keeps what
# it has and deletes its box (the engine stops a solve that's out of memory or too slow itself; this catches the rest)
STALL_MS = int(float(os.environ.get("FEA_STALL_S") or 1800) * 1000)
MAX_USD_DEFAULT = 3.0
LEFTOVER_GRACE_MS = int(float(os.environ.get("FEA_LEFTOVER_GRACE_S") or 600) * 1000)
CARD_DAYS = 3
ACTIVE = ("booting", "installing", "running", "done", "deleting")
METRIC_WORDS = {"force_N": "force", "push_N": "push-in", "insert_peak_N": "insertion peak"}

app.event("fea_study", "An FEA study: its box, its parts' counts and numbers, its chart", stream="fea/studies", kind="observation")
app.event("fea_case", "One part of a study: its stage and its numbers", stream="fea/cases", kind="observation", retention="signal")

state = {"started_at": int(time.time() * 1000), "calls": 0, "failed": 0, "last_error": None, "polls": 0,
         "boxes_rented": 0, "boxes_deleted": 0, "last_reap": None}
locks = {}
locks_guard = threading.Lock()


def now_ms():
    return int(time.time() * 1000)


def pt(ms):
    return time.strftime("%a %b %-d %-I:%M %p", time.localtime(ms / 1000)) if ms else None


def lock(sid):
    with locks_guard:
        return locks.setdefault(sid, threading.RLock())


# ---------------------------------------------------------------------------------------------
# What it keeps: one folder per study in its home


def study_dir(sid):
    return os.path.join(HOME, "studies", sid)


def load(sid):
    try:
        with open(os.path.join(study_dir(sid), "study.json")) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def save(st):
    d = study_dir(st["id"])
    os.makedirs(d, exist_ok=True)
    tmp = os.path.join(d, "study.json.tmp")
    with open(tmp, "w") as f:
        json.dump(st, f, indent=1)
    os.replace(tmp, os.path.join(d, "study.json"))


def all_studies():
    root = os.path.join(HOME, "studies")
    out = []
    for sid in sorted(os.listdir(root)) if os.path.isdir(root) else []:
        st = load(sid)
        if st:
            out.append(st)
    return sorted(out, key=lambda s: s.get("created") or 0)


@contextlib.contextmanager
def holding(sid):
    """A study as it is now, held: nothing else changes it meanwhile, and what the block changes is kept once it
    ends well (an error keeps nothing)."""
    with lock(sid):
        st = load(sid)
        if st is None:
            raise ValueError(f"no study {sid}")
        yield st
        save(st)


def find(study):
    """A study by its id or the words of its name (the newest that matches)."""
    if not study:
        raise ValueError("study: give its id (run answered it) or its name")
    st = load(study)
    if st:
        return st
    words = study.lower()
    hits = [s for s in all_studies() if words in s["id"] or words in (s.get("name") or "").lower()]
    if not hits:
        raise ValueError(f"no study {study!r}: status lists them")
    return hits[-1]


def keypair():
    """This appendage's own ssh key, which every box it rents lets in as root."""
    key = os.path.join(HOME, "id_ed25519")
    if not os.path.exists(key):
        os.makedirs(HOME, mode=0o700, exist_ok=True)
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", f"analysis-{NAME}",
                        "-f", key], check=True)
    with open(key + ".pub") as f:
        return key, f.read().strip()


# ---------------------------------------------------------------------------------------------
# The world outside, each in one place: Hetzner's API and the box (over ssh)


def api(method, path, body=None):
    return hetzner.api(method, path, body)


def hetzner_key():
    """This appendage's key as a Hetzner SSH key (its id), made once: a box made with it gets no root password,
    so Hetzner neither emails one nor makes root change it at first login (which refuses key logins too)."""
    _, pub = keypair()
    body = " ".join(pub.split()[:2])
    for k in api("GET", "/ssh_keys?per_page=50").get("ssh_keys") or []:
        if " ".join((k.get("public_key") or "").split()[:2]) == body:
            return k["id"]
    name = f"analysis-{NAME}-{os.uname().nodename.split('.')[0].lower()}"
    return api("POST", "/ssh_keys", {"name": name, "public_key": pub, "labels": {"made_by": NAME}})["ssh_key"]["id"]


def ssh_argv(st):
    key, _ = keypair()
    return ["ssh", "-i", key, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=accept-new", "-o", f"UserKnownHostsFile={os.path.join(study_dir(st['id']), 'known_hosts')}",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4", "-o", "LogLevel=ERROR", f"root@{st['box']['ip']}"]


def remote(st, command, stdin=None, timeout=60):
    """Runs a command on the study's box: (exit code, stdout, stderr). stdin may be bytes or a file."""
    argv = ssh_argv(st) + ["bash", "-lc", shlex.quote(command)]
    try:
        done = subprocess.run(argv, input=stdin if isinstance(stdin, bytes) else None,
                              stdin=stdin if hasattr(stdin, "read") else (None if isinstance(stdin, bytes) else subprocess.DEVNULL),
                              capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, b"", f"timed out after {timeout} s".encode()
    return done.returncode, done.stdout, done.stderr


def remote_to_file(st, command, stdin, out_file, timeout):
    argv = ssh_argv(st) + ["bash", "-lc", shlex.quote(command)]
    try:
        done = subprocess.run(argv, input=stdin, stdout=out_file, stderr=subprocess.PIPE, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, None, b"timed out"
    return done.returncode, None, done.stderr


def text(b):
    return b.decode("utf-8", "replace") if isinstance(b, bytes) else (b or "")


def must(st, command, stdin=None, timeout=120, what="the box"):
    code, out, err = remote(st, command, stdin=stdin, timeout=timeout)
    if code != 0:
        raise RuntimeError(f"{what} said (exit {code}): {(text(err) or text(out)).strip()[-800:]}")
    return text(out)


# ---------------------------------------------------------------------------------------------
# The parts, brought here and checked


def bring(src, dest_dir, name):
    """A part's file in dest_dir, from a path on this machine. Answers its path here."""
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.abspath(os.path.expanduser(str(src)))
    if not os.path.isfile(path):
        raise spec.SpecError(f"part {name}: {src} isn't a file here")
    ext = os.path.splitext(path)[1].lower()
    if ext not in spec.PART_EXT:
        raise spec.SpecError(f"part {name}: {os.path.basename(path)} isn't an STL or STEP file (convert makes one)")
    dest = os.path.join(dest_dir, name + (".step" if ext == ".stp" else ext))
    shutil.copyfile(path, dest)
    return dest


def plan(parts, load_, material, mesh, folder, cores=None, mem_gb=None, rnd=1, have=()):
    """Every part checked and made a case: ({name: case}, {name: what the check found}, warnings). Nothing's rented."""
    base_load = spec.load(load_) if load_ is not None else None
    cases, found, warnings = {}, {}, []
    entries = spec.parts_list(parts)
    fine = bool((mesh or {}).get("check"))
    for name, src, over in entries:
        if name in have:
            warnings.append(f"{name} is in the study already: it's replaced and run again")
        part = bring(src, os.path.join(folder, "parts"), name)
        mat = spec.material(over.get("material", material))
        if base_load is None and not over.get("load"):
            raise spec.SpecError("load: say how the parts are loaded")
        ld = spec.load(spec.merge(base_load or {}, over.get("load") or {}))
        ms = spec.mesh(spec.merge(mesh or {}, over.get("mesh") or {}))
        if part.endswith(".stl"):
            stats = spec.stl_stats(part)
            if stats["bodies"] > 1 and ms.get("body") is None:
                raise spec.SpecError(f"{name}: {os.path.basename(src)} holds {stats['bodies']} separate bodies: give one "
                                     "part per file, or mesh.body to pick one (0 is the largest)")
            if not stats["watertight"]:
                warnings.append(f"{name}: the STL has {stats['open_edges']} open edges; the box tries to close them")
            stats = spec.pick_body(stats, ms.get("body"))
            checked, w = spec.check_against(ld, stats, name)
            warnings += w
            found[name] = dict({k: v for k, v in stats.items() if k not in ("vertices", "faces") and not k.startswith("_")},
                               **checked)
        else:
            found[name] = {"file": os.path.basename(part), "checked": "a STEP file: its selectors are checked on the box"}
        contact_or_big = ld["type"] != "press" or mat.get("model") == "neo-hooke" or ld.get("large") or ld.get("nonlinear")
        ms.setdefault("budget", 40_000 if contact_or_big else 150_000)
        case = {"name": name, "part": os.path.basename(part), "material": mat, "mesh": {k: v for k, v in ms.items() if k != "check"},
                "load": ld, "round": rnd, "heavy": bool(contact_or_big)}
        cases[name] = case
        if fine:
            twin = json.loads(json.dumps(case))
            twin["name"] = f"{name}-fine"
            twin["mesh"]["per_wall"] = float(ms.get("per_wall") or 2.0) * 1.4
            twin["mesh"]["budget"] = min(600_000, int(ms["budget"] * 2.7))
            if ms.get("size"):
                twin["mesh"]["size"] = float(ms["size"]) / 1.4
            twin["mesh_of"] = name
            cases[twin["name"]] = twin
    if cores:
        share(cases, cores, mem_gb)
    return cases, found, warnings


def share(cases, cores, mem_gb=None):
    """Each case's threads and memory on the box. A contact, nonlinear or hyperelastic case (heavy) gets the whole box,
    so heavy cases run one at a time: their solver's memory grows with every increment, and three sharing a cpx51
    each ran out of its third. Linear presses are small and split the box between them; the box's runner starts a
    case only when its threads and memory are free."""
    whole = int(mem_gb * 1024 * 0.9) if mem_gb else None
    light = [c for c in cases.values() if not c.get("heavy")]
    lthreads = max(2, min(8, cores // max(1, min(len(light), cores // 2))))
    for c in cases.values():
        threads = min(8, cores) if c.get("heavy") else lthreads
        c["threads"] = threads
        if whole:
            c["mem_mb"] = whole if c.get("heavy") else int(whole * min(1.0, threads / cores))


def write_cases(folder, cases):
    """Each case's folder as the box gets it: case.json and its part, under <folder>/box/cases."""
    root = os.path.join(folder, "box", "cases")
    for name, case in cases.items():
        d = os.path.join(root, name)
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
        shutil.copyfile(os.path.join(folder, "parts", case["part"]), os.path.join(d, case["part"]))
        with open(os.path.join(d, "case.json"), "w") as f:
            json.dump(case, f, indent=1)
    return root


def pack(folder, names=None):
    argv = ["tar", "-czf", "-", "--no-xattrs"]
    if os.uname().sysname == "Darwin":
        argv.append("--no-mac-metadata")
    argv += ["-C", folder] + (list(names) if names else ["."])
    done = subprocess.run(argv, capture_output=True, env=dict(os.environ, COPYFILE_DISABLE="1"))
    if done.returncode != 0:
        raise RuntimeError(f"couldn't pack {folder}: {text(done.stderr).strip()}")
    return done.stdout


# ---------------------------------------------------------------------------------------------
# The box's runner


def box_study_json(st):
    return {"id": st["id"], "round": st["round"], "slots": st["slots"], "mem_mb": st.get("mem_mb"), "python": BOX_PYTHON,
            "engine": os.environ.get("FEA_BOX_ENGINE") or f"{BOX_FEA}/bin/engine.py", "reference": st.get("reference"), "target": st.get("target"),
            "cases": {n: {k: c.get(k) for k in ("threads", "mem_mb", "heavy", "round", "timeout_s")} for n, c in st["cases"].items()}}


def push_study_json(st):
    data = json.dumps(box_study_json(st), indent=1).encode()
    must(st, f"mkdir -p {BOX_FEA} && cat > {BOX_FEA}/study.json.tmp && mv {BOX_FEA}/study.json.tmp {BOX_FEA}/study.json",
         stdin=data, what="writing the study on the box")


def upload_cases(st, names=None):
    root = os.path.join(study_dir(st["id"]), "box", "cases")
    names = [n for n in (names or st["cases"])]
    for n in names:
        must(st, f"rm -rf {BOX_FEA}/cases/{shlex.quote(n)}", what=f"clearing {n} on the box")
    must(st, f"mkdir -p {BOX_FEA}/cases && tar -xzf - -C {BOX_FEA}/cases", stdin=pack(root, names), timeout=300,
         what="putting the parts on the box")


UNIT = """[Unit]
Description=fea case runner
After=network.target

[Service]
ExecStart=/usr/bin/python3 {fea}/bin/runner.py serve --home {fea}
Restart=always
RestartSec=5
KillMode=process

[Install]
WantedBy=multi-user.target
"""


def start_runner(st):
    """Starts the runner under systemd, so it outlives any ssh session (and the cases outlive a restart of it)."""
    must(st, "cat > /etc/systemd/system/fea-runner.service && systemctl daemon-reload && systemctl enable --now fea-runner",
         stdin=UNIT.format(fea=BOX_FEA).encode(), what="starting the runner")


def install_runner(st):
    must(st, f"mkdir -p {BOX_FEA}/bin && tar -xzf - -C {BOX_FEA}/bin", stdin=pack(os.path.join(HERE, "fea_box")),
         what="putting the engine on the box")
    upload_cases(st)
    push_study_json(st)
    start_runner(st)


def read_status(st):
    code, out, err = remote(st, f"cat {BOX_FEA}/status.json", timeout=30)
    if code != 0:
        raise RuntimeError(text(err).strip() or f"exit {code}")
    return json.loads(text(out))


def drop_request(st, case, req):
    must(st, f"mkdir -p {BOX_FEA}/jobs/{shlex.quote(case)} && cat > {BOX_FEA}/jobs/{shlex.quote(case)}/request.json",
         stdin=json.dumps(req).encode(), what=f"asking the runner about {case}")


def fetch_results(st, frd=False, cases=None):
    """Brings the results to the study's results folder here and notes where the pictures are: (files, bytes)."""
    args = ["--home", BOX_FEA] + (["--frd"] if frd else [])
    for c in cases or []:
        args += ["--case", c]
    listing = json.loads(must(st, f"python3 {BOX_FEA}/bin/runner.py files " + " ".join(shlex.quote(a) for a in args),
                              what="listing the results"))
    files, size = listing["files"], listing["bytes"]
    dest = st["results"]
    os.makedirs(dest, exist_ok=True)
    if size > shutil.disk_usage(dest).free - 5e9:
        raise RuntimeError(f"the results are {size / 1e9:.1f} GB and there is too little room here")
    if files:
        with tempfile.TemporaryFile() as tar:
            code, _, err = remote_to_file(st, f"cd {BOX_FEA} && tar -czf - -T -", ("\n".join(files) + "\n").encode(), tar,
                                          timeout=1800)
            if code != 0:
                raise RuntimeError(f"couldn't pack the results on the box: {text(err).strip()[-500:]}")
            tar.seek(0)
            done = subprocess.run(["tar", "-xzf", "-", "-C", dest], stdin=tar, capture_output=True)
            if done.returncode != 0:
                raise RuntimeError(f"couldn't unpack the results here: {text(done.stderr).strip()}")
    pictures = st.setdefault("pictures", {})
    for rel in files:
        if rel.endswith(".png"):
            pictures[rel] = os.path.join(dest, rel)
    st["fetched_at"] = now_ms()
    st["fetched_round"] = st["round"]
    st["fetched"] = {"files": len(files), "bytes": size}
    return len(files), size


# ---------------------------------------------------------------------------------------------
# What it records


def overall(status):
    cases = (status or {}).get("cases") or []
    if not cases:
        return 0.0
    total = 0.0
    weights = {"meshing": (0.02, 0.1), "writing the deck": (0.12, 0.03), "solving": (0.15, 0.75),
               "reading the results": (0.9, 0.04), "drawing": (0.94, 0.05)}
    for c in cases:
        s = c.get("state")
        if s in ("done", "failed", "cancelled"):
            total += 1
        elif s == "running":
            start, span = weights.get(c.get("stage") or "", (0.01, 0.0))
            total += start + span * float(c.get("fraction") or 0)
    return round(total / len(cases), 3)


def counts_line(counts):
    order = ["running", "queued", "done", "failed", "cancelled"]
    return ", ".join(f"{counts[k]} {k}" for k in order if counts.get(k))


def first_line(text_, n):
    lines = (text_ or "").strip().splitlines()
    return lines[0][:n] if lines else None


def value_of(row):
    h = row.get("headline") or {}
    metric = h.get("metric")
    return metric, (h.get(metric) if metric else None)


def compare_rows(st):
    comp = (st.get("status") or {}).get("compare") or {}
    return {r["case"]: r for r in comp.get("rows") or []}


def case_facts(st, i, c):
    name = c["name"]
    facts = {"study": st["id"], "case": name, "order": i, "state": c.get("state")}
    if c.get("state") == "running":
        stage = c.get("stage") or "starting"
        frac = c.get("fraction")
        facts["stage"] = f"{stage} {round(frac * 100)}%" if stage == "solving" and frac is not None else stage
    h = c.get("headline") or {}
    metric, value = value_of(c)
    if value is not None:
        facts.update(metric=metric, value=round(value, 3), unit="N")
    for k in ("stiffness_N_per_mm", "force_N", "push_N", "radial_N", "insert_peak_N", "pull_peak_N", "seated_slide_N"):
        if h.get(k) is not None:
            facts[k] = round(h[k], 4)
    if c.get("strain_pct") is not None:
        facts["strain_pct"] = round(c["strain_pct"], 2)
    if c.get("of_strength") is not None:
        facts["of_strength"] = c["of_strength"]
    row = compare_rows(st).get(name) or {}
    for k in ("of_target", "verdict", "mesh_change"):
        if row.get(k) is not None:
            facts[k] = row[k]
    if c.get("complete") is False:
        facts["reached"] = c.get("reached")
    if c.get("warnings"):
        facts["warnings"] = [first_line(w, 200) for w in c["warnings"]][:5]
    if c.get("error"):
        facts["error"] = first_line(c["error"], 300)
    if c.get("elements"):
        facts["elements"] = c["elements"]
    if c.get("mass_g") is not None:
        facts["mass_g"] = c["mass_g"]
    pics = {k: ref for k in ("setup", "strain", "curve")
            for ref in [(st.get("pictures") or {}).get(f"cases/{name}/{k}.png")] if ref}
    if pics:
        facts["pictures"] = pics
    if st.get("fetched_at") and c.get("state") in ("done", "failed"):
        facts["result"] = os.path.join(st["results"], "cases", name, "result.json")
    return {k: v for k, v in facts.items() if v is not None}


def study_facts(st):
    status = st.get("status") or {}
    cases = status.get("cases") or []
    comp = status.get("compare") or {}
    facts = {
        "study": st["id"], "name": st.get("name"), "state": st["state"], "round": st["round"],
        "done": st["state"] in ("done", "deleting", "deleted", "failed") and st.get("done_round") == st["round"],
        "box": {k: st["box"].get(k) for k in ("id", "name", "ip", "type", "location")} if st.get("box") else None,
        "box_state": st.get("box_state"), "delete_by": st.get("delete_by"), "created": st.get("created"),
        "counts": status.get("counts"), "progress": overall(status), "load": (st.get("load") or {}).get("type"),
        "material": (st.get("material") or {}).get("name") if isinstance(st.get("material"), dict) else st.get("material"),
        "reference": st.get("reference"), "target": comp.get("target"),
        "parts": [{k: v for k, v in case_facts(st, i, c).items() if k in ("case", "state", "stage", "value", "of_target",
                                                                         "verdict", "strain_pct", "error")}
                  for i, c in enumerate(cases)],
        "chart": (st.get("pictures") or {}).get("compare.png"), "results": st.get("results"), "fetched_at": st.get("fetched_at"),
        "error": st.get("error"), "cost": st.get("cost"), "currency": st.get("currency"), "hours": st.get("hours"),
        "verified_clean": st.get("verified_clean"),
    }
    return {k: v for k, v in facts.items() if v is not None}


def summary(st):
    status = st.get("status") or {}
    s = st["state"]
    name = st.get("name") or st["id"]
    if s == "booting":
        return f"{name}: renting the box"
    if s == "installing":
        return f"{name}: installing CalculiX on the box"
    if s == "running":
        return f"{name}: {counts_line(status.get('counts') or {}) or 'starting'}"
    if s in ("done", "deleting", "deleted") and status.get("cases"):
        bits = []
        for c in status["cases"][:5]:
            metric, v = value_of(c)
            if v is not None:
                bits.append(f"{c['name']} {v:.3g} N {METRIC_WORDS.get(metric, '')}".rstrip())
            elif c.get("state") != "done":
                bits.append(f"{c['name']} {c.get('state')}")
        line = "; ".join(bits)
        if s == "deleted":
            return f"{name}: {line}" + (f" (box deleted, cost {st['cost']} {st.get('currency') or ''})".rstrip()
                                          if st.get("cost") is not None else "")
        return f"{name} is done: {line}"
    if s == "deleting":
        return f"{name}: deleting the box"
    if s == "failed":
        return f"{name} failed: {st.get('error')}"
    return f"{name}: {s}"


def emit_study(st, force=False):
    facts = study_facts(st)
    key = json.dumps({k: facts.get(k) for k in ("state", "round", "done", "counts", "box_state", "error", "verified_clean",
                                                 "fetched_at", "parts", "chart")}, sort_keys=True, default=str)
    last = st.get("_emitted") or {}
    stale = now_ms() - (last.get("at") or 0) > 600_000 and st["state"] in ("running", "installing", "booting")
    if force or key != last.get("key") or stale:
        about = [f"fea-study:{st['id']}"] + (st.get("about") or [])
        app.emit("fea_study", {"entity": f"fea-study:{st['id']}", "summary": summary(st), "facts": facts}, about=about[1:] or None)
        st["_emitted"] = {"key": key, "at": now_ms()}


def case_line(f):
    name = f["case"]
    if f.get("state") == "running":
        return f"{name}: {f.get('stage') or 'running'}"
    if f.get("value") is not None:
        line = f"{name}: {f['value']:.3g} N {METRIC_WORDS.get(f.get('metric'), '')}".rstrip()
        if f.get("verdict"):
            line += f", {f['verdict']}"
        return line
    if f.get("error"):
        return f"{name} failed: {f['error']}"
    return f"{name}: {f.get('state')}"


def emit_cases(st):
    sent = st.setdefault("_cases_emitted", {})
    for i, c in enumerate((st.get("status") or {}).get("cases") or []):
        facts = case_facts(st, i, c)
        key = json.dumps({k: v for k, v in facts.items()}, sort_keys=True, default=str)
        if key != sent.get(c["name"]):
            app.emit("fea_case", {"entity": f"fea-case:{st['id']}-{c['name']}", "summary": case_line(facts), "facts": facts},
                     about=[f"fea-study:{st['id']}"])
            sent[c["name"]] = key


# ---------------------------------------------------------------------------------------------
# Moving a study along (the background loop, and tools that need it to be current)


def advance(st):
    """One step of a study's life, by what the box says: booting, installing, running, done, deleting, deleted."""
    s = st["state"]
    t = now_ms()
    if s == "deleted":
        return
    if st.get("delete_by") and t >= st["delete_by"] and s != "deleting":
        if s in ("running", "done") and st.get("fetched_round") != st["round"]:
            try:
                fetch_results(st)
            except Exception as e:
                st["error"] = f"couldn't fetch before its delete-by: {e}"
        st["box_state"] = "past its delete-by"
        st["done_round"] = st["round"]
        st["state"] = s = "deleting"
    if s == "booting":
        code, _, _ = remote(st, "true", timeout=20)
        if code == 0:
            st["state"], st["box_state"] = "installing", "installing CalculiX"
        elif t - st["created"] > 15 * 60_000:
            return fail(st, "the box never answered ssh in 15 minutes")
    elif s == "installing":
        code, out, _ = remote(st, f"cat {BOX}/fea-failed 2>/dev/null; test -f {BOX}/fea-ready && echo READY; "
                                  "cloud-init status 2>/dev/null; true", timeout=30)
        out = text(out)
        first = out.strip().splitlines()[0] if out.strip() else ""
        ended = "READY" not in out and re.search(r"^status: (done|error)", out, re.M)
        if first.endswith(" failed") or ended:
            _, logs, _ = remote(st, f"tail -n 15 {BOX}/apt.log {BOX}/pip.log {BOX}/fea-setup.log 2>/dev/null", timeout=30)
            what = first if first.endswith(" failed") else "the setup script stopped before it finished"
            return fail(st, f"setting the box up: {what}; {text(logs).strip()[-600:]}")
        if "READY" in out:
            install_runner(st)
            st["state"], st["box_state"] = "running", "running the parts"
            st["running_since"] = now_ms()
        elif t - st["created"] > 30 * 60_000:
            return fail(st, "setting the box up took over 30 minutes")
    elif s in ("running", "done"):
        try:
            status = read_status(st)
            st["status"], st["unreachable_since"] = status, None
            st.pop("error", None)
        except Exception as e:
            st["unreachable_since"] = st.get("unreachable_since") or t
            if t - st["unreachable_since"] > 20 * 60_000:
                st["error"] = f"the box hasn't answered for {round((t - st['unreachable_since']) / 60000)} min: {e}"
            return
        stalled = stalls(st, status, t) if s == "running" else []
        if stalled:
            try:
                fetch_results(st)
            except Exception:
                pass
            return fail(st, f"{', '.join(stalled)} made no progress in {STALL_MS // 60000} min: the study stopped "
                            "and its box was deleted")
        if s == "running" and status.get("state") == "done" and status.get("round") == st["round"] and status.get("compare"):
            try:
                fetch_results(st)
            except Exception as e:
                st["error"] = f"couldn't fetch the results: {e}"
                return
            st["state"], st["done_round"], st["done_at"] = "done", st["round"], now_ms()
            st["box_state"] = f"kept {st['keep_minutes']} min for follow-ups"
            emit_cases(st)
        else:
            emit_cases(st)
            if s == "done" and t - (st.get("done_at") or t) > st["keep_minutes"] * 60_000:
                st["state"], st["box_state"] = "deleting", "deleting"
    if st["state"] == "deleting":
        delete_box(st)


def stalls(st, status, t):
    """The running parts whose progress hasn't moved in STALL_MS."""
    seen = st.setdefault("progress_seen", {})
    out = []
    for row in status.get("cases") or []:
        if row.get("state") != "running":
            seen.pop(row["name"], None)
            continue
        key = [row.get("stage"), row.get("fraction"), row.get("increments"), row.get("round")]
        prev = seen.get(row["name"])
        if not prev or prev["key"] != key:
            seen[row["name"]] = {"key": key, "since": t}
        elif t - prev["since"] > STALL_MS:
            out.append(row["name"])
    return out


def fail(st, why):
    st["error"] = why
    st["done_round"] = st["round"]
    st["state"] = "deleting"
    st["failed"] = True
    delete_box(st)


def delete_box(st):
    """Deletes the box and checks Hetzner holds nothing of it any more: the server, its IPs, volumes, snapshots."""
    box = st.get("box") or {}
    sid = box.get("id")
    if sid:
        try:
            api("DELETE", f"/servers/{sid}")
        except hetzner.HetznerError as e:
            if e.status != 404:
                st["error"] = f"couldn't delete the box yet: {e}"
                return
    left = leftovers(sid)
    if left is None:
        return
    st["server_gone_at"] = st.get("server_gone_at") or now_ms()
    if left and now_ms() - st["server_gone_at"] < LEFTOVER_GRACE_MS:
        st["box_state"] = "deleted; waiting for Hetzner to remove its IPs"
        return  # Hetzner removes a server's own IPs a little after the server: look again next pass
    st["deleted_at"] = st.get("deleted_at") or st["server_gone_at"]
    st["verified_clean"] = not left
    st["left"] = left
    st["cost"], st["hours"] = hetzner.cost(st["created"], st["deleted_at"], st.get("price_hourly"))
    st["state"] = "failed" if st.get("failed") else "deleted"
    st["box_state"] = "deleted, nothing left" if not left else f"deleted, but left: {', '.join(left)}"
    state["boxes_deleted"] += 1


def leftovers(server_id):
    """What Hetzner still holds of a server: [] when nothing; None while it's still going."""
    try:
        api("GET", f"/servers/{server_id}")
        return None
    except hetzner.HetznerError as e:
        if e.status != 404:
            return None
    left = []
    for ip in api("GET", "/primary_ips").get("primary_ips") or []:
        if ip.get("assignee_id") == server_id:
            left.append(f"primary IP {ip.get('ip')}")
    for v in api("GET", "/volumes").get("volumes") or []:
        if v.get("server") == server_id:
            left.append(f"volume {v.get('name')}")
    for im in api("GET", "/images?type=snapshot&type=backup").get("images") or []:
        if (im.get("created_from") or {}).get("id") == server_id:
            left.append(f"image {im.get('description') or im.get('id')}")
    return left


def step(sid):
    with lock(sid):
        st = load(sid)
        if st is None or st["state"] not in ACTIVE:
            return
        before = st["state"]
        try:
            advance(st)
        except Exception as e:
            st["error"] = f"{type(e).__name__}: {e}"
            state["last_error"] = f"{st['id']}: {st['error']}"
        emit_study(st, force=st["state"] != before)
        save(st)


def reap():
    """Deletes any box this appendage rented that's past its delete-by or that no study of its own holds."""
    try:
        servers = api("GET", f"/servers?label_selector=made_by%3D{NAME}").get("servers") or []
    except hetzner.HetznerError as e:
        state["last_error"] = f"reaping: {e}"
        return []
    mine = {(s.get("box") or {}).get("id"): s for s in all_studies() if s["state"] in ACTIVE}
    gone = []
    for srv in servers:
        due = hetzner.parse_label_time((srv.get("labels") or {}).get("delete_by", ""))
        overdue = bool(due and now_ms() > due)
        held = mine.get(srv["id"])
        if held:
            if overdue:
                step(held["id"])
            continue
        if not overdue and srv.get("labels", {}).get("study") and young(srv):
            continue  # one run is renting right now, before its study is kept
        try:
            api("DELETE", f"/servers/{srv['id']}")
            gone.append(srv["name"])
        except hetzner.HetznerError as e:
            state["last_error"] = f"reaping {srv['name']}: {e}"
    state["last_reap"] = {"at": now_ms(), "deleted": gone}
    return gone


def young(srv, minutes=10):
    import datetime
    try:
        made = datetime.datetime.fromisoformat(srv["created"].replace("Z", "+00:00")).timestamp() * 1000
    except (KeyError, ValueError, AttributeError):
        return False
    return now_ms() - made < minutes * 60_000


def slug(text_):
    s = re.sub(r"[^a-z0-9]+", "-", (text_ or "").lower()).strip("-")
    return s[:28].strip("-") or "study"


def server_type_info(server_type):
    try:
        types = api("GET", f"/server_types?name={server_type}").get("server_types") or []
    except hetzner.HetznerError:
        return None, None
    return (types[0].get("cores"), types[0].get("memory")) if types else (None, None)


# ---------------------------------------------------------------------------------------------
# Tools

PARTS_SCHEMA = {"description": "The parts: STL or STEP files (paths on this machine), "
                               "as a list, or {name: file | {file, material?, load?, mesh?}} to name them and change "
                               "anything for one."}
LOAD_SCHEMA = {"type": "object", "description": "How every part is loaded (a part may change it): {type: press, fix, move, "
                                                "direction, travel, rigid?, steps?, at?} or {type: bore | pin, diameter, "
                                                "axis, span?, center?, fix?, friction?, slide?, from_end?, entry_radius?, "
                                                "pull?, depth?, at?}. Selectors (fix, move): side/depth, box, near/radius, "
                                                "cylinder, all, any, not; surface: false takes interior nodes too."}
MATERIAL_SCHEMA = {"description": "pla, petg, asa or tpu (materials lists each with its datasheet), or {E, nu, model: "
                                  "linear | neo-hooke, density?, strength?, elongation?}, or {base: petg, E: ...}."}
MESH_SCHEMA = {"type": "object", "description": "{per_wall: elements across a wall (default 2), size: one size for all, "
                                                "min, max, budget: most elements (default 150k for a linear press, 40k "
                                                "otherwise), body: which body of a file holding several, check: true adds "
                                                "a finer twin of each part}."}


@app.tool(name="fea_check", schema={
    "type": "object",
    "properties": {"parts": PARTS_SCHEMA, "load": LOAD_SCHEMA, "material": MATERIAL_SCHEMA, "mesh": MESH_SCHEMA},
    "required": ["parts", "load"],
}, read_only=True)
def check(parts, load=None, material="petg", mesh=None):
    """Checks parts and a load as run would, renting nothing: each part's size, volume, bodies and whether it's closed, what each selector takes of its vertices, the fit's interference; errors for what would fail."""
    state["calls"] += 1
    folder = tempfile.mkdtemp(prefix="fea-check-", dir=HOME)
    try:
        cases, found, warnings = plan(parts, load, material, mesh, folder)
        return {"ok": True, "parts": found, "cases": {n: {"material": c["material"]["name"], "load": c["load"]["type"],
                                                         "budget": c["mesh"].get("budget")} for n, c in cases.items()},
                "warnings": warnings}
    finally:
        shutil.rmtree(folder, ignore_errors=True)


@app.tool(name="fea_run", speaks_for_person=True, schema={
    "type": "object",
    "properties": {
        "parts": PARTS_SCHEMA, "load": LOAD_SCHEMA, "material": MATERIAL_SCHEMA, "mesh": MESH_SCHEMA,
        "reference": {"type": "string", "description": "The part the others are judged against (its name): each gets its ratio to it and a verdict."},
        "target": {"type": "object", "description": "Or the value to hit: {value: N, tol: 0.1}, in the load's headline (press: force at full travel; bore/pin: push-in; slide: insertion peak)."},
        "name": {"type": "string", "description": "What the study's called, in a few words (glide ribs)."},
        "about": {"type": "array", "items": {"type": "string"}, "description": "Entities it's for, on its events."},
        "server_type": {"type": "string", "description": "Hetzner server type (default cpx51: 16 shared vCPU, 32 GB)."},
        "location": {"type": "string", "description": "Hetzner location (default hil, Hillsboro)."},
        "hours": {"type": "number", "description": "Its delete-by, in hours from now (default 4, at most 24): the box goes then whatever it's doing, after fetching what it can."},
        "max_usd": {"type": "number", "description": "The most the box may cost (default 3): its delete-by comes early enough that it can't bill more."},
        "keep_minutes": {"type": "integer", "description": "How long the box stays up once every part is done, for add or shell (default 20; 0 deletes it at once)."},
        "timeout_minutes": {"type": "number", "description": "How long one part's solve may take (default 90)."},
        "results": {"type": "string", "description": "Where results land here (default: the extension's home, results/<study id>)."},
    },
    "required": ["parts", "load"],
})
def run(parts, ctx, load=None, material="petg", mesh=None, reference=None, target=None, name=None, about=None,
        server_type="cpx51", location="hil", hours=4, keep_minutes=20,
        timeout_minutes=90, results=None, max_usd=MAX_USD_DEFAULT):
    "Rents a Hetzner box (the user's money, held under max_usd) and works out each part's stiffness under the load. Answers at once with the study's id; fea_status follows it, and a fea_study event says when it's done."
    state["calls"] += 1
    try:
        if not hetzner.token():
            raise ValueError("no Hetzner key: connect Hetzner Cloud in Switch's connections")
        if not (0 < float(hours) <= MAX_HOURS):
            raise ValueError(f"hours: its delete-by is at most {MAX_HOURS} h out")
        label = name or (list(parts)[0] if isinstance(parts, dict) else os.path.basename(str((parts or [""])[0] if isinstance(parts, list) else parts)))
        sid = f"{slug(label if isinstance(label, str) else 'study')}-{time.strftime('%m%d-%H%M')}"
        if load_(sid):
            sid += f"-{int(time.time()) % 1000}"
        cores, mem = server_type_info(server_type)
        if cores is None:
            raise ValueError(f"Hetzner has no server type {server_type!r}")
        folder = study_dir(sid)
        os.makedirs(folder, exist_ok=True)
        cases, found, warnings = plan(parts, load, material, mesh, folder, cores=cores, mem_gb=mem)
        for c in cases.values():
            c["timeout_s"] = int(float(timeout_minutes) * 60)
        if reference and reference not in cases:
            raise ValueError(f"reference {reference!r} isn't one of the parts ({', '.join(cases)})")
        write_cases(folder, cases)
        hourly, currency = hetzner.price(server_type, location)
        created = now_ms()
        if hourly:
            # Hetzner bills each started hour: the delete-by (enforced here and labelled on the box) holds the spend
            affordable = math.floor(float(max_usd) / hourly)
            if affordable < 1:
                raise ValueError(f"max_usd ${float(max_usd):.2f} is under one hour of {server_type} (${hourly:.3f})")
            if affordable < float(hours):
                warnings.append(f"its delete-by is {affordable} h out, not {float(hours):g}: that's what ${float(max_usd):.2f} buys")
                hours = affordable
        delete_by = created + int(float(hours) * 3_600_000)
        _, pubkey = keypair()
        labels = {"temp": "true", "made_by": NAME, "study": sid[:63], "delete_by": hetzner.label_time(delete_by)}
        body = {"name": f"temp-fea-{sid}"[:63].strip("-"), "server_type": server_type, "location": location,
                "image": "ubuntu-24.04", "labels": labels, "start_after_create": True, "ssh_keys": [hetzner_key()],
                "user_data": cloud_init(pubkey), "public_net": {"enable_ipv4": True, "enable_ipv6": True}}
        try:
            made = api("POST", "/servers", body)["server"]
        except hetzner.HetznerError as e:
            hint = ""
            if e.code == "resource_limit_exceeded":
                if "dedicated" in (e.message or "").lower():
                    hint = " Dedicated (ccx) types are over this account's dedicated core limit: use a shared type like cpx51."
                else:
                    held = [f"{s['name']} ({s['type']})" for s in hetzner.inventory()["servers"]]
                    hint = (f" The project's other servers hold the cores ({', '.join(held) or 'none listed'}): a smaller "
                            "type (cpx41, 8 cores), or once they're gone.")
            raise ValueError(f"Hetzner wouldn't rent the box: {e.message}.{hint}")
        state["boxes_rented"] += 1
        first = next(iter(cases.values()))
        st = {"id": sid, "name": name or sid, "state": "booting", "round": 1, "created": created, "delete_by": delete_by,
              "keep_minutes": int(keep_minutes), "cases": cases, "slots": cores,
              "mem_mb": int(mem * 1024 * 0.9) if mem else None, "about": about or [], "reference": reference, "target": target,
              "load": first["load"], "material": first["material"], "checked": found,
              "results": os.path.abspath(os.path.expanduser(results)) if results else os.path.join(RESULTS_ROOT, sid),
              "price_hourly": hourly, "currency": currency, "box_state": "booting",
              "box": {"id": made["id"], "name": made["name"], "ip": made["public_net"]["ipv4"]["ip"], "type": server_type,
                      "location": location}}
        with lock(sid):
            emit_study(st, force=True)
            save(st)
        return {"study": sid, "box": dict(st["box"], price=f"{hourly:.3f} {currency}/h" if hourly else "price unknown",
                                          cores=cores, memory_gb=mem),
                "parts": {n: {"material": c["material"]["name"], "threads": c.get("threads"), "budget": c["mesh"].get("budget")}
                          for n, c in cases.items()},
                "checked": found, "delete_by": pt(delete_by), "results": st["results"],
                "warnings": warnings,
                "next": "The box installs CalculiX and gmsh (about 3 min), then the parts run; status shows each one."}
    except Exception as e:
        state["failed"] += 1
        state["last_error"] = f"run: {e}"
        raise


def load_(sid):
    return load(sid)


def refreshed(sid):
    """The study with the box's newest status, when it's running."""
    with lock(sid):
        st = load(sid)
        if st["state"] in ("running", "done"):
            try:
                st["status"] = read_status(st)
                emit_cases(st)
                save(st)
            except Exception as e:
                st["status_error"] = str(e)
        return st


def part_rows(st):
    rows = []
    for i, c in enumerate((st.get("status") or {}).get("cases") or []):
        f = case_facts(st, i, c)
        f.pop("study", None)
        f.pop("order", None)
        h = c.get("headline") or {}
        if h:
            f["headline"] = {k: v for k, v in h.items() if v not in (None, {}, [])}
        if c.get("elapsed_s") is not None:
            f["elapsed_s"] = c["elapsed_s"]
        rows.append(f)
    return rows


@app.tool(name="fea_status", read_only=True)
def status(study: str = ""):
    """How a study stands: its box, cost and delete-by, and each part (stage, numbers, verdict, strain, its pictures' paths, its result.json), the chart's path; with no study, every study and everything in the Hetzner project."""
    state["calls"] += 1
    if study:
        st = refreshed(find(study)["id"])
        end = st.get("deleted_at") or now_ms()
        cost, hours = hetzner.cost(st["created"], end, st.get("price_hourly"))
        comp = (st.get("status") or {}).get("compare") or {}
        return {"study": st["id"], "name": st.get("name"), "state": st["state"], "box_state": st.get("box_state"),
                "box": st.get("box"), "round": st["round"], "load": st.get("load"),
                "material": {k: (st.get("material") or {}).get(k) for k in ("name", "E", "nu", "model", "source")},
                "counts": (st.get("status") or {}).get("counts"), "parts": part_rows(st),
                "reference": st.get("reference"), "target": comp.get("target"), "tol": comp.get("tol"),
                "chart": (st.get("pictures") or {}).get("compare.png"), "results": st["results"],
                "fetched": st.get("fetched"), "delete_by": pt(st.get("delete_by")), "cost": cost, "currency": st.get("currency"),
                "hours": hours, "verified_clean": st.get("verified_clean"), "left": st.get("left"),
                "error": st.get("error") or st.get("status_error") or (comp.get("error") if comp else None)}
    out = {"token": bool(hetzner.token()), "studies": [
        {"study": s["id"], "name": s.get("name"), "state": s["state"], "box_state": s.get("box_state"),
         "created": pt(s.get("created")), "counts": (s.get("status") or {}).get("counts"),
         "cost": s.get("cost"), "verified_clean": s.get("verified_clean")} for s in all_studies()[-10:]]}
    if out["token"]:
        try:
            inv = hetzner.inventory()
            for srv in inv["servers"]:
                due = hetzner.parse_label_time(srv["labels"].get("delete_by", ""))
                srv["delete_by"] = pt(due) if due else None
                if due and now_ms() > due:
                    srv["overdue"] = True
            out["hetzner"] = inv
        except hetzner.HetznerError as e:
            out["hetzner_error"] = str(e)
    out.update(state)
    return out


@app.tool(name="fea_materials", read_only=True)
def materials():
    "The materials run knows by name, each with its modulus, Poisson's ratio, model, strength and the datasheet it's from."
    return {k: {f: v.get(f) for f in ("name", "E", "E_z", "nu", "model", "strength", "strength_z", "elongation", "density",
                                       "source") if v.get(f) is not None}
            for k, v in spec.MATERIALS.items()}


def live(st):
    if st["state"] not in ("running", "done"):
        raise ValueError(f"{st['id']} is {st['state']}: its box isn't running the parts" +
                         (" yet (status says how setting it up goes)" if st["state"] in ("booting", "installing") else ""))


@app.tool(name="fea_add", speaks_for_person=True, schema={
    "type": "object",
    "properties": {
        "study": {"type": "string", "description": "The study (its id or name)."},
        "parts": PARTS_SCHEMA, "load": LOAD_SCHEMA, "material": MATERIAL_SCHEMA, "mesh": MESH_SCHEMA,
        "rerun": {"type": "array", "items": {"type": "string"}, "description": "Parts to run again as they are."},
    },
    "required": ["study"],
})
def add(study, ctx, parts=None, load=None, material=None, mesh=None, rerun=None):
    "Puts more parts in a study whose box is still up (the study's load and material unless given), or runs some again: the box, and its bill, run on until they're done."
    state["calls"] += 1
    sid = find(study)["id"]
    with holding(sid) as st:
        live(st)
        rnd = st["round"] + 1
        new = {}
        warnings = []
        if parts:
            new, found, warnings = plan(parts, load if load is not None else st["load"], material or st["material"],
                                        mesh, study_dir(sid), cores=st["slots"],
                                        mem_gb=(st["mem_mb"] / 1024 / 0.9) if st.get("mem_mb") else None,
                                        rnd=rnd, have=set(st["cases"]))
            first = next(iter(st["cases"].values()))
            for c in new.values():
                c["timeout_s"] = first.get("timeout_s")
            st.setdefault("checked", {}).update(found)
            write_cases(study_dir(sid), new)
        again = [c for c in (rerun or []) if c in st["cases"]]
        missing = [c for c in (rerun or []) if c not in st["cases"]]
        if missing:
            raise ValueError(f"no part {', '.join(missing)} in {st['id']}")
        if not new and not again:
            raise ValueError("add needs parts or rerun")
        if new:
            upload_cases(st, list(new))
        st["cases"].update(new)
        for c in again:
            st["cases"][c]["round"] = rnd
            drop_request(st, c, {"rerun": True})
        st["round"] = rnd
        st.pop("done_round", None)
        st["state"] = "running"
        st["box_state"] = "running the parts"
        st.pop("_cases_emitted", None)
        push_study_json(st)
        emit_study(st, force=True)
    return {"study": st["id"], "round": st["round"], "added": list(new), "rerun": again, "warnings": warnings}


@app.tool(name="fea_cancel", schema={
    "type": "object",
    "properties": {"study": {"type": "string"},
                   "parts": {"type": "array", "items": {"type": "string"}, "description": "Parts to stop (default: every one not finished)."}},
    "required": ["study"],
})
def cancel(study, parts=None):
    "Stops parts (queued ones never start). Once nothing's left running the study is done as usual: fetched, then deleted."
    state["calls"] += 1
    st = refreshed(find(study)["id"])
    live(st)
    cs = {c["name"]: c for c in (st.get("status") or {}).get("cases") or []}
    unknown = [n for n in parts or [] if n not in cs]
    if unknown:
        raise ValueError(f"no part {', '.join(unknown)} in {st['id']}")
    names = parts or [n for n, c in cs.items() if c.get("state") not in ("done", "failed", "cancelled")]
    with lock(st["id"]):
        for n in names:
            drop_request(st, n, {"cancel": True})
    return {"study": st["id"], "cancelling": names}


@app.tool(name="fea_fetch", schema={
    "type": "object",
    "properties": {"study": {"type": "string"},
                   "parts": {"type": "array", "items": {"type": "string"}, "description": "Only these parts (default: all)."},
                   "frd": {"type": "boolean", "description": "The solver's field file too (job.frd, kept only with keep_frd; can be big)."}},
    "required": ["study"],
})
def fetch(study, parts=None, frd=False):
    """Brings results here now (each part's result.json, pictures, deck and logs, the study's compare.json and chart) into the study's results folder."""
    state["calls"] += 1
    with holding(find(study)["id"]) as st:
        live(st)
        n, size = fetch_results(st, frd=frd, cases=parts)
    return {"study": st["id"], "results": st["results"], "files": n, "mb": round(size / 1e6, 2),
            "chart": (st.get("pictures") or {}).get("compare.png")}


@app.tool(name="fea_shell", schema={
    "type": "object",
    "properties": {"study": {"type": "string"},
                   "command": {"type": "string", "description": "A shell command, run on the box in /root/fea (cases/<part>/ holds each part's files), the venv's python first on PATH."},
                   "timeout_s": {"type": "integer", "description": "How long it may run (default 300, at most 540)."}},
    "required": ["study", "command"],
})
def shell(study, command, timeout_s=300):
    "Runs a command on a study's box (a look at a part's log or deck, a script against its results) and answers its output."
    state["calls"] += 1
    st = find(study)
    live(st)
    timeout_s = max(5, min(int(timeout_s), 540))
    code, out, err = remote(st, f"export PATH={BOX}/venv/bin:$PATH; cd {BOX_FEA} && " + command, timeout=timeout_s)
    out, err = text(out), text(err)
    return {"exit": code, "stdout": out[-20000:], "stderr": err[-5000:], "cut": len(out) > 20000}


@app.tool(name="fea_delete", schema={
    "type": "object",
    "properties": {"study": {"type": "string"},
                   "fetch": {"type": "boolean", "description": "Fetch results first when this round's aren't here yet (default true)."}},
    "required": ["study"],
})
def delete(study, fetch=True):
    "Takes a study's box down now, after fetching its results, and checks Hetzner holds nothing of it; answers what it cost."
    state["calls"] += 1
    with holding(find(study)["id"]) as st:
        if st["state"] in ("deleted", "failed") and st.get("verified_clean") is not None:
            return {"study": st["id"], "state": st["state"], "verified_clean": st["verified_clean"], "cost": st.get("cost")}
        if fetch and st["state"] in ("running", "done") and st.get("fetched_round") != st["round"]:
            try:
                fetch_results(st)
            except Exception as e:
                st["error"] = f"couldn't fetch before deleting: {e}"
        st["done_round"] = st["round"]
        st["state"] = "deleting"
        for _ in range(24):
            delete_box(st)
            if st["state"] != "deleting":
                break
            time.sleep(DELETE_WAIT_S)
        emit_study(st, force=True)
    return {"study": st["id"], "state": st["state"], "box_state": st.get("box_state"), "verified_clean": st.get("verified_clean"),
            "left": st.get("left"), "cost": st.get("cost"), "currency": st.get("currency"), "hours": st.get("hours"),
            "results": st["results"], "error": st.get("error")}


def health():
    "How fea itself is: its key, the studies it holds, its last poll and error."
    return dict(state, token=bool(hetzner.token()), active=[s["id"] for s in all_studies() if s["state"] in ACTIVE])


def heartbeat():
    active = [s for s in all_studies() if s["state"] in ACTIVE]
    error = state["last_error"]
    facts = dict(state, token=bool(hetzner.token()), active=[s["id"] for s in active])
    word = (f"{len(active)} {'study' if len(active) == 1 else 'studies'} running") if active else "Ready"
    app.emit("heartbeat", {"entity": f"sense:{NAME}", "summary": f"{word}; last failed: {error}" if error else word, "facts": facts})


@app.background
def loop():
    last_beat = last_reap = 0.0
    while True:
        for st in all_studies():
            if st["state"] in ACTIVE:
                step(st["id"])
                state["polls"] += 1
        t = time.time()
        if hetzner.token() and t - last_reap >= REAP_S:
            reap()
            last_reap = t
        if t - last_beat >= HEARTBEAT_S:
            heartbeat()
            last_beat = t
        time.sleep(POLL_S)


PIP = ["numpy", "scipy", "matplotlib", "gmsh", "trimesh", "rtree", "networkx"]  # networkx: trimesh graph ops (fix_normals, split) on some parts
APT = ["calculix-ccx", "python3-venv", "python3-pip", "libglu1-mesa", "libxcursor1", "libxft2", "libxinerama1",
       "libgl1", "libxrender1", "libxext6", "rsync"]


def cloud_init(pubkey, pip=(), setup=()):
    """The box's first boot: the appendage's key for root, CalculiX from Ubuntu, a venv with gmsh, numpy, scipy,
    matplotlib, trimesh and rtree, then /root/fea-ready (or /root/fea-failed with the step that failed)."""
    pkgs = " ".join(PIP + [p for p in pip if p not in PIP])
    steps = [
        ("installing CalculiX", "export DEBIAN_FRONTEND=noninteractive; apt-get update -q > /root/apt.log 2>&1; "
                                f"apt-get install -y -q {' '.join(APT)} >> /root/apt.log 2>&1 && command -v ccx"),
        ("making the venv", f"python3 -m venv /root/venv && /root/venv/bin/pip install -q {pkgs} > /root/pip.log 2>&1 && "
                            "/root/venv/bin/python -c 'import gmsh, trimesh, rtree, scipy, matplotlib, networkx'"),
    ] + [(f"setup step {i + 1}", s_) for i, s_ in enumerate(setup)]
    script = ["set -o pipefail"]
    for name, s_ in steps:
        script.append(f"( {s_} ) || {{ echo {shlex.quote(name + ' failed')} > /root/fea-failed; exit 1; }}")
    script.append("touch /root/fea-ready")
    lines = [
        "#cloud-config",
        "ssh_authorized_keys:",
        f"  - {pubkey.strip()}",
        "write_files:",
        "  - path: /root/fea-setup.sh",
        "    permissions: '0700'",
        "    content: |",
        "      #!/bin/bash",
    ] + [f"      {line}" for line in script] + [
        "runcmd:",
        "  - [bash, -lc, '/root/fea-setup.sh > /root/fea-setup.log 2>&1']",
    ]
    return "\n".join(lines) + "\n"
