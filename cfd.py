"""cfd: runs an OpenFOAM case matrix on a temporary Hetzner box it rents, watches, extends and deletes.

The box runs cfd_box/runner.py under systemd, which schedules the cases, judges each one's convergence and runs the
unconverged ones further; this server reads its status.json over ssh, records each study and case (cfd_study and
cfd_case events), fetches the results once the matrix is done, and deletes the box (verifying nothing is left).
Renting is the user's money: cfd_start, cfd_add and cfd_extend speak for the user; the loop only watches, fetches and
deletes.
"""

import contextlib
import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import threading
import time

import cfd_spec as spec
import hetzner
from server import app, config, part_home

NAME = "cfd"
HERE = os.path.dirname(os.path.abspath(__file__))
HOME = part_home(NAME)
POLL_S = float(os.environ.get("CFD_POLL_S") or 30)
HEARTBEAT_S = float(os.environ.get("CFD_HEARTBEAT_S") or 600)
REAP_S = float(os.environ.get("CFD_REAP_S") or 600)
BOX = (os.environ.get("CFD_BOX_ROOT") or "/root").rstrip("/")
"""The box's root: /root on a real box; a temp folder in tests, where commands run here."""
BOX_CFD, BOX_STUDY, BOX_CASES = f"{BOX}/cfd", f"{BOX}/study", f"{BOX}/cases"
RESULTS_ROOT = os.path.join(os.path.expanduser(config()["results"]), "cfd") if config().get("results") else os.path.join(HOME, "results")
"""Where each study's results land: config.json's results/cfd, else results/ in its home."""
MAX_HOURS = 24
DELETE_WAIT_S = float(os.environ.get("CFD_DELETE_WAIT_S") or 5)
LEFTOVER_GRACE_MS = int(float(os.environ.get("CFD_LEFTOVER_GRACE_S") or 600) * 1000)
ACTIVE = ("booting", "installing", "running", "done", "deleting")
UPLOAD_EXCLUDE = [".git", "__pycache__", "*.pyc", ".venv", "venv", "node_modules", ".DS_Store", "._*", "processor*",
                  "results"]
UPLOAD_MAX_MB = 500

app.event("cfd_study", "A CFD study's state: its box, its cases' counts and verdicts, its results", stream="cfd/studies", kind="observation")
app.event("cfd_case", "One case of a study: its state, iteration, convergence", stream="cfd/cases", kind="observation", retention="signal")

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
        raise ValueError("study: give its id (start answered it) or its name")
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
    return api("POST", "/ssh_keys", {"name": name, "public_key": pub, "labels": {"made_by": "cfd"}})["ssh_key"]["id"]


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
                              stdin=stdin if hasattr(stdin, "read") else None,
                              capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, b"", f"timed out after {timeout} s".encode()
    return done.returncode, done.stdout, done.stderr


def text(b):
    return b.decode("utf-8", "replace") if isinstance(b, bytes) else (b or "")


def must(st, command, stdin=None, timeout=120, what="the box"):
    code, out, err = remote(st, command, stdin=stdin, timeout=timeout)
    if code != 0:
        raise RuntimeError(f"{what} said (exit {code}): {(text(err) or text(out)).strip()[-800:]}")
    return text(out)


# ---------------------------------------------------------------------------------------------
# Moving files


def pack(folder, exclude=UPLOAD_EXCLUDE):
    """A folder as a gzipped tar (bytes), less what isn't the study's own."""
    argv = ["tar", "-czf", "-", "--no-xattrs"]
    if os.uname().sysname == "Darwin":
        argv.append("--no-mac-metadata")
    for pat in exclude:
        argv += ["--exclude", pat]
    argv += ["-C", folder, "."]
    done = subprocess.run(argv, capture_output=True, env=dict(os.environ, COPYFILE_DISABLE="1"))
    if done.returncode != 0:
        raise RuntimeError(f"couldn't pack {folder}: {text(done.stderr).strip()}")
    return done.stdout


def folder_mb(folder):
    total = 0
    for dirpath, dirnames, filenames in os.walk(folder):
        dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules", ".venv", "venv", "__pycache__", "results")
                       and not d.startswith("processor")]
        for f in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, f))
            except OSError:
                pass
    return total / 1e6


def upload_study(st):
    must(st, f"mkdir -p {BOX_STUDY} && tar -xzf - -C {BOX_STUDY}", stdin=pack(st["src"]), timeout=300,
         what="unpacking the study on the box")


def resolve_source(sid, study):
    """The study folder on this machine, as it is."""
    path = os.path.abspath(os.path.expanduser(study))
    if not os.path.isdir(path):
        raise ValueError(f"study: {study} isn't a folder here")
    return path, path


# ---------------------------------------------------------------------------------------------
# The box's runner


def box_study_json(st):
    return {"id": st["id"], "round": st["round"], "slots": st["slots"], "study_dir": BOX_STUDY, "cases_dir": BOX_CASES,
            "of_bashrc": st.get("of_bashrc") or "", "path": [f"{BOX}/venv/bin"], "env": st.get("env") or {},
            "cases": st["cases"]}


def push_study_json(st):
    data = json.dumps(box_study_json(st), indent=1).encode()
    must(st, f"mkdir -p {BOX_CFD} && cat > {BOX_CFD}/study.json.tmp && mv {BOX_CFD}/study.json.tmp {BOX_CFD}/study.json",
         stdin=data, what="writing the study on the box")


UNIT = """[Unit]
Description=cfd case matrix runner
After=network.target

[Service]
ExecStart=/usr/bin/python3 {cfd}/bin/runner.py serve --home {cfd}
Restart=always
RestartSec=5
KillMode=process

[Install]
WantedBy=multi-user.target
"""


def start_runner(st):
    """Starts the runner under systemd, so it outlives any ssh session (and the cases outlive a restart of it)."""
    unit = UNIT.format(cfd=BOX_CFD).encode()
    must(st, "cat > /etc/systemd/system/cfd-runner.service && systemctl daemon-reload && systemctl enable --now cfd-runner",
         stdin=unit, what="starting the runner")


def install_runner(st):
    must(st, f"mkdir -p {BOX_CFD}/bin && tar -xzf - -C {BOX_CFD}/bin", stdin=pack(os.path.join(HERE, "cfd_box")),
         what="putting the runner on the box")
    upload_study(st)
    push_study_json(st)
    start_runner(st)


def read_status(st):
    code, out, err = remote(st, f"cat {BOX_CFD}/status.json", timeout=30)
    if code != 0:
        raise RuntimeError(text(err).strip() or f"exit {code}")
    return json.loads(text(out))


def drop_request(st, case, req):
    data = json.dumps(req).encode()
    must(st, f"mkdir -p {BOX_CFD}/jobs/{shlex.quote(case)} && cat > {BOX_CFD}/jobs/{shlex.quote(case)}/request.json",
         stdin=data, what=f"asking the runner about {case}")


# ---------------------------------------------------------------------------------------------
# Fetching results


def fetch_results(st, fields=False, include=(), exclude=(), cases=None):
    """Brings the cases' results to the study's results folder here: (files, bytes)."""
    args = ["--home", BOX_CFD] + (["--fields"] if fields else [])
    for g in include:
        args += ["--include", g]
    for g in exclude:
        args += ["--exclude", g]
    for c in cases or []:
        args += ["--case", c]
    listing = json.loads(must(st, f"python3 {BOX_CFD}/bin/runner.py files " + " ".join(shlex.quote(a) for a in args),
                              what="listing the results"))
    files, size = listing["files"], listing["bytes"]
    dest = st["results"]
    os.makedirs(dest, exist_ok=True)
    free = shutil.disk_usage(dest).free
    if size > free - 5e9:
        raise RuntimeError(f"the results are {size / 1e9:.1f} GB and there is {free / 1e9:.1f} GB free here: "
                           "fetch fewer (cases, exclude) or make room first")
    if files:
        listing_bytes = ("\n".join(files) + "\n").encode()
        with tempfile.TemporaryFile() as tar:
            code, _, err = remote_to_file(st, f"cd {BOX_CASES} && tar -czf - -T -", listing_bytes, tar, timeout=1800)
            if code != 0:
                raise RuntimeError(f"couldn't pack the results on the box: {text(err).strip()[-500:]}")
            tar.seek(0)
            done = subprocess.run(["tar", "-xzf", "-", "-C", dest], stdin=tar, capture_output=True)
            if done.returncode != 0:
                raise RuntimeError(f"couldn't unpack the results here: {text(done.stderr).strip()}")
    status = read_status(st)
    with open(os.path.join(dest, "status.json"), "w") as f:
        json.dump(status, f, indent=1)
    with open(os.path.join(dest, "matrix.json"), "w") as f:
        json.dump(st["cases"], f, indent=1)
    st["fetched_at"] = now_ms()
    st["fetched_round"] = st["round"]
    st["fetched"] = {"files": len(files), "bytes": size}
    return len(files), size


def remote_to_file(st, command, stdin, out_file, timeout):
    argv = ssh_argv(st) + ["bash", "-lc", shlex.quote(command)]
    try:
        done = subprocess.run(argv, input=stdin, stdout=out_file, stderr=subprocess.PIPE, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, None, b"timed out"
    return done.returncode, None, done.stderr


# ---------------------------------------------------------------------------------------------
# What it records


def overall(status):
    cases = (status or {}).get("cases") or []
    if not cases:
        return 0.0
    total = 0.0
    for c in cases:
        s = c.get("state")
        if s in ("done", "failed", "cancelled"):
            total += 1
        elif s in ("running", "extending"):
            total += 0.1 + 0.85 * (c.get("progress") or 0)
        elif s == "post":
            total += 0.95
        elif s == "building":
            total += 0.05
    return round(total / len(cases), 3)


def counts_line(counts):
    order = ["running", "extending", "building", "post", "queued", "done", "failed", "cancelled"]
    return ", ".join(f"{counts[k]} {k}" for k in order if counts.get(k))


def first_line(text_, n):
    lines = (text_ or "").strip().splitlines()
    return lines[0][:n] if lines else None


def brief_case(c):
    out = {k: c.get(k) for k in ("name", "state", "time", "end_time", "converged", "extensions") if c.get(k) is not None}
    if c.get("error"):
        out["error"] = first_line(c["error"], 160)
    return out


def study_facts(st):
    status = st.get("status") or {}
    cases = status.get("cases") or []
    facts = {
        "study": st["id"], "name": st.get("name"), "state": st["state"], "round": st["round"],
        "done": st["state"] in ("done", "deleting", "deleted", "failed") and st.get("done_round") == st["round"],
        "box": {k: st["box"].get(k) for k in ("id", "name", "ip", "type", "location")} if st.get("box") else None,
        "box_state": st.get("box_state"), "delete_by": st.get("delete_by"), "created": st.get("created"),
        "counts": status.get("counts"), "progress": overall(status), "cases": [brief_case(c) for c in cases],
        "results": st.get("results"), "fetched_at": st.get("fetched_at"), "error": st.get("error"),
        "cost": st.get("cost"), "currency": st.get("currency"), "hours": st.get("hours"),
        "verified_clean": st.get("verified_clean"),
    }
    return {k: v for k, v in facts.items() if v is not None}


def summary(st):
    status = st.get("status") or {}
    counts = counts_line(status.get("counts") or {})
    s = st["state"]
    name = st.get("name") or st["id"]
    if s == "booting":
        return f"{name}: renting the box"
    if s == "installing":
        return f"{name}: installing OpenFOAM on the box"
    if s == "running":
        return f"{name}: {counts or 'starting'}"
    if s == "done":
        return f"{name} is done: {counts}; results in {st['results']}"
    if s == "deleting":
        return f"{name}: deleting the box"
    if s == "deleted":
        return f"{name}: box deleted" + (f" (cost {st['cost']} {st.get('currency') or ''})" if st.get("cost") is not None else "")
    if s == "failed":
        return f"{name} failed: {st.get('error')}"
    return f"{name}: {s}"


def emit_study(st, force=False):
    facts = study_facts(st)
    key = json.dumps({k: facts.get(k) for k in ("state", "round", "done", "counts", "box_state", "error", "verified_clean",
                                                 "fetched_at", "cases")}, sort_keys=True, default=str)
    last = st.get("_emitted") or {}
    stale = now_ms() - (last.get("at") or 0) > 600_000 and st["state"] in ("running", "installing", "booting")
    if force or key != last.get("key") or stale:
        app.emit("cfd_study", {"entity": f"cfd-study:{st['id']}", "summary": summary(st), "facts": facts}, about=st.get("about") or None)
        st["_emitted"] = {"key": key, "at": now_ms()}


def emit_cases(st):
    sent = st.setdefault("_cases_emitted", {})
    for i, c in enumerate((st.get("status") or {}).get("cases") or []):
        name = c["name"]
        drift = None
        for m in (c.get("monitors") or {}).values():
            if m.get("rel_drift") is not None:
                drift = max(drift or 0, m["rel_drift"])
        res = max(c["residuals"].values()) if c.get("residuals") else None
        facts = {"study": st["id"], "case": name, "order": i, "state": c.get("state"), "stage": c.get("stage"),
                 "time": c.get("time"), "end_time": c.get("end_time"), "progress": c.get("progress"),
                 "converged": c.get("converged"), "why": c.get("why"), "extensions": c.get("extensions"),
                 "max_residual": res, "drift": drift, "error": first_line(c.get("error"), 200)}
        facts = {k: v for k, v in facts.items() if v is not None}
        key = json.dumps({k: facts.get(k) for k in ("state", "stage", "converged", "extensions", "error")}, sort_keys=True)
        prev = sent.get(name) or {}
        moved = abs((facts.get("progress") or 0) - (prev.get("progress") or 0)) >= 0.05
        if key != prev.get("key") or moved:
            label = f"{name}: {c.get('state')}"
            if c.get("time") is not None and c.get("end_time"):
                label += f" at {c['time']:g} of {c['end_time']:g}"
            if c.get("converged") is not None and c.get("state") == "done":
                label += ", converged" if c["converged"] else f", not converged ({c.get('why')})"
            app.emit("cfd_case", {"entity": f"cfd-case:{st['id']}-{name}", "summary": label, "facts": facts},
                     about=[f"cfd-study:{st['id']}"])
            sent[name] = {"key": key, "progress": facts.get("progress") or 0}


# ---------------------------------------------------------------------------------------------
# Moving a study along (the background loop, and tools that need it to be current)


def advance(st):
    """One step of a study's life, by what the box says: booting, installing, running, done, deleting, deleted."""
    s = st["state"]
    t = now_ms()
    if s in ("deleted",):
        return
    if st.get("delete_by") and t >= st["delete_by"] and s not in ("deleting",):
        if s in ("running", "done") and st.get("fetched_round") != st["round"]:
            try:
                fetch_results(st)
            except Exception as e:
                st["error"] = f"couldn't fetch before its delete-by: {e}"
        st["box_state"] = "past its delete-by"
        if st.get("done_round") != st["round"]:
            st["done_round"] = st["round"]
        st["state"] = "deleting"
        s = "deleting"
    if s == "booting":
        code, _, _ = remote(st, "true", timeout=20)
        if code == 0:
            st["state"], st["box_state"] = "installing", "installing OpenFOAM"
        elif t - st["created"] > 15 * 60_000:
            return fail(st, "the box never answered ssh in 15 minutes")
    elif s == "installing":
        code, out, _ = remote(st, f"cat {BOX}/cfd-failed 2>/dev/null; test -f {BOX}/cfd-ready && echo READY; "
                                  f"cat {BOX}/of-bashrc 2>/dev/null; cloud-init status 2>/dev/null; true", timeout=30)
        out = text(out)
        first = out.strip().splitlines()[0] if out.strip() else ""
        # first boot over with neither marker written: the setup script itself died (it never got to say which step)
        ended = "READY" not in out and re.search(r"^status: (done|error)", out, re.M)
        if first.endswith(" failed") or ended:
            _, logs, _ = remote(st, f"tail -n 15 {BOX}/of-install.log {BOX}/pip.log {BOX}/cfd-setup.log 2>/dev/null", timeout=30)
            what = first if first.endswith(" failed") else "the setup script stopped before it finished"
            return fail(st, f"setting the box up: {what}; {text(logs).strip()[-600:]}")
        if "READY" in out:
            bashrc = [l for l in out.splitlines() if l.endswith("/etc/bashrc")]
            st["of_bashrc"] = bashrc[0] if bashrc else ""
            version = re.search(r"openfoam(\d+)", st["of_bashrc"])
            st["of_version"] = version.group(1) if version else None
            install_runner(st)
            st["state"], st["box_state"] = "running", "running the matrix"
            st["running_since"] = now_ms()
        elif t - st["created"] > 45 * 60_000:
            return fail(st, "setting the box up took over 45 minutes")
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
        emit_cases(st)
        if s == "running" and status.get("state") == "done" and status.get("round") == st["round"]:
            try:
                fetch_results(st)
            except Exception as e:
                st["error"] = f"couldn't fetch the results: {e}"
                return
            st["state"], st["done_round"], st["done_at"] = "done", st["round"], now_ms()
            st["box_state"] = f"kept {st['keep_minutes']} min for follow-ups"
        elif s == "done" and t - (st.get("done_at") or t) > st["keep_minutes"] * 60_000:
            st["state"], st["box_state"] = "deleting", "deleting"
    if st["state"] == "deleting":
        delete_box(st)


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
        return None  # still being deleted
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
        servers = api("GET", "/servers?label_selector=made_by%3Dcfd").get("servers") or []
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
                step(held["id"])  # its own study fetches what it can, then deletes it
            continue
        if not overdue and srv.get("labels", {}).get("study") and young(srv):
            continue  # one start is renting right now, before its study is kept
        try:
            api("DELETE", f"/servers/{srv['id']}")
            gone.append(srv["name"])
        except hetzner.HetznerError as e:
            state["last_error"] = f"reaping {srv['name']}: {e}"
    state["last_reap"] = {"at": now_ms(), "deleted": gone}
    return gone


# ---------------------------------------------------------------------------------------------
# Tools


def young(srv, minutes=10):
    try:
        import datetime
        made = datetime.datetime.fromisoformat(srv["created"].replace("Z", "+00:00")).timestamp() * 1000
    except (KeyError, ValueError, AttributeError):
        return False
    return now_ms() - made < minutes * 60_000


def slug(text_):
    s = re.sub(r"[^a-z0-9]+", "-", (text_ or "").lower()).strip("-")
    return s[:28].strip("-") or "study"


def server_cores(server_type):
    try:
        types = api("GET", f"/server_types?name={server_type}").get("server_types") or []
    except hetzner.HetznerError:
        return None
    return types[0].get("cores") if types else None


@app.tool(name="cfd_start", speaks_for_person=True, schema={
    "type": "object",
    "properties": {
        "study": {"type": "string", "description": "The study folder: a path here. Its files go to /root/study on the box (less .git, caches, venvs, results and processor folders)."},
        "cases": {"description": "The case matrix: an object {defaults, vars, cases}, or a file in the study folder (default matrix.json). See the description for each case's fields."},
        "name": {"type": "string", "description": "What the study's called, in a few words (inlet matrix)."},
        "server_type": {"type": "string", "description": "Hetzner server type (default cpx51: 16 shared vCPU, 32 GB). Dedicated types over the account's core limit are refused by Hetzner."},
        "location": {"type": "string", "description": "Hetzner location (default hil, Hillsboro)."},
        "slots": {"type": "integer", "description": "Cores the matrix may use at once (default: the box's cores)."},
        "hours": {"type": "number", "description": "Its delete-by, in hours from now (default 8, at most 24): the box goes then whatever it's doing, after fetching what it can."},
        "keep_minutes": {"type": "integer", "description": "How long the box stays up once the matrix ends, for extend, add or shell (default 60; 0 deletes it at once)."},
        "results": {"type": "string", "description": "Where results land here (default: the extension's home, results/<study id>)."},
        "pip": {"type": "array", "items": {"type": "string"}, "description": "Python packages for the box's venv, which the study's commands find on PATH (numpy is always there)."},
        "setup": {"type": "array", "items": {"type": "string"}, "description": "More shell commands run once on the box after OpenFOAM is in."},
        "env": {"type": "object", "description": "Environment for every case's commands."},
        "about": {"type": "array", "items": {"type": "string"}, "description": "Entities it's for, on its events."},
    },
    "required": ["study"],
})
def start(study, ctx, cases=None, name=None, server_type="cpx51", location="hil", slots=None, hours=8, keep_minutes=60,
          results=None, pip=None, setup=None, env=None, about=None):
    "Rents a Hetzner box (the user's money, until its delete-by at most) and runs a case matrix on it. Answers at once with the study's id; cfd_status follows it, and a cfd_study event says when it's done."
    state["calls"] += 1
    try:
        if not hetzner.token():
            raise ValueError("no Hetzner key: connect Hetzner Cloud in Switch's connections")
        if not (0 < float(hours) <= MAX_HOURS):
            raise ValueError(f"hours: its delete-by is at most {MAX_HOURS} h out")
        sid = f"{slug(name or os.path.basename(str(study).rstrip('/')))}-{time.strftime('%m%d-%H%M')}"
        if load(sid):
            sid += f"-{int(time.time()) % 1000}"
        src, origin = resolve_source(sid, study)
        mb = folder_mb(src)
        if mb > UPLOAD_MAX_MB:
            raise ValueError(f"the study folder is {mb:.0f} MB: keep only what builds and runs the cases there (results "
                             f"and meshes stay out), at most {UPLOAD_MAX_MB} MB")
        cores = server_cores(server_type)
        if cores is None:
            raise ValueError(f"Hetzner has no server type {server_type!r}")
        slots = int(slots or cores)
        matrix = spec.load(cases, src)
        expanded, warnings = spec.expand(matrix, src, BOX_STUDY, BOX_CASES, slots=slots)
        hourly, currency = hetzner.price(server_type, location)
        created = now_ms()
        delete_by = created + int(float(hours) * 3_600_000)
        _, pubkey = keypair()
        labels = {"temp": "true", "made_by": "cfd", "study": sid[:63], "delete_by": hetzner.label_time(delete_by)}
        body = {"name": f"temp-cfd-{sid}"[:63].strip("-"), "server_type": server_type, "location": location,
                "image": "ubuntu-24.04", "labels": labels, "start_after_create": True, "ssh_keys": [hetzner_key()],
                "user_data": cloud_init(pubkey, pip=pip or [], setup=setup or []),
                "public_net": {"enable_ipv4": True, "enable_ipv6": True}}
        try:
            made = api("POST", "/servers", body)["server"]
        except hetzner.HetznerError as e:
            hint = (" Dedicated (ccx) types are over this account's dedicated core limit: use a shared type like cpx51."
                    if e.code == "resource_limit_exceeded" else "")
            raise ValueError(f"Hetzner wouldn't rent the box: {e.message}.{hint}")
        state["boxes_rented"] += 1
        st = {"id": sid, "name": name or sid, "state": "booting", "round": 1, "created": created, "delete_by": delete_by,
              "keep_minutes": int(keep_minutes), "src": src, "origin": origin, "matrix": matrix, "cases": expanded,
              "slots": slots, "env": env or {}, "about": about or [], "results": os.path.abspath(os.path.expanduser(results)) if results else os.path.join(RESULTS_ROOT, sid),
              "price_hourly": hourly, "currency": currency, "box_state": "booting",
              "box": {"id": made["id"], "name": made["name"], "ip": made["public_net"]["ipv4"]["ip"], "type": server_type,
                      "location": location}}
        with lock(sid):
            emit_study(st, force=True)
            save(st)
        price_line = f"{hourly:.3f} {currency}/h" if hourly else "price unknown"
        return {"study": sid, "box": dict(st["box"], price=price_line, cores=cores), "cases": list(expanded),
                "slots": slots, "delete_by": pt(delete_by), "results": st["results"],
                "warnings": warnings,
                "next": "The box installs OpenFOAM (about 10 min), then the runner starts; status shows each case."}
    except Exception as e:
        state["failed"] += 1
        state["last_error"] = f"start: {e}"
        raise


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


def case_rows(status):
    rows = []
    for c in (status or {}).get("cases") or []:
        row = {k: c.get(k) for k in ("name", "state", "stage", "time", "end_time", "progress", "converged", "why",
                                     "extensions", "residuals", "monitors", "sec_per_step", "elapsed_s", "error", "post")
               if c.get(k) not in (None, {}, [])}
        rows.append(row)
    return rows


@app.tool(name="cfd_status", read_only=True)
def status(study: str = ""):
    """How a study stands: its box, cost so far, delete-by and each case (state, iteration, residuals, monitors' drift,
    verdict, extensions, post output). With no study: every study, and everything in the Hetzner project (servers with
    their labels, IPs, volumes, snapshots), each box past its delete-by flagged."""
    state["calls"] += 1
    if study:
        st = refreshed(find(study)["id"])
        end = st.get("deleted_at") or now_ms()
        cost, hours = hetzner.cost(st["created"], end, st.get("price_hourly"))
        return {"study": st["id"], "name": st.get("name"), "state": st["state"], "box_state": st.get("box_state"),
                "box": st.get("box"), "openfoam": st.get("of_version"), "round": st["round"],
                "counts": (st.get("status") or {}).get("counts"), "cores_used": (st.get("status") or {}).get("cores_used"),
                "slots": st.get("slots"), "load": (st.get("status") or {}).get("load"),
                "cases": case_rows(st.get("status")), "results": st["results"], "fetched": st.get("fetched"),
                "delete_by": pt(st.get("delete_by")), "keep_minutes": st.get("keep_minutes"), "cost": cost, "currency": st.get("currency"), "hours": hours,
                "verified_clean": st.get("verified_clean"), "left": st.get("left"), "error": st.get("error") or st.get("status_error")}
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


def next_round(st):
    st["round"] += 1
    st.pop("done_round", None)
    st["state"] = "running" if st["state"] in ("done", "running") else st["state"]
    st["box_state"] = "running the matrix"
    emit_study(st, force=True)


def live(st):
    if st["state"] not in ("running", "done"):
        raise ValueError(f"{st['id']} is {st['state']}: its box isn't running the matrix" +
                         (" yet (status says how setting it up goes)" if st["state"] in ("booting", "installing") else ""))


@app.tool(name="cfd_add", speaks_for_person=True, schema={
    "type": "object",
    "properties": {
        "study": {"type": "string", "description": "The study (its id or name)."},
        "cases": {"description": "More cases, as start's matrix ({defaults, vars, cases}, or a file in the study folder): the study's defaults and vars apply unless given. A case already in the matrix is replaced and run again."},
        "rerun": {"type": "array", "items": {"type": "string"}, "description": "Cases to run again from scratch as they are (after changing the study's scripts, say)."},
    },
    "required": ["study"],
})
def add(study, ctx, cases=None, rerun=None):
    "Puts more cases into a running study's matrix (or reruns some), with the study folder sent again first: the box, and its bill, run on until they're done."
    state["calls"] += 1
    sid = find(study)["id"]
    with holding(sid) as st:
        live(st)
        if st["origin"] != st["src"]:
            st["src"], _ = resolve_source(st["id"], st["origin"])
        new = {}
        warnings = []
        if cases:
            matrix = spec.load(cases, st["src"])
            base = st.get("matrix") or {}
            merged = {"defaults": spec.merge(base.get("defaults") or {}, matrix.get("defaults") or {}),
                      "vars": dict(base.get("vars") or {}, **(matrix.get("vars") or {})), "cases": matrix.get("cases")}
            new, warnings = spec.expand(merged, st["src"], BOX_STUDY, BOX_CASES, slots=st["slots"])
        again = [c for c in (rerun or []) if c in st["cases"]] + [c for c in new if c in st["cases"]]
        missing = [c for c in (rerun or []) if c not in st["cases"]]
        if missing:
            raise ValueError(f"no case {', '.join(missing)} in {st['id']}")
        if not new and not again:
            raise ValueError("add needs cases or rerun")
        upload_study(st)
        st["cases"].update(new)
        if cases:
            raw = (st["matrix"].setdefault("cases", {}))
            if isinstance(raw, dict) and isinstance(matrix.get("cases"), dict):
                raw.update(matrix["cases"])
        for c in again:  # the runner sees its requests before the new round, so the round can't read as done
            drop_request(st, c, {"rerun": True})
        next_round(st)
        push_study_json(st)
    return {"study": st["id"], "round": st["round"], "added": [c for c in new if c not in again], "rerun": again,
            "warnings": warnings}


@app.tool(name="cfd_extend", speaks_for_person=True, schema={
    "type": "object",
    "properties": {
        "study": {"type": "string"},
        "cases": {"type": "array", "items": {"type": "string"}, "description": "Finished cases to run on (default: every case that ended unconverged)."},
        "to": {"type": "number", "description": "The end time (iteration) to run to."},
        "by": {"type": "number", "description": "Or how far past its current end to run."},
        "averages": {"type": "string", "enum": ["continue", "restart"], "description": "continue: field averages carry on over the whole run; restart: they start again at the restart, covering the extension alone (default continue)."},
    },
    "required": ["study"],
})
def extend(study, ctx, cases=None, to=None, by=None, averages="continue"):
    "Runs finished cases on from their latest time (decomposing again, solving, reconstructing), averages continued or restarted: the box, and its bill, run on until they're done."
    state["calls"] += 1
    if not to and not by:
        raise ValueError("extend needs to (an end time) or by")
    st = refreshed(find(study)["id"])
    live(st)
    cs = {c["name"]: c for c in (st.get("status") or {}).get("cases") or []}
    unknown = [n for n in cases or [] if n not in cs]
    if unknown:
        raise ValueError(f"no case {', '.join(unknown)} in {st['id']}")
    names = cases or [n for n, c in cs.items() if c.get("state") == "done" and c.get("converged") is False]
    if not names:
        raise ValueError("no case to extend: give cases (every case that ended unconverged is the default, and there are none)")
    busy = [n for n in names if (cs.get(n) or {}).get("state") not in ("done", "failed", "cancelled")]
    if busy:
        raise ValueError(f"{', '.join(busy)} hasn't finished: extend runs finished cases on")
    with holding(st["id"]) as st:
        for n in names:  # requests first, then the round: the runner never sees the new round with nothing to do
            drop_request(st, n, {"extend": {"to": to, "by": by, "averages": averages}})
        next_round(st)
        push_study_json(st)
    return {"study": st["id"], "round": st["round"], "extending": names, "to": to, "by": by, "averages": averages}


def parse_delete_by(value, now):
    """A delete-by as given to keep: hours from now (a number), or an ISO time (local time unless it says its zone)."""
    import datetime
    if isinstance(value, bool):
        raise ValueError("delete_by: hours from now, or an ISO time")
    if isinstance(value, (int, float)):
        return now + int(float(value) * 3_600_000)
    text_ = str(value).strip()
    try:
        return now + int(float(text_) * 3_600_000)
    except ValueError:
        pass
    try:
        t = datetime.datetime.fromisoformat(text_.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"delete_by: {value!r} is neither hours from now nor an ISO time")
    if t.tzinfo is None:
        t = t.astimezone()  # local time
    return int(t.timestamp() * 1000)


def relabel(server_id, delete_by):
    """Sets a box's delete_by label (its other labels kept) and answers the label as Hetzner reads it back."""
    labels = dict(api("GET", f"/servers/{server_id}")["server"].get("labels") or {})
    labels["delete_by"] = hetzner.label_time(delete_by)
    api("PUT", f"/servers/{server_id}", {"labels": labels})
    return (api("GET", f"/servers/{server_id}")["server"].get("labels") or {}).get("delete_by")


def keep_study(st, keep_minutes=None, delete_by=None, now=None):
    """Changes a held study's keep and delete-by: the box's delete_by label first (so reap agrees), then the study."""
    now = now or now_ms()
    if st["state"] not in ACTIVE or st["state"] == "deleting":
        raise ValueError(f"{st['id']} is {st['state']}: its box is gone or going")
    if keep_minutes is None and delete_by is None:
        raise ValueError("keep needs keep_minutes or delete_by")
    if keep_minutes is not None and int(keep_minutes) < 0:
        raise ValueError("keep_minutes: 0 or more")
    due = None
    if delete_by is not None:
        due = parse_delete_by(delete_by, now)
        cap = st["created"] + MAX_HOURS * 3_600_000
        if due <= now:
            raise ValueError(f"delete_by: {pt(due)} has passed (cfd_delete takes the box down now)")
        if due > cap:
            raise ValueError(f"delete_by: at most {MAX_HOURS} h after the study was made, {pt(cap)}")
    label = None
    box = (st.get("box") or {}).get("id")
    if due is not None:
        if box:
            label = relabel(box, due)
            if label != hetzner.label_time(due):
                raise ValueError(f"Hetzner reads the box's delete_by back as {label!r}, not {hetzner.label_time(due)}")
        st["delete_by"] = due
    elif box:
        label = (api("GET", f"/servers/{box}")["server"].get("labels") or {}).get("delete_by")
    if keep_minutes is not None:
        st["keep_minutes"] = int(keep_minutes)
        if st["state"] == "done":
            st["done_at"] = now  # a kept box's keep counts from now
            st["box_state"] = f"kept {st['keep_minutes']} min for follow-ups"
    return label


@app.tool(name="cfd_keep", speaks_for_person=True, schema={
    "type": "object",
    "properties": {
        "study": {"type": "string", "description": "The study (its id or name)."},
        "keep_minutes": {"type": "integer", "description": "How long the box stays up once the matrix ends (on a study already done and kept: from now; 0 deletes it at its next look)."},
        "delete_by": {"description": "Its new delete-by: hours from now (a number), or an ISO time (local unless it gives its zone), at most 24 h after the study was made. Later keeps the box (and its bill) longer; sooner takes it down sooner."},
    },
    "required": ["study"],
})
def keep(study, ctx=None, keep_minutes=None, delete_by=None):
    "Pushes a live study's box self-delete back or pulls it in: its keep after the matrix ends and its hard delete-by (the box's Hetzner label too). Answers both, and the label read back from Hetzner."
    state["calls"] += 1
    try:
        with holding(find(study)["id"]) as st:
            label = keep_study(st, keep_minutes=keep_minutes, delete_by=delete_by)
            emit_study(st, force=True)
        return {"study": st["id"], "state": st["state"], "keep_minutes": st["keep_minutes"],
                "delete_by": pt(st["delete_by"]), "delete_by_ms": st["delete_by"], "label": label,
                "box_state": st.get("box_state")}
    except Exception as e:
        state["failed"] += 1
        state["last_error"] = f"keep: {e}"
        raise


@app.tool(name="cfd_cancel", schema={
    "type": "object",
    "properties": {"study": {"type": "string"},
                   "cases": {"type": "array", "items": {"type": "string"}, "description": "Cases to stop (default: every one not finished)."}},
    "required": ["study"],
})
def cancel(study, ctx, cases=None):
    "Stops cases (queued ones never start). Once nothing's left running the study is done as usual: fetched, then deleted."
    state["calls"] += 1
    st = refreshed(find(study)["id"])
    live(st)
    cs = {c["name"]: c for c in (st.get("status") or {}).get("cases") or []}
    unknown = [n for n in cases or [] if n not in cs]
    if unknown:
        raise ValueError(f"no case {', '.join(unknown)} in {st['id']}")
    names = cases or [n for n, c in cs.items() if c.get("state") not in ("done", "failed", "cancelled")]
    with lock(st["id"]):
        for n in names:
            drop_request(st, n, {"cancel": True})
    return {"study": st["id"], "cancelling": names}


@app.tool(name="cfd_fetch", schema={
    "type": "object",
    "properties": {"study": {"type": "string"},
                   "cases": {"type": "array", "items": {"type": "string"}, "description": "Only these cases (default: all)."},
                   "fields": {"type": "boolean", "description": "Bring each case's latest time folder too (its fields: can be big)."},
                   "include": {"type": "array", "items": {"type": "string"}, "description": "More to bring: globs in a case (VTK/*, 300/U)."},
                   "exclude": {"type": "array", "items": {"type": "string"}, "description": "Globs to leave out."}},
    "required": ["study"],
})
def fetch(study, cases=None, fields=False, include=None, exclude=None):
    """Brings results here now: each case less its mesh, processor and time folders (logs, postProcessing, what post
    wrote; the latest fields with fields), plus status.json and matrix.json, into the study's results folder."""
    state["calls"] += 1
    with holding(find(study)["id"]) as st:
        live(st)
        n, size = fetch_results(st, fields=fields, include=include or [], exclude=exclude or [], cases=cases)
    return {"study": st["id"], "results": st["results"], "files": n, "mb": round(size / 1e6, 1)}


@app.tool(name="cfd_shell", schema={
    "type": "object",
    "properties": {"study": {"type": "string"},
                   "command": {"type": "string", "description": "A shell command, run on the box in the study folder (/root/study), with OpenFOAM's environment and the venv on PATH; the cases are in /root/cases."},
                   "sync": {"type": "boolean", "description": "Send the study folder again first (default true), so scripts changed here run there."},
                   "timeout_s": {"type": "integer", "description": "How long it may run (default 300, at most 540)."}},
    "required": ["study", "command"],
})
def shell(study, command, sync=True, timeout_s=300):
    "Runs a command on a study's box (a post-processing script against the cases, a look at a log) and answers its output."
    state["calls"] += 1
    st = find(study)
    live(st)
    timeout_s = max(5, min(int(timeout_s), 540))
    if sync:
        with holding(st["id"]) as st:
            if st["origin"] != st["src"]:
                st["src"], _ = resolve_source(st["id"], st["origin"])
            upload_study(st)
    prefix = (f"source {shlex.quote(st['of_bashrc'])} >/dev/null 2>&1; " if st.get("of_bashrc") else "") + \
             f"export PATH={BOX}/venv/bin:$PATH STUDY={BOX_STUDY} CASES={BOX_CASES}; cd {BOX_STUDY} && "
    code, out, err = remote(st, prefix + command, timeout=timeout_s)
    out, err = text(out), text(err)
    return {"exit": code, "stdout": out[-20000:], "stderr": err[-5000:], "cut": len(out) > 20000}


@app.tool(name="cfd_delete", schema={
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
    "How cfd itself is: its token, the studies it holds, its last poll and error."
    return dict(state, token=bool(hetzner.token()),
                active=[s["id"] for s in all_studies() if s["state"] in ACTIVE])


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


OPENFOAM = [
    # the newest ESI OpenFOAM in its Debian repo (openfoam2606-default and so on), its bashrc's path kept
    "curl -s https://dl.openfoam.com/add-debian-repo.sh | bash > /root/of-repo.log 2>&1",
    "V=$(apt-cache search --names-only '^openfoam2[0-9]+-default$' | awk '{print $1}' | sort | tail -1); "
    "[ -n \"$V\" ] || { echo 'no openfoam package in the repo' > /root/of-install.log; exit 1; }; "
    "DEBIAN_FRONTEND=noninteractive apt-get install -y $V > /root/of-install.log 2>&1 && "
    "echo /usr/lib/openfoam/${V%-default}/etc/bashrc > /root/of-bashrc",
]


def cloud_init(pubkey, pip=(), setup=()):
    """The box's first boot: the appendage's key for root, OpenFOAM, a venv with the study's Python packages,
    then /root/cfd-ready (or /root/cfd-failed with the step that failed)."""
    steps = list(OPENFOAM)
    pkgs = " ".join(["numpy"] + [p for p in pip if p != "numpy"])
    steps.append(f"python3 -m venv /root/venv && /root/venv/bin/pip install -q {pkgs} > /root/pip.log 2>&1")
    steps += list(setup)
    script = ["set -o pipefail"]
    names = ["adding the OpenFOAM repository", "installing OpenFOAM", "making the venv"] + [f"setup step {i + 1}" for i in range(len(setup))]
    for i, s in enumerate(steps):
        script.append(f"( {s} ) || {{ echo {shlex.quote(names[i] + ' failed')} > /root/cfd-failed; exit 1; }}")
    script.append("touch /root/cfd-ready")
    lines = [
        "#cloud-config",
        "ssh_authorized_keys:",
        f"  - {pubkey.strip()}",
        "package_update: true",
        "packages: [python3-venv, python3-pip, curl, htop, rsync]",
        "write_files:",
        "  - path: /root/cfd-setup.sh",
        "    permissions: '0700'",
        "    content: |",
        "      #!/bin/bash",
    ] + [f"      {line}" for line in script] + [
        "runcmd:",
        "  - [bash, -lc, '/root/cfd-setup.sh > /root/cfd-setup.log 2>&1']",
    ]
    return "\n".join(lines) + "\n"
