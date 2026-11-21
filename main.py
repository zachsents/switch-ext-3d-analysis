"""analysis: 3D analysis of parts and models as one extension, on the one server in server.py: printability geometry and
mesh-python (mesh.py), FEA and CFD on rented Hetzner boxes (fea.py, cfd.py), photoreal Blender renders (render.py),
and a listing's photos with sizes measured off them (photo.py). Each was its own Switchboard appendage until now.
"""
import time

import cfd
import fea
import mesh
import photo
import render
import sbmcp
from server import app

DAY_MS = 86_400_000


def line(kind, st, overall):
    """A study as its widget row draws it: its ring (running, done, failed), name, how far, and when its box goes."""
    active = st["state"] in ("booting", "installing", "running")
    counts = (st.get("status") or {}).get("counts") or {}
    total = sum(counts.values())
    finished = sum(counts.get(k, 0) for k in ("done", "failed", "cancelled"))
    wrong = st.get("error")
    return {"id": f"{kind}-{st['id']}", "label": f"{kind.upper()} {st.get('name') or st['id']}",
            "mark": "running" if active else "stop" if st.get("failed") or wrong else "",
            "detail": wrong or (st.get("box_state") if active else "") or "", "alert": bool(wrong),
            "values": [f"{finished}/{total}"] if total else [],
            "progress": round(overall(st.get("status")) * 100) if active else None,
            "endsAt": st.get("delete_by") if active else None}


@app.tool(name="studies", read_only=True)
def studies():
    "The FEA and CFD studies running now or ended in the last day, as the studies widget draws them."
    now = int(time.time() * 1000)
    rows = []
    for kind, part in (("fea", fea), ("cfd", cfd)):
        for st in part.all_studies():
            ended = st.get("deleted_at") or st.get("done_at") or st.get("created") or 0
            if st["state"] in part.ACTIVE or now - ended < DAY_MS:
                rows.append((st.get("created") or 0, line(kind, st, part.overall)))
    return {"studies": [r for _, r in sorted(rows, key=lambda x: x[0], reverse=True)]}


app.widget("studies", "Studies", lambda: sbmcp.surface("studies", [
    {"id": "root", "component": "Lines", "children": {"componentId": "study", "path": "/studies"}},
    {"id": "study", "component": "Row", "label": {"@path": "label"}, "mark": {"@path": "mark"},
     "detail": {"@path": "detail"}, "alert": {"@path": "alert"}, "values": {"@path": "values"},
     "progress": {"@path": "progress"}, "endsAt": {"@path": "endsAt"}},
]), tool="studies", refresh=["analysis.fea_study", "analysis.cfd_study"])


@app.tool(name="health", read_only=True)
def health():
    "How each part is: mesh's environment (versions, Embree, Blender), render's Blender and AI key, photo's counts, fea's and cfd's Hetzner key and the studies they hold."
    out = {}
    for name, fn in (("mesh", mesh.health_facts), ("render", render.health), ("photo", photo.health),
                     ("fea", fea.health), ("cfd", cfd.health)):
        try:
            out[name] = fn()
        except Exception as e:  # noqa: BLE001
            out[name] = {"error": f"{type(e).__name__}: {e}"[:300]}
    return out


if __name__ == "__main__":
    app.run()
