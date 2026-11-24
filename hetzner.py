"""Hetzner Cloud: the one place fea and cfd reach it (api()), with the user's Hetzner key (a connection).

Every box carries labels temp=true, made_by=fea|cfd, study=<the study's id> and delete_by=<YYYYMMDDTHHMMZ> (UTC),
so anything left behind says whose it is and when it goes.
"""

import datetime
import json
import math
import urllib.error
import urllib.request

from server import key

API = "https://api.hetzner.cloud/v1"


class HetznerError(RuntimeError):
    def __init__(self, status, code, message):
        super().__init__(f"Hetzner {status} {code}: {message}")
        self.status, self.code, self.message = status, code, message


def token():
    """The user's Hetzner Cloud API token (a key connection), or None."""
    return key("hetzner")


def api(method, path, body=None, timeout=30):
    """One call to Hetzner's API: its JSON answer. Raises HetznerError with what Hetzner said."""
    tok = token()
    if not tok:
        raise HetznerError(0, "no_token", "no Hetzner key: connect Hetzner Cloud in Switch's connections")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read()).get("error") or {}
        except ValueError:
            err = {}
        raise HetznerError(e.code, err.get("code", "http_error"), err.get("message", str(e)))
    except urllib.error.URLError as e:
        raise HetznerError(0, "unreachable", str(e.reason))


def label_time(ms):
    """A time as a label value, in UTC: 20261119T1100Z (Hetzner label values allow only [A-Za-z0-9._-], no '+')."""
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y%m%dT%H%MZ")


def parse_label_time(text):
    """A delete_by label back to ms since 1970, or None. Reads 20261119T1100Z and the older 20261119T1100+0000."""
    if not isinstance(text, str):
        return None
    t = text.strip()
    if t.endswith("Z"):
        t = t[:-1] + "+0000"
    try:
        return int(datetime.datetime.strptime(t, "%Y%m%dT%H%M%z").timestamp() * 1000)
    except ValueError:
        return None


def price(server_type, location):
    """(hourly price with VAT, currency) for a type in a location, from Hetzner's pricing, or (None, None)."""
    try:
        p = api("GET", "/pricing")["pricing"]
    except (HetznerError, KeyError):
        return None, None
    for st in p.get("server_types") or []:
        if st.get("name") == server_type:
            for pr in st.get("prices") or []:
                if pr.get("location") == location:
                    return float(pr["price_hourly"]["gross"]), p.get("currency")
    return None, p.get("currency")


def cost(created_ms, ended_ms, hourly):
    """What a box cost: Hetzner bills each started hour."""
    if hourly is None:
        return None, None
    hours = max(1, math.ceil((ended_ms - created_ms) / 3_600_000))
    return round(hours * hourly, 3), hours


def inventory():
    """Everything in the project that costs money while it exists: servers (with their labels), primary and
    floating IPs, volumes, snapshots and backups."""
    out = {}
    for key, path in (("servers", "/servers"), ("primary_ips", "/primary_ips"), ("floating_ips", "/floating_ips"),
                      ("volumes", "/volumes"), ("images", "/images?type=snapshot&type=backup")):
        items = api("GET", path).get(key) or []
        if key == "servers":
            out[key] = [{"id": s["id"], "name": s["name"], "status": s.get("status"), "type": (s.get("server_type") or {}).get("name"),
                         "created": s.get("created"), "labels": s.get("labels") or {},
                         "ip": ((s.get("public_net") or {}).get("ipv4") or {}).get("ip")} for s in items]
        else:
            out[key] = [{"id": i["id"], "name": i.get("name") or i.get("description"), "labels": i.get("labels") or {}}
                        for i in items]
    return out
