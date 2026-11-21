"""photo: a product listing's photos, and a part's size measured off a photo against a known pitch or length.

Sizing a connector off its Amazon listing used to be an hour of hand work: pull the image ids and variant JSON out of
the product page with browser JS, curl each photo, then throwaway PIL/scipy scripts to find the pads, fit their pitch
in pixels, crop and zoom, and scale the body by it. This is that, in a call or two:

- photos(listing): every photo of the listing (this variant's, or every variant's), at full size, numbered on one
  contact sheet, each said to be a photo, a drawing or an infographic, with the numbers printed on it (macOS Vision),
  the sizes its title, bullets and spec tables state, and, given a pitch, every photo measured.
- measure(photo): finds rows of evenly spaced features (pads, pins, holes) by itself, and measures the part along and
  across each row, its length at offsets from it, and the features in pairs either side of it, in mm against a known
  pitch, a known length, or two points a known distance apart, with a picture of what it measured drawn on the photo.
- look(photo, box): a region zoomed, with a grid in the photo's own pixels, for picking those points.

Amazon pages load by curl; when Amazon puts up a human check, they load in a background tab of the user's browser
(switch-ext-browser, through browser.py), and are never solved. Other stores' pages give their og:image, schema.org images
and large <img>s. Image work runs in photo_worker.py in mesh's environment (meshpython.py: niced, memory-capped).
"""

import concurrent.futures
import functools
import hashlib
import html
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.parse

import sbmcp
from browser import Browser, BrowserError
from server import app, part_home

NAME = "photo-measure"
HERE = os.path.dirname(os.path.abspath(__file__))
HOME = part_home("photo")
"""Where it keeps listings' photos and what it fetched."""
HEARTBEAT_S = float(os.environ.get("PHOTO_MEASURE_HEARTBEAT_S") or 1800)
SESSION = "photo-measure"
WORKER = os.path.join(HERE, "photo_worker.py")
WITH = ["--with", "pyobjc-framework-Vision", "--with", "pyobjc-framework-Quartz"]
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
MAX_PHOTOS = 30
CACHE_S = 6 * 3600
"""A listing's page is read again after this long, or with refresh."""
BROWSER = Browser(app, session=SESSION)

app.event("listing", "A listing's photos and stated sizes, as last looked up", stream="parts/listings", kind="observation")
app.event("measurement", "What a photo measured", stream="parts/measurements", kind="observation")

state = {"started_at": int(time.time() * 1000), "calls": 0, "failed": 0, "last_error": None, "listings": 0,
         "photos": 0, "measured": 0, "chrome_pages": 0, "last": None}
locks = {}
locks_lock = threading.Lock()


class Blocked(Exception):
    """The store put up a human check, to curl and in Chrome."""


# The world outside, in these few functions, so tests put fakes here. ------------------------------------------------


def http_get(url, timeout=30):
    """A page's text by curl, which Amazon serves where Python's own client gets a human check."""
    p = subprocess.run(["curl", "-s", "-L", "--compressed", "--max-time", str(timeout), "-A", UA,
                        "-H", "Accept-Language: en-US,en;q=0.9", "-w", "\n@@STATUS@@%{http_code}", url],
                       capture_output=True, timeout=timeout + 10)
    out = p.stdout.decode("utf-8", "ignore")
    body, _, code = out.rpartition("\n@@STATUS@@")
    return int(code or 0), body


def download(url, path, timeout=60):
    """One photo to path; raises when it isn't one."""
    tmp = path + ".part"
    p = subprocess.run(["curl", "-s", "-L", "--fail", "--max-time", str(timeout), "-A", UA, "-o", tmp, url],
                       capture_output=True, timeout=timeout + 10)
    if p.returncode != 0:
        raise RuntimeError(f"couldn't download {url} (curl {p.returncode})")
    os.replace(tmp, path)


def browser(args, timeout=90):
    """One op in the user's browser (browser.py): answers {ok, output, error}."""
    try:
        return {"ok": True, "output": BROWSER.op(args, timeout=timeout)}
    except BrowserError as e:
        return {"ok": False, "error": str(e)[:400]}


def worker(command, args, timeout=300):
    """The image work, in photo_worker.py under meshpython.py (mesh's environment, niced and memory-capped): answers
    its JSON. It runs in a process group of its own, so a timeout stops all of it, uv and Python included."""
    p = subprocess.Popen([sys.executable, os.path.join(HERE, "meshpython.py"), *WITH, "-I", WORKER, command], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        out, err = p.communicate(json.dumps(args), timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        p.communicate()
        raise RuntimeError(f"the image worker took over {timeout} s on {command}, and was stopped")
    out = out.strip()
    try:
        res = json.loads(out[out.find("{"):]) if "{" in out else None
    except ValueError:
        res = None
    if res is None:
        raise RuntimeError(f"the image worker failed: {(err or out).strip()[-600:]}")
    if res.get("error"):
        raise RuntimeError(res["error"])
    return res


# Listings -----------------------------------------------------------------------------------------------------------

ASIN = re.compile(r"^[A-Z0-9]{10}$")


def parse_listing(listing):
    """{site, key, url} for an ASIN, an Amazon link, or any other product page."""
    s = str(listing or "").strip()
    if not s:
        raise ValueError("give a listing: an ASIN, an Amazon link or a product page's URL")
    if ASIN.match(s.upper()) and not s.lower().startswith("http"):
        a = s.upper()
        return {"site": "amazon", "key": a, "url": f"https://www.amazon.com/dp/{a}"}
    if not re.match(r"^https?://", s):
        s = "https://" + s
    u = urllib.parse.urlparse(s)
    host = u.netloc.lower()
    if re.search(r"(^|\.)amazon\.[a-z.]+$", host) or host in ("a.co", "amzn.to"):
        m = re.search(r"/(?:dp|gp/product|gp/aw/d|product)/([A-Z0-9]{10})", u.path, re.I)
        if m:
            a = m.group(1).upper()
            return {"site": "amazon", "key": a, "url": f"https://{host if 'amazon.' in host else 'www.amazon.com'}/dp/{a}"}
    slug = re.sub(r"[^a-z0-9]+", "-", (host.replace("www.", "") + u.path).lower()).strip("-")[:60]
    return {"site": host.replace("www.", ""), "key": f"{slug}-{hashlib.sha1(s.encode()).hexdigest()[:8]}", "url": s}


def _strip(fragment):
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", fragment or "", flags=re.S | re.I)
    t = re.sub(r"<br\s*/?>", "\n", t, flags=re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t)
    t = re.sub(r"[ \t\r\f\v‎‏]+", " ", t)
    return re.sub(r"\s*\n\s*", "\n", t).strip()


def _section(page, ident, length=60000):
    """The HTML from the element with this id on (a slice, since nothing here needs it parsed whole)."""
    i = page.find(f'id="{ident}"')
    return page[i:i + length] if i >= 0 else ""


def _js_string(s, start):
    """A JavaScript string literal starting at s[start] (its quote), decoded."""
    q, i, out = s[start], start + 1, []
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            n = s[i + 1]
            if n == "u" and i + 5 < len(s):
                try:
                    out.append(chr(int(s[i + 2:i + 6], 16)))
                    i += 6
                    continue
                except ValueError:
                    pass
            out.append({"n": "\n", "t": "\t", "r": "\r"}.get(n, n))
            i += 2
            continue
        if c == q:
            return "".join(out)
        out.append(c)
        i += 1
    raise ValueError("unterminated string")


def _amazon_url(u):
    """A photo's largest form, without the experiment folder Amazon sometimes puts in front of it."""
    return re.sub(r"/images/W/[^/]+/images/I/", "/images/I/", u)


def amazon_images(page):
    """This variant's photos, every variant's by name, the variant this page is, and each variant's ASIN."""
    initial, variants, landing, to_asin = [], {}, None, {}
    for m in re.finditer(r"parseJSON\((['\"])", page):
        try:
            data = json.loads(_js_string(page, m.start(1)))
        except (ValueError, IndexError):
            continue
        if isinstance(data, list) and data and isinstance(data[0], dict) and ("hiRes" in data[0] or "large" in data[0]):
            if not initial:
                initial = [_amazon_url(x.get("hiRes") or x.get("large")) for x in data if x.get("hiRes") or x.get("large")]
        elif isinstance(data, dict) and isinstance(data.get("colorImages"), dict):
            for name, imgs in data["colorImages"].items():
                variants[name] = [_amazon_url(x.get("hiRes") or x.get("large")) for x in imgs or []
                                  if isinstance(x, dict) and (x.get("hiRes") or x.get("large"))]
            landing = data.get("landingAsinColor") or landing
            for name, v in (data.get("colorToAsin") or {}).items():
                if isinstance(v, dict) and v.get("asin"):
                    to_asin[name] = v["asin"]
    if not initial:
        initial = [_amazon_url(u) for u in dict.fromkeys(re.findall(r'"hiRes":"(https://[^"]+)"', page))]
    if landing and landing not in (None, "initial") and variants.get(landing) and not initial:
        initial = variants[landing]
    return initial, variants, landing, to_asin


def amazon_text(page):
    """Its title, bullets, description and spec rows."""
    title = _strip(re.search(r'id="productTitle"[^>]*>(.*?)</span>', page, re.S).group(1)) \
        if re.search(r'id="productTitle"[^>]*>(.*?)</span>', page, re.S) else ""
    bullets = []
    fb = _section(page, "feature-bullets", 20000)
    for li in re.findall(r"<li[^>]*>(.*?)</li>", fb, re.S):
        t = _strip(li)
        if t and t not in bullets:
            bullets.append(t)
    desc = ""
    d = _section(page, "productDescription", 20000)
    if d:
        d = d[d.find(">") + 1:]
        end = d.find("</div>")
        desc = _strip(d[:end if end > 0 else 6000])[:3000]
    specs = {}
    for ident in ("productOverview_feature_div", "prodDetails", "productDetails_techSpec_section_1",
                  "technicalSpecifications_feature_div"):
        sec = _section(page, ident, 60000)
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", sec, re.S):
            cells = [_strip(c) for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)]
            cells = [c for c in cells if c]
            if len(cells) == 2 and len(cells[0]) < 60 and cells[0] not in specs:
                specs[cells[0]] = cells[1][:300]
    for li in re.findall(r"<li[^>]*>(.*?)</li>", _section(page, "detailBullets_feature_div", 20000), re.S):
        t = _strip(li)
        if ":" in t:
            k, _, v = t.partition(":")
            k = k.strip(" \n:")
            if k and len(k) < 60 and k not in specs:
                specs[k] = v.strip()[:300]
    return title, bullets, desc, specs


CHROME_JS = r"""(() => {
  const txt = s => { const e = document.querySelector(s); return e ? e.innerText.trim() : ''; };
  const lits = [];
  for (const t of [...document.scripts].map(x => x.textContent).filter(t => t.includes('parseJSON('))) {
    let i = 0;
    while ((i = t.indexOf('parseJSON(', i)) >= 0) {
      const q = t[i + 10];
      if (q !== "'" && q !== '"') { i += 10; continue; }
      let j = i + 11;
      while (j < t.length && !(t[j] === q && t[j - 1] !== '\\')) j++;
      const lit = t.slice(i + 10, j + 1);
      if (lit.includes('hiRes') || lit.includes('colorImages')) lits.push('parseJSON(' + lit + ')');
      i = j;
    }
  }
  const html = ['<span id="productTitle">' + txt('#productTitle') + '</span>',
    '<div id="feature-bullets"><ul>' + [...document.querySelectorAll('#feature-bullets li')].map(l => '<li>' + l.innerText + '</li>').join('') + '</ul></div>',
    '<div id="productDescription">' + txt('#productDescription') + '</div>',
    '<div id="prodDetails"><table>' + [...document.querySelectorAll('#prodDetails tr, #productOverview_feature_div tr, #technicalSpecifications_feature_div tr')].map(tr => '<tr>' + [...tr.querySelectorAll('th,td')].map(c => '<td>' + c.innerText + '</td>').join('') + '</tr>').join('') + '</table></div>',
    '<div id="detailBullets_feature_div"><ul>' + [...document.querySelectorAll('#detailBullets_feature_div li')].map(l => '<li>' + l.innerText + '</li>').join('') + '</ul></div>',
    '<script>' + lits.join('\n') + '</script>'].join('\n');
  window.__photoMeasure = JSON.stringify({captcha: !!document.querySelector('form[action*="validateCaptcha"]'), html});
  return window.__photoMeasurePart(0);
})()"""
CHUNK = 15000
"""What one browser answer carries of the page (it cuts answers at about 24,000 characters)."""
PART_JS = r"""window.__photoMeasurePart = (start) => { const t = window.__photoMeasure || '';
  return '@@PM@@' + JSON.stringify({len: t.length, end: Math.min(t.length, start + %d), part: t.slice(start, start + %d)}); }; 'ok'""" % (CHUNK, CHUNK)


def _answer(res):
    out = res.get("output")
    text = out if isinstance(out, str) else json.dumps(out) if out is not None else ""
    if not res.get("ok") or "@@PM@@" not in text:
        raise RuntimeError(f"Chrome couldn't read the page: {res.get('error') or text[:300] or 'it answered nothing'}")
    raw = text[text.index("@@PM@@") + 6:]
    raw = raw[:raw.rfind("}") + 1]
    return json.loads(raw)


def chrome_page(url):
    """The page's parts this reads, from a background tab of the user's browser."""
    state["chrome_pages"] += 1
    steps = [{"op": "navigate", "url": url}, {"op": "wait", "seconds": 2}, {"op": "js", "code": PART_JS},
             {"op": "js", "code": CHROME_JS, "timeout": 30}]
    piece = _answer(browser({"op": "steps", "session": SESSION, "steps": steps}, timeout=90))
    text = piece["part"]
    while piece["end"] < piece["len"]:
        piece = _answer(browser({"op": "js", "session": SESSION, "code": f"window.__photoMeasurePart({piece['end']})"},
                                timeout=30))
        text += piece["part"]
    try:
        browser({"op": "release", "session": SESSION}, timeout=20)
    except Exception:
        pass
    data = json.loads(text)
    if data.get("captcha"):
        raise Blocked("Amazon put up a human check in Chrome too")
    return data["html"]


GENERIC_IMG = re.compile(r"""(?:content|src|href|data-src|data-old-hires|data-zoom-image)\s*=\s*["']((?:https?:)?//[^"'\s>]+?\.(?:jpe?g|png|webp)(?:\?[^"'\s>]*)?)["']""", re.I)


def generic_images(page, url):
    """A product page's photos: og:image and twitter:image, schema.org's image, eBay's and AliExpress's galleries, and
    the page's other large-looking images, best first."""
    found = []

    def add(u):
        u = html.unescape(u.strip())
        if u.startswith("//"):
            u = "https:" + u
        u = urllib.parse.urljoin(url, u)
        if u not in found:
            found.append(u)

    for m in re.finditer(r'<meta[^>]+(?:property|name)=["\'](?:og:image|twitter:image)(?::src)?["\'][^>]*content=["\']([^"\']+)', page, re.I):
        add(m.group(1))
    for m in re.finditer(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', page, re.S | re.I):
        try:
            data = json.loads(m.group(1))
        except ValueError:
            continue
        stack = [data]
        while stack:
            d = stack.pop()
            if isinstance(d, list):
                stack += d
            elif isinstance(d, dict):
                img = d.get("image")
                for x in img if isinstance(img, list) else [img] if img else []:
                    add(x["url"] if isinstance(x, dict) and x.get("url") else str(x))
                stack += [v for v in d.values() if isinstance(v, (dict, list))]
    for gid in dict.fromkeys(re.findall(r"i\.ebayimg\.com/images/g/([A-Za-z0-9~_-]+)/s-l\d+", page)):
        add(f"https://i.ebayimg.com/images/g/{gid}/s-l1600.jpg")
    m = re.search(r'"imagePathList"\s*:\s*\[(.*?)\]', page)
    if m:
        for u in re.findall(r'"(https?:[^"]+)"', m.group(1)):
            add(u.replace("\\/", "/"))
    for m in GENERIC_IMG.finditer(page):
        u = m.group(1)
        if re.search(r"(sprite|icon|logo|favicon|badge|pixel|spacer|avatar|flag)", u, re.I) or "i.ebayimg.com/images/g/" in u:
            continue
        add(u)
    # a store's page shows other products too: when several photos are named for this one, keep those
    ids = [t for t in re.findall(r"[A-Za-z0-9]+", urllib.parse.urlparse(url).path) if len(t) >= 4 and re.search(r"\d", t)]
    own = [u for u in found if any(re.search(rf"(?<![A-Za-z0-9]){re.escape(t)}(?![0-9])", u) for t in ids)]
    return own if len(own) >= 3 else found


def generic_text(page):
    title = ""
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]*content=["\']([^"\']+)', page, re.I) or \
        re.search(r"<title[^>]*>(.*?)</title>", page, re.S | re.I)
    if m:
        title = _strip(m.group(1))[:300]
    desc = ""
    m = re.search(r'<meta[^>]+(?:property|name)=["\'](?:og:description|description)["\'][^>]*content=["\']([^"\']+)', page, re.I)
    if m:
        desc = _strip(m.group(1))[:3000]
    specs = {}
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S)[:400]:
        cells = [c for c in (_strip(c) for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)) if c]
        if len(cells) == 2 and len(cells[0]) < 60 and len(cells[1]) < 300 and cells[0] not in specs:
            specs[cells[0]] = cells[1]
    return title, [], desc, specs


UNIT = {"mm": 1.0, "millimeter": 1.0, "millimeters": 1.0, "cm": 10.0, "centimeter": 10.0, "centimeters": 10.0,
        "in": 25.4, "inch": 25.4, "inches": 25.4, '"': 25.4, "″": 25.4, "m": 1000.0, "meter": 1000.0, "meters": 1000.0}
SIZE = re.compile(r"(\d+(?:\.\d+)?)(?:\s*[x×*]\s*(\d+(?:\.\d+)?))?(?:\s*[x×*]\s*(\d+(?:\.\d+)?))?\s*"
                  r"(millimeters?|mm|centimeters?|cm|inches|inch|in\b|meters?\b|m\b|\"|″)", re.I)


NOT_SIZES = re.compile(r"rank|review|asin|date|upc|ean|part number|model|number of|count|quantity|warranty", re.I)


def stated_sizes(title, bullets, desc, specs):
    """Every size the listing states in words: where, the words around it, and each number in mm."""
    out = []
    places = [("title", title)] + [("bullet", b) for b in bullets] + [("description", desc)] + \
        [(f"spec: {k}", f"{k}: {v}") for k, v in specs.items() if not NOT_SIZES.search(k)]
    seen = set()
    for where, text in places:
        for m in SIZE.finditer(text or ""):
            if m.start() > 0 and re.match(r"[#,.\d]", text[m.start() - 1]):
                continue  # part of a larger number (a rank, a count)
            unit = m.group(4).lower()
            if unit == "in" and re.match(r"\s+[A-Z]", text[m.end():m.end() + 2]):
                continue  # "in Tools & Home Improvement"
            if unit in ("m", "meter", "meters") and not re.search(r"\d\s*m(eters?)?\b", m.group(0)):
                continue
            f = UNIT.get(unit, UNIT.get(unit.rstrip("s"), 1.0))
            nums = [float(g) * f for g in m.groups()[:3] if g]
            a, b = max(0, m.start() - 50), min(len(text), m.end() + 30)
            snippet = re.sub(r"\s+", " ", text[a:b]).strip()
            key = (where, m.group(0).lower())
            if key in seen:
                continue
            seen.add(key)
            out.append({"where": where, "says": m.group(0), "mm": [round(x, 3) for x in nums], "context": snippet})
    return out[:40]


def stated_pitch(stated):
    """The pitch the listing states, when it states one: (mm, where it says so)."""
    for s in stated:
        if re.search(r"pitch|spacing|centers?\b|centres?\b", s["context"], re.I) and s["mm"] and s["mm"][0] < 20:
            return s["mm"][0], f"{s['where']}: “{s['context']}”"
    return None, None


def _lock(key):
    with locks_lock:
        return locks.setdefault(key, threading.Lock())


def listing_dir(key):
    d = os.path.join(HOME, "listings", re.sub(r"[^A-Za-z0-9._-]+", "-", key))
    os.makedirs(d, exist_ok=True)
    return d


def read_listing(listing, variants=False, refresh=False):
    """The listing's page read (cached for CACHE_S) and its photos downloaded: its saved record."""
    info = parse_listing(listing)
    d = listing_dir(info["key"])
    saved = os.path.join(d, "listing.json")
    with _lock(info["key"]):
        if os.path.exists(saved) and not refresh:
            with open(saved) as f:
                rec = json.load(f)
            if time.time() - rec.get("read_at", 0) < CACHE_S and (rec.get("variants_asked") or not variants):
                return rec
        how = "curl"
        status, page = http_get(info["url"])
        if info["site"] == "amazon":
            blocked = "validateCaptcha" in page or "colorImages" not in page and '"hiRes"' not in page
            if blocked:
                page = chrome_page(info["url"])
                how = "chrome"
            if "productTitle" not in page and "colorImages" not in page:
                raise RuntimeError(f"Amazon has no listing {info['key']} (or it's gone)")
            initial, by_variant, landing, to_asin = amazon_images(page)
            title, bullets, desc, specs = amazon_text(page)
        else:
            if status >= 400 or len(page) < 500:
                raise RuntimeError(f"{info['url']} answered {status}")
            initial, by_variant, landing, to_asin = generic_images(page, info["url"]), {}, None, {}
            title, bullets, desc, specs = generic_text(page)
        urls = list(initial)
        from_variant = {u: landing or "this one" for u in urls}
        if variants:
            for name, us in by_variant.items():
                for u in us:
                    if u not in from_variant:
                        urls.append(u)
                        from_variant[u] = name
        urls = urls[:MAX_PHOTOS * 2]
        photos = []

        def fetch_one(i, u):
            ext = os.path.splitext(urllib.parse.urlparse(u).path)[1].lower()
            ext = ext if ext in (".jpg", ".jpeg", ".png", ".webp") else ".jpg"
            name = re.sub(r"[^A-Za-z0-9._+-]+", "-", os.path.basename(urllib.parse.urlparse(u).path))[:80] or f"photo{i}"
            name = re.sub(r"\.(jpe?g|png|webp)$", "", name, flags=re.I)
            path = os.path.join(d, f"{name}-{hashlib.sha1(u.encode()).hexdigest()[:6]}{ext}")
            if not os.path.exists(path) or refresh:
                download(u, path)
            with open(path, "rb") as f:
                head = f.read(16)
            if not (head.startswith(b"\xff\xd8") or head.startswith(b"\x89PNG") or head[8:12] == b"WEBP"):
                os.remove(path)
                raise RuntimeError("not an image")
            return path

        with concurrent.futures.ThreadPoolExecutor(6) as pool:
            futs = [(u, pool.submit(fetch_one, i, u)) for i, u in enumerate(urls)]
            for u, fut in futs:
                try:
                    photos.append({"url": u, "path": fut.result(), "variant": from_variant.get(u)})
                except Exception:
                    pass
        photos = keep_photos(photos)
        if not photos:
            raise RuntimeError(f"found no photos on {info['url']}")
        rec = dict(info, title=title, bullets=bullets, description=desc, specs=specs, variant=landing,
                   variants=sorted(by_variant), variant_asins=to_asin, read_by=how, read_at=time.time(),
                   variants_asked=bool(variants), photos=photos)
        with open(saved, "w") as f:
            json.dump(rec, f)
        return rec


MIN_SIDE = 300
"""Photos smaller than this on their long side are thumbnails, left out when larger ones are there."""


def image_size(path):
    """(width, height) from a JPEG's, PNG's or WebP's header, or None."""
    import struct
    with open(path, "rb") as f:
        head = f.read(64)
        if head.startswith(b"\x89PNG") and len(head) >= 24:
            return struct.unpack(">II", head[16:24])
        if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
            if head[12:16] == b"VP8X":
                return 1 + int.from_bytes(head[24:27], "little"), 1 + int.from_bytes(head[27:30], "little")
            if head[12:16] == b"VP8 ":
                w, h = struct.unpack("<HH", head[26:30])
                return w & 0x3FFF, h & 0x3FFF
            if head[12:16] == b"VP8L":
                b = head[21:25]
                return 1 + (((b[1] & 0x3F) << 8) | b[0]), 1 + (((b[3] & 0xF) << 10) | (b[2] << 2) | ((b[1] & 0xC0) >> 6))
            return None
        if not head.startswith(b"\xff\xd8"):
            return None
        f.seek(2)
        while True:
            m = f.read(2)
            if len(m) < 2 or m[0] != 0xFF:
                return None
            while m[1] == 0xFF:
                m = m[:1] + f.read(1)
            if 0xC0 <= m[1] <= 0xCF and m[1] not in (0xC4, 0xC8, 0xCC):
                f.read(3)
                h, w = struct.unpack(">HH", f.read(4))
                return w, h
            (n,) = struct.unpack(">H", f.read(2))
            f.seek(n - 2, 1)


def keep_photos(photos):
    """The page's photos without thumbnails, and each picture once (the largest of the sizes a store serves it in)."""
    best = {}
    for p in photos:
        try:
            p["size"] = image_size(p["path"])
        except Exception:
            p["size"] = None
        w, h = p["size"] or (0, 0)
        stem = re.sub(r"-[0-9a-f]{6}\.[a-z]+$", "", os.path.basename(p["path"]))
        if stem not in best or w * h > (best[stem]["size"] or (0, 0))[0] * (best[stem]["size"] or (0, 0))[1]:
            best[stem] = p
    kept = [p for p in photos if any(p is b for b in best.values())]
    large = [p for p in kept if max(p["size"] or (0, 0)) >= MIN_SIDE]
    return (large or kept)[:MAX_PHOTOS]


# Photos given to measure and look ------------------------------------------------------------------------------------


def resolve_photo(photo):
    """A local path for a photo given as a listing's photo by number, a URL, or a path on this machine."""
    s = str(photo or "").strip()
    if not s:
        raise ValueError("give a photo: a listing's photo by number (B0FF1ZQTFD#2), a URL or a path")
    cache = os.path.join(HOME, "cache")
    os.makedirs(cache, exist_ok=True)
    m = re.match(r"^(.+)#(\d+)$", s)
    if m and not s.startswith("/"):
        rec = read_listing(m.group(1))
        n = int(m.group(2))
        if not 1 <= n <= len(rec["photos"]):
            raise ValueError(f"{rec['key']} has photos 1 to {len(rec['photos'])}, not {n}")
        return rec["photos"][n - 1]["path"], {"listing": rec["key"], "n": n, "title": rec.get("title")}
    if re.match(r"^https?://", s):
        path = os.path.join(cache, hashlib.sha1(s.encode()).hexdigest()[:16] + ".img")
        if not os.path.exists(path):
            download(s, path)
        return path, {"url": s}
    path = os.path.expanduser(s)
    if not os.path.isfile(path):
        raise ValueError(f"there's no photo at {s}")
    return path, {"path": path}


def _photo_id(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def summarize_row(r):
    """A row in a line: what it is and what it measured."""
    if r.get("pitch_mm"):
        s = f"{r['features']} × {r['pitch_mm']:.2f} mm pitch, {r['along_mm']:.2f} mm long on the row's line"
        for pr in r.get("pairs", [])[:2]:
            s += f"; pair {pr['cc_mm']:.2f} mm c-c, ⌀{pr['size_mm'][0]:.2f}"
        if r.get("warnings"):
            s += " (angled)" if any("angle" in w or "tilted" in w for w in r["warnings"]) else ""
        return s
    return f"{r['features']} × {r['pitch_px']:.1f} px pitch, {r['along_px']:.0f} px long"


def _ref_args(pitch_mm, length_mm, ref_points, ref_mm):
    if sum(x is not None for x in (pitch_mm, length_mm, ref_mm)) > 1:
        raise ValueError("give one reference: pitch_mm, length_mm, or ref_points with ref_mm")
    if ref_mm is not None:
        if not (isinstance(ref_points, list) and len(ref_points) == 2):
            raise ValueError("ref_mm needs ref_points: two [x, y] points in the photo's pixels, ref_mm apart")
        return {"points": ref_points, "mm": float(ref_mm)}
    if pitch_mm is not None:
        if float(pitch_mm) <= 0:
            raise ValueError("pitch_mm must be more than 0")
        return {"pitch_mm": float(pitch_mm)}
    if length_mm is not None:
        return {"length_mm": float(length_mm)}
    return {}


def run_measure(path, ref, box=None, count=None, row=None, points=None, picture=True):
    """The worker's measurement of one photo, its picture kept in the cache."""
    pid = _photo_id(path)
    key = hashlib.sha1(json.dumps([pid, ref, box, count, row, points], sort_keys=True).encode()).hexdigest()[:12]
    out = os.path.join(HOME, "cache", f"measured-{key}.jpg") if picture else None
    os.makedirs(os.path.join(HOME, "cache"), exist_ok=True)
    res = worker("measure", {"photo": path, "reference": ref, "box": box, "count": count, "row": row,
                             "points": points or {}, "out": out})
    res["photo_sha"] = pid
    res["key"] = key
    if out and os.path.exists(out):
        res["picture_path"] = out
    return res


# Tools --------------------------------------------------------------------------------------------------------------


def _tool(fn):
    """Counts its calls and keeps its last error."""
    @functools.wraps(fn)
    def wrapped(*a, **k):
        state["calls"] += 1
        try:
            return fn(*a, **k)
        except Exception as e:
            state["failed"] += 1
            state["last_error"] = f"{fn.__name__}: {type(e).__name__}: {e}"[:400]
            raise
    return wrapped


PHOTOS_SCHEMA = {"type": "object", "properties": {
    "listing": {"type": "string", "description": "An Amazon ASIN or link, or any store's product page URL."},
    "variants": {"type": "boolean", "description": "Every variant's photos too (another pin count, size or colour), each "
                                                   "marked with its variant; default only this one's."},
    "pitch_mm": {"type": "number", "description": "Measure every photo against this pitch (the listing's own, or a "
                                                  "standard like 2.54). Without it, the pitch the listing states is used "
                                                  "when it states one."},
    "count": {"type": "integer", "description": "How many features the row has (pins, pads): picks that row when a photo "
                                                "has several."},
    "measure": {"type": "boolean", "description": "Measure the photos (default true when a pitch is given or stated)."},
    "refresh": {"type": "boolean", "description": "Read the page and its photos again rather than from the last 6 hours."},
}, "required": ["listing"]}


@app.tool(read_only=True, schema=PHOTOS_SCHEMA)
@_tool
def photos(listing: str, variants: bool = False, pitch_mm: float = None, count: int = None, measure: bool = None,
           refresh: bool = False, ctx=None):
    """A product listing's photos at full size, on one numbered contact sheet, in one call: which are dimensioned
    drawings and the numbers printed on each (read by macOS Vision), the sizes its title, bullets and spec tables
    state, and every photo measured against a pitch (pitch_mm, else the one it states): each row of pads or pins found,
    the part's length along it and the pairs either side (magnets, mounting holes), and what most photos agree on. A
    photo is then <key>#<number> for measure and look. Reads Amazon (this variant, or with variants every one) and any
    store's product page."""
    if ctx:
        ctx.progress("reading the listing")
    rec = read_listing(listing, variants=variants, refresh=refresh)
    state["listings"] += 1
    paths = [p["path"] for p in rec["photos"]][:MAX_PHOTOS]
    if ctx:
        ctx.progress(f"reading {len(paths)} photos")
    sheet = os.path.join(listing_dir(rec["key"]), "sheet.jpg")
    scan = worker("scan", {"photos": paths, "sheet": sheet})
    state["photos"] += len(paths)
    stated = stated_sizes(rec.get("title"), rec.get("bullets"), rec.get("description"), rec.get("specs") or {})
    pitch_from = None
    if pitch_mm is None:
        pitch_mm, pitch_from = stated_pitch(stated)
    else:
        pitch_from = "given"
    do_measure = measure if measure is not None else pitch_mm is not None
    out_photos = []
    for p, s in zip(rec["photos"][:MAX_PHOTOS], scan["photos"]):
        entry = {"photo": f"{rec['key']}#{s['n']}", "n": s["n"], "kind": s.get("kind"), "size_px": s.get("size_px"),
                 "url": p["url"]}
        if p.get("variant") and variants:
            entry["variant"] = p["variant"]
        if s.get("numbers"):
            entry["numbers"] = [dict({"text": ln["text"], "at_px": ln["box"]},
                                     **{k: ln[k] for k in ("reads", "mm", "tol", "diameter", "radius") if k in ln})
                                for ln in s["numbers"]][:40]
        if s.get("words") and s.get("kind") != "photo":
            entry["words"] = s["words"][:12]
        if s.get("error"):
            entry["error"] = s["error"]
        out_photos.append(entry)
    measured = []
    if do_measure and pitch_mm:
        if ctx:
            ctx.progress(f"measuring against a {pitch_mm} mm pitch")
        ref = {"pitch_mm": float(pitch_mm)}

        def one(entry):
            if entry.get("kind") == "drawing" or entry.get("error"):
                return entry, None
            try:
                return entry, run_measure(rec["photos"][entry["n"] - 1]["path"], ref, count=count, picture=False)
            except Exception as e:
                return entry, {"error": str(e)[:200]}

        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            for entry, res in pool.map(one, out_photos):
                if not res:
                    continue
                if res.get("error"):
                    entry["measure_error"] = res["error"]
                    continue
                rows = [r for r in res["rows"] if r["features"] >= 4 or (count and r["features"] == count)]
                if rows:
                    entry["rows"] = [{"line": summarize_row(r), "features": r["features"], "pitch_px": r["pitch_px"],
                                      "along_mm": r["along_mm"], "across_mm": r["across_mm"],
                                      "pairs": [{"cc_mm": q["cc_mm"], "size_mm": q["size_mm"], "offset_mm": q["offset_mm"]}
                                                for q in r["pairs"][:3]],
                                      "profile": [[q["offset_mm"], q["length_mm"]] for q in r["profile"]],
                                      "warnings": r["warnings"], "center_px": r["center_px"]} for r in rows]
                    measured.append((entry["n"], rows))
        state["measured"] += len(measured)
    agree = consensus(measured, count or pin_count(rec))
    picture = sheet if os.path.exists(sheet) else None
    drawings = [e for e in out_photos if e.get("kind") == "drawing"]
    result = {"listing": {"key": rec["key"], "site": rec["site"], "url": rec["url"], "title": rec.get("title"),
                          "variant": rec.get("variant"), "read_by": rec.get("read_by")},
              "photos": out_photos, "drawings": [e["photo"] for e in drawings], "stated": stated,
              "sheet": picture}
    if variants and rec.get("variant_asins"):
        result["variant_asins"] = rec["variant_asins"]
    if rec.get("specs"):
        result["specs"] = rec["specs"]
    if rec.get("bullets"):
        result["bullets"] = rec["bullets"]
    if pitch_mm:
        result["pitch_mm"] = pitch_mm
        result["pitch_from"] = pitch_from
    if agree:
        result["agree"] = agree
    entity = f"listing:{rec['site'].split('.')[0]}-{rec['key'].lower()}"[:120]
    facts = {"title": rec.get("title"), "url": rec["url"], "photos": len(out_photos), "sheet": picture,
             "drawings": [e["photo"] for e in drawings], "stated": [s["says"] for s in stated][:20]}
    for e in drawings:
        facts[f"drawing_{e['n']}_numbers"] = [x.get("reads") or x["text"] for x in e.get("numbers", [])][:40]
    if agree:
        facts["agree"] = agree
    app.emit("listing", {"entity": entity, "summary": _listing_summary(rec, out_photos, agree), "facts": facts},
             about=[entity])
    blocks = [sbmcp.text(json.dumps(result, ensure_ascii=False))]
    if os.path.exists(sheet):
        blocks.append(sbmcp.image(sheet, "image/jpeg"))
    for e in drawings[:3]:
        blocks.append(sbmcp.image(rec["photos"][e["n"] - 1]["path"], _mime(rec["photos"][e["n"] - 1]["path"])))
    return sbmcp.content(*blocks, structured=result)


def _mime(path):
    with open(path, "rb") as f:
        head = f.read(12)
    return "image/png" if head.startswith(b"\x89PNG") else "image/webp" if head[8:12] == b"WEBP" else "image/jpeg"


def _listing_summary(rec, out_photos, agree):
    n = len(out_photos)
    d = sum(1 for e in out_photos if e.get("kind") == "drawing")
    s = f"{(rec.get('title') or rec['key'])[:80]}: {n} photo{'s' * (n != 1)}"
    if d:
        s += f", {d} drawing{'s' * (d != 1)}"
    if agree:
        s += f"; {agree['line']}"
    return s


def pin_count(rec):
    """How many pins the variant (or the title, when it names one count) says the part has: 5Pin/N, 6P, 4 Pin."""
    for text in (rec.get("variant") or "", rec.get("title") or ""):
        counts = set(re.findall(r"(?<![\d.])(\d{1,2})\s*-?\s*(?:pins?|p)\b", text, re.I))
        if len(counts) == 1:
            return int(counts.pop())
    return None


def _median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    m = len(xs) // 2
    return round(xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2, 2)


def consensus(measured, count=None):
    """What the photos agree on, for the row size most of them show (or count): its rows' lengths along the line,
    grouped where they agree within 4% (a face and a flange, or a male and a female half, come apart), and their pairs."""
    if not measured:
        return None
    by_n = {}
    for n, rows in measured:
        for r in rows:
            by_n.setdefault(r["features"], []).append((n, r))
    k = count if count in by_n else max(by_n, key=lambda c: (len(by_n[c]), c))
    rows = sorted(by_n[k], key=lambda q: q[1]["along_mm"] or 0)
    groups = []
    for n, r in rows:
        if r["along_mm"] is None:
            continue
        if groups and r["along_mm"] - groups[-1][-1][1]["along_mm"] <= 0.04 * r["along_mm"]:
            groups[-1].append((n, r))
        else:
            groups.append([(n, r)])
    out = []
    for g in sorted(groups, key=len, reverse=True):
        item = {"along_mm": _median([r["along_mm"] for _, r in g]), "rows": len(g), "photos": sorted({n for n, _ in g}),
                "each_mm": [r["along_mm"] for _, r in g]}
        # the pairs either side (magnets, holes), each kind where its rows agree within 4%
        found = sorted((q for _, r in g for q in r["pairs"] if q.get("cc_mm")), key=lambda q: q["cc_mm"])
        kinds = []
        for q in found:
            if kinds and q["cc_mm"] - kinds[-1][-1]["cc_mm"] <= 0.04 * q["cc_mm"]:
                kinds[-1].append(q)
            else:
                kinds.append([q])
        need = 2 if len(g) > 1 else 1
        item["pairs"] = [{"cc_mm": _median([q["cc_mm"] for q in k]), "size_mm": _median([q["size_mm"][0] for q in k]),
                          "rows": len(k)} for k in sorted(kinds, key=len, reverse=True) if len(k) >= need][:3]
        out.append(item)
    if any(g["rows"] > 1 for g in out):
        out = [g for g in out if g["rows"] > 1]  # a length only one row shows is likelier a stray than the part
    out = out[:4]
    parts = []
    for g in out[:3]:
        t = f"{g['along_mm']} mm ({g['rows']} row{'s' * (g['rows'] != 1)}, photo{'s' * (len(g['photos']) != 1)} " \
            f"{', '.join(map(str, g['photos']))}"
        for q in g["pairs"][:2]:
            t += f"; pair {q['cc_mm']} mm c-c, ⌀{q['size_mm']}"
        parts.append(t + ")")
    return {"features": k, "lengths": out, "line": f"{k}-feature rows {' or '.join(parts)} long on the row's line"}


MEASURE_SCHEMA = {"type": "object", "properties": {
    "photo": {"type": "string", "description": "A listing's photo by its number as photos numbers them (B0FF1ZQTFD#2), "
                                               "a URL, or a path (~/Desktop/part.jpg)."},
    "pitch_mm": {"type": "number", "description": "The reference: the features' centre-to-centre spacing (2.54, 2.5, "
                                                  "1.27 …). Each row is scaled by its own pitch."},
    "length_mm": {"type": "number", "description": "Or: the part's known length along its row (a stated body length)."},
    "ref_points": {"type": "array", "items": {"type": "array", "items": {"type": "number"}},
                   "description": "Or: two [x, y] points in the photo's pixels (look shows them) that are ref_mm apart, "
                                  "like a ruler's marks or a stated size's ends."},
    "ref_mm": {"type": "number", "description": "How far apart ref_points are, in mm."},
    "box": {"type": "array", "items": {"type": "number"},
            "description": "[x0, y0, x1, y1] in the photo's pixels: measure only there (one part of several)."},
    "count": {"type": "integer", "description": "How many features the row has: picks that row."},
    "row": {"type": "integer", "description": "Which of the rows found to measure (0 is the likeliest)."},
    "points": {"type": "object", "description": "Distances to measure, by name: {\"body\": [[x, y], [x, y]]} in the "
                                                "photo's pixels, scaled as the reference says (else by the first row's "
                                                "pitch)."},
}, "required": ["photo"]}


@app.tool(read_only=True, schema=MEASURE_SCHEMA)
@_tool
def measure(photo: str, pitch_mm: float = None, length_mm: float = None, ref_points: list = None, ref_mm: float = None,
            box: list = None, count: int = None, row: int = None, points: dict = None, ctx=None):
    """A part's size off one photo: finds the rows of evenly spaced features (pads, pins, holes) itself, and for each
    the pitch, the part's length along the row's line and across it, its length at offsets either side (a plate or
    flange wider than its face), and the features in pairs either side of the middle (magnets, mounting holes: c-c,
    size, offset), in mm against the reference given (pitch_mm, length_mm, or ref_points with ref_mm), with warnings
    when it's seen at an angle. points adds distances picked by hand (look gives their pixels). Answers with the photo
    drawn on (rows, ends, pairs, points) to check by eye."""
    ref = _ref_args(pitch_mm, length_mm, ref_points, ref_mm)
    path, where = resolve_photo(photo)
    if ctx:
        ctx.progress("measuring")
    res = run_measure(path, ref, box=box, count=count, row=row, points=points)
    state["measured"] += 1
    picture = res.get("picture_path")
    rows = res["rows"]
    result = {"photo": photo, "source": where, "size_px": res["size_px"], "reference": ref or None,
              "rows": rows, "points": res["points"], "mm_per_px": res.get("mm_per_px"), "picture": picture}
    if not rows:
        result["note"] = ("no row of evenly spaced features found: give box around the part, count, or measure points "
                          "against ref_points/ref_mm")
    if rows:
        result["lines"] = [f"{chr(65 + i)}: {summarize_row(r)}" for i, r in enumerate(rows)]
    entity = f"photo:{res['photo_sha'][:16]}"
    facts = {"photo": photo, "reference": ref, "picture": picture, "rows": [summarize_row(r) for r in rows[:4]],
             "points": {p["name"]: p["mm"] for p in res["points"]}}
    about = [entity] + ([f"listing:amazon-{where['listing'].lower()}"] if where.get("listing") and ASIN.match(where["listing"]) else [])
    app.emit("measurement", {"entity": entity, "summary": f"{photo}: " + ("; ".join(facts["rows"][:2]) or "no rows found"),
                             "facts": facts}, about=about)
    blocks = [sbmcp.text(json.dumps(result, ensure_ascii=False))]
    if res.get("picture_path"):
        blocks.append(sbmcp.image(res["picture_path"], "image/jpeg"))
    return sbmcp.content(*blocks, structured=result)


LOOK_SCHEMA = {"type": "object", "properties": {
    "photo": MEASURE_SCHEMA["properties"]["photo"],
    "box": {"type": "array", "items": {"type": "number"},
            "description": "[x0, y0, x1, y1] in the photo's pixels to zoom into; default the whole photo."},
}, "required": ["photo"]}


@app.tool(read_only=True, schema=LOOK_SCHEMA)
@_tool
def look(photo: str, box: list = None):
    """A photo, or a region of it zoomed in, with a grid labelled in the photo's own pixels: for reading off the points
    to give measure (points, ref_points)."""
    path, where = resolve_photo(photo)
    os.makedirs(os.path.join(HOME, "cache"), exist_ok=True)
    out = os.path.join(HOME, "cache", f"look-{hashlib.sha1(json.dumps([path, box]).encode()).hexdigest()[:12]}.jpg")
    res = worker("look", {"photo": path, "box": box, "out": out})
    result = {"photo": photo, "source": where, "size_px": res["size_px"], "box": res["box"], "grid_px": res["grid_px"],
              "zoom": res["zoom"]}
    return sbmcp.content(sbmcp.text(json.dumps(result)), sbmcp.image(out, "image/jpeg"), structured=result)


def health():
    """How photo-measure is: when it started, its calls and failures, the listings, photos and measurements it did."""
    listings = os.path.join(HOME, "listings")
    kept = len(os.listdir(listings)) if os.path.isdir(listings) else 0
    return dict(state, listings_kept=kept)


def heartbeat():
    """Says how it is."""
    error = state["last_error"]
    app.emit("heartbeat", {"entity": f"sense:{NAME}", "summary": f"Last failed: {error}" if error else "Ready",
                           "facts": dict(state)})


KEEP_LISTINGS_S = 30 * 86400
KEEP_CACHE_S = 7 * 86400


def prune(now=None):
    """Listings not read for 30 days and other photos and pictures not used for a week go, so its folder stays small."""
    now = now or time.time()
    gone = 0
    root = os.path.join(HOME, "listings")
    for name in os.listdir(root) if os.path.isdir(root) else []:
        d = os.path.join(root, name)
        saved = os.path.join(d, "listing.json")
        when = os.path.getmtime(saved) if os.path.exists(saved) else os.path.getmtime(d)
        if now - when > KEEP_LISTINGS_S:
            shutil.rmtree(d, ignore_errors=True)
            gone += 1
    cache = os.path.join(HOME, "cache")
    for name in os.listdir(cache) if os.path.isdir(cache) else []:
        f = os.path.join(cache, name)
        if now - os.path.getmtime(f) > KEEP_CACHE_S:
            os.remove(f)
            gone += 1
    return gone


@app.background
def beat():
    while True:
        try:
            prune()
        except OSError as e:
            state["last_error"] = f"prune: {e}"
        heartbeat()
        time.sleep(HEARTBEAT_S)
