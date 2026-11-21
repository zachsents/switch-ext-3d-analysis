"""The user's browser, through the browser extension's tools, called as another extension's tools.

An extension declares them in its reaches (`reaches={"calls": ["browser_act", "browser_run"]}`) and makes one
`Browser(app)`. `op(args)` takes the ops a Switchboard macOS appendage's `browser` action took, which the browser
extension's `act` takes as they are: navigate {url}, js {code}, text, look, click {element | match}, type {text,
element?, submit?}, key {keys}, scroll {direction?}, back, reload, wait {text, gone?, timeout?} | {seconds}, upload
{element, paths}, value {element}, tabs, attach {tab}, release, screenshot, sign_in {account?, otp?}, autofill {element,
pick?}, steps [ops]. `show` and `minimize` change nothing here (the tab stays in the background). What the browser
extension doesn't keep (fill_secret, save_account, accounts) raises BrowserError.
"""
import json


class BrowserError(Exception):
    pass


UNOFFERED = {"fill_secret", "save_account", "accounts"}
QUIET = {"show", "minimize"}


class Browser:
    def __init__(self, app, session=None):
        """`app` is the extension's sbmcp.Server; `session` names its tab (default: the extension's name)."""
        self.app = app
        self.session = session or app.name

    def tool(self, name, wait=120, **arguments):
        """One browser extension tool's words (`act` or `run`), waiting up to `wait` seconds."""
        arguments.setdefault("session", self.session)
        try:
            # As the agent sees it: the browser extension's own tool, under its extension's name.
            result = self.app.call(f"browser_{name}", {k: v for k, v in arguments.items() if v is not None}, timeout=wait)
        except RuntimeError as e:
            raise BrowserError(f"{name}: {e}") from None
        words = "\n".join(b.get("text", "") for b in result.get("content", []) if b.get("type") == "text")
        if result.get("isError"):
            raise BrowserError(f"{arguments.get('op') or name}: {words[:300] or 'it failed'}")
        return words

    def js(self, code, timeout=60):
        """An expression's value in the session's tab (parsed when it's JSON)."""
        words = self.tool("act", wait=timeout + 15, op="js", code=code, timeout=timeout)
        try:
            return json.loads(words)
        except ValueError:
            return None if words == "(no value)" else words

    def op(self, args, timeout=60):
        """One op as the macOS appendage's browser action took it; answers its output."""
        args = dict(args)
        op = args.get("op")
        if args.get("session"):
            self.session = args.pop("session")
        if op in UNOFFERED:
            raise BrowserError(f"{op} isn't offered: the browser extension fills sign-ins and cards from the browser's own saved ones")
        if op in QUIET:
            return {}
        if op == "steps":
            out = None
            for step in args.get("steps") or []:
                out = self.op(step, timeout)
            return out
        if op == "js":
            return self.js(args["code"], timeout=float(args.get("timeout") or timeout))
        if op == "navigate":
            self.tool("act", wait=timeout, op="navigate", url=args["url"])
            return self.js("({url: location.href, title: document.title})")
        return self.tool("act", wait=timeout + 15, **args)
