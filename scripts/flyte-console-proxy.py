#!/usr/bin/env python3
"""Serve a working Flyte console on one local port.

    ./scripts/flyte-console-proxy.py          # then open http://localhost:8085/console/

Normally invoked as `just flyte-ui`. Ctrl-C stops the proxy and both forwards.

WHY THIS EXISTS
---------------
flyteconsole and flyteadmin are separate Services that only work together behind
something that merges them onto one origin:

    path            flyteconsole            flyteadmin
    /console        serves the SPA          404
    /api/v1/*       returns SPA HTML (!)    returns JSON

Port-forwarding `svc/flyteconsole` alone therefore yields a console that loads,
renders "Select a project to get started", and lists nothing -- because its
`GET /api/v1/projects` gets flyteconsole's own catch-all HTML back with a 200,
not JSON. The console has no way to report that, so the failure looks like an
empty or unauthenticated install. It is neither: `useAuth: false` on this
cluster, and the projects are present in flyteadmin the whole time.

In a normal deployment an **ingress** does the merging (`INGRESS_ENABLED=true`
in the flyte-core chart, routing /console to one backend and /api to the other).
This cluster runs Traefik but has no Flyte ingress, so this script stands in for
one locally: it opens both port-forwards itself and serves the merged origin.

PORTS
-----
Listens on 8085 (RUNNING.md §0). The two port-forwards it manages use 18083 and
18088 -- deliberately above the 2000-9999 band that check-ports.sh audits, so
they need no entry in the port map and cannot collide with a forward you already
have open on 8083.
"""

import argparse
import http.server
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

# Prefixes that belong to flyteadmin. Everything else goes to flyteconsole.
# /api/v1 is the REST gateway the console actually calls; the rest are the auth
# and health endpoints it probes on load (harmless 404s while useAuth is false).
ADMIN_PREFIXES = (
    "/api/",
    "/healthcheck",
    "/.well-known/",
    "/me",
    "/login",
    "/logout",
    "/oauth2/",
    "/openapi",
)

# Headers a proxy must not forward verbatim (RFC 9110 hop-by-hop).
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


def wait_for_port(port: int, timeout: float = 30.0) -> bool:
    """Block until something accepts connections on localhost:port."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as s:
            s.settimeout(1.0)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.5)
    return False


class PortForward:
    """A `kubectl port-forward` that restarts itself when it dies.

    Not optional in practice: a port-forward left idle for hours goes away
    silently, and the only symptom is the proxy answering 502 while its own
    process is still healthy -- which reads as "the proxy broke" rather than
    "one of its two tunnels did". Observed after ~10 hours idle.
    """

    def __init__(self, namespace, service, local, remote, context):
        self.namespace, self.service = namespace, service
        self.local, self.remote, self.context = local, remote, context
        self.proc = None
        self.start()

    def _cmd(self):
        cmd = ["kubectl"]
        if self.context:
            cmd += ["--context", self.context]
        return cmd + ["port-forward", "-n", self.namespace,
                      f"svc/{self.service}", f"{self.local}:{self.remote}"]

    def start(self):
        self.proc = subprocess.Popen(self._cmd(), stdout=subprocess.DEVNULL,
                                     stderr=subprocess.PIPE)
        if not wait_for_port(self.local):
            self.proc.terminate()
            err = self.proc.stderr.read().decode(errors="replace").strip() if self.proc.stderr else ""
            sys.exit(f"error: port-forward to {self.service} did not come up on :{self.local}\n{err}")

    def ensure_alive(self):
        """Restart if the process exited or the port stopped accepting."""
        dead = self.proc.poll() is not None
        if not dead:
            with socket.socket() as s:
                s.settimeout(1.0)
                dead = s.connect_ex(("127.0.0.1", self.local)) != 0
        if dead:
            sys.stderr.write(f"  port-forward to {self.service} died; restarting\n")
            try:
                self.proc.terminate()
            except Exception:
                pass
            self.start()

    def terminate(self):
        try:
            self.proc.terminate()
        except Exception:
            pass


def watchdog(forwards, stop_after=None):
    """Poll the forwards so a dead tunnel is repaired before the next request."""
    while True:
        time.sleep(15)
        for f in forwards:
            try:
                f.ensure_alive()
            except SystemExit:
                raise
            except Exception as e:  # never let the watchdog kill the proxy
                sys.stderr.write(f"  watchdog error on {f.service}: {e}\n")


def make_handler(console_url: str, admin_url: str):
    class ProxyHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # quieter than the default
            if self.path.startswith("/api/"):
                sys.stderr.write(f"  {self.command} {self.path} -> {args[1]}\n")

        def _upstream_for(self, path: str) -> str:
            if path.startswith(ADMIN_PREFIXES):
                return admin_url
            return console_url

        def _proxy(self):
            upstream = self._upstream_for(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None

            headers = {
                k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP
            }
            # Present the upstream's own Host so neither backend gets confused by
            # the proxy's port.
            headers["Host"] = upstream.split("//", 1)[1]

            req = urllib.request.Request(
                upstream + self.path, data=body, headers=headers, method=self.command
            )
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    self._relay(resp.status, resp.headers, resp.read())
            except urllib.error.HTTPError as e:
                # 404s from admin's auth probes are normal; pass them through.
                self._relay(e.code, e.headers, e.read())
            except urllib.error.URLError as e:
                msg = f"upstream {upstream} unreachable: {e.reason}".encode()
                self._relay(502, {"Content-Type": "text/plain"}, msg)

        def _relay(self, status, headers, payload: bytes):
            self.send_response(status)
            for k, v in headers.items():
                if k.lower() in HOP_BY_HOP or k.lower() == "content-length":
                    continue
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        # The console issues all of these.
        do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = _proxy

    return ProxyHandler


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8085, help="local port to serve on (default: 8085)")
    ap.add_argument("--namespace", default=os.environ.get("FLYTE_NS", "flyte"))
    ap.add_argument("--context", default=os.environ.get("KCTX") or None,
                    help="kube context; defaults to $KCTX, else the current context")
    ap.add_argument("--console-port", type=int, default=18083)
    ap.add_argument("--admin-port", type=int, default=18088)
    # Bind 0.0.0.0 by default for the same reason the host UIs do: a browser
    # outside this VM (WSL2 host, another machine) has to reach it.
    ap.add_argument("--bind", default="0.0.0.0")
    args = ap.parse_args()

    ctx = args.context or "(current context)"
    print(f"==> port-forwarding flyteconsole and flyteadmin from {ctx}, namespace {args.namespace}")
    forwards = [
        PortForward(args.namespace, "flyteconsole", args.console_port, 80, args.context),
        PortForward(args.namespace, "flyteadmin", args.admin_port, 80, args.context),
    ]
    # Repairs a tunnel that died while idle, before the next request hits it.
    threading.Thread(target=watchdog, args=(forwards,), daemon=True).start()

    def shutdown(*_):
        print("\n==> stopping port-forwards")
        for p in forwards:
            p.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    handler = make_handler(
        console_url=f"http://127.0.0.1:{args.console_port}",
        admin_url=f"http://127.0.0.1:{args.admin_port}",
    )
    server = http.server.ThreadingHTTPServer((args.bind, args.port), handler)
    print(f"==> console ready at http://localhost:{args.port}/console/   (Ctrl-C to stop)")
    try:
        server.serve_forever()
    finally:
        shutdown()


if __name__ == "__main__":
    main()
