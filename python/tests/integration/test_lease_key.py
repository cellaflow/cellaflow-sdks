"""A step body that forwards its call's idempotency key, against a real engine:
a holder killed after its request landed and before it recorded the result is
taken over, and the new holder's request carries the same key, so a destination
that deduplicates on it applies the effect once."""

# `tool(...)` with arguments is not typed yet; the unit tests cast it instead.
# mypy: disable-error-code="untyped-decorator"

import json
import subprocess
import sys
import textwrap
import threading
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List

from cellaflow import IdempotencyScope, current_lease, durable_tools, tool

TTL_MS = 3000


def destination() -> Any:
    """A payments service that applies each Idempotency-Key once, and a request
    without one every time."""
    applied: Dict[str, str] = {}
    requests: List[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            key = self.headers.get("Idempotency-Key") or f"unkeyed-{len(requests)}"
            requests.append(key)
            if key not in applied:
                applied[key] = f"refund-{len(applied) + 1}"
            body = json.dumps({"id": applied[key]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}/refunds"
    return server, url, applied, requests


def post(url: str, headers: Dict[str, str]) -> str:
    request = urllib.request.Request(url, data=b"{}", headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        return str(json.loads(response.read())["id"])


def test_a_holder_killed_after_its_request_landed_is_applied_once(
    target: str,
) -> None:
    server, url, applied, requests = destination()
    order_id = f"order-{uuid.uuid4().hex[:8]}"
    script = textwrap.dedent(
        f"""
        import json, threading, urllib.request
        from cellaflow import IdempotencyScope, current_lease, durable_tools, tool

        @tool(tool_name="refund", scope=IdempotencyScope.SCOPE_SHARED,
              shared_on=["order_id"])
        def refund(order_id):
            headers = current_lease().idempotency_headers()
            req = urllib.request.Request({url!r}, data=b"{{}}", headers=headers,
                                         method="POST")
            urllib.request.urlopen(req, timeout=10).read()
            print("sent", flush=True)
            threading.Event().wait()  # killed here, before the result is recorded

        with durable_tools("holder-{uuid.uuid4().hex[:8]}", target={target!r},
                           coordination_id={order_id!r}, lease_ttl_ms={TTL_MS}):
            refund({order_id!r})
        """
    )
    try:
        _run_holder_then_taker(target, url, order_id, script, applied, requests)
    finally:
        server.shutdown()
        server.server_close()


def _run_holder_then_taker(
    target: str,
    url: str,
    order_id: str,
    script: str,
    applied: Dict[str, str],
    requests: List[str],
) -> None:
    child = subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True
    )
    try:
        out = child.stdout
        assert out is not None
        line: List[str] = []
        reader = threading.Thread(
            target=lambda: line.append(out.readline()), daemon=True
        )
        reader.start()
        reader.join(30)
        assert line and "sent" in line[0], "the holder did not send in time"
    finally:
        child.kill()
        child.wait()
        if child.stdout is not None:
            child.stdout.close()

    @tool(
        tool_name="refund",
        scope=IdempotencyScope.SCOPE_SHARED,
        shared_on=["order_id"],
        lease_ttl_ms=TTL_MS,
    )
    def refund(order_id: str) -> str:
        return post(url, current_lease().idempotency_headers())

    with durable_tools(
        f"taker-{uuid.uuid4().hex[:8]}", target=target, coordination_id=order_id
    ):
        refund_id = refund(order_id)
    # Two requests, one key (the call's digest), one refund: the taker got the
    # first one back.
    assert len(requests) == 2 and requests[0] == requests[1]
    assert len(requests[0]) == 64
    assert all(c in "0123456789abcdef" for c in requests[0])
    assert len(applied) == 1
    assert refund_id == "refund-1"
