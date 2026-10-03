"""A real loopback HTTP server standing in for a running ``query-api``.

Shared by the CLI preflight tests and the DB-backed enable tests. A REAL
server, not a mocked client: what these tests exercise is the decision the
CLI makes from a wire response, and mocking ``_fetch_go_plane_registry``
would skip the parsing -- which is exactly where a wrong answer would come
from.

Imported as a bare sibling module (``from _go_api_fake_query_api import
...``), matching ``_go_schema_digest``/``_go_registered_documents`` in this
directory; there is no ``__init__.py`` here, so a relative import does not
resolve.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from dev_health_ops.api.graphql.go_api_operation_catalog import catalog_entries
from dev_health_ops.api.graphql.go_api_schema_digest import current_schema_digest

__all__ = ["FakeQueryAPI", "registry_payload"]


class FakeQueryAPI:
    """A real HTTP server answering /registry with a caller-chosen body."""

    def __init__(self, payload: Any, *, status: int = 200, path: str = "/registry"):
        self._payload = payload
        self._status = status
        self._path = path
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - stdlib interface
                if self.path != outer._path:
                    self.send_error(404)
                    return
                body = (
                    outer._payload
                    if isinstance(outer._payload, bytes)
                    else json.dumps(outer._payload).encode()
                )
                self.send_response(outer._status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: Any) -> None:
                pass

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> str:
        self._thread.start()
        host, port = self._server.server_address[:2]
        # server_address's first element is typed str | bytes; it is always
        # a str for an AF_INET socket, but format it explicitly so the URL
        # can never come out as "b'127.0.0.1'".
        if isinstance(host, bytes):
            host = host.decode()
        return f"http://{host}:{port}"

    def __exit__(self, *exc: Any) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def registry_payload(
    *, schema_digest: str | None = None, operations: dict[str, str] | None = None
) -> dict[str, Any]:
    """A /registry body that agrees with this checkout unless told otherwise."""
    if schema_digest is None:
        schema_digest = current_schema_digest()
    if operations is None:
        operations = dict(catalog_entries())
    return {
        "schema_digest": schema_digest,
        "operations": [
            {"operation": operation, "document_digest": digest}
            for operation, digest in sorted(operations.items())
        ],
    }
