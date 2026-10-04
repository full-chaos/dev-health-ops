"""Re-record python_raw_http_golden.json.gz from the REAL Python api.

Run from the repository root with the project environment (uv.lock):

    PYTHONPATH=src .venv/bin/python internal/apiservice/testdata/gen_raw_http_golden.py

It serves a route-less FastAPI app (the api's rate limiter on app.state,
register_middleware, register_exception_handlers, default CORS config) with
the REAL uvicorn (http=auto) on a loopback port. It replays every recorded
case's exact request bytes over TCP, one connection each, and rewrites the
Python-observed fields of the case: status line, status, headers (in wire
order), body. A case's decided metadata (kind, go_status, method, target) and
the timing scenarios, whose recorded outcome is status lines only, are kept.
Test-side tooling only: nothing imports it.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import socket
import sys
import threading
import time

PATH = "internal/apiservice/testdata/python_raw_http_golden.json.gz"


def build_app():
    os.environ.pop("CORS_ALLOWED_ORIGINS", None)
    from fastapi import FastAPI

    import dev_health_ops.api._errors as errors
    import dev_health_ops.api._middleware as middleware
    from dev_health_ops.api.middleware.rate_limit import limiter

    app = FastAPI()
    app.state.limiter = limiter
    middleware.register_middleware(app)
    errors.register_exception_handlers(app)
    return app


def serve(app) -> int:
    import uvicorn

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=port, http="auto", log_level="warning"
        )
    )
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(200):
        if server.started:
            return port
        time.sleep(0.05)
    raise SystemExit("uvicorn did not start")


def exchange(port: int, request: bytes) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
        conn.sendall(request)
        chunks = []
        while True:
            try:
                data = conn.recv(65536)
            except (TimeoutError, ConnectionResetError):
                # uvicorn may close an answered connection with a reset.
                break
            if not data:
                break
            chunks.append(data)
    return b"".join(chunks)


def parse(raw: bytes, method: str) -> dict:
    head, _, rest = raw.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status_line = lines[0]
    names: list[str] = []
    headers: dict[str, list[str]] = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        names.append(name)
        headers.setdefault(name.lower(), []).append(value.strip())
    length = headers.get("content-length")
    body = rest if length is None else rest[: int(length[0])]
    if method == "HEAD":
        body = b""
    return {
        "status_line": status_line,
        "status": int(status_line.split(" ")[1]),
        "header_names": names,
        "headers": dict(sorted(headers.items())),
        "body": body.decode("utf-8", "replace"),
    }


def main() -> None:
    golden = json.load(gzip.open(PATH))
    port = serve(build_app())
    for case in golden["cases"]:
        raw = exchange(port, base64.b64decode(case["request"]))
        if not raw:
            raise SystemExit(f"no answer for {case['method']} {case['target']}")
        case.update(parse(raw, case["method"]))
    import starlette

    golden["generated_by"] = (
        "dev_health_ops.api._middleware.register_middleware + _errors.register_exception_handlers on a "
        f"route-less FastAPI app, starlette {starlette.__version__}, served by uvicorn (http=auto) from uv.lock, "
        "driven with raw HTTP/1.1 bytes: internal/apiservice/testdata/gen_raw_http_golden.py"
    )
    sys.stdout.buffer.write(json.dumps(golden, sort_keys=True).encode())


if __name__ == "__main__":
    main()
