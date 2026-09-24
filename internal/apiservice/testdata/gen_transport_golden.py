"""Record python_transport_golden.json from the REAL Python api middleware.

Run from the repository root with the project environment (uv.lock):

    PYTHONPATH=src .venv/bin/python internal/apiservice/testdata/gen_transport_golden.py

It builds a route-less FastAPI app with the api's rate limiter on app.state
(as api/main.py sets it), dev_health_ops.api._middleware.register_middleware
and _errors.register_exception_handlers, per CORS config,
and records every response to the full request matrix below through
fastapi.testclient. A generated X-Request-ID is recorded as "<generated>".
Test-side tooling only: nothing imports it.
"""

from __future__ import annotations

import importlib
import itertools
import json
import os
import sys
from typing import Any

AXES: dict[str, list[Any]] = {
    "config": ["default", "list", "star"],
    "method": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    "origin": [
        None,
        "",
        "https://a.example",
        "http://localhost:3000",
        "https://evil.example",
    ],
    "access_control_request_method": [None, "", "POST", "post", "TRACE"],
    "access_control_request_headers": [
        None,
        "",
        "authorization",
        "Authorization, X-Org-Id",
        "x-custom",
        "authorization,",
        "content-type ,accept",
    ],
    "access_control_request_private_network": [None, "true"],
}
CONFIGS = {
    "default": None,
    "list": "https://a.example, https://b.example,,",
    "star": "*",
}
HEADER_NAMES = {
    "origin": "Origin",
    "access_control_request_method": "Access-Control-Request-Method",
    "access_control_request_headers": "Access-Control-Request-Headers",
    "access_control_request_private_network": "Access-Control-Request-Private-Network",
}
ROW_LAYOUT = [
    "config",
    "method",
    "origin",
    "access_control_request_method",
    "access_control_request_headers",
    "access_control_request_private_network",
    "response",
]


def client_for(origins: str | None):
    if origins is None:
        os.environ.pop("CORS_ALLOWED_ORIGINS", None)
    else:
        os.environ["CORS_ALLOWED_ORIGINS"] = origins
    import dev_health_ops.api._errors as errors
    import dev_health_ops.api._middleware as middleware

    importlib.reload(middleware)
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from dev_health_ops.api.middleware.rate_limit import limiter

    app = FastAPI()
    # api/main.py sets the limiter the slowapi middleware reads.
    app.state.limiter = limiter
    middleware.register_middleware(app)
    errors.register_exception_handlers(app)
    return TestClient(app, raise_server_exceptions=False)


def main() -> None:
    import starlette

    responses: list[dict] = []
    index: dict[str, int] = {}
    cases: list[list[int]] = []
    for config_index, config in enumerate(AXES["config"]):
        client = client_for(CONFIGS[config])
        ranges = [range(len(AXES[name])) for name in ROW_LAYOUT[1:6]]
        for picks in itertools.product(*ranges):
            method = AXES["method"][picks[0]]
            headers = {}
            for name, pick in zip(ROW_LAYOUT[2:6], picks[1:]):
                value = AXES[name][pick]
                if value is not None:
                    headers[HEADER_NAMES[name]] = value
            response = client.request(method, "/api/v1/nope", headers=headers)
            recorded: dict[str, list[str]] = {}
            for key, value in response.headers.multi_items():
                recorded.setdefault(key.lower(), []).append(value)
            if "x-request-id" in recorded:
                recorded["x-request-id"] = ["<generated>"]
            entry = {
                "body": response.text,
                "headers": dict(sorted(recorded.items())),
                "status": response.status_code,
            }
            key = json.dumps(entry, sort_keys=True)
            if key not in index:
                index[key] = len(responses)
                responses.append(entry)
            cases.append([config_index, *picks, index[key]])
    golden = {
        "axes": AXES,
        "cases": cases,
        "configs": CONFIGS,
        "generated_by": (
            "dev_health_ops.api._middleware.register_middleware + _errors.register_exception_handlers on a "
            f"route-less FastAPI app, starlette {starlette.__version__} (uv.lock), via fastapi.testclient: "
            "internal/apiservice/testdata/gen_transport_golden.py"
        ),
        "responses": responses,
        "row_layout": ROW_LAYOUT,
    }
    json.dump(golden, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
