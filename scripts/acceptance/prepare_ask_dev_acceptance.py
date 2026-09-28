#!/usr/bin/env python3
"""Shared HTTP-acceptance-probe infrastructure.

CHAOS-6262 deleted this file's original subject -- seeding the Ask Dev
entitlement state and certifying the acceptance provider (``prepare``,
``_enable_features``, ``provision_multi_org``, and the CLI ``main`` that
drove them, along with the boot-sequence call site in
``armed_corpus_boot.sh``) -- along with the REST routes it exercised.
``AcceptanceFailure``/``AcceptanceApi``/``_require`` remain: they are
generic HTTP-acceptance-probe helpers with retained, non-Ask-Dev consumers
(``tests/acceptance/corpus/test_principals.py``,
``scripts/acceptance/acceptance_artifact.py``).
"""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class AcceptanceFailure(RuntimeError):
    """A deterministic acceptance prerequisite was not established."""


class AcceptanceApi:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token: str | None = None

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=20) as response:  # noqa: S310
                response_body = response.read()
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise AcceptanceFailure(
                f"{method} {path} returned HTTP {exc.code}: {detail}"
            ) from exc
        except URLError as exc:
            raise AcceptanceFailure(f"{method} {path} failed: {exc.reason}") from exc
        if not response_body:
            return None
        try:
            return json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise AcceptanceFailure(
                f"{method} {path} returned non-JSON content"
            ) from exc


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AcceptanceFailure(message)
