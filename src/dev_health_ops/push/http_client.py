"""httpx client wrapper for `dev-hops push batch`/`status` (CHAOS-2700 decision 1/2).

``httpx.AsyncClient`` (already a pyproject dependency, used server-side in
``api/services/oauth.py``/``api/admin/routers/*``) + ``dev_health_ops.
connectors.utils.retry.retry_with_backoff`` (generic, already implements
exponential backoff + a ``retry_after_seconds`` duck-type for Retry-After
honoring) -- no new HTTP/retry dependency for the CLI. `push batch`/`push
status` are ``async def`` handlers; ``cli.py``'s dispatch loop already runs
them via ``asyncio.run`` whenever ``inspect.iscoroutinefunction(func)`` is
true, so this composes with the existing CLI for free.

Retries ALL 503s regardless of error code, plus 429 (honoring
``Retry-After``) and network/timeout errors (master-spec CC16/CC29: the
server can return `stream_unavailable`, `ingest_temporarily_unavailable`, or
`auth_not_configured` at 503 -- the retry predicate keys on status class,
never the code string). Does NOT retry 4xx contract errors (400/401/403/
404/409/413/422) -- retrying those wastes CI minutes and can flip a 409
idempotency-conflict into a false "it eventually worked" (brief decision 2).
"""

from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import dataclass
from typing import Any

import httpx

from dev_health_ops.connectors.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

USER_AGENT = "dev-hops-push-cli"

#: CLI-tuned retry params (brief decision 2): max_delay lowered from the
#: connectors default of 60.0s -- CI job time budgets are tighter than
#: long-running background sync jobs. Also the cap applied to a
#: server-supplied `Retry-After` (see `_parse_retry_after`) -- a
#: misbehaving/malicious server must not be able to park a CI job past
#: this CLI's own documented retry budget.
_RETRY_KWARGS: dict[str, Any] = {
    "max_retries": 5,
    "initial_delay": 1.0,
    "max_delay": 30.0,
    "backoff_factor": 2.0,
}

_RETRYABLE_STATUS_CODES = frozenset({429, 503})

#: Defense-in-depth (Codex adversarial review finding): a misconfigured
#: proxy/debug endpoint between the CLI and the API could echo request
#: headers -- including `Authorization: Bearer fcpush_...` -- back into an
#: error response body. Redact anything matching the real token shape
#: (`fcpush_` + `secrets.token_urlsafe(32)`, CC14 -- base64 URL-safe
#: alphabet) or a generic bearer-credential pattern before any
#: server-supplied text is ever logged (by the shared retry decorator,
#: which logs the exception string on every retry) or printed.
_TOKEN_PATTERN = re.compile(r"fcpush_[A-Za-z0-9_-]+")
_BEARER_PATTERN = re.compile(r"(?i)bearer\s+\S+")


def redact_secrets(text: str) -> str:
    text = _TOKEN_PATTERN.sub("fcpush_[REDACTED]", text)
    text = _BEARER_PATTERN.sub("Bearer [REDACTED]", text)
    return text


def _redact_value(value: Any) -> Any:
    """Recursively applies `redact_secrets` to every string anywhere in
    `value` -- including dict KEYS, not just values (Codex adversarial-
    review finding, round 4: a malformed/proxy-generated error item could
    echo a credential as a key name, e.g. ``{"Authorization: Bearer
    fcpush_...": "x"}``, and a keys-only-passthrough redaction would leak
    it straight into ``IngestApiError.errors`` and `--json` CLI output).
    Non-string/dict/list values pass through unchanged."""
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, dict):
        return {redact_secrets(str(k)): _redact_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_value(v) for v in value]
    return value


class IngestTransientError(Exception):
    """Retryable failure: network/timeout error, 429, or any 503 (master-spec
    CC16 -- ALL 503s are retried regardless of error code)."""

    def __init__(
        self, message: str, *, retry_after_seconds: float | None = None
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class IngestApiError(Exception):
    """Non-retryable API error: a 4xx contract error, or a 5xx that survived
    the retry budget. Carries the parsed ``{"error": {...}}`` envelope
    (master-spec CC16) so ``output.py`` can render it faithfully."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.errors = errors or []


@dataclass(frozen=True)
class IngestClientConfig:
    api_url: str
    token: str
    org_id: str


def _parse_retry_after(response: httpx.Response) -> float | None:
    """Parses the numeric delta-seconds form of `Retry-After`, clamped to
    this CLI's own `max_delay` (Codex adversarial-review finding: an
    unclamped server-supplied value like `Retry-After: 86400` would park a
    CI job for a day despite the documented 30s retry-delay ceiling).
    Malformed, negative, or non-finite (`inf`/`nan` -- `float()` accepts
    both) values fall back to `None`, same as the HTTP-date form (rare for
    this API) -- the caller's exponential backoff, itself bounded by
    `max_delay`, takes over."""
    header = response.headers.get("retry-after")
    if header is None:
        return None
    try:
        seconds = float(header)
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return min(seconds, _RETRY_KWARGS["max_delay"])


def parse_error_envelope(
    response: httpx.Response,
) -> tuple[str, str, list[dict[str, Any]]]:
    """Best-effort parse of the ``{"error": {code, message, errors}}`` shape
    (master-spec CC16). Falls back to a generic code/message if the body
    isn't that shape (e.g. an upstream proxy's HTML error page) -- the CLI
    must never crash on a malformed error body.

    Every extracted string is passed through `redact_secrets` before
    returning -- this is the single choke point every retryable/non-
    retryable error path (including the shared retry decorator's own
    per-attempt logging) reads from, so sanitizing here protects all of
    them at once (Codex adversarial-review finding)."""
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        body = None
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        # `code` is redacted too (round-3 finding): a malformed proxy/debug
        # response could reflect the token through ANY string field, not
        # just `message` -- `_raise_for_response` formats `code` directly
        # into the exception string the shared retry decorator logs.
        code = redact_secrets(str(error.get("code", "unknown_error")))
        message = redact_secrets(str(error.get("message") or response.text[:500]))
        raw_errors = error.get("errors")
        errors: list[dict[str, Any]] = []
        if isinstance(raw_errors, list):
            for item in raw_errors:
                if isinstance(item, dict):
                    errors.append(_redact_value(item))
        return code, message, errors
    fallback = response.text[:500] or response.reason_phrase
    return "unknown_error", redact_secrets(fallback), []


def _raise_for_response(response: httpx.Response) -> None:
    if response.status_code < 400:
        return
    if response.status_code in _RETRYABLE_STATUS_CODES:
        code, message, _errors = parse_error_envelope(response)
        raise IngestTransientError(
            f"{response.status_code} {code}: {message}",
            retry_after_seconds=_parse_retry_after(response),
        )
    code, message, errors = parse_error_envelope(response)
    raise IngestApiError(response.status_code, code, message, errors=errors)


def auth_headers(config: IngestClientConfig) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {config.token}",
        # Not currently checked server-side for token-authed requests (the
        # org id is resolved from the token row itself -- see auth.py's
        # module docstring), sent for forward-compat / defense-in-depth,
        # mirroring the Idempotency-Key body+header double-send (CC2).
        "X-Org-Id": config.org_id,
        "User-Agent": USER_AGENT,
    }


@retry_with_backoff(exceptions=(IngestTransientError,), **_RETRY_KWARGS)
async def _request(
    client: httpx.AsyncClient, method: str, url: str, **kwargs: Any
) -> httpx.Response:
    try:
        response = await client.request(method, url, **kwargs)
    except httpx.TransportError as exc:
        # Covers connection failures, timeouts, and protocol errors -- all
        # network-layer, all retryable (brief decision 2).
        raise IngestTransientError(f"transport error: {exc}") from exc
    _raise_for_response(response)
    return response


async def post_validate(
    client: httpx.AsyncClient, config: IngestClientConfig, envelope_json: bytes
) -> dict[str, Any]:
    response = await _request(
        client,
        "POST",
        f"{config.api_url}/api/v1/external-ingest/validate",
        content=envelope_json,
        headers={**auth_headers(config), "Content-Type": "application/json"},
    )
    return response.json()


async def post_batch(
    client: httpx.AsyncClient,
    config: IngestClientConfig,
    envelope_json: bytes,
    *,
    idempotency_key: str,
) -> tuple[int, dict[str, Any]]:
    response = await _request(
        client,
        "POST",
        f"{config.api_url}/api/v1/external-ingest/batches",
        content=envelope_json,
        headers={
            **auth_headers(config),
            "Content-Type": "application/json",
            # Idempotency-Key header is optional-but-must-match the body's
            # idempotencyKey (CC2); the CLI always sends both, byte-identical.
            "Idempotency-Key": idempotency_key,
        },
    )
    return response.status_code, response.json()


async def get_batch_status(
    client: httpx.AsyncClient, config: IngestClientConfig, ingestion_id: str
) -> dict[str, Any]:
    response = await _request(
        client,
        "GET",
        f"{config.api_url}/api/v1/external-ingest/batches/{ingestion_id}",
        headers=auth_headers(config),
    )
    return response.json()


async def get_schema_document(
    client: httpx.AsyncClient, api_url: str
) -> dict[str, Any] | None:
    """Best-effort fetch of ``GET /schemas`` (public, no auth) for its
    ``limits`` object (brief decision 6 amended -- server-reported limits
    preferred over the hardcoded default). Returns ``None`` on ANY failure
    (network error, non-2xx, malformed JSON) -- this is a courtesy
    pre-check, never a hard dependency; `POST /batches` remains the
    authoritative enforcement point regardless."""
    try:
        response = await client.get(
            f"{api_url}/api/v1/external-ingest/schemas", timeout=10.0
        )
        response.raise_for_status()
        body = response.json()
    except Exception:
        logger.debug("GET /schemas limits pre-check failed; using local defaults")
        return None
    return body if isinstance(body, dict) else None


__all__ = [
    "IngestApiError",
    "IngestClientConfig",
    "IngestTransientError",
    "auth_headers",
    "get_batch_status",
    "get_schema_document",
    "parse_error_envelope",
    "post_batch",
    "post_validate",
    "redact_secrets",
]
