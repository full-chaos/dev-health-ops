"""REST forwarder for ``POST /api/v1/investment/explain`` (CHAOS-4977 step
5b) -- a sibling of :mod:`go_api_dispatcher`, not an extension of it.

**Why a separate module.** ``go_api_dispatcher`` routes GraphQL documents
by digest through the Postgres ``go_api_routing_state`` registry -- a
document-shaped key this REST route has no analog for (team-lead ruling,
CHAOS-4977: "a NEW path-keyed REST forwarder..., NOT an extension of the
digest dispatcher"). This module matches exactly one fixed path and is
gated by one env-var switch, ``GO_API_INVESTMENT_EXPLAIN_ENABLED`` -- the
SAME name query-api's own ``routeswitch.DynamicSwitch`` reads (see
``cmd/query-api/investment_explain_route.go``), so the two processes stay
in lock-step from one operator-set variable, with no Postgres registry row
required for this route.

**Target URL / auth / org-context propagation** reuse exactly what
``go_api_dispatcher`` uses: ``GO_API_QUERY_API_URL`` as the query-api base
URL, and :func:`issue_effective_principal_envelope` (with tier/licensed-
features resolved the same way ``graphql/app.py``'s ``get_context`` does)
minted into an ``Authorization: Bearer`` header on the outbound request.
Unlike the GraphQL context, a REST request here already carries an
authenticated ``AuthenticatedUser`` (``get_current_user`` 401s before this
module ever runs), so there is no ``context.user is None`` branch to
mirror.

**Streaming is the one real behavioral difference from
``go_api_dispatcher``.** ``_forward_to_go`` there buffers the whole
upstream response (``await client.post(...)``) before returning it --
correct for a GraphQL response, which is one JSON document. This route's
Go handler writes ``keep_alive`` ``" "`` bytes across the wire every 5
seconds while ``ExplainInvestmentMix`` runs (see
``cmd/query-api/investment_explain_route.go``'s ``writeKeepAliveJSON``);
buffering the whole response here would collect those keep-alive bytes
into one read that only completes once the WHOLE call finishes, which
defeats their purpose (a proxy/load balancer sees no traffic during the
gap and can time the connection out anyway). ``httpx.AsyncClient.send(...,
stream=True)`` reads only the response headers before returning, so the
non-200 fallback decision below still happens before this module commits
to a streamed response -- the body itself is then handed to Starlette's
``StreamingResponse`` unread, one chunk at a time, and the underlying
upstream connection is closed via ``BackgroundTask(upstream.aclose)``
after the client finishes reading (or disconnects) -- the standard
httpx+FastAPI reverse-proxy idiom.

**Fallback semantics** mirror ``go_api_dispatcher.GoApiDispatchRouter.
_fallback``: any failure before or during the *decision* (switch off, no
target URL, envelope resolution/signing failure, connect/timeout to
query-api, a non-200 status) returns ``None`` and the caller runs the
existing Python ``explain_investment_mix`` path unchanged. Once
query-api's stream has actually started (headers received, 200 status),
this module no longer falls back -- an in-flight streamed response can't
be un-sent, matching ``go_api_dispatcher``'s own read-only "never fall
back after dispatch" rule (that rule's stated reason, ambiguous write
outcome, does not even apply here as sharply since this endpoint's writes
are cache-only/idempotent, but the mechanical reason -- bytes already on
the wire -- applies regardless).
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import TYPE_CHECKING

import httpx
from starlette.background import BackgroundTask
from starlette.requests import Request
from starlette.responses import StreamingResponse

from dev_health_ops.db import get_postgres_session

from .investment_explain_dispatch_telemetry import (
    INVESTMENT_EXPLAIN_DISPATCH_ATTEMPTED_TOTAL,
    INVESTMENT_EXPLAIN_DISPATCH_FALLBACK_TOTAL,
    INVESTMENT_EXPLAIN_DISPATCH_SERVED_GO_TOTAL,
    INVESTMENT_EXPLAIN_DISPATCH_STREAM_TRUNCATED_TOTAL,
)
from .principal_envelope import issue_effective_principal_envelope

if TYPE_CHECKING:
    from dev_health_ops.api.services.auth import AuthenticatedUser
    from dev_health_ops.licensing.types import LicenseTier

logger = logging.getLogger(__name__)

__all__ = ["maybe_dispatch_investment_explain_to_go"]

#: Same name as cmd/query-api/investment_explain_route.go's
#: investmentExplainEnabledEnvVar -- one operator-set variable gates both
#: processes' routing of this one path.
_ENABLED_ENV_VAR = "GO_API_INVESTMENT_EXPLAIN_ENABLED"

_ENABLED_TRUTHY = {"1", "t", "true"}

_TARGET_PATH = "/api/v1/investment/explain"

#: The one operation this dispatcher handles -- kept as a literal label
#: value rather than dropped, see investment_explain_dispatch_telemetry's
#: comment on why these counters carry an `operation` label at all.
_OPERATION_LABEL = "investment_explain"

#: Connect-phase timeout, reusing go_api_dispatcher's own knob so an
#: operator tuning query-api reachability tunes both dispatchers at once.
_DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0

#: Per-chunk read timeout while streaming the response body: 3x the Go
#: handler's 5-second keep-alive cadence (writeKeepAliveJSON), so a
#: legitimate keep-alive gap never trips this timeout on its own, while a
#: genuinely stalled upstream still gets noticed.
_STREAM_READ_TIMEOUT_SECONDS = 15.0


def _investment_explain_dispatch_enabled() -> bool:
    """Case-insensitive match on Go's strconv.ParseBool "true" spellings
    only ("1"/"t"/"true") -- this is a one-way operator toggle, not a
    place that needs strconv.ParseBool's false spellings too, since
    anything not recognized here already means "off"."""
    raw = (os.getenv(_ENABLED_ENV_VAR) or "").strip().lower()
    return raw in _ENABLED_TRUTHY


async def _resolve_envelope_inputs(
    org_id: str,
) -> tuple[LicenseTier | None, list[str] | None]:
    """Mirrors graphql/app.py's get_context tier/licensed_features
    resolution exactly (same two service calls, same best-effort-on-
    failure contract: both stay None on any error, never coerced to a
    default)."""
    try:
        org_uuid = uuid.UUID(org_id)
    except ValueError:
        return None, None

    from dev_health_ops.api.services.licensing import (
        resolve_licensed_features_async,
        resolve_org_tier_async,
    )

    try:
        async with get_postgres_session() as db:
            tier = await resolve_org_tier_async(db, org_uuid)
            licensed_features = await resolve_licensed_features_async(db, org_uuid)
    except Exception:
        logger.warning(
            "investment_explain_dispatch.envelope_inputs_failed org_id=%s",
            org_id,
            exc_info=True,
        )
        return None, None
    return tier, licensed_features


def _build_http_client() -> httpx.AsyncClient:
    """A fresh client per call, not go_api_dispatcher's process-lifetime
    singleton: this seam exists so tests can monkeypatch it to an
    httpx.MockTransport-backed client (the established convention in
    test_go_api_dispatcher.py) without a real network call. A per-call
    client is closed explicitly on every return path below (including the
    streamed-success path, via BackgroundTask)."""
    return httpx.AsyncClient()


def _fallback(reason: str) -> None:
    logger.info("investment_explain_dispatch.fallback plane=python reason=%s", reason)
    INVESTMENT_EXPLAIN_DISPATCH_FALLBACK_TOTAL.labels(reason=reason).inc()
    return None


async def maybe_dispatch_investment_explain_to_go(
    request: Request,
    *,
    current_user: AuthenticatedUser,
    llm_provider: str,
    force_refresh: bool,
) -> StreamingResponse | None:
    """Returns a streaming response already reading from query-api, or
    None to signal "run the existing Python handler" -- the caller
    (api/main.py's investment_explain) is expected to treat None exactly
    like go_api_dispatcher's callers treat its own None return."""
    INVESTMENT_EXPLAIN_DISPATCH_ATTEMPTED_TOTAL.labels(operation=_OPERATION_LABEL).inc()

    if not _investment_explain_dispatch_enabled():
        _fallback("disabled")
        return None

    base_url = os.getenv("GO_API_QUERY_API_URL")
    if not base_url:
        _fallback("no_target_url")
        return None

    tier, licensed_features = await _resolve_envelope_inputs(current_user.org_id)
    if tier is None or licensed_features is None:
        _fallback("envelope_inputs_missing")
        return None

    try:
        envelope = issue_effective_principal_envelope(
            current_user, tier=tier, licensed_features=licensed_features
        )
    except Exception:
        logger.exception("investment_explain_dispatch.envelope_signing_failed")
        _fallback("envelope_signing_error")
        return None

    body = await request.body()
    outbound_url = f"{base_url.rstrip('/')}{_TARGET_PATH}"
    outbound_params = {
        "llm_provider": llm_provider,
        "force_refresh": "true" if force_refresh else "false",
    }
    outbound_headers = {
        "Authorization": f"Bearer {envelope}",
        "Content-Type": "application/json",
    }
    timeout = httpx.Timeout(
        connect=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
        read=_STREAM_READ_TIMEOUT_SECONDS,
        write=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
        pool=_DEFAULT_CONNECT_TIMEOUT_SECONDS,
    )

    client = _build_http_client()
    try:
        upstream_request = client.build_request(
            "POST",
            outbound_url,
            content=body,
            params=outbound_params,
            headers=outbound_headers,
            timeout=timeout,
        )
    except httpx.InvalidURL:
        # A malformed GO_API_QUERY_API_URL (operator misconfiguration,
        # not per-request attacker-controlled input) must still fall back
        # to Python rather than crash every request -- build_request was
        # previously OUTSIDE any try/except here, so this exception
        # escaped uncaught. httpx.InvalidURL is a bare Exception subclass,
        # NOT an httpx.HTTPError subclass (confirmed via
        # httpx.InvalidURL.__mro__ on a live install, not assumed from
        # the name) -- catching httpx.HTTPError alone, matching the
        # pattern below, would NOT have caught this. Caught by codex
        # round 2.
        await client.aclose()
        _fallback("build_request_error")
        return None
    try:
        upstream = await client.send(upstream_request, stream=True)
    except httpx.TimeoutException:
        await client.aclose()
        _fallback("go_timeout")
        return None
    except httpx.HTTPError:
        await client.aclose()
        _fallback("go_connection_error")
        return None

    if upstream.status_code != 200:
        await upstream.aclose()
        await client.aclose()
        _fallback("go_non_200")
        return None

    logger.info(
        "investment_explain_dispatch.served_go plane=go org_id=%s",
        current_user.org_id,
    )
    INVESTMENT_EXPLAIN_DISPATCH_SERVED_GO_TOTAL.labels(operation=_OPERATION_LABEL).inc()

    async def _close_upstream() -> None:
        await upstream.aclose()
        await client.aclose()

    async def _stream_with_truncation_telemetry():
        # Once these headers are sent, this dispatcher can no longer fall
        # back (see the module docstring's "never fall back after
        # dispatch" section) -- a read timeout/disconnect here is silent
        # otherwise. This is the only telemetry for that specific failure
        # class; codex round 1 (P2) found neither a log line nor a
        # counter existed for it.
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
        except httpx.HTTPError:
            logger.warning(
                "investment_explain_dispatch.stream_truncated plane=go org_id=%s",
                current_user.org_id,
                exc_info=True,
            )
            INVESTMENT_EXPLAIN_DISPATCH_STREAM_TRUNCATED_TOTAL.labels(
                operation=_OPERATION_LABEL
            ).inc()
            raise

    return StreamingResponse(
        _stream_with_truncation_telemetry(),
        status_code=200,
        media_type=upstream.headers.get("content-type", "application/json"),
        background=BackgroundTask(_close_upstream),
    )
