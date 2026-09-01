"""The Python edge dispatcher (CHAOS-4697) -- the hop that decides, per
``/graphql`` request, whether query-api (Go) owns the operation, mints a
signed effective-principal envelope, forwards, and falls back to Python
safely on every failure. Without this module, ``go_api_routing_state`` rows
enable nothing: ``PostgresSwitch`` is only consulted *inside* query-api,
and nothing routed a request there before this.

**The insertion seam.** ``get_context`` (``app.py``) is a FastAPI
``Depends()`` hook: it RETURNS a ``GraphQLContext``, and a dependency
cannot short-circuit the response FastAPI eventually sends -- so the
dispatch decision cannot live there. The actual per-request entrypoint is
``strawberry.http.async_base_view.AsyncBaseHTTPView.run`` (via
``strawberry.fastapi.router.GraphQLRouter``'s ``handle_http_get``/
``handle_http_post``, both of which do nothing but ``await self.run(...)``
and return whatever it returns). :class:`GoApiDispatchRouter` overrides
``run`` and is used in place of the plain ``GraphQLRouter`` in
``create_graphql_app`` -- this is "wrap the route", not "hook the context
getter", and it has a concrete benefit beyond satisfying that constraint:
``run`` receives the ALREADY-BUILT ``context`` (``get_context`` already
ran as the FastAPI dependency), so ``context.user``/``.tier``/
``.licensed_features`` -- each independently resolved once per request --
are reused here rather than a second time.

**The document digest.** ``go_api_document_digest.document_digest`` is the
ONE place this module computes a digest, pinned against Go's own producer
by a cross-language conformance test -- see that module's docstring for
the measured Python/Go whitespace divergence this guards against.

**Forwarding.** The original POST body is forwarded completely unchanged
(``await request.body()``, no re-parse/re-dump) -- see
:func:`_build_outbound_body`'s docstring for the one case (an incoming
GET, CHAOS-4706) where a body must be constructed at all, and why that
construction is not the GraphQL-reprint defect class CHAOS-4696 documents.

**Read-only assumption.** Every operation reachable through this
dispatcher today is a read-only, idempotent GraphQL *query* -- the ONLY
reason falling back to Python **after** attempting Go is safe (plan §5:
never fall back after dispatch once a write outcome may be ambiguous). Do
not extend this dispatcher to mutations without re-deciding that -- see
CHAOS-4697's brief, "Wave 7".

**rollout_percentage / eligible_orgs are NOT enforced here**, matching
``routeswitch.PostgresSwitch.Enabled`` on the Go side (it takes no org
argument). A ``canary`` row is "on for everyone, revocable", not "on for
N% of traffic" -- this dispatcher does not pretend otherwise.

**Observability (CHAOS-4710).** With ``OTEL_ENABLED=false`` (the deliberate
local posture), the counters above are recorded but exported nowhere, and
every ``logger.*`` call before this ticket sat on a failure/fallback
branch -- a request Go served successfully was silent by construction,
making a routing-row enablement unfalsifiable from outside. Two additions,
both independent of OTel posture:

* An unconditional INFO line on every terminal plane decision (served-go
  in :func:`_forward_to_go`'s 200 branch; every fallback in
  :meth:`GoApiDispatchRouter._fallback`), naming the operation, the plane,
  and -- for the served-go line specifically -- the document digest. Judged
  against sampling and decided against it: this method only runs for
  requests whose document digest already matched the registered-operation
  catalog (``GO_API_DISPATCH_ATTEMPTED_TOTAL``'s population, not all
  GraphQL traffic -- the overwhelming-majority "not a Go-eligible document"
  case returns before either log site and stays silent, exactly as
  before), so volume is bounded by dispatch attempts, not site traffic. An
  operator clicking once in a browser must reliably see that one request;
  a sampled line that misses it would be worse than none.
* ``x-dev-health-plane: go|python`` on the HTTP response, gated by
  ``GO_API_PLANE_HEADER_ENABLED`` (see :func:`_plane_header_enabled`) --
  default OFF, set on the local stack only (ruling: team-lead,
  2026-09-01). Applied once, in :meth:`GoApiDispatchRouter.run`, after the
  plane is already decided (dispatched vs. ``super().run()``) so it always
  reflects the response actually being sent, including every fallback path
  -- a header that claimed ``go`` after a fallback would be worse than no
  header at all.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import TYPE_CHECKING, Any

import httpx
from starlette.requests import Request
from starlette.responses import Response
from strawberry.fastapi import GraphQLRouter
from strawberry.http.typevars import Context as _Context
from strawberry.http.typevars import RootValue as _RootValue
from strawberry.types.unset import UNSET, UnsetType

from dev_health_ops.db import get_postgres_session

from .go_api_dispatch_telemetry import (
    GO_API_DISPATCH_ATTEMPTED_TOTAL,
    GO_API_DISPATCH_DIGEST_MISS_TOTAL,
    GO_API_DISPATCH_FALLBACK_TOTAL,
    GO_API_DISPATCH_LATENCY_SECONDS,
    GO_API_DISPATCH_SERVED_GO_TOTAL,
)
from .go_api_document_digest import document_digest
from .go_api_operation_catalog import operation_for_digest
from .go_api_registry import lookup_routing_state
from .principal_envelope import issue_effective_principal_envelope

if TYPE_CHECKING:
    from .context import GraphQLContext

logger = logging.getLogger(__name__)

__all__ = ["GoApiDispatchRouter"]

_DEFAULT_DISPATCH_TIMEOUT_SECONDS = 5.0

#: Modes for which the routing row explicitly says "not reachable" -- the
#: safe default, same as no row at all (models/go_api_registry.py's
#: MODES docstring: "python and disabled are both 'not reachable'").
_SAFE_DEFAULT_MODES = frozenset({"python", "disabled"})
#: Modes for which the row says "reachable" -- see RoutingState's
#: docstring; canary and primary are the only two.
_REACHABLE_MODES = frozenset({"canary", "primary"})

#: CHAOS-4710 deliverable 2. Response header naming the plane that actually
#: served the request -- gated by GO_API_PLANE_HEADER_ENABLED (see
#: _plane_header_enabled below), OFF by default. Documented beside this
#: dispatcher's other GO_API_* knobs (GO_API_QUERY_API_URL above,
#: GO_API_DISPATCH_TIMEOUT_SECONDS below): set on the local stack's `api`
#: service only (ruling: team-lead, 2026-09-01) -- the env var lives in the
#: repo-root compose.yml, outside this repo, not in this PR.
_PLANE_HEADER_NAME = "x-dev-health-plane"

_PLANE_HEADER_TRUTHY = {"1", "true", "yes", "on"}


def _plane_header_enabled() -> bool:
    """Default OFF, fail *quiet* not fail *on*: an unset, empty, or
    unrecognized value all mean "do not emit the header" -- there is no
    misconfiguration shape here that turns the header on by accident."""
    return (os.getenv("GO_API_PLANE_HEADER_ENABLED") or "").strip().lower() in (
        _PLANE_HEADER_TRUTHY
    )


def _with_plane_header(response: Response, plane: str) -> Response:
    """Stamp `response` with the plane that actually served it, iff the
    env gate is on. Called from exactly one place (`GoApiDispatchRouter.run`,
    after the plane is already decided) so every response this router
    returns -- served-go AND every fallback path -- gets a header that
    reflects the truth, never a stale or optimistic guess."""
    if _plane_header_enabled():
        response.headers[_PLANE_HEADER_NAME] = plane
    return response


_http_client: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient()
    return _http_client


def _dispatch_timeout_seconds() -> float:
    raw = os.getenv("GO_API_DISPATCH_TIMEOUT_SECONDS")
    if not raw:
        return _DEFAULT_DISPATCH_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_DISPATCH_TIMEOUT_SECONDS
    # codex round 2 (P2, EXECUTED): float("inf") does not raise ValueError
    # and `inf > 0` is True, so a misconfigured (or "helpfully" set to
    # "unlimited") env var used to disable the fallback timeout entirely --
    # a Go service that accepts the connection but never responds would
    # hang every dispatched request forever, uncounted, defeating the
    # entire fail-closed contract this module exists to provide. Require a
    # genuinely finite, positive value.
    if not math.isfinite(value) or value <= 0:
        return _DEFAULT_DISPATCH_TIMEOUT_SECONDS
    return value


async def _extract_operation(
    request: Request,
) -> tuple[str, dict[str, Any] | None, str | None] | None:
    """Extract ``(query_text, variables, operation_name)`` from the
    request, losslessly -- JSON-decoding a POST body or URL-decoding GET
    query params, never parsing/reprinting the GraphQL document itself.
    Returns ``None`` when there is no usable query text (malformed body,
    missing ``query`` field/param) -- the caller treats that exactly like
    "not a Go-eligible request" and lets strawberry's normal path handle
    (and error on, if appropriate) the request.
    """
    if request.method == "GET":
        query = request.query_params.get("query")
        if not query:
            return None
        variables_raw = request.query_params.get("variables")
        variables: dict[str, Any] | None = None
        if variables_raw:
            try:
                variables = json.loads(variables_raw)
            except json.JSONDecodeError:
                return None
        operation_name = request.query_params.get("operationName") or None
        return query, variables, operation_name

    if request.method == "POST":
        body = await request.body()
        if not body:
            return None
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        query = parsed.get("query")
        if not isinstance(query, str) or not query:
            return None
        variables = parsed.get("variables")
        if variables is not None and not isinstance(variables, dict):
            variables = None
        operation_name = parsed.get("operationName")
        if not isinstance(operation_name, str):
            operation_name = None
        return query, variables, operation_name

    return None


async def _build_outbound_body(
    request: Request,
    query_text: str,
    variables: dict[str, Any] | None,
    operation_name: str | None,
) -> bytes:
    """The bytes POSTed to query-api's ``/query``.

    For an original POST request: the ORIGINAL body, byte-for-byte,
    unchanged -- ``await request.body()`` returns the cached bytes read
    during :func:`_extract_operation` (Starlette caches ``Request.body()``,
    so this is not a second read of the socket). This is genuinely
    verbatim: no re-parse, no re-dump.

    For an original GET request (CHAOS-4706 -- query-api's ``/query`` is
    POST-only, and a GET has no body to forward, verbatim or otherwise):
    the edge always forwards as POST, constructing a JSON body from the
    already-extracted ``(query_text, variables, operation_name)``. This is
    a REPORTED, deliberate decision (CHAOS-4706 option 2: "the edge always
    forwards as POST, normalising the method at the dispatcher"), not a
    silent papering-over -- see CHAOS-4697's final report. It is NOT the
    graphql-print defect class CHAOS-4696 documents: ``json.dumps`` of a
    Python string is a lossless, well-defined transform (escaping control/
    quote characters per the JSON spec), never a GraphQL
    parse-and-reformat -- the exact query TEXT bytes Go's
    ``json.Unmarshal`` recovers on the other end are the same bytes
    :func:`_extract_operation` read from the URL (via a lossless URL
    decode) and the same bytes this module's :func:`document_digest`
    hashed. The clean fix (query-api accepting GET, mirroring what this
    Python edge already does) is CHAOS-4706's, not this dispatcher's.
    """
    if request.method == "POST":
        return await request.body()

    payload: dict[str, Any] = {"query": query_text}
    if variables is not None:
        payload["variables"] = variables
    if operation_name is not None:
        payload["operationName"] = operation_name
    return json.dumps(payload).encode("utf-8")


class GoApiDispatchRouter(GraphQLRouter[_Context, _RootValue]):
    """A :class:`~strawberry.fastapi.GraphQLRouter` that dispatches
    Go-eligible, Go-enabled operations to query-api before falling back to
    strawberry's own execution -- see this module's docstring for why
    ``run`` (not ``context_getter``) is the override point.
    """

    async def run(
        self,
        request: Any,
        context: Any = UNSET,
        root_value: Any = UNSET,
    ) -> Any:
        if self.is_websocket_request(request):
            # Subscriptions are out of scope: every operation this
            # dispatcher can ever route is a registered query document
            # (see go_api_operations.json) -- there is no websocket path
            # to intercept.
            return await super().run(
                request=request, context=context, root_value=root_value
            )

        if not isinstance(context, UnsetType):
            dispatched = await self._maybe_dispatch_to_go(request, context)
            if dispatched is not None:
                return _with_plane_header(dispatched, "go")

        response = await super().run(
            request=request, context=context, root_value=root_value
        )
        return _with_plane_header(response, "python")

    async def _maybe_dispatch_to_go(
        self, request: Request, context: GraphQLContext
    ) -> Response | None:
        """Returns a :class:`Response` to send directly (Go served the
        request), or ``None`` to mean "run strawberry's normal Python
        path" -- every ``return None`` in this method and the ones it
        calls is a deliberate fall-back-to-Python decision, and every one
        is telemetered (see ``go_api_dispatch_telemetry.py``) except the
        very first ("not configured at all" / "not a Go-eligible
        document"), which is the overwhelming-majority case and would
        otherwise dominate the fallback counters with noise.
        """
        base_url = os.getenv("GO_API_QUERY_API_URL")
        if not base_url:
            # No query-api configured for this edge process at all --
            # the master kill switch, and the default (compose overlay is
            # Wave-0 shape locally; nothing routes anywhere until this is
            # explicitly set). Not telemetered: this is "the dispatcher is
            # off", not a per-request fallback decision.
            return None
        schema_digest = os.getenv("GO_API_SCHEMA_DIGEST")
        if not schema_digest:
            return None

        try:
            extracted = await _extract_operation(request)
        except Exception:
            logger.exception("go_api_dispatch.extract_failed")
            return None
        if extracted is None:
            return None
        query_text, variables, operation_name = extracted

        try:
            doc_digest = document_digest(query_text)
        except Exception:
            # A client-supplied query text can contain bytes that decode to
            # a valid Python str but do not re-encode to UTF-8 (e.g. a lone
            # surrogate from a `\uD800`-shaped JSON escape) -- json.loads
            # accepts it, but document_digest's .encode("utf-8") raises.
            # This must never escape as an unhandled exception (that would
            # turn an authenticated request into a 500 instead of a normal
            # Python-served response, AND go uncounted) -- fall back to
            # Python's own execution, which parses/validates the query
            # itself and will reject it the same way it always has.
            logger.exception(
                "go_api_dispatch.digest_computation_failed",
                extra={"query_length": len(query_text)},
            )
            GO_API_DISPATCH_FALLBACK_TOTAL.labels(
                operation="unknown", reason="digest_computation_error"
            ).inc()
            return None

        selected_operation = operation_for_digest(doc_digest)
        if selected_operation is None:
            # Not a document this edge's catalog recognizes -- the
            # overwhelming majority of GraphQL traffic. Not a fallback;
            # there was never a Go candidate here.
            return None

        GO_API_DISPATCH_ATTEMPTED_TOTAL.labels(operation=selected_operation).inc()

        try:
            async with get_postgres_session() as session:
                routing = await lookup_routing_state(
                    session,
                    schema_digest=schema_digest,
                    document_digest=doc_digest,
                    selected_operation=selected_operation,
                )
        except Exception:
            logger.exception(
                "go_api_dispatch.registry_lookup_failed",
                extra={"operation": selected_operation},
            )
            return self._fallback(selected_operation, "registry_lookup_error")

        if routing is None:
            return self._fallback(selected_operation, "no_routing_row")
        if routing.mode in _SAFE_DEFAULT_MODES:
            return self._fallback(selected_operation, f"mode_{routing.mode}")
        if routing.mode == "shadow":
            logger.error(
                "go_api_dispatch.shadow_mode_not_implemented: mode='shadow' "
                "is configured for operation=%s but the shadow executor "
                "does not exist (go_api_comparator.py is comparison-only "
                "over already-captured snapshots; ruled SKIP for local, "
                "CHAOS-4697 brief). Serving Python -- but this must NEVER "
                "read as 'not canaried': someone turned shadow on and it "
                "is not running.",
                selected_operation,
            )
            return self._fallback(selected_operation, "mode_shadow_unimplemented")
        if routing.mode not in _REACHABLE_MODES:
            # Defensive: the DB CHECK constraint on `mode` should make
            # this unreachable. Named separately from the vocabulary
            # above so a future mode addition fails loudly here instead
            # of silently matching the wrong branch.
            logger.error(
                "go_api_dispatch.unknown_mode",
                extra={"operation": selected_operation, "mode": routing.mode},
            )
            return self._fallback(selected_operation, "unknown_mode")

        # rollout_percentage / eligible_orgs are deliberately NOT enforced
        # here -- see this module's docstring. canary/primary both mean
        # "on for everyone, revocable" at this layer.

        if context.user is None:
            # Never forward unauthenticated -- fail closed to Python.
            return self._fallback(selected_operation, "unauthenticated")
        if context.tier is None or context.licensed_features is None:
            # Best-effort resolution in get_context can legitimately fail
            # (see context.py's docstring); the envelope issuer requires
            # both as non-optional kwargs, so this is fail-closed, not a
            # bug to paper over with a default tier/empty feature list.
            return self._fallback(selected_operation, "envelope_inputs_missing")

        try:
            envelope = issue_effective_principal_envelope(
                context.user,
                tier=context.tier,
                licensed_features=context.licensed_features,
            )
        except Exception:
            logger.exception(
                "go_api_dispatch.envelope_signing_failed",
                extra={"operation": selected_operation},
            )
            return self._fallback(selected_operation, "envelope_signing_error")

        try:
            outbound_body = await _build_outbound_body(
                request, query_text, variables, operation_name
            )
        except Exception:
            logger.exception(
                "go_api_dispatch.build_outbound_body_failed",
                extra={"operation": selected_operation},
            )
            return self._fallback(selected_operation, "build_outbound_body_error")

        return await self._forward_to_go(
            base_url, selected_operation, doc_digest, envelope, outbound_body
        )

    async def _forward_to_go(
        self,
        base_url: str,
        selected_operation: str,
        doc_digest: str,
        envelope: str,
        outbound_body: bytes,
    ) -> Response | None:
        timeout = _dispatch_timeout_seconds()
        started = time.monotonic()
        client = _get_http_client()
        try:
            resp = await client.post(
                f"{base_url.rstrip('/')}/query",
                content=outbound_body,
                headers={
                    "Authorization": f"Bearer {envelope}",
                    "Content-Type": "application/json",
                },
                timeout=timeout,
            )
        except httpx.TimeoutException:
            GO_API_DISPATCH_LATENCY_SECONDS.labels(
                plane="go", outcome="timeout"
            ).observe(time.monotonic() - started)
            return self._fallback(selected_operation, "go_timeout")
        except httpx.ConnectError:
            GO_API_DISPATCH_LATENCY_SECONDS.labels(
                plane="go", outcome="connection_error"
            ).observe(time.monotonic() - started)
            return self._fallback(selected_operation, "go_connection_error")
        except httpx.HTTPError:
            logger.exception(
                "go_api_dispatch.go_request_failed",
                extra={"operation": selected_operation},
            )
            GO_API_DISPATCH_LATENCY_SECONDS.labels(
                plane="go", outcome="request_error"
            ).observe(time.monotonic() - started)
            return self._fallback(selected_operation, "go_request_error")

        elapsed = time.monotonic() - started

        if resp.status_code == 200:
            GO_API_DISPATCH_LATENCY_SECONDS.labels(
                plane="go", outcome="served"
            ).observe(elapsed)
            GO_API_DISPATCH_SERVED_GO_TOTAL.labels(operation=selected_operation).inc()
            # CHAOS-4710 deliverable 1: the ONE line proving a specific
            # request was served by Go, independent of OTel export posture
            # (OTEL_ENABLED=false exports GO_API_DISPATCH_SERVED_GO_TOTAL
            # nowhere locally -- this line works regardless). Unconditional,
            # not sampled -- see this module's docstring for why.
            logger.info(
                "go_api_dispatch.served_go operation=%s plane=go document_digest=%s",
                selected_operation,
                doc_digest,
            )
            return Response(
                content=resp.content,
                status_code=200,
                media_type=resp.headers.get("content-type", "application/json"),
            )

        if resp.status_code == 404:
            # Post-CHAOS-4696: this means DIGEST DRIFT between the edge's
            # catalog and the deployed query-api binary, not "unregistered"
            # -- we already matched the catalog locally before forwarding.
            logger.error(
                "go_api_dispatch.go_404_digest_miss: operation=%s digest=%s "
                "matched this edge's catalog but query-api 404'd -- this is "
                "DRIFT, not an unregistered document. ALERT.",
                selected_operation,
                doc_digest,
            )
            GO_API_DISPATCH_DIGEST_MISS_TOTAL.labels(operation=selected_operation).inc()
            GO_API_DISPATCH_LATENCY_SECONDS.labels(
                plane="go", outcome="digest_miss"
            ).observe(elapsed)
            return self._fallback(selected_operation, "go_404_digest_miss")

        if resp.status_code == 405:
            # Should not occur -- this dispatcher always forwards as POST
            # (CHAOS-4706). Kept as a distinct, named reason rather than
            # folded into "unexpected status" so a recurrence is obvious.
            logger.error(
                "go_api_dispatch.go_405_method_not_allowed: unexpected -- "
                "the dispatcher always forwards as POST",
                extra={"operation": selected_operation},
            )
            GO_API_DISPATCH_LATENCY_SECONDS.labels(
                plane="go", outcome="method_not_allowed"
            ).observe(elapsed)
            return self._fallback(selected_operation, "go_405_method_not_allowed")

        if 500 <= resp.status_code < 600:
            GO_API_DISPATCH_LATENCY_SECONDS.labels(plane="go", outcome="5xx").observe(
                elapsed
            )
            return self._fallback(selected_operation, "go_5xx")

        logger.error(
            "go_api_dispatch.go_unexpected_status",
            extra={"operation": selected_operation, "status": resp.status_code},
        )
        GO_API_DISPATCH_LATENCY_SECONDS.labels(
            plane="go", outcome="unexpected_status"
        ).observe(elapsed)
        return self._fallback(selected_operation, "go_unexpected_status")

    @staticmethod
    def _fallback(operation: str, reason: str) -> None:
        # CHAOS-4710: the single funnel every fallback decision passes
        # through -- one consistent INFO line here means the plane
        # decision is never half-instrumented, regardless of whether the
        # specific reason ALSO gets one of the pre-existing
        # logger.exception/logger.error calls upstream (those narrate the
        # exceptional condition; this one narrates the plane decision
        # itself, uniformly, for every reason including the quiet ones
        # like "mode_python" that previously logged nothing at all).
        logger.info(
            "go_api_dispatch.fallback operation=%s plane=python reason=%s",
            operation,
            reason,
        )
        GO_API_DISPATCH_FALLBACK_TOTAL.labels(operation=operation, reason=reason).inc()
        return None
