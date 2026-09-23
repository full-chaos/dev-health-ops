"""
Structural tests for the work-unit explain route's mounting (CHAOS-2351).

CHAOS-6241 deleted this route's Python body (it is served by query-api,
see main.py's work_unit_explain_endpoint and _raise_served_by_go_api) --
every behavioural test that exercised that body (missing-key/invalid-
provider 4xx shapes, streaming-vs-JSON error shape, the 404-on-unknown-
work-unit happy path) is gone with it. What remains here is structural
only: the route's signature/decorator still carry the properties that
mattered before the body was deleted, and a future accidental removal of
either (e.g. while adding a new Go-served route nearby) is still worth
catching source-inspection style, cheaply, with no HTTP client needed.
"""

from __future__ import annotations

import inspect


def test_work_unit_explain_endpoint_has_rate_limit_decorator():
    """work_unit_explain_endpoint must carry @limiter.limit('20/minute').

    Uses source inspection — the same technique used in test_login_rate_limit.py
    — because slowapi wraps the handler at decoration time, making runtime
    reflection unreliable.
    """
    from dev_health_ops.api import main as main_module

    source = inspect.getsource(main_module)

    # Find the block around work_unit_explain_endpoint
    # The decorator must appear between the @app.post and the async def.
    endpoint_idx = source.find("async def work_unit_explain_endpoint(")
    assert endpoint_idx != -1, "work_unit_explain_endpoint not found in main.py"

    # Look at the 300 chars before the def for the decorator
    preamble = source[max(0, endpoint_idx - 300) : endpoint_idx]
    assert '@limiter.limit("20/minute")' in preamble, (
        "work_unit_explain_endpoint is missing @limiter.limit('20/minute'). "
        "Authenticated callers can amplify LLM spend without a rate limit."
    )


def test_work_unit_explain_endpoint_accepts_request_param():
    """work_unit_explain_endpoint must accept a Request parameter for slowapi.

    slowapi's @limiter.limit decorator requires the handler to accept a
    fastapi.Request argument so it can extract the client IP / key.
    """
    from dev_health_ops.api.main import work_unit_explain_endpoint

    sig = inspect.signature(work_unit_explain_endpoint)
    assert "request" in sig.parameters, (
        "work_unit_explain_endpoint must have a 'request: Request' parameter "
        "for the slowapi rate limiter to function."
    )
