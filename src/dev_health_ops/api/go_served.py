"""The refusal a Go-served REST route's Python handler answers with.

A route the Go api serves keeps its Python route mounted with the handler body
reduced to :func:`_raise_served_by_go_api` (CHAOS-6241's 32 routes in
``api/main.py``, and every family deleted after them): the path, methods,
parameters, ``Depends`` gates, ``response_model`` and rate-limit decorators stay
exactly as they were, so the OpenAPI entry and the auth gate run before the
refusal as before, and only the body that computed a response is gone.

This module is the ONE definition. ``ci/check_go_served_paths.py`` recognises a
stub by the call NAME ``_raise_served_by_go_api``, so the function keeps that
name in every module that calls it, and a stub body may be nothing else.
"""

from __future__ import annotations

import logging
from typing import NoReturn

from fastapi import HTTPException

logger = logging.getLogger(__name__)


class GoServedRouteUnavailableError(HTTPException):
    """A query-api-served REST route reached its Python handler, which has
    no implementation (CHAOS-6241).

    Its own exception type rather than a bare HTTPException so an operator
    can grep for it, an alert can match on it, and a future test can assert
    it without matching on message text. Mirrors
    ``graphql.schema.GoServedOperationUnavailableError``, the equivalent
    shape for the GraphQL surface.
    """

    def __init__(self, path: str) -> None:
        super().__init__(
            status_code=500,
            detail=(
                f"{path} is served by query-api and has no Python "
                "implementation. web/ingress routing did not intercept this "
                "request: verify deploy values.prod.yaml's "
                "ingress.queryApiPaths lists this path, that web's "
                "BACKEND_URL reaches the ingress controller (CHAOS-6239), "
                "and that the matching GO_API_*_ENABLED route-gate env is "
                "true on query-api (cmd/query-api/main.go)."
            ),
        )


def _raise_served_by_go_api(path: str) -> NoReturn:
    """Fail loudly for a REST route query-api owns (CHAOS-6241).

    ONE structured error line before raising, per the standing telemetry
    rule: the exception reaches the client as a 500 with a JSON body but no
    server context, so without this the operator sees a 500 pointing at
    nothing -- which is always deploy/ingress skew, never a bug in the
    request. Mirrors ``graphql.schema._raise_served_by_query_api``.
    """
    logger.error(
        "rest.route_served_by_query_api",
        extra={
            "path": path,
            "reason": (
                "query-api owns this REST path and it has no Python "
                "implementation; reaching this handler means ingress "
                "routing did not intercept -- check deploy "
                "values.prod.yaml's ingress.queryApiPaths and query-api's "
                "GO_API_*_ENABLED gate for this path"
            ),
        },
    )
    raise GoServedRouteUnavailableError(path)
