"""The refusal a Python REST route gives once a Go service owns its path.

A route whose handler body was deleted (CHAOS-6241's 32 query-api routes,
CHAOS-6817's billing routes on go-api) stays mounted with its original
signature, decorators and auth dependency, so its OpenAPI entry and its 401
behaviour are unchanged, and its body is ``raise_served_by_go_api(path,
plane)``. Reaching it means the deployed ingress did not send the path to the
Go service that owns it: ``ci/check_go_served_paths.py`` fails a stub whose
path is not in ``ci/go_served_paths.tsv`` (the paths the ingress routes to Go).

The answer is HTTP 500 with a JSON detail naming the path, the plane and what
to check, plus ONE structured error line, because the client would otherwise
see a 500 pointing at nothing: it is always deploy/ingress skew, never a bug in
the request.
"""

from __future__ import annotations

import logging
from typing import NoReturn

from fastapi import HTTPException

logger = logging.getLogger(__name__)

QUERY_API = "query-api"
GO_API = "go-api"

_CHECKS = {
    QUERY_API: (
        "web/ingress routing did not intercept this request: verify deploy "
        "values.prod.yaml's ingress.queryApiPaths lists this path, that web's "
        "BACKEND_URL reaches the ingress controller (CHAOS-6239), and that the "
        "matching GO_API_*_ENABLED route-gate env is true on query-api "
        "(cmd/query-api/main.go)."
    ),
    GO_API: (
        "ingress routing did not intercept this request: verify the deployed "
        "ingress lists this path under go-api (values ingress.goApiPaths, "
        "ci/go_served_paths.tsv) and that dev-health-ops-go-api registers the "
        "route (cmd/dho api)."
    ),
}


class GoServedRouteUnavailableError(HTTPException):
    """A Go-served REST route reached its Python handler, which has no
    implementation (CHAOS-6241).

    Its own exception type rather than a bare HTTPException so an operator
    can grep for it, an alert can match on it, and a test can assert it
    without matching on message text. Mirrors
    ``graphql.schema.GoServedOperationUnavailableError``, the equivalent
    shape for the GraphQL surface.
    """

    def __init__(self, path: str, plane: str = QUERY_API) -> None:
        super().__init__(
            status_code=500,
            detail=(
                f"{path} is served by {plane} and has no Python "
                f"implementation. {_CHECKS[plane]}"
            ),
        )


def raise_served_by_go_api(path: str, plane: str = QUERY_API) -> NoReturn:
    """Fail loudly for a REST route a Go service owns (CHAOS-6241, 6817)."""
    event = (
        "rest.route_served_by_query_api"
        if plane == QUERY_API
        else "rest.route_served_by_go_api"
    )
    logger.error(
        event,
        extra={
            "path": path,
            "plane": plane,
            "reason": (
                f"{plane} owns this REST path and it has no Python "
                f"implementation; reaching this handler means {_CHECKS[plane]}"
            ),
        },
    )
    raise GoServedRouteUnavailableError(path, plane)
