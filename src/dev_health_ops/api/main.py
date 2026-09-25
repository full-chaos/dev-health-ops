from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime

from fastapi import Depends, FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from dev_health_ops.logging_config import configure_logging
from dev_health_ops.sentry import init_sentry
from dev_health_ops.tracing import init_tracing

# Configure structured JSON logging, Sentry, and OpenTelemetry as early as possible
configure_logging()
init_sentry()
init_tracing()

from dev_health_ops.api.external_ingest import router as external_ingest_router
from dev_health_ops.api.external_ingest.errors import (
    register_external_ingest_error_handlers,
)
from dev_health_ops.api.external_ingest.status import (
    status_router as external_ingest_status_router,
)
from dev_health_ops.api.go_served import raise_served_by_go_api
from dev_health_ops.api.internal import router as internal_acr_router
from dev_health_ops.api.middleware.rate_limit import limiter
from dev_health_ops.api.product_telemetry import router as product_telemetry_router
from dev_health_ops.api.telemetry.router import router as telemetry_router

from ._errors import (
    _generic_exception_handler as _generic_exception_handler,
)
from ._errors import (
    register_exception_handlers,
)
from ._health import (
    _check_celery_health,
    _check_clickhouse_health,
    _check_go_worker_presence,
    _check_postgres_health,
    _check_rate_limiter_backend,
    _check_redis_health,
    _expected_worker_groups,
)
from ._lifespan import lifespan
from ._middleware import register_middleware
from ._observability import register_observability
from .admin import router as admin_router
from .admin.impersonation import router as impersonation_router
from .auth import router as auth_router
from .auth.router import get_current_user
from .billing import router as billing_router
from .dev.router import (
    AskDevApiError,
    ask_dev_error_handler,
    ask_dev_validation_error_handler,
)
from .dev.router import (
    router as dev_router,
)
from .graphql.app import create_graphql_app
from .ingest import router as ingest_router
from .licensing import router as licensing_router
from .models.filters import (
    DrilldownRequest,
    ExplainRequest,
    FilterOptionsResponse,
    HomeRequest,
    InvestmentExplainRequest,
    InvestmentFlowRequest,
    SankeyRequest,
    WorkUnitRequest,
)
from .models.schemas import (
    AggregatedFlameResponse,
    DrilldownResponse,
    ExplainResponse,
    FlameResponse,
    HealthResponse,
    HeatmapResponse,
    HomeResponse,
    InvestmentMixExplanation,
    InvestmentResponse,
    InvestmentSunburstSlice,
    MetaResponse,
    OpportunitiesResponse,
    PersonDrilldownResponse,
    PersonMetricResponse,
    PersonSearchResult,
    PersonSummaryResponse,
    QuadrantResponse,
    SankeyResponse,
    WorkUnitExplanation,
    WorkUnitInvestment,
)
from .orgs import router as orgs_router
from .services.auth import AuthenticatedUser
from .webhooks import router as webhooks_router

logger = logging.getLogger(__name__)


app = FastAPI(
    title="Dev Health Ops API",
    version="1.0.0",
    docs_url="/docs",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.state.limiter = limiter
register_exception_handlers(app)
register_external_ingest_error_handlers(app)
app.add_exception_handler(AskDevApiError, ask_dev_error_handler)
app.add_exception_handler(RequestValidationError, ask_dev_validation_error_handler)

register_middleware(app)

graphql_app = create_graphql_app()
app.include_router(graphql_app, prefix="/graphql")
app.include_router(webhooks_router)
app.include_router(admin_router)
app.include_router(impersonation_router)
app.include_router(auth_router)
app.include_router(billing_router)
app.include_router(dev_router)
app.include_router(licensing_router)
app.include_router(telemetry_router)
app.include_router(product_telemetry_router)
app.include_router(ingest_router)
app.include_router(external_ingest_router)
app.include_router(external_ingest_status_router)
app.include_router(internal_acr_router)
app.include_router(orgs_router)

register_observability(app)


@app.api_route("/health", methods=["GET", "HEAD"], response_model=HealthResponse)
async def health() -> HealthResponse | JSONResponse:
    """Deep health check: verifies all critical dependencies.

    Returns 200 when all required services (Postgres, ClickHouse, Redis)
    are reachable. Returns 503 if any required service is down.
    """
    services: dict[str, str] = {}
    required_statuses: list[str] = []

    # Run all checks concurrently for speed
    results = await asyncio.gather(
        _check_postgres_health(),
        _check_clickhouse_health(),
        _check_redis_health(),
        return_exceptions=True,
    )

    for result in results:
        if isinstance(result, BaseException):
            services["unknown"] = "error"
            required_statuses.append("error")
        else:
            key, status_val = result
            services[key] = status_val
            required_statuses.append(status_val)

    # Rate-limiter backend is informational — not counted toward overall health.
    _, rl_status = await _check_rate_limiter_backend()
    services["rate_limiter"] = rl_status

    overall = (
        "ok"
        if all(s in ("ok", "not_configured") for s in required_statuses)
        else "down"
    )
    response = HealthResponse(status=overall, services=services)
    if overall != "ok":
        content = (
            response.model_dump()
            if hasattr(response, "model_dump")
            else response.dict()
        )
        return JSONResponse(status_code=503, content=content)
    return response


@app.api_route("/ready", methods=["GET", "HEAD"])
async def ready() -> JSONResponse:
    """Readiness probe: fast check that the API process is up.

    Unlike /health, this does NOT verify external dependencies.
    Kubernetes/Docker should use this for liveness probes and /health
    for readiness probes.
    """
    return JSONResponse(status_code=200, content={"status": "ready"})


@app.api_route("/health/workers", methods=["GET", "HEAD"])
async def health_workers() -> JSONResponse:
    """Worker fleet health check.

    Separated from /health because Celery inspect.ping is slow (~2s).
    Use this for worker monitoring dashboards, not for SSR readiness.

    Three states, selected by ``EXPECTED_WORKER_GROUPS``:

    - Unset (legacy/Celery deployments): Celery ``inspect.ping`` is
      authoritative. Zero responding workers ("no_workers") is a failure,
      not "ok" -- CHAOS-3942: the endpoint used to fold that case into
      "ok", hiding the one condition it was built to detect.
    - Set but empty/malformed (e.g. ``","`` or whitespace): a declared-but-
      unparseable fleet is a misconfiguration, not "no fleet declared" --
      this fails closed rather than silently falling back to Celery mode,
      where a stray Celery worker could mask a broken Go deployment.
    - Set with at least one group (Go worker deployments -- e.g. Full
      Chaos's own production, which runs no Celery workers at all): the
      declared Go worker groups' heartbeat presence in
      ``public.worker_instances`` is authoritative. Celery is still
      reported, but its "no_workers" reading is relabeled "retired"
      (informational) since that is expected on a Go-only deployment, not
      a failure.
    """
    expected_groups = _expected_worker_groups()

    if expected_groups is None:
        key, status_val = await _check_celery_health()
        overall = "ok" if status_val == "ok" else "down"
        status_code = 200 if overall == "ok" else 503
        return JSONResponse(
            status_code=status_code,
            content={"status": overall, "services": {key: status_val}},
        )

    if not expected_groups:
        return JSONResponse(
            status_code=503,
            content={
                "status": "down",
                "services": {"expected_worker_groups": "misconfigured"},
            },
        )

    go_statuses = await _check_go_worker_presence(expected_groups)
    _, celery_status = await _check_celery_health()
    if celery_status == "no_workers":
        celery_status = "retired"

    services = {f"go_worker:{group}": status for group, status in go_statuses.items()}
    services["celery"] = celery_status

    complete = set(go_statuses) == set(expected_groups)
    overall = (
        "ok"
        if complete and all(status == "ok" for status in go_statuses.values())
        else "down"
    )
    status_code = 200 if overall == "ok" else 503
    return JSONResponse(
        status_code=status_code,
        content={"status": overall, "services": services},
    )


async def keep_alive_wrapper(coro):
    """
    Yields whitespace every 5 seconds while waiting for the coroutine to finish.
    The final JSON is yielded as the last chunk.
    """
    task = asyncio.create_task(coro)
    try:
        while True:
            done, pending = await asyncio.wait([task], timeout=5)
            if done:
                result = await task
                if hasattr(result, "model_dump_json"):
                    yield result.model_dump_json()
                else:
                    yield json.dumps(result)
                break
            # Yield whitespace to keep proxy/load-balancer connection alive
            yield " "
    except Exception:
        logger.exception("Streaming error in keep_alive_wrapper")
        yield json.dumps(
            {
                "error": "Streaming error",
                "detail": "An internal error has occurred.",
            }
        )
        yield json.dumps(
            {
                "error": "Streaming error",
                "detail": "An internal streaming error occurred.",
            }
        )


# The 32 query-api stubs below keep the private name CHAOS-6241 gave the
# refusal (ci/check_go_served_paths.py recognises it); it is the one definition
# in api/go_served.py.
_raise_served_by_go_api = raise_served_by_go_api


# --- CHAOS-6241: the 32 REST routes below are served by query-api. Each
# handler's body is reduced to _raise_served_by_go_api(): the route stays
# mounted, with its original signature (path, methods, params, Depends,
# response_model, rate-limit decorator) unchanged, so its OpenAPI schema
# entry and its dependency-injected auth gate (get_current_user runs BEFORE
# the body, same as before) stay exactly as they were -- only the body that
# used to compute a response is gone. See internal/goapiproof/
# goserved_ledger.json's analogous GraphQL-side deletions and
# graphql/models/data_health.py's metric_lineage for the same pattern. ---


@app.get("/api/v1/meta", response_model=MetaResponse)
async def meta() -> MetaResponse | JSONResponse:
    """Return backend metadata including DB kind, version, limits, and supported endpoints."""
    _raise_served_by_go_api("/api/v1/meta")


@app.post("/api/v1/home", response_model=HomeResponse)
@limiter.limit("60/minute")
async def home_post(
    request: Request,
    payload: HomeRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> HomeResponse:
    _raise_served_by_go_api("/api/v1/home")


@app.get("/api/v1/home", response_model=HomeResponse)
@limiter.limit("60/minute")
async def home(
    request: Request,
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> HomeResponse:
    _raise_served_by_go_api("/api/v1/home")


@app.post("/api/v1/explain", response_model=ExplainResponse)
async def explain_post(
    payload: ExplainRequest, current_user: AuthenticatedUser = Depends(get_current_user)
) -> ExplainResponse:
    _raise_served_by_go_api("/api/v1/explain")


@app.get("/api/v1/explain", response_model=ExplainResponse)
@limiter.limit("20/minute")
async def explain(
    request: Request,
    response: Response,
    metric: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ExplainResponse:
    _raise_served_by_go_api("/api/v1/explain")


@app.get("/api/v1/heatmap", response_model=HeatmapResponse)
@limiter.limit("20/minute")
async def heatmap(
    request: Request,
    type: str,
    metric: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    x: str = "",
    y: str = "",
    limit: int = 50,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> HeatmapResponse:
    _raise_served_by_go_api("/api/v1/heatmap")


@app.post("/api/v1/work-units", response_model=list[WorkUnitInvestment])
async def work_units_post(
    payload: WorkUnitRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> list[WorkUnitInvestment]:
    _raise_served_by_go_api("/api/v1/work-units")


@app.get("/api/v1/work-units", response_model=list[WorkUnitInvestment])
async def work_units(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 200,
    include_textual: bool = True,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> list[WorkUnitInvestment]:
    _raise_served_by_go_api("/api/v1/work-units")


@app.post(
    "/api/v1/work-units/{work_unit_id}/explain",
    response_model=WorkUnitExplanation,
)
@limiter.limit("20/minute")
async def work_unit_explain_endpoint(
    request: Request,
    work_unit_id: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    llm_provider: str = "auto",
    llm_model: str | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> WorkUnitExplanation:
    """Generate an LLM explanation for a work unit's precomputed investment view."""
    _raise_served_by_go_api("/api/v1/work-units/{work_unit_id}/explain")


@app.get("/api/v1/flame", response_model=FlameResponse)
@limiter.limit("20/minute")
async def flame(
    request: Request,
    entity_type: str,
    entity_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> FlameResponse:
    _raise_served_by_go_api("/api/v1/flame")


@app.get("/api/v1/flame/aggregated", response_model=AggregatedFlameResponse)
@limiter.limit("20/minute")
async def flame_aggregated(
    request: Request,
    mode: str,
    start_date: date | None = None,
    end_date: date | None = None,
    range_days: int = 30,
    team_id: str = "",
    repo_id: str = "",
    provider: str = "",
    work_scope_id: str = "",
    limit: int = 500,
    min_value: int = 1,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> AggregatedFlameResponse:
    """Get an aggregated flame graph for cycle breakdown or code hotspots."""
    _raise_served_by_go_api("/api/v1/flame/aggregated")


@app.get("/api/v1/quadrant", response_model=QuadrantResponse)
@limiter.limit("60/minute")
async def quadrant(
    request: Request,
    type: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
    start_date: date | None = None,
    end_date: date | None = None,
    bucket: str = "week",
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> QuadrantResponse:
    _raise_served_by_go_api("/api/v1/quadrant")


@app.post("/api/v1/drilldown/prs", response_model=DrilldownResponse)
@limiter.limit("60/minute")
async def drilldown_prs_post(
    request: Request,
    payload: DrilldownRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> DrilldownResponse:
    _raise_served_by_go_api("/api/v1/drilldown/prs")


@app.get("/api/v1/drilldown/prs", response_model=DrilldownResponse)
@limiter.limit("60/minute")
async def drilldown_prs(
    request: Request,
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> DrilldownResponse:
    _raise_served_by_go_api("/api/v1/drilldown/prs")


@app.post("/api/v1/drilldown/issues", response_model=DrilldownResponse)
@limiter.limit("60/minute")
async def drilldown_issues_post(
    request: Request,
    payload: DrilldownRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> DrilldownResponse:
    _raise_served_by_go_api("/api/v1/drilldown/issues")


@app.get("/api/v1/drilldown/issues", response_model=DrilldownResponse)
@limiter.limit("60/minute")
async def drilldown_issues(
    request: Request,
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> DrilldownResponse:
    _raise_served_by_go_api("/api/v1/drilldown/issues")


@app.get("/api/v1/people", response_model=list[PersonSearchResult])
@limiter.limit("60/minute")
async def people_search(
    request: Request,
    q: str = "",
    limit: int = 20,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> list[PersonSearchResult]:
    _raise_served_by_go_api("/api/v1/people")


@app.get("/api/v1/people/{person_id}/summary", response_model=PersonSummaryResponse)
@limiter.limit("60/minute")
async def people_summary(
    person_id: str,
    request: Request,
    range_days: int = 14,
    compare_days: int = 14,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> PersonSummaryResponse:
    _raise_served_by_go_api("/api/v1/people/{person_id}/summary")


@app.get("/api/v1/people/{person_id}/metric", response_model=PersonMetricResponse)
async def people_metric(
    person_id: str,
    metric: str,
    request: Request,
    range_days: int = 14,
    compare_days: int = 14,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> PersonMetricResponse:
    _raise_served_by_go_api("/api/v1/people/{person_id}/metric")


@app.get(
    "/api/v1/people/{person_id}/drilldown/prs",
    response_model=PersonDrilldownResponse,
)
@limiter.limit("60/minute")
async def people_drilldown_prs(
    person_id: str,
    request: Request,
    range_days: int = 14,
    limit: int = 50,
    cursor: datetime | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> PersonDrilldownResponse:
    _raise_served_by_go_api("/api/v1/people/{person_id}/drilldown/prs")


@app.get(
    "/api/v1/people/{person_id}/drilldown/issues",
    response_model=PersonDrilldownResponse,
)
@limiter.limit("60/minute")
async def people_drilldown_issues(
    person_id: str,
    request: Request,
    range_days: int = 14,
    limit: int = 50,
    cursor: datetime | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> PersonDrilldownResponse:
    _raise_served_by_go_api("/api/v1/people/{person_id}/drilldown/issues")


@app.get("/api/v1/opportunities", response_model=OpportunitiesResponse)
@limiter.limit("60/minute")
async def opportunities(
    request: Request,
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> OpportunitiesResponse:
    _raise_served_by_go_api("/api/v1/opportunities")


@app.post("/api/v1/opportunities", response_model=OpportunitiesResponse)
@limiter.limit("60/minute")
async def opportunities_post(
    request: Request,
    payload: HomeRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> OpportunitiesResponse:
    _raise_served_by_go_api("/api/v1/opportunities")


@app.get("/api/v1/investment", response_model=InvestmentResponse)
@limiter.limit("60/minute")
async def investment(
    request: Request,
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
    start_date: date | None = None,
    end_date: date | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> InvestmentResponse:
    _raise_served_by_go_api("/api/v1/investment")


@app.post("/api/v1/investment", response_model=InvestmentResponse)
@limiter.limit("60/minute")
async def investment_post(
    request: Request,
    payload: HomeRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> InvestmentResponse:
    _raise_served_by_go_api("/api/v1/investment")


@app.get(
    "/api/v1/investment/sunburst",
    response_model=list[InvestmentSunburstSlice],
)
async def investment_sunburst(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 500,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> list[InvestmentSunburstSlice]:
    _raise_served_by_go_api("/api/v1/investment/sunburst")


@app.post(
    "/api/v1/investment/explain",
    response_model=InvestmentMixExplanation,
)
@limiter.limit("20/minute")
async def investment_explain(
    request: Request,
    payload: InvestmentExplainRequest,
    llm_provider: str = "auto",
    force_refresh: bool = False,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> InvestmentMixExplanation:
    # CHAOS-6241: the CHAOS-4977 step-5b Go-dispatch forward
    # (maybe_dispatch_investment_explain_to_go) is removed along with the
    # rest of this body -- ingress.queryApiPaths (CHAOS-6239) now routes
    # this path to query-api before it ever reaches Python, making the
    # per-request dispatch decision moot. The dispatcher module and its own
    # tests (tests/api/graphql/test_investment_explain_dispatcher.py) are
    # untouched; they test the dispatcher function directly, not this route.
    _raise_served_by_go_api("/api/v1/investment/explain")


@app.post("/api/v1/investment/flow", response_model=SankeyResponse)
@limiter.limit("20/minute")
async def investment_flow(
    request: Request,
    payload: InvestmentFlowRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> SankeyResponse:
    _raise_served_by_go_api("/api/v1/investment/flow")


@app.post("/api/v1/investment/flow/repo-team", response_model=SankeyResponse)
async def investment_flow_repo_team(
    payload: InvestmentFlowRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> SankeyResponse:
    _raise_served_by_go_api("/api/v1/investment/flow/repo-team")


@app.get("/api/v1/sankey", response_model=SankeyResponse)
@limiter.limit("60/minute")
async def sankey_get(
    request: Request,
    response: Response,
    mode: str = "investment",
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
    start_date: date | None = None,
    end_date: date | None = None,
    window_start: date | None = None,
    window_end: date | None = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> SankeyResponse:
    _raise_served_by_go_api("/api/v1/sankey")


@app.post("/api/v1/sankey", response_model=SankeyResponse)
@limiter.limit("60/minute")
async def sankey_post(
    request: Request,
    payload: SankeyRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> SankeyResponse:
    _raise_served_by_go_api("/api/v1/sankey")


@app.get("/api/v1/filters/options", response_model=FilterOptionsResponse)
@limiter.limit("60/minute")
async def filter_options(
    request: Request,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> FilterOptionsResponse:
    _raise_served_by_go_api("/api/v1/filters/options")
