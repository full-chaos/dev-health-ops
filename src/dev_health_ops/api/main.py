from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
import logging
import os
from typing import Any, List, Literal, cast
from urllib.parse import urlparse


from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json

from dev_health_ops.metrics.sinks.factory import detect_backend, SinkBackend


from .models.filters import (
    DrilldownRequest,
    ExplainRequest,
    FilterOptionsResponse,
    HomeRequest,
    InvestmentExplainRequest,
    InvestmentFlowRequest,
    MetricFilter,
    SankeyRequest,
    ScopeFilter,
    TimeFilter,
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
    InvestmentResponse,
    InvestmentSunburstSlice,
    InvestmentMixExplanation,
    MetaResponse,
    OpportunitiesResponse,
    QuadrantResponse,
    WorkUnitExplanation,
    WorkUnitInvestment,
    PersonDrilldownResponse,
    PersonMetricResponse,
    PersonSearchResult,
    PersonSummaryResponse,
    SankeyResponse,
)
from .queries.client import clickhouse_client, query_dicts, close_global_client
from .queries.drilldown import fetch_issues, fetch_pull_requests
from .queries.filters import fetch_filter_options
from .services.cache import create_cache
from .services.explain import build_explain_response
from .services.filtering import scope_filter_for_metric, time_window
from .services.home import build_home_response
from .services.investment import build_investment_response, build_investment_sunburst
from .services.investment_flow import (
    build_investment_flow_response,
    build_investment_repo_team_flow_response,
)
from .services.investment_mix_explain import explain_investment_mix
from .services.opportunities import build_opportunities_response
from .services.people import (
    build_person_drilldown_issues_response,
    build_person_drilldown_prs_response,
    build_person_metric_response,
    build_person_summary_response,
    search_people_response,
)
from .services.heatmap import build_heatmap_response
from .services.flame import build_flame_response
from .services.aggregated_flame import build_aggregated_flame_response
from .services.quadrant import build_quadrant_response
from .services.sankey import build_sankey_response
from .services.work_units import build_work_unit_investments
from .services.work_unit_explain import explain_work_unit
from .graphql.app import create_graphql_app
from .webhooks import router as webhooks_router

HOME_CACHE = create_cache(ttl_seconds=60)
EXPLAIN_CACHE = create_cache(ttl_seconds=120)


def _sanitize_for_log(value: Any) -> str:
    """
    Remove characters that could be used to forge or split log entries.

    This is intentionally minimal to avoid changing functional behavior:
    it strips carriage returns, newlines, and other non-printable
    control characters.
    """
    if value is None:
        return ""
    text = str(value)
    # Remove CR/LF explicitly, then strip remaining control chars
    text = text.replace("\r", "").replace("\n", "")
    return "".join(ch for ch in text if ch >= " " and ch != "\x7f")


logger = logging.getLogger(__name__)

_FORBIDDEN_QUERY_PARAMS = {
    "compare_to",
    "rank",
    "percentile",
    "score",
    "leaderboard",
    "top",
    "bottom",
}


def _db_url() -> str:
    dsn = os.getenv("DATABASE_URI") or os.getenv("DATABASE_URL")
    if dsn:
        return dsn

    # Fail fast if no database configuration is provided to avoid
    # accidentally connecting to an unintended default in production.
    raise RuntimeError(
        "Database configuration is missing: set DATABASE_URI or DATABASE_URL "
        "(e.g. 'clickhouse://localhost:8123/default')."
    )


def _check_sqlalchemy_health(dsn: str) -> bool:
    from sqlalchemy import create_engine, text

    engine = create_engine(dsn, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        engine.dispose()


async def _check_sqlalchemy_health_async(dsn: str) -> bool:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(dsn, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        await engine.dispose()


def _dsn_uses_async_driver(dsn: str) -> bool:
    scheme = urlparse(dsn).scheme.lower()
    return "+asyncpg" in scheme or "+aiosqlite" in scheme


def _check_mongo_health(dsn: str) -> bool:
    from pymongo import MongoClient

    client = MongoClient(dsn, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
        return True
    except Exception:
        return False
    finally:
        try:
            client.close()
        except Exception:
            pass


async def _check_database_service(dsn: str) -> tuple[str, str]:
    try:
        backend = detect_backend(dsn)
    except Exception as exc:
        logger.warning(
            "Unable to detect database backend for health check: %s",
            _sanitize_for_log(exc),
        )
        return "database", "down"

    if backend == SinkBackend.CLICKHOUSE:
        try:
            async with clickhouse_client(dsn) as sink:
                rows = await query_dicts(sink, "SELECT 1 AS ok", {})
            return backend.value, "ok" if rows else "down"
        except Exception:
            return backend.value, "down"

    if backend in (SinkBackend.SQLITE, SinkBackend.POSTGRES):
        if _dsn_uses_async_driver(dsn):
            ok = await _check_sqlalchemy_health_async(dsn)
        else:
            ok = await asyncio.to_thread(_check_sqlalchemy_health, dsn)
        return backend.value, "ok" if ok else "down"

    if backend == SinkBackend.MONGO:
        ok = await asyncio.to_thread(_check_mongo_health, dsn)
        return backend.value, "ok" if ok else "down"

    return backend.value, "down"


def _filters_from_query(
    scope_type: str,
    scope_id: str,
    range_days: int,
    compare_days: int,
    start_date: date | None = None,
    end_date: date | None = None,
) -> MetricFilter:
    return MetricFilter(
        time=TimeFilter(
            range_days=range_days,
            compare_days=compare_days,
            start_date=start_date,
            end_date=end_date,
        ),
        scope=ScopeFilter(
            level=cast(
                Literal["org", "team", "repo", "service", "developer"], scope_type
            ),
            ids=[scope_id] if scope_id else [],
        ),
    )


def _reject_comparative_params(request: Request) -> None:
    for key in request.query_params.keys():
        if key in _FORBIDDEN_QUERY_PARAMS:
            raise HTTPException(
                status_code=400,
                detail="Comparative parameters are not supported.",
            )


def _bounded_limit_param(limit: int, max_limit: int) -> int:
    if limit <= 0:
        return min(50, max_limit)
    return min(max(limit, 1), max_limit)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_global_client()


app = FastAPI(
    title="Dev Health Ops API",
    version="1.0.0",
    docs_url="/docs",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

graphql_app = create_graphql_app()
app.include_router(graphql_app, prefix="/graphql")
app.include_router(webhooks_router)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse | JSONResponse:
    services = {}
    try:
        db_url = _db_url()
    except Exception as exc:
        logger.warning(
            "Database configuration missing for health check: %s",
            _sanitize_for_log(exc),
        )
        services["database"] = "down"
    else:
        db_key, db_status = await _check_database_service(db_url)
        services[db_key] = db_status

    try:
        services["redis"] = HOME_CACHE.status()
    except Exception:
        services["redis"] = "down"

    status = "ok" if all(state == "ok" for state in services.values()) else "down"
    response = HealthResponse(status=status, services=services)
    if status != "ok":
        content = (
            response.model_dump()
            if hasattr(response, "model_dump")
            else response.dict()
        )
        return JSONResponse(status_code=503, content=content)
    return response


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


@app.get("/api/v1/meta", response_model=MetaResponse)
async def meta() -> MetaResponse | JSONResponse:
    """
    Return backend metadata including DB kind, version, limits, and supported endpoints.
    """
    db_url = _db_url()
    backend = detect_backend(db_url).value  # Get string value from enum

    try:
        # Simple meta response using direct ClickHouse query
        version = "unknown"
        coverage: dict = {}
        if backend == "clickhouse":
            async with clickhouse_client(db_url) as sink:
                try:
                    result = sink.query_dicts("SELECT version() AS version", {})
                    version = str(result[0]["version"]) if result else "unknown"
                except Exception:
                    # Silently ignore version query failures - not critical for meta endpoint
                    pass

        return MetaResponse(
            backend=backend,
            version=version,
            last_ingest_at=None,
            coverage=coverage,
            limits={"max_days": 365, "max_repos": 1000},
            supported_endpoints=[
                "/api/v1/home",
                "/api/v1/quadrant",
                "/api/v1/flame",
                "/api/v1/heatmap",
                "/api/v1/work-units",
                "/api/v1/sankey",
                "/api/v1/investment",
                "/api/v1/opportunities",
                "/graphql",
            ],
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Metadata unavailable") from exc


@app.post("/api/v1/home", response_model=HomeResponse)
async def home_post(payload: HomeRequest) -> HomeResponse:
    try:
        return await build_home_response(
            db_url=_db_url(),
            filters=payload.filters,
            cache=HOME_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/home", response_model=HomeResponse)
async def home(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
) -> HomeResponse:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, compare_days, start_date, end_date
        )
        result = await build_home_response(
            db_url=_db_url(),
            filters=filters,
            cache=HOME_CACHE,
        )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/explain", response_model=ExplainResponse)
async def explain_post(payload: ExplainRequest) -> ExplainResponse:
    try:
        return await build_explain_response(
            db_url=_db_url(),
            metric=payload.metric,
            filters=payload.filters,
            cache=EXPLAIN_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/explain", response_model=ExplainResponse)
async def explain(
    response: Response,
    metric: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
) -> ExplainResponse:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, compare_days, start_date, end_date
        )
        result = await build_explain_response(
            db_url=_db_url(),
            metric=metric,
            filters=filters,
            cache=EXPLAIN_CACHE,
        )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/heatmap", response_model=HeatmapResponse)
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
) -> HeatmapResponse:
    _reject_comparative_params(request)
    try:
        return await build_heatmap_response(
            db_url=_db_url(),
            type=type,
            metric=metric,
            scope_type=scope_type,
            scope_id=scope_id,
            range_days=range_days,
            start_date=start_date,
            end_date=end_date,
            x=x or None,
            y=y or None,
            limit=_bounded_limit_param(limit, 200),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/work-units", response_model=list[WorkUnitInvestment])
async def work_units_post(payload: WorkUnitRequest) -> List[WorkUnitInvestment]:
    try:
        include_textual = (
            True if payload.include_textual is None else payload.include_textual
        )
        if hasattr(payload.filters, "model_dump"):
            filter_payload = payload.filters.model_dump(mode="json")
        else:
            filter_payload = payload.filters.dict()
        log_limit = str(payload.limit or 200).replace("\r", "").replace("\n", "")
        log_include_textual = str(include_textual).replace("\r", "").replace("\n", "")
        logger.debug(
            "WorkUnits POST request include_textual=%s limit=%s filters=%s",
            log_include_textual,
            log_limit,
            filter_payload,
        )
        result = await build_work_unit_investments(
            db_url=_db_url(),
            filters=payload.filters,
            limit=payload.limit or 200,
            include_text=include_textual,
        )
        logger.debug("WorkUnits POST returned count=%s", len(result))
        return result
    except Exception as exc:
        logger.exception("WorkUnits POST failed")
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


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
) -> List[WorkUnitInvestment]:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, range_days, start_date, end_date
        )
        if hasattr(filters, "model_dump"):
            filter_payload = filters.model_dump(mode="json")
        else:
            filter_payload = filters.dict()
        log_limit = str(limit).replace("\r", "").replace("\n", "")
        log_include_textual = str(include_textual).replace("\r", "").replace("\n", "")
        logger.debug(
            "WorkUnits GET request include_textual=%s limit=%s filters=%s",
            log_include_textual,
            log_limit,
            filter_payload,
        )
        result = await build_work_unit_investments(
            db_url=_db_url(),
            filters=filters,
            limit=limit,
            include_text=include_textual,
        )
        logger.debug("WorkUnits GET returned count=%s", len(result))
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        logger.exception("WorkUnits GET failed")
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post(
    "/api/v1/work-units/{work_unit_id}/explain",
    response_model=WorkUnitExplanation,
)
async def work_unit_explain_endpoint(
    work_unit_id: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
    llm_provider: str = "auto",
    llm_model: str | None = None,
) -> WorkUnitExplanation | StreamingResponse:
    """
    Generate an LLM explanation for a work unit's precomputed investment view.

    This endpoint follows the Investment model rules:
    - LLMs explain results, they NEVER compute them
    - Only allowed inputs passed to LLM (investment vectors, evidence metadata,
      evidence quality band, time span)
    - Responses use probabilistic language (appears, leans, suggests)

    Args:
        work_unit_id: The work unit to explain
        scope_type: Scope level (org, team, repo)
        scope_id: Scope identifier
        range_days: Time window in days
        start_date: Optional start date
        end_date: Optional end date
        llm_provider: LLM provider to use (auto, openai, anthropic, mock)
        llm_model: Optional model version override

    Returns:
        WorkUnitExplanation with summary, rationale, and uncertainty disclosure
    """
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, range_days, start_date, end_date
        )
        investments = await build_work_unit_investments(
            db_url=_db_url(),
            filters=filters,
            limit=1,
            include_text=True,
            work_unit_id=work_unit_id,
        )

        if not investments:
            raise HTTPException(
                status_code=404,
                detail=f"Work unit {work_unit_id} not found",
            )

        target_investment = investments[0]
        logger.info(
            "Generating streaming explanation for work_unit_id=%s",
            _sanitize_for_log(work_unit_id),
        )

        # Return streaming response with keep-alive pings
        return StreamingResponse(
            keep_alive_wrapper(
                explain_work_unit(
                    investment=target_investment,
                    llm_provider=llm_provider,
                    llm_model=llm_model,
                )
            ),
            media_type="application/json",
        )

    except HTTPException:
        raise
    except Exception as exc:
        safe_work_unit_id = work_unit_id.replace("\r", "").replace("\n", "")
        logger.exception("Work unit explain failed for %s", safe_work_unit_id)
        raise HTTPException(status_code=503, detail="Explanation unavailable") from exc


@app.get("/api/v1/flame", response_model=FlameResponse)
async def flame(
    request: Request,
    entity_type: str,
    entity_id: str,
) -> FlameResponse:
    _reject_comparative_params(request)
    try:
        return await build_flame_response(
            db_url=_db_url(),
            entity_type=entity_type,
            entity_id=entity_id,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/flame/aggregated", response_model=AggregatedFlameResponse)
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
) -> AggregatedFlameResponse:
    """
    Get an aggregated flame graph for cycle breakdown or code hotspots.

    Args:
        mode: "cycle_breakdown" or "code_hotspots"
        start_date: Start of time window (defaults to range_days ago)
        end_date: End of time window (defaults to today)
        range_days: Number of days if dates not specified
        team_id: Filter by team
        repo_id: Filter by repo (for code_hotspots)
        provider: Filter by provider (for cycle_breakdown)
        work_scope_id: Filter by work scope
        limit: Max number of items (default 500)
        min_value: Minimum value threshold (default 1)
    """
    _reject_comparative_params(request)

    if mode not in ("cycle_breakdown", "code_hotspots", "throughput"):
        raise HTTPException(
            status_code=400,
            detail="mode must be 'cycle_breakdown', 'code_hotspots', or 'throughput'",
        )

    # Calculate date window

    if end_date is None:
        end_day = date.today()
    else:
        end_day = end_date

    if start_date is None:
        start_day = end_day - timedelta(days=range_days)
    else:
        start_day = start_date

    try:
        return await build_aggregated_flame_response(
            db_url=_db_url(),
            mode=mode,  # type: ignore
            start_day=start_day,
            end_day=end_day,
            team_id=team_id or None,
            repo_id=repo_id or None,
            provider=provider or None,
            work_scope_id=work_scope_id or None,
            limit=min(max(limit, 1), 1000),
            min_value=max(min_value, 0),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/quadrant", response_model=QuadrantResponse)
async def quadrant(
    request: Request,
    type: str,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
    start_date: date | None = None,
    end_date: date | None = None,
    bucket: str = "week",
) -> QuadrantResponse:
    _reject_comparative_params(request)
    try:
        return await build_quadrant_response(
            db_url=_db_url(),
            type=type,
            scope_type=scope_type,
            scope_id=scope_id,
            range_days=range_days,
            start_date=start_date,
            end_date=end_date,
            bucket=bucket,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/drilldown/prs", response_model=DrilldownResponse)
async def drilldown_prs_post(payload: DrilldownRequest) -> DrilldownResponse:
    try:
        start_day, end_day, _, _ = time_window(payload.filters)
        async with clickhouse_client(_db_url()) as sink:
            scope_filter, scope_params = await scope_filter_for_metric(
                sink, metric_scope="repo", filters=payload.filters
            )
            items = await fetch_pull_requests(
                sink,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                limit=payload.limit or 50,
            )
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/drilldown/prs", response_model=DrilldownResponse)
async def drilldown_prs(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
) -> DrilldownResponse:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, range_days, start_date, end_date
        )
        start_day, end_day, _, _ = time_window(filters)
        async with clickhouse_client(_db_url()) as sink:
            scope_filter, scope_params = await scope_filter_for_metric(
                sink, metric_scope="repo", filters=filters
            )
            items = await fetch_pull_requests(
                sink,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/drilldown/issues", response_model=DrilldownResponse)
async def drilldown_issues_post(payload: DrilldownRequest) -> DrilldownResponse:
    try:
        start_day, end_day, _, _ = time_window(payload.filters)
        async with clickhouse_client(_db_url()) as sink:
            scope_filter, scope_params = await scope_filter_for_metric(
                sink, metric_scope="team", filters=payload.filters
            )
            items = await fetch_issues(
                sink,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                limit=payload.limit or 50,
            )
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/drilldown/issues", response_model=DrilldownResponse)
async def drilldown_issues(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
) -> DrilldownResponse:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, range_days, start_date, end_date
        )
        start_day, end_day, _, _ = time_window(filters)
        async with clickhouse_client(_db_url()) as sink:
            scope_filter, scope_params = await scope_filter_for_metric(
                sink, metric_scope="team", filters=filters
            )
            items = await fetch_issues(
                sink,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return DrilldownResponse(items=items)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/people", response_model=list[PersonSearchResult])
async def people_search(
    request: Request,
    q: str = "",
    limit: int = 20,
) -> list[PersonSearchResult]:
    _reject_comparative_params(request)
    try:
        return await search_people_response(
            db_url=_db_url(),
            query=q,
            limit=_bounded_limit_param(limit, 50),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/people/{person_id}/summary", response_model=PersonSummaryResponse)
async def people_summary(
    person_id: str,
    request: Request,
    range_days: int = 14,
    compare_days: int = 14,
) -> PersonSummaryResponse:
    _reject_comparative_params(request)
    try:
        return await build_person_summary_response(
            db_url=_db_url(),
            person_id=person_id,
            range_days=range_days,
            compare_days=compare_days,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Person not found") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/people/{person_id}/metric", response_model=PersonMetricResponse)
async def people_metric(
    person_id: str,
    metric: str,
    request: Request,
    range_days: int = 14,
    compare_days: int = 14,
) -> PersonMetricResponse:
    _reject_comparative_params(request)
    try:
        return await build_person_metric_response(
            db_url=_db_url(),
            person_id=person_id,
            metric=metric,
            range_days=range_days,
            compare_days=compare_days,
        )
    except ValueError as exc:
        detail = (
            "Metric not supported"
            if str(exc) == "metric not supported"
            else "Person not found"
        )
        status = 400 if detail == "Metric not supported" else 404
        raise HTTPException(status_code=status, detail=detail) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get(
    "/api/v1/people/{person_id}/drilldown/prs",
    response_model=PersonDrilldownResponse,
)
async def people_drilldown_prs(
    person_id: str,
    request: Request,
    range_days: int = 14,
    limit: int = 50,
    cursor: datetime | None = None,
) -> PersonDrilldownResponse:
    _reject_comparative_params(request)
    try:
        return await build_person_drilldown_prs_response(
            db_url=_db_url(),
            person_id=person_id,
            range_days=range_days,
            limit=_bounded_limit_param(limit, 200),
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Person not found") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get(
    "/api/v1/people/{person_id}/drilldown/issues",
    response_model=PersonDrilldownResponse,
)
async def people_drilldown_issues(
    person_id: str,
    request: Request,
    range_days: int = 14,
    limit: int = 50,
    cursor: datetime | None = None,
) -> PersonDrilldownResponse:
    _reject_comparative_params(request)
    try:
        return await build_person_drilldown_issues_response(
            db_url=_db_url(),
            person_id=person_id,
            range_days=range_days,
            limit=_bounded_limit_param(limit, 200),
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Person not found") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/opportunities", response_model=OpportunitiesResponse)
async def opportunities(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 14,
    compare_days: int = 14,
    start_date: date | None = None,
    end_date: date | None = None,
) -> OpportunitiesResponse:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, compare_days, start_date, end_date
        )
        result = await build_opportunities_response(
            db_url=_db_url(),
            filters=filters,
            cache=HOME_CACHE,
        )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/opportunities", response_model=OpportunitiesResponse)
async def opportunities_post(payload: HomeRequest) -> OpportunitiesResponse:
    try:
        return await build_opportunities_response(
            db_url=_db_url(),
            filters=payload.filters,
            cache=HOME_CACHE,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/investment", response_model=InvestmentResponse)
async def investment(
    response: Response,
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
    start_date: date | None = None,
    end_date: date | None = None,
) -> InvestmentResponse:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, range_days, start_date, end_date
        )
        result = await build_investment_response(db_url=_db_url(), filters=filters)
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/investment", response_model=InvestmentResponse)
async def investment_post(payload: HomeRequest) -> InvestmentResponse:
    try:
        return await build_investment_response(
            db_url=_db_url(), filters=payload.filters
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


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
) -> List[InvestmentSunburstSlice]:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, range_days, start_date, end_date
        )
        result = await build_investment_sunburst(
            db_url=_db_url(), filters=filters, limit=limit
        )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post(
    "/api/v1/investment/explain",
    response_model=InvestmentMixExplanation,
)
async def investment_explain(
    payload: InvestmentExplainRequest,
    llm_provider: str = "auto",
    force_refresh: bool = False,
):
    try:
        logger.info("Generating streaming investment explanation")
        return StreamingResponse(
            keep_alive_wrapper(
                explain_investment_mix(
                    db_url=_db_url(),
                    filters=payload.filters,
                    theme=payload.theme,
                    subcategory=payload.subcategory,
                    llm_provider=llm_provider,
                    llm_model=payload.llm_model,
                    force_refresh=force_refresh,
                )
            ),
            media_type="application/json",
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Investment explain failed")
        raise HTTPException(status_code=503, detail="Explanation unavailable") from exc


@app.post("/api/v1/investment/flow", response_model=SankeyResponse)
async def investment_flow(payload: InvestmentFlowRequest) -> SankeyResponse:
    try:
        return await build_investment_flow_response(
            db_url=_db_url(),
            filters=payload.filters,
            theme=payload.theme,
            flow_mode=payload.flow_mode,
            drill_category=payload.drill_category,
            top_n_repos=payload.top_n_repos,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Investment flow failed")
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/investment/flow/repo-team", response_model=SankeyResponse)
async def investment_flow_repo_team(payload: InvestmentFlowRequest) -> SankeyResponse:
    try:
        return await build_investment_repo_team_flow_response(
            db_url=_db_url(),
            filters=payload.filters,
            theme=payload.theme,
        )
    except Exception as exc:
        logger.exception("Investment repo-team flow failed")
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/sankey", response_model=SankeyResponse)
async def sankey_get(
    response: Response,
    mode: str = "investment",
    scope_type: str = "org",
    scope_id: str = "",
    range_days: int = 30,
    start_date: date | None = None,
    end_date: date | None = None,
    window_start: date | None = None,
    window_end: date | None = None,
) -> SankeyResponse:
    try:
        filters = _filters_from_query(
            scope_type, scope_id, range_days, range_days, start_date, end_date
        )
        result = await build_sankey_response(
            db_url=_db_url(),
            mode=mode,
            filters=filters,
            window_start=window_start,
            window_end=window_end,
        )
        if response is not None:
            response.headers["X-DevHealth-Deprecated"] = "use POST with filters"
        return result
    except Exception as exc:
        logger.exception("Sankey GET failed for mode=%s", _sanitize_for_log(mode))
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.post("/api/v1/sankey", response_model=SankeyResponse)
async def sankey_post(payload: SankeyRequest) -> SankeyResponse:
    try:
        return await build_sankey_response(
            db_url=_db_url(),
            mode=payload.mode,
            filters=payload.filters,
            context=payload.context,
            window_start=payload.window_start,
            window_end=payload.window_end,
        )
    except Exception as exc:
        logger.exception(
            "Sankey POST failed for mode=%s", _sanitize_for_log(payload.mode)
        )
        raise HTTPException(status_code=503, detail="Data unavailable") from exc


@app.get("/api/v1/filters/options", response_model=FilterOptionsResponse)
async def filter_options() -> FilterOptionsResponse:
    try:
        async with clickhouse_client(_db_url()) as sink:
            options = await fetch_filter_options(sink)
        return FilterOptionsResponse(**options)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Data unavailable") from exc
