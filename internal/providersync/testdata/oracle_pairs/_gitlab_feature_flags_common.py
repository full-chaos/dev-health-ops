from __future__ import annotations

import asyncio
import json
import pathlib
import sys
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from internal.providersync.testdata.python_oracle_loader import (
    _force_annotation_evaluation,
    _install_module,
    _install_namespace,
    _install_package,
    _load_source_module,
    _purge_dev_health_modules,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
CLIENT_SOURCE = REPO_ROOT / "src/dev_health_ops/providers/gitlab/feature_flags.py"
PROCESSOR_SOURCE = REPO_ROOT / "src/dev_health_ops/processors/gitlab_feature_flags.py"
BUILDER_SOURCE = REPO_ROOT / "src/dev_health_ops/work_graph/builder.py"
IDS_SOURCE = REPO_ROOT / "src/dev_health_ops/work_graph/ids.py"
MODELS_SOURCE = REPO_ROOT / "src/dev_health_ops/work_graph/models.py"
WORKER_SOURCE = REPO_ROOT / "src/dev_health_ops/workers/feature_flag_sync.py"


class _Record:
    def __init__(self, **values: Any) -> None:
        self.__dict__.update(values)


class _GoCompatibleFloat(float):
    """Keep Python float typing while matching Go's `%g` wire spelling."""

    def __repr__(self) -> str:
        return format(self, ".15g")


class _CapturingEdgeSink:
    def __init__(self) -> None:
        self.edges: list[Any] = []

    def ensure_schema(self) -> None:
        return None

    def write_work_graph_edges(self, records: list[Any]) -> None:
        self.edges.extend(records)

    def close(self) -> None:
        return None


class _WorkerMetricsSink:
    def __init__(self, _dsn: str) -> None:
        return None

    def write_feature_flags(self, _records: list[Any]) -> None:
        return None

    def write_feature_flag_events(self, _records: list[Any]) -> None:
        return None

    def close(self) -> None:
        return None


class _APIException(Exception):
    pass


class _AuthenticationException(_APIException):
    pass


class _RateLimitException(_APIException):
    def __init__(
        self,
        message: str = "rate limited",
        *,
        retry_after_seconds: float | None = None,
        signal: object | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds
        self.signal = signal


class _UsageRecorder:
    def __init__(self, *, resolver: object) -> None:
        self.resolver = resolver

    def record(self, **_values: Any) -> None:
        return None

    def drain(self) -> list[dict[str, Any]]:
        return []


class _RateLimitSignal:
    def __init__(self, **_values: Any) -> None:
        return None

    @staticmethod
    def reset_at_from_epoch_seconds(_value: object) -> None:
        return None


class _BudgetDimension:
    REST_CORE = "rest_core"


class _HTTPResponse:
    def __init__(
        self,
        status_code: int,
        payload: object,
        *,
        headers: dict[str, str] | None = None,
        url: str,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.url = url
        self.text = json.dumps(payload)

    def json(self) -> object:
        return self._payload


class _AsyncClient:
    pass


class _TimeoutException(Exception):
    pass


class _ConnectError(Exception):
    pass


def _configure_modules() -> None:
    _install_namespace()
    _install_package("dev_health_ops.api")
    _install_package("dev_health_ops.api.utils")
    _install_module(
        "httpx",
        {
            "AsyncClient": _AsyncClient,
            "Response": _HTTPResponse,
            "TimeoutException": _TimeoutException,
            "ConnectError": _ConnectError,
        },
    )
    _install_module(
        "dev_health_ops.api.utils.logging",
        {"sanitize_for_log": lambda value: str(value)},
    )
    _install_module(
        "dev_health_ops.connectors.exceptions",
        {
            "APIException": _APIException,
            "AuthenticationException": _AuthenticationException,
            "RateLimitException": _RateLimitException,
        },
    )
    _install_module(
        "dev_health_ops.providers._ratelimit",
        {
            "gitlab_403_is_rate_limited": lambda headers: (
                str(headers.get("Retry-After", "")).strip() != ""
                or str(headers.get("RateLimit-Remaining", "")).strip() == "0"
            ),
            "gitlab_resolve_retry_after_seconds": lambda headers: 0.0,
        },
    )
    _install_module("dev_health_ops.providers.usage", {"UsageRecorder": _UsageRecorder})
    _install_module(
        "dev_health_ops.providers.gitlab.budget",
        {"GITLAB_USAGE_RESOLVER": object()},
    )
    _install_module(
        "dev_health_ops.sync.budget_types", {"BudgetDimension": _BudgetDimension}
    )
    _install_module(
        "dev_health_ops.sync.rate_limit_signal", {"RateLimitSignal": _RateLimitSignal}
    )
    _install_module(
        "dev_health_ops.metrics.schemas",
        {"FeatureFlagEventRecord": _Record, "FeatureFlagRecord": _Record},
    )


def _load_targets() -> tuple[Any, Any]:
    # The generic runner imports this helper as a plain repository module. Load
    # the two fixed production sources under their real module names, with only
    # dependency-free shape stubs for imports unrelated to this provider-local
    # boundary. The client and both normalizers themselves are never copied.
    _purge_dev_health_modules()
    _configure_modules()
    client_module = _load_source_module(
        "dev_health_ops.providers.gitlab.feature_flags", CLIENT_SOURCE
    )
    _force_annotation_evaluation(
        "dev_health_ops.providers.gitlab.feature_flags", client_module
    )
    processor_module = _load_source_module(
        "dev_health_ops.processors.gitlab_feature_flags", PROCESSOR_SOURCE
    )
    _force_annotation_evaluation(
        "dev_health_ops.processors.gitlab_feature_flags", processor_module
    )
    return client_module, processor_module


def _load_edge_targets() -> tuple[Any, Any]:
    """Load the production builder and worker edge path with narrow sinks.

    The feature-flag worker owns latest-event selection and evidence assembly;
    the builder owns deterministic edge IDs and persisted edge fields.  The
    oracle executes both production methods and captures their records without
    importing the application or opening a ClickHouse connection.
    """
    _install_package("dev_health_ops.work_graph")
    _install_package("dev_health_ops.work_graph.extractors")
    _install_module(
        "dev_health_ops.metrics.schemas",
        {
            "FeatureFlagEventRecord": _Record,
            "FeatureFlagRecord": _Record,
            "FeatureFlagLinkRecord": _Record,
            "WorkGraphEdgeRecord": _Record,
            "WorkGraphIssuePRRecord": _Record,
            "WorkGraphPRCommitRecord": _Record,
            "WorkGraphProjectionRunRecord": _Record,
        },
    )
    _install_module(
        "dev_health_ops.metrics.sinks.factory",
        {"create_sink": lambda _dsn: _CapturingEdgeSink()},
    )
    _install_module(
        "dev_health_ops.metrics.sinks.clickhouse.idempotency",
        {"WORK_ITEMS_DEDUPED": "work_items"},
    )
    _install_module(
        "dev_health_ops.work_graph.extractors.text_parser",
        {
            "RefType": type("RefType", (), {}),
            "extract_flag_key_refs": lambda *_args, **_kwargs: [],
            "extract_github_issue_refs": lambda *_args, **_kwargs: [],
            "extract_gitlab_issue_refs": lambda *_args, **_kwargs: [],
            "extract_jira_keys": lambda *_args, **_kwargs: [],
            "extract_pr_refs": lambda *_args, **_kwargs: [],
            "extract_squash_pr_refs": lambda *_args, **_kwargs: [],
        },
    )
    _install_module(
        "dev_health_ops.work_graph.operational_edges",
        {"build_operational_incident_edges": lambda *_args, **_kwargs: []},
    )
    _load_source_module("dev_health_ops.work_graph.models", MODELS_SOURCE)
    _load_source_module("dev_health_ops.work_graph.ids", IDS_SOURCE)
    builder_module = _load_source_module(
        "dev_health_ops.work_graph.builder", BUILDER_SOURCE
    )
    _force_annotation_evaluation("dev_health_ops.work_graph.builder", builder_module)
    _install_module(
        "dev_health_ops.workers.async_runner",
        {"run_async": lambda coroutine: asyncio.run(coroutine)},
    )
    worker_module = _load_source_module(
        "dev_health_ops.workers.feature_flag_sync", WORKER_SOURCE
    )
    _force_annotation_evaluation(
        "dev_health_ops.workers.feature_flag_sync", worker_module
    )
    return builder_module, worker_module


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _status(case: dict[str, Any], endpoint: str) -> int:
    return int(case.get(f"{endpoint}_status", 200))


def _headers(
    case: dict[str, Any], endpoint: str, page: int | None = None
) -> dict[str, str]:
    configured = case.get(f"{endpoint}_headers", {})
    if isinstance(configured, list) and page is not None and page - 1 < len(configured):
        configured = configured[page - 1]
    if not isinstance(configured, dict):
        configured = {}
    return {str(key): str(value) for key, value in configured.items()}


class _FeatureFlagsTransport:
    def __init__(self, case: dict[str, Any]) -> None:
        self.case = case
        self.requests: list[str] = []
        self.is_closed = False

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> _HTTPResponse:
        if method != "GET":
            raise AssertionError(f"unexpected method {method}")
        params = params or {}
        request_path = path
        if params:
            request_path += "?" + urlencode(params)
        self.requests.append(request_path)
        if path.endswith("/feature_flags"):
            page = int(params.get("page", 1))
            pages = self.case.get("flags_pages", [])
            payload = pages[page - 1] if page - 1 < len(pages) else []
            headers = _headers(self.case, "flags", page)
            next_pages = self.case.get("next_pages", [])
            if (
                isinstance(next_pages, list)
                and page - 1 < len(next_pages)
                and next_pages[page - 1]
            ):
                headers.setdefault("X-Next-Page", str(next_pages[page - 1]))
            return _HTTPResponse(
                _status(self.case, "flags"), payload, headers=headers, url=request_path
            )
        if "/projects/" in path:
            return _HTTPResponse(
                _status(self.case, "project"),
                self.case.get("project_payload", {}),
                headers=_headers(self.case, "project"),
                url=request_path,
            )
        raise AssertionError(f"unexpected path {path}")


@dataclass(frozen=True)
class FeatureFlagsTrace:
    flags: list[dict[str, Any]]
    events: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    project_key: str
    requests: list[str]
    error: str


def trace_fields() -> frozenset[str]:
    return frozenset(field.name for field in fields(FeatureFlagsTrace))


def _error_class(exc: Exception) -> str:
    if isinstance(exc, _RateLimitException):
        return "rate_limited"
    if isinstance(exc, _AuthenticationException):
        return "authentication"
    if isinstance(exc, _APIException):
        return "api"
    return type(exc).__name__


def _build_worker_edges(
    *,
    raw_flags: list[dict[str, Any]],
    project_key: str,
    org_id: str,
    project_id_or_path: str,
    normalized_at: datetime,
) -> list[dict[str, Any]]:
    """Execute the real worker/builder edge construction into a capture sink."""
    builder_module, worker_module = _load_edge_targets()
    edge_sink = _CapturingEdgeSink()
    builder_module.create_sink = lambda _dsn: edge_sink
    production_builder = builder_module.WorkGraphBuilder

    class _DeterministicBuilder(production_builder):  # type: ignore[valid-type,misc]
        def __init__(self, config: Any) -> None:
            super().__init__(config)
            self._now = normalized_at

    builder_module.WorkGraphBuilder = _DeterministicBuilder

    class _PinnedDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
            if tz is None:
                return normalized_at.replace(tzinfo=None)
            return normalized_at.astimezone(tz)

    # WorkGraphEdge's production dataclass defaults call the models module's
    # datetime.now directly for discovered_at/last_synced. Pin that real
    # default factory to the oracle observation so timestamps remain
    # comparable to the Go route without replacing the builder methods.
    sys.modules["dev_health_ops.work_graph.models"].datetime = _PinnedDateTime  # type: ignore[attr-defined]

    class _WorkerClient:
        def __init__(self, *, private_token: str, base_url: str) -> None:
            if not private_token or not base_url:
                raise AssertionError("worker client was not configured")

        async def __aenter__(self) -> _WorkerClient:
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

        async def get_feature_flags(self, _project: str) -> list[dict[str, Any]]:
            return raw_flags

        async def get_project_name(self, _project: str) -> str:
            return project_key

    _install_module(
        "dev_health_ops.providers.gitlab.feature_flags",
        {"GitLabFeatureFlagsClient": _WorkerClient},
    )
    _install_module(
        "dev_health_ops.metrics.sinks.clickhouse",
        {"ClickHouseMetricsSink": _WorkerMetricsSink},
    )
    _install_module(
        "dev_health_ops.metrics.job_work_items",
        {"attach_work_item_partial_observations": lambda *_args, **_kwargs: None},
    )
    _install_module(
        "dev_health_ops.providers.usage", {"drain_provider_usage": lambda _client: []}
    )
    worker_module._sync_gitlab_feature_flags(
        db_url="clickhouse://capture",
        org_id=org_id,
        credentials={"token": "oracle", "gitlab_url": "https://gitlab.test"},
        sync_options={"project_id": project_id_or_path},
    )
    rows: list[dict[str, Any]] = []
    for record in edge_sink.edges:
        row = vars(record).copy()
        # The Go generic encoder spells a float with strconv's `%g`, while
        # Python repr(1.0) retains the trailing `.0`. Preserve the production
        # float type and only align the two lossless type-tag wire spellings.
        row["confidence"] = _GoCompatibleFloat(row["confidence"])
        rows.append(row)
    return rows


def build_trace(case: dict[str, Any]) -> dict[str, Any]:
    client_module, processor_module = _load_targets()
    normalized_at = _parse_time(case["normalized_at"])

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
            if tz is None:
                return normalized_at.replace(tzinfo=None)
            return normalized_at.astimezone(tz)

    # The production normalizer stamps its own `now`; pin that live call to
    # the same deterministic observation used by the Go route so all semantic
    # fields remain comparable while the source functions stay executable.
    processor_module.datetime = _FixedDateTime
    transport = _FeatureFlagsTransport(case)
    client = client_module.GitLabFeatureFlagsClient(
        private_token="oracle",
        base_url="https://gitlab.test",
        max_retries=int(case.get("max_retries", 1)),
        per_page=int(case.get("per_page", 100)),
    )
    client._client = transport
    project_id_or_path = str(case["project_id_or_path"])
    raw_flags: list[dict[str, Any]] = []
    project_key = project_id_or_path
    error = ""
    try:
        raw_flags = asyncio.run(
            client.get_feature_flags(
                project_id_or_path, per_page=int(case.get("per_page", 100))
            )
        )
        project_key = asyncio.run(client.get_project_name(project_id_or_path))
        flags = processor_module.normalize_gitlab_feature_flags(
            raw_flags,
            project_key=project_key,
            org_id=str(case.get("org_id", "org-acme")),
        )
        events = processor_module.snapshot_gitlab_feature_flag_events(
            raw_flags,
            project_key=project_key,
            org_id=str(case.get("org_id", "org-acme")),
            observed_at=normalized_at,
        )
        edges = _build_worker_edges(
            raw_flags=raw_flags,
            project_key=project_key,
            org_id=str(case.get("org_id", "org-acme")),
            project_id_or_path=str(case["project_id_or_path"]),
            normalized_at=normalized_at,
        )
        trace = FeatureFlagsTrace(
            flags=sorted(
                (vars(row) for row in flags),
                key=lambda row: (row["flag_key"], row["environment"]),
            ),
            events=sorted(
                (vars(row) for row in events),
                key=lambda row: (row["flag_key"], row["environment"]),
            ),
            edges=sorted(
                edges,
                key=lambda row: (row["edge_id"], row["edge_type"]),
            ),
            project_key=project_key,
            requests=["/api/v4" + path for path in transport.requests],
            error=error,
        )
    except Exception as exc:  # noqa: BLE001 - oracle records provider outcome
        error = _error_class(exc)
        trace = FeatureFlagsTrace(
            flags=[],
            events=[],
            edges=[],
            project_key=project_key,
            requests=["/api/v4" + path for path in transport.requests],
            error=error,
        )
    return asdict(trace)
