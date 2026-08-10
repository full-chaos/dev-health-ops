"""Load fixed live Python sources for checked-in Go parity oracles.

The Go quality job intentionally has only a stock Python interpreter.  These
oracles must execute the production functions they compare, but importing a
``dev_health_ops`` package normally also runs unrelated application initializers
that require the full service dependency set (including SQLAlchemy).

This loader therefore creates an isolated project namespace and executes only
the five fixed oracle sources, plus three dependency-free support modules.  No
caller-controlled module name or source path is ever imported.  The target
source itself remains live: editing a production function changes oracle output.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = (ROOT / "src").resolve(strict=True)


def _source(relative_path: str) -> Path:
    """Resolve one checked-in source path once, while building the allowlist."""
    return (SOURCE_ROOT / relative_path).resolve(strict=True)


_BUDGET_TYPES_SOURCE = _source("dev_health_ops/sync/budget_types.py")
_DATASETS_SOURCE = _source("dev_health_ops/sync/datasets.py")
_USAGE_SOURCE = _source("dev_health_ops/providers/usage.py")
_EXCEPTIONS_SOURCE = _source("dev_health_ops/exceptions.py")
_HTTP_SOURCE = _source("dev_health_ops/providers/_http.py")
_RATELIMIT_SOURCE = _source("dev_health_ops/providers/_ratelimit.py")
_RATE_LIMIT_SIGNAL_SOURCE = _source("dev_health_ops/sync/rate_limit_signal.py")
_GITLAB_RATELIMIT_SOURCE = _source("dev_health_ops/providers/gitlab/ratelimit.py")
_LAUNCHDARKLY_PROCESSOR_SOURCE = _source("dev_health_ops/processors/launchdarkly.py")
_GITHUB_BUDGET_SOURCE = _source("dev_health_ops/providers/github/budget.py")
_GITLAB_BUDGET_SOURCE = _source("dev_health_ops/providers/gitlab/budget.py")
_LINEAR_BUDGET_SOURCE = _source("dev_health_ops/providers/linear/budget.py")
_JIRA_BUDGET_SOURCE = _source("dev_health_ops/providers/jira/budget.py")
_LAUNCHDARKLY_BUDGET_SOURCE = _source("dev_health_ops/providers/launchdarkly/budget.py")
_DATASET_ADAPTERS_SOURCE = _source("dev_health_ops/processors/dataset_adapters.py")
_BASE_GIT_SOURCE = _source("dev_health_ops/processors/base_git.py")
_COMPLEXITY_SOURCE = _source("dev_health_ops/analytics/complexity.py")
_GITHUB_CODE_CLIENT_SOURCE = _source("dev_health_ops/providers/github/code_client.py")
_GITHUB_WORK_ITEM_OPTIONS_SOURCE = _source(
    "dev_health_ops/providers/github/work_item_options.py"
)
_GITLAB_CODE_CLIENT_SOURCE = _source("dev_health_ops/providers/gitlab/code_client.py")
_GITLAB_COMMITS_SOURCE = _source("dev_health_ops/providers/gitlab/commits.py")
_GITLAB_COMMIT_STATS_SOURCE = _source("dev_health_ops/providers/gitlab/commit_stats.py")
_GITLAB_INSTANCE_SOURCE = _source("dev_health_ops/providers/gitlab/instance.py")
_GITLAB_REPOSITORY_SOURCE = _source("dev_health_ops/providers/gitlab/repository.py")
_GITLAB_PROCESSOR_SOURCE = _source("dev_health_ops/processors/gitlab.py")
_FETCH_UTILS_SOURCE = _source("dev_health_ops/processors/fetch_utils.py")
_GIT_MODEL_SOURCE = _source("dev_health_ops/models/git.py")
_REPOSITORY_ROWS_SOURCE = _source("dev_health_ops/storage/repository_rows.py")
_RELEASE_REF_SOURCE = _source("dev_health_ops/processors/release_ref.py")
_TESTOPS_SCHEMAS_SOURCE = _source("dev_health_ops/metrics/testops_schemas.py")
_JUNIT_PARSER_SOURCE = _source("dev_health_ops/parsers/junit.py")
_COVERAGE_PARSER_SOURCE = _source("dev_health_ops/parsers/coverage.py")
_TESTOPS_TESTS_SOURCE = _source("dev_health_ops/processors/testops_tests.py")
_TESTOPS_COVERAGE_SOURCE = _source("dev_health_ops/processors/testops_coverage.py")
_TESTOPS_INGEST_SOURCE = _source("dev_health_ops/processors/testops_ingest.py")
_PIPELINE_BASE_SOURCE = _source("dev_health_ops/providers/_base.py")
_CI_ACCEPTANCE_SOURCE = _source("dev_health_ops/providers/ci_acceptance.py")
_GITHUB_TESTOPS_PIPELINE_SOURCE = _source(
    "dev_health_ops/providers/github/testops_pipeline.py"
)
_GITLAB_TESTOPS_PIPELINE_SOURCE = _source(
    "dev_health_ops/providers/gitlab/testops_pipeline.py"
)
_OPERATIONAL_ORDERING_SOURCE = _source(
    "dev_health_ops/models/operational_ordering_types.py"
)
_OPERATIONAL_ORDERING_CODEC_SOURCE = _source(
    "dev_health_ops/models/operational_ordering_codec.py"
)
_OPERATIONAL_ORDERING_RULES_SOURCE = _source(
    "dev_health_ops/models/operational_ordering.py"
)
_OPERATIONAL_SOURCE = _source("dev_health_ops/models/operational.py")
_OPERATIONAL_IDENTITY_SOURCE = _source("dev_health_ops/models/operational_identity.py")
_OPERATIONAL_MIGRATION_SOURCE = _source(
    "dev_health_ops/providers/operational_migration.py"
)
_JSM_MODELS_SOURCE = _source("dev_health_ops/providers/jira/jsm_models.py")
_JSM_INCIDENTS_SOURCE = _source("dev_health_ops/providers/jira/jsm_incidents.py")
_LICENSE_TYPES_SOURCE = _source("dev_health_ops/licensing/types.py")
_LICENSE_REGISTRY_SOURCE = _source("dev_health_ops/licensing/registry.py")
_FEATURE_POLICY_SOURCE = _source("dev_health_ops/licensing/feature_policy.py")

_STATUS_MAPPING_SOURCE = _source("dev_health_ops/providers/status_mapping.py")
_WORK_ITEMS_MODEL_SOURCE = _source("dev_health_ops/models/work_items.py")

# The ClickHouse metrics sink package, for the direct work-item destination
# projection oracle. Every one of these is a real production source; the list
# is long because the target is a package __init__ that composes fourteen
# mixins, and each one has to be in sys.modules before that __init__ runs
# (see _target_clickhouse_metrics_sink).
_CLICKHOUSE_DEDUP_SOURCE = _source("dev_health_ops/clickhouse_dedup.py")
_METRICS_SCHEMAS_SOURCE = _source("dev_health_ops/metrics/schemas.py")
_WORK_ITEM_MODELS_SOURCE = _source("dev_health_ops/models/work_items.py")
_AI_ATTRIBUTION_MODELS_SOURCE = _source("dev_health_ops/models/ai_attribution.py")
_AI_WORKFLOW_MODELS_SOURCE = _source("dev_health_ops/models/ai_workflow.py")
_AI_GOVERNANCE_MODELS_SOURCE = _source("dev_health_ops/audit/ai_governance/models.py")
_RECOMMENDATION_SNAPSHOT_SOURCE = _source("dev_health_ops/recommendations/snapshot.py")
_CLICKHOUSE_MIGRATIONS_SOURCE = _source(
    "dev_health_ops/migrations/clickhouse/__init__.py"
)
_METRICS_SINK_BASE_SOURCE = _source("dev_health_ops/metrics/sinks/base.py")
_METRICS_SINK_FACTORY_SOURCE = _source("dev_health_ops/metrics/sinks/factory.py")
_CLICKHOUSE_SINK_PACKAGE = "dev_health_ops/metrics/sinks/clickhouse"
_CLICKHOUSE_INSERT_SOURCE = _source(f"{_CLICKHOUSE_SINK_PACKAGE}/_insert.py")
_CLICKHOUSE_CONNECTION_SOURCE = _source(f"{_CLICKHOUSE_SINK_PACKAGE}/connection.py")
_CLICKHOUSE_CORE_SOURCE = _source(f"{_CLICKHOUSE_SINK_PACKAGE}/core.py")
_CLICKHOUSE_MIXIN_SOURCES: tuple[tuple[str, Path], ...] = tuple(
    (
        f"dev_health_ops.metrics.sinks.clickhouse.{module}",
        _source(f"{_CLICKHOUSE_SINK_PACKAGE}/{module}.py"),
    )
    for module in (
        "ai_attribution",
        "ai_governance",
        "ai_impact",
        "ai_workflow",
        "ci",
        "compounding_risk",
        "dora",
        "investment",
        "llm_tokens",
        "recommendations",
        "wellbeing",
        "work_graph",
    )
)
_CLICKHOUSE_SINK_SOURCE = _source(f"{_CLICKHOUSE_SINK_PACKAGE}/__init__.py")

_SAFE_SOURCE_MODULES: dict[str, Path] = {
    "dev_health_ops.sync.budget_types": _BUDGET_TYPES_SOURCE,
    "dev_health_ops.sync.datasets": _DATASETS_SOURCE,
    "dev_health_ops.providers.usage": _USAGE_SOURCE,
    "dev_health_ops.providers.github.work_item_options": _GITHUB_WORK_ITEM_OPTIONS_SOURCE,
}


def _target_launchdarkly_processor() -> None:
    _install_module(
        "dev_health_ops.metrics.schemas",
        {"FeatureFlagEventRecord": object, "FeatureFlagRecord": object},
    )


def _target_budget() -> None:
    _load_safe_source("dev_health_ops.sync.budget_types")
    _load_safe_source("dev_health_ops.sync.datasets")
    _load_safe_source("dev_health_ops.providers.usage")
    _install_module(
        "dev_health_ops.workers.sync_bootstrap", {"SyncTaskContext": object}
    )


def _unsupported_dependency(*_args: Any, **_kwargs: Any) -> Any:
    """Fail loudly if an oracle starts exercising an intentionally absent path."""
    raise RuntimeError("parity oracle attempted an unsupported application dependency")


def _target_dataset_adapters() -> None:
    _load_safe_source("dev_health_ops.sync.datasets")
    _install_module(
        "dev_health_ops.providers.utils",
        {
            "env_flag": lambda _name, default: default,
            "env_int": lambda _name, default: default,
        },
    )
    _load_safe_source("dev_health_ops.providers.github.work_item_options")
    _install_module(
        "dev_health_ops.credentials.resolver",
        {
            "github_credentials_from_mapping": _unsupported_dependency,
            "gitlab_credentials_from_mapping": _unsupported_dependency,
            "jira_credentials_from_mapping": _unsupported_dependency,
            "pagerduty_credentials_from_mapping": _unsupported_dependency,
            "resolve_gitlab_url": _unsupported_dependency,
        },
    )
    _install_module("dev_health_ops.metrics.sinks.ingestion", {"IngestionSink": object})
    _install_module(
        "dev_health_ops.providers.usage",
        {
            "PROVIDER_USAGE_OBSERVATION_KEY": "provider_usage_observations",
            "attach_partial_observations": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.storage",
        {
            "resolve_db_type": _unsupported_dependency,
            "run_with_store": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.workers.async_runner", {"run_async": _unsupported_dependency}
    )
    _install_module(
        "dev_health_ops.workers.sync_bootstrap",
        {"ProviderRuntime": object, "SyncTaskContext": object},
    )


class _OracleGitFile:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)


class _OracleAsyncBatchCollector:
    def __init__(self, callback: Callable[[list[Any]], Any]) -> None:
        self._callback = callback
        self._items: list[Any] = []

    async def __aenter__(self) -> _OracleAsyncBatchCollector:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        await self._callback(self._items)

    def add(self, item: Any) -> None:
        self._items.append(item)

    async def maybe_flush(self) -> None:
        return None


def _target_base_git() -> None:
    """Stub base_git.py's heavy, unrelated imports (CHAOS-3122).

    Most base_git.py imports remain unrelated to row construction and are
    stubbed so this loader stays dependency-light. The real complexity module
    is loaded because the GitLab files traversal oracle executes the live
    processor's scanner gate before the worker persistence boundary.
    """
    _load_source_module("dev_health_ops.analytics.complexity", _COMPLEXITY_SOURCE)
    _install_module(
        "dev_health_ops.metrics.schemas",
        {"FileComplexitySnapshot": object, "RepoComplexityDaily": object},
    )
    # GitPullRequest must be a real, introspectable class: build_git_pull_request
    # returns `GitPullRequest(**values)` and the oracle reads the result's
    # attributes back out. A plain kwargs-storing stand-in is sufficient --
    # nothing here depends on SQLAlchemy instrumentation.
    _install_module(
        "dev_health_ops.models.git",
        {
            "CiPipelineRun": type(
                "CiPipelineRun",
                (),
                {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
            ),
            "Deployment": type(
                "Deployment",
                (),
                {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
            ),
            "GitBlame": object,
            "GitCommitStat": object,
            "GitFile": _OracleGitFile,
            "GitPullRequest": type(
                "GitPullRequest",
                (),
                {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
            ),
        },
    )
    _install_module(
        "dev_health_ops.processors.fetch_utils",
        {"AsyncBatchCollector": _OracleAsyncBatchCollector},
    )
    # False keeps base_git.py's `elif CONNECTORS_AVAILABLE:` branch from firing,
    # so it never needs dev_health_ops.connectors.utils stubbed too.
    _install_module("dev_health_ops.utils", {"CONNECTORS_AVAILABLE": False})


def _target_github_code_client() -> None:
    """Stub code_client.py's imports, INCLUDING httpx itself (CHAOS-3162).

    ``_pull_from_item`` is a pure ``Mapping -> GitHubPullData`` function: it
    never touches httpx, an HTTP connection, or any of this module's other
    imports. The only reason loading the file needs them satisfied at all is
    that Python evaluates every top-level `import`/`from` statement when a
    module loads, regardless of which names the function you actually want
    ends up using.

    CORRECTED (CHAOS-3162, fourth adversarial review): an earlier version of
    this docstring claimed `from __future__ import annotations` (present at
    the top of code_client.py) makes stubbing httpx as a fully EMPTY module
    safe, because "an annotation referencing a stub attribute that doesn't
    exist is never evaluated." That was true only as long as nothing ever
    resolved those annotations -- true of `_pull_from_item` itself, NOT true
    in general: `load_live_module` now calls `_force_annotation_evaluation`,
    which resolves every annotation via `typing.get_type_hints`, and
    `_lowered_github_headers(response: httpx.Response)` genuinely needs
    `httpx.Response` to exist. An empty httpx stub loaded successfully and
    silently, with the broken annotation never surfacing until something
    (mypy, `get_type_hints`, an eager-evaluation Python) actually looked --
    the SAME class of bug as the RateLimitGate incident, just a level
    deeper (in a *different* stubbed module) than the one that shipped it.
    httpx therefore needs every name an ANNOTATION in this file
    dereferences, even though `_pull_from_item` itself never touches httpx
    at runtime.

    Verified empirically: loading the real file under this stub set and
    calling `_pull_from_item({"user": {"login": True}, ...})` still returns
    `author_login='True'` -- the live `str(user["login"])` call, not a
    re-implementation of it -- and `_force_annotation_evaluation` now
    passes cleanly against every function/class this module defines.
    """
    _install_module("httpx", {"Response": object, "AsyncBaseTransport": object})
    _install_module(
        "dev_health_ops.connectors.models",
        {
            "FileBlame": object,
            "SecurityAlertData": type(
                "SecurityAlertData",
                (),
                {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
            ),
        },
    )
    _install_module(
        "dev_health_ops.exceptions",
        {
            "APIException": Exception,
            "AuthenticationException": Exception,
            "NotFoundException": Exception,
            "RateLimitException": Exception,
        },
    )
    _install_module(
        "dev_health_ops.providers._http",
        {
            "GITHUB_DIAGNOSTIC_HEADER_NAMES": (),
            "InstrumentedRESTCore": object,
            "_default_is_retryable_status": lambda *_args, **_kwargs: False,
            "github_rest_base_url": lambda *_args, **_kwargs: "",
        },
    )
    _install_module("dev_health_ops.providers.github.client", {"GitHubAuth": object})
    _install_module(
        "dev_health_ops.providers.github.graphql",
        {
            "BLAME_QUERY": "",
            "blame_variables": lambda *_args, **_kwargs: {},
            "build_blob_texts_query": lambda *_args, **_kwargs: "",
            "github_graphql_url": lambda *_args, **_kwargs: "",
            "parse_blame_response": lambda *_args, **_kwargs: None,
            "parse_blob_texts_response": lambda *_args, **_kwargs: None,
            "raise_for_graphql_errors": lambda *_args, **_kwargs: None,
        },
    )
    _install_module(
        "dev_health_ops.providers.github.ratelimit",
        {
            "classify_github_403": lambda *_args, **_kwargs: None,
            "github_retry_after_seconds": lambda *_args, **_kwargs: None,
        },
    )
    _install_module("dev_health_ops.sync.budget_types", {"BudgetDimension": object})
    _install_module(
        "dev_health_ops.sync.rate_limit_signal", {"RateLimitSignal": object}
    )


def _target_gitlab_code_client() -> None:
    """Load the executable GitLab code client and its fixed transport stack."""
    _load_safe_source("dev_health_ops.sync.budget_types")
    _load_safe_source("dev_health_ops.sync.datasets")
    _load_safe_source("dev_health_ops.providers.usage")
    _install_module(
        "dev_health_ops.workers.sync_bootstrap", {"SyncTaskContext": object}
    )
    _load_source_module("dev_health_ops.exceptions", _EXCEPTIONS_SOURCE)
    _load_source_module(
        "dev_health_ops.sync.rate_limit_signal", _RATE_LIMIT_SIGNAL_SOURCE
    )
    _load_source_module("dev_health_ops.providers._ratelimit", _RATELIMIT_SOURCE)
    _load_source_module("dev_health_ops.providers._http", _HTTP_SOURCE)
    _load_source_module(
        "dev_health_ops.providers.gitlab.ratelimit", _GITLAB_RATELIMIT_SOURCE
    )
    _load_source_module("dev_health_ops.providers.gitlab.budget", _GITLAB_BUDGET_SOURCE)
    _install_module(
        "dev_health_ops.connectors.models",
        {
            "Repository": type(
                "Repository",
                (),
                {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
            ),
            "SecurityAlertData": type(
                "SecurityAlertData",
                (),
                {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
            ),
        },
    )


_GITHUB_PROCESSOR_SOURCE = _source("dev_health_ops/processors/github.py")


def _target_gitlab_repository() -> None:
    _load_source_module(
        "dev_health_ops.providers.gitlab.instance", _GITLAB_INSTANCE_SOURCE
    )


class _OracleGitCommitStat:
    def __init__(self, **values: Any) -> None:
        self.__dict__.update(values)


def _target_github_processor() -> None:
    """Stub processors/github.py's heavy, unrelated imports (CHAOS-3162).

    _collect_github_pr_objects's list-inclusion decision (the `if until is
    not None: ... continue` / `if since is not None: ... break` pair) is
    the ONLY logic github_prs_window.py's live-execution harness exercises
    -- it substitutes a fake client (via a monkeypatched
    _github_code_client_from_connector) whose get_pull_detail raises a
    sentinel the instant it is called, so the harness never reaches
    build_git_pull_request or any of the other names this module imports.
    Every stub below only needs to satisfy the *name* at module-load time,
    exactly like _target_base_git's -- github.py does not carry `from
    __future__ import annotations`, so a handful of these (GitSyncStore,
    used in a `store: GitSyncStore | Any` parameter annotation) must be
    real class OBJECTS, not instances, so Python's `X | Y` union-type
    syntax evaluates without error at `def` time.

    CI incident (CHAOS-3162, after this pair's third review landed):
    CONNECTORS_AVAILABLE was originally stubbed False, the same way
    _target_base_git's own doc comment recommends ("keeps the branch from
    firing, never needs connectors.utils stubbed too"). That is safe for
    base_git.py, which has no annotation depending on a name from that
    branch's `else:` fallback -- but github.py's own `else:` branch (fired
    when CONNECTORS_AVAILABLE is False) sets `RateLimitGate = None` at
    module scope, and two functions annotate a parameter as
    `gate: RateLimitGate | None = None` -- `None | None` raises
    `TypeError: unsupported operand type(s) for |`. This is a bug in
    processors/github.py's own fallback shape, not something a stub can
    route around by picking a *different* falsy value -- the only way to
    avoid it is to take the OTHER branch (CONNECTORS_AVAILABLE = True) and
    give every name pulled in by the `elif CONNECTORS_AVAILABLE:` import
    block (below) a real placeholder class instead of the one that
    actually crashes.

    Why this shipped green locally and only failed in CI: Python 3.14
    (this loader's own local interpreter) defaults to PEP 649 deferred
    annotation evaluation -- `gate: RateLimitGate | None` is not actually
    evaluated at `def` time on 3.14, only the FIRST time something reads
    `__annotations__` (mypy, `typing.get_type_hints`, or an older,
    pre-3.14 Python, which is what CI's isolated job runs and evaluates
    eagerly). `load_live_module` now forces that evaluation immediately
    after every load (see _force_annotation_evaluation below), specifically
    so this class of stub-shape bug fails on the very next line locally,
    on ANY Python version, instead of shipping through a 3.14 blind spot.
    """
    _install_module(
        "dev_health_ops.analytics.complexity",
        {"DEFAULT_COMPLEXITY_CONFIG_PATH": None, "ComplexityScanner": object},
    )
    _install_module("dev_health_ops.credentials.types", {"GitHubCredentials": object})
    _install_module("dev_health_ops.exceptions", {"RateLimitException": Exception})
    _install_module("dev_health_ops.metrics.sinks.ingestion", {"IngestionSink": object})
    _install_module(
        "dev_health_ops.models.git",
        {
            "CiPipelineRun": object,
            "Deployment": object,
            "GitBlame": object,
            "GitCommit": type(
                "GitCommit",
                (),
                {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
            ),
            "GitCommitStat": _OracleGitCommitStat,
            "GitPullRequest": object,
            "GitPullRequestReview": object,
            "Repo": object,
        },
    )
    _install_module(
        "dev_health_ops.processors.base_git",
        {
            "BaseGitProcessor": object,
            "backfill_file_records": _unsupported_dependency,
            "blame_backfill_needed": _unsupported_dependency,
            "build_ci_pipeline_run": _unsupported_dependency,
            "build_deployment": _unsupported_dependency,
            "build_git_pull_request": _unsupported_dependency,
            "check_backfill_needs": _unsupported_dependency,
            "historical_backfill_day": _unsupported_dependency,
            "resolve_commit_stats_limit": _unsupported_dependency,
            "select_unblamed_paths": _unsupported_dependency,
            "write_historical_complexity": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.processors.fetch_utils",
        {
            "AsyncBatchCollector": object,
            "SyncBatchCollector": object,
            "safe_parse_datetime": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.processors.release_ref",
        {"get_release_ref_enrichment": _unsupported_dependency},
    )
    _install_module(
        "dev_health_ops.processors.storage_protocol", {"GitSyncStore": object}
    )
    _install_module(
        "dev_health_ops.processors.testops_ingest",
        {
            "MAX_ARTIFACTS_PER_RUN": 0,
            "MAX_RUNS_PER_SYNC": 0,
            "ingest_report_members": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.providers.github.client",
        {"GitHubAuth": object, "GitHubWorkClient": object},
    )
    _install_module(
        "dev_health_ops.providers.pr_state",
        {"normalize_pr_state": _unsupported_dependency},
    )
    _install_module(
        "dev_health_ops.providers.usage",
        {"drain_provider_usage": _unsupported_dependency},
    )
    # CONNECTORS_AVAILABLE = True (not the _target_base_git precedent of
    # False -- see the module-load CI incident above): this takes
    # github.py's `elif CONNECTORS_AVAILABLE:` branch instead of its
    # `else:` fallback, which is what actually needs every name below to
    # be a real class, not a sentinel.
    _install_module(
        "dev_health_ops.utils",
        {
            "AGGREGATE_STATS_MARKER": "__AGGREGATE__",
            "BATCH_SIZE": 1000,
            "CONNECTORS_AVAILABLE": True,
            "is_skippable": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.connectors",
        {
            "BatchResult": object,
            "ConnectorException": Exception,
            "GitHubConnector": object,
        },
    )
    _install_module("dev_health_ops.connectors.models", {"Repository": object})
    _install_module(
        "dev_health_ops.connectors.utils",
        {"RateLimitConfig": object, "RateLimitGate": object},
    )
    # The third-party PyGithub package (`import github`), not
    # dev_health_ops -- also only imported inside the
    # `elif CONNECTORS_AVAILABLE:` branch, and also only needs to be
    # importable, not functional, for this pair's execution path.
    _install_module("github", {"RateLimitExceededException": Exception})


def _target_gitlab_processor() -> None:
    """Load the live GitLab incident producer with only its pure dependencies."""
    _target_jsm_incidents()
    _install_module(
        "dev_health_ops.models.atlassian_ops",
        {
            "AtlassianOpsAlert": object,
            "AtlassianOpsIncident": object,
            "AtlassianOpsSchedule": object,
        },
    )
    _load_source_module(
        "dev_health_ops.providers.operational_migration",
        _OPERATIONAL_MIGRATION_SOURCE,
    )
    _load_source_module("dev_health_ops.analytics.complexity", _COMPLEXITY_SOURCE)
    _install_module(
        "dev_health_ops.connectors.models",
        {
            name: object
            for name in ("Author", "Repository", "RepoStats", "SecurityAlertData")
        },
    )
    _install_module("dev_health_ops.metrics.sinks.ingestion", {"IngestionSink": object})

    class _Incident:
        def __init__(self, **kwargs: Any) -> None:
            self.__dict__.update(kwargs)

    _install_module(
        "dev_health_ops.models.git",
        {
            "CiPipelineRun": object,
            "Deployment": object,
            "GitBlame": object,
            "GitCommit": object,
            "GitCommitStat": object,
            "GitPullRequest": object,
            "GitPullRequestReview": object,
            "Incident": _Incident,
            "Repo": object,
        },
    )
    _install_module(
        "dev_health_ops.processors.base_git",
        {
            name: object
            for name in (
                "BaseGitProcessor",
                "backfill_file_records",
                "blame_backfill_needed",
                "build_ci_pipeline_run",
                "build_deployment",
                "build_git_pull_request",
                "check_backfill_needs",
                "historical_backfill_day",
                "resolve_commit_stats_limit",
                "select_unblamed_paths",
                "write_historical_complexity",
            )
        },
    )
    _install_module(
        "dev_health_ops.utils",
        {
            "BATCH_SIZE": 1000,
            "CONNECTORS_AVAILABLE": True,
            "AGGREGATE_STATS_MARKER": "__AGGREGATE__",
            "is_skippable": _unsupported_dependency,
        },
    )
    _load_source_module("dev_health_ops.processors.fetch_utils", _FETCH_UTILS_SOURCE)
    _install_module(
        "dev_health_ops.processors.release_ref",
        {"get_release_ref_enrichment": _unsupported_dependency},
    )
    _install_module(
        "dev_health_ops.processors.storage_protocol", {"GitSyncStore": object}
    )
    _install_module(
        "dev_health_ops.processors.testops_ingest",
        {
            "MAX_ARTIFACTS_PER_RUN": 0,
            "MAX_RUNS_PER_SYNC": 0,
            "ingest_report_members": _unsupported_dependency,
        },
    )
    _install_module(
        "dev_health_ops.processors.testops_tests",
        {"process_gitlab_test_report": _unsupported_dependency},
    )
    _install_module(
        "dev_health_ops.providers.gitlab.commit_stats",
        {"build_gitlab_commit_stat_values": _unsupported_dependency},
    )
    _install_module(
        "dev_health_ops.providers.gitlab.commits",
        {"build_gitlab_commit_values": _unsupported_dependency},
    )
    _load_source_module(
        "dev_health_ops.providers.gitlab.instance", _GITLAB_INSTANCE_SOURCE
    )
    _install_module(
        "dev_health_ops.providers.gitlab.repository",
        {"build_gitlab_repository_values": _unsupported_dependency},
    )
    _install_module(
        "dev_health_ops.providers.pr_state",
        {"normalize_pr_state": _unsupported_dependency},
    )
    _install_module(
        "dev_health_ops.connectors",
        {
            "BatchResult": object,
            "ConnectorException": Exception,
            "GitLabConnector": object,
        },
    )
    _install_module(
        "dev_health_ops.connectors.utils",
        {"RateLimitConfig": object, "RateLimitGate": object},
    )


def _target_testops_ingest() -> None:
    """Load only the report schemas, parsers, and pure ingestion producers."""
    _load_source_module(
        "dev_health_ops.metrics.testops_schemas", _TESTOPS_SCHEMAS_SOURCE
    )
    _load_source_module("dev_health_ops.parsers.junit", _JUNIT_PARSER_SOURCE)
    _load_source_module("dev_health_ops.parsers.coverage", _COVERAGE_PARSER_SOURCE)
    _load_source_module(
        "dev_health_ops.processors.testops_tests", _TESTOPS_TESTS_SOURCE
    )
    _load_source_module(
        "dev_health_ops.processors.testops_coverage", _TESTOPS_COVERAGE_SOURCE
    )


def _target_github_testops_pipeline() -> None:
    """Load the active GitHub TestOps adapter and its executable dependencies."""
    _load_safe_source("dev_health_ops.sync.budget_types")
    _load_safe_source("dev_health_ops.sync.datasets")
    _load_safe_source("dev_health_ops.providers.usage")
    _install_module(
        "dev_health_ops.workers.sync_bootstrap", {"SyncTaskContext": object}
    )
    _install_module(
        "dev_health_ops.exceptions",
        {
            "APIException": type("APIException", (Exception,), {}),
            "AuthenticationException": type(
                "AuthenticationException", (Exception,), {}
            ),
        },
    )
    _load_source_module(
        "dev_health_ops.metrics.testops_schemas", _TESTOPS_SCHEMAS_SOURCE
    )
    _install_module(
        "dev_health_ops.providers._http",
        {"GITHUB_DIAGNOSTIC_HEADER_NAMES": ()},
    )
    _load_source_module("dev_health_ops.providers.github.budget", _GITHUB_BUDGET_SOURCE)
    _load_source_module("dev_health_ops.providers._base", _PIPELINE_BASE_SOURCE)
    _load_source_module("dev_health_ops.providers.ci_acceptance", _CI_ACCEPTANCE_SOURCE)


def _target_gitlab_testops_pipeline() -> None:
    """Load the active GitLab TestOps adapter and executable dependencies."""
    _load_safe_source("dev_health_ops.sync.budget_types")
    _load_safe_source("dev_health_ops.sync.datasets")
    _load_safe_source("dev_health_ops.providers.usage")
    _install_module(
        "dev_health_ops.workers.sync_bootstrap", {"SyncTaskContext": object}
    )
    _install_module(
        "dev_health_ops.exceptions",
        {
            "APIException": type("APIException", (Exception,), {}),
            "AuthenticationException": type(
                "AuthenticationException", (Exception,), {}
            ),
        },
    )
    _load_source_module(
        "dev_health_ops.metrics.testops_schemas", _TESTOPS_SCHEMAS_SOURCE
    )
    _install_module(
        "dev_health_ops.providers._http",
        {"GITLAB_DIAGNOSTIC_HEADER_NAMES": ()},
    )
    _load_source_module("dev_health_ops.providers.gitlab.budget", _GITLAB_BUDGET_SOURCE)
    _load_source_module("dev_health_ops.providers._base", _PIPELINE_BASE_SOURCE)
    _load_source_module("dev_health_ops.providers.ci_acceptance", _CI_ACCEPTANCE_SOURCE)


def _target_jsm_incidents() -> None:
    """Load the incident dataclasses and JSM boundary model without ORM init."""
    _load_source_module(
        "dev_health_ops.models.operational_ordering_types",
        _OPERATIONAL_ORDERING_SOURCE,
    )
    _load_source_module(
        "dev_health_ops.models.operational_ordering_codec",
        _OPERATIONAL_ORDERING_CODEC_SOURCE,
    )
    _load_source_module(
        "dev_health_ops.models.operational_ordering",
        _OPERATIONAL_ORDERING_RULES_SOURCE,
    )
    _load_source_module("dev_health_ops.models.operational", _OPERATIONAL_SOURCE)
    _load_source_module(
        "dev_health_ops.models.operational_identity", _OPERATIONAL_IDENTITY_SOURCE
    )
    _load_source_module("dev_health_ops.providers.jira.jsm_models", _JSM_MODELS_SOURCE)


def _target_feature_policy() -> None:
    """Load the policy's real type and registry inputs without licensing init."""
    _load_source_module("dev_health_ops.licensing.types", _LICENSE_TYPES_SOURCE)
    _load_source_module("dev_health_ops.licensing.registry", _LICENSE_REGISTRY_SOURCE)


def _target_status_mapping() -> None:
    """Load the REAL work-item vocabulary that the status mapper validates against.

    status_mapping.py derives _VALID_STATUS_CATEGORIES and _VALID_WORK_ITEM_TYPES
    from work_items.py's Literal aliases via get_args, and the loader uses them to
    decide which category names are real -- an unrecognised one is skipped in
    silence. Stubbing that module would hand-maintain a SECOND copy of the valid
    vocabulary, and the "invalid category is silently skipped" pin would then be
    proven against the copy rather than against production, which is the precise
    drift this framework exists to prevent. work_items.py imports only stdlib
    (uuid, dataclasses, datetime, typing), so the real module loads unmodified.
    """
    module = _load_source_module(
        "dev_health_ops.models.work_items", _WORK_ITEMS_MODEL_SOURCE
    )
    # PRODUCER PROVENANCE GUARD. `python3` on a developer machine is often an
    # ambient virtualenv with dev_health_ops editable-installed against a
    # DIFFERENT worktree, and a lane has already lost time comparing Go against
    # another checkout's Python. load_live_module asserts this for the
    # allowlisted module it loads; the dependency loaded HERE had no such
    # check, so a wrong-checkout work_items -- which defines the valid category
    # and type vocabulary the loader validates against -- could have been
    # picked up silently. Comparing against the wrong producer must fail
    # loudly, not merely be unlikely.
    origin = getattr(module, "__file__", None)
    if origin is None or Path(origin).resolve() != _WORK_ITEMS_MODEL_SOURCE:
        raise RuntimeError(
            f"oracle loaded work_items from {origin!r}, expected "
            f"{_WORK_ITEMS_MODEL_SOURCE} -- the comparison would have run against "
            "another checkout's producer"
        )


def _target_fetch_utils() -> None:
    _install_module("dev_health_ops.utils", {"BATCH_SIZE": 1000})


def _target_clickhouse_metrics_sink() -> None:
    """Compose the real ClickHouseMetricsSink without the application stack.

    The target here is the sink package's own __init__, so the oracle built on
    it constructs the SAME class production constructs -- not a local
    re-composition of two mixins that a future mixin override could silently
    diverge from.

    Two things make a plain ``import dev_health_ops.metrics.sinks.clickhouse``
    impossible under the go-quality interpreter, and both are package
    __init__ side effects rather than anything the sink itself needs:

      * ``metrics/sinks/__init__.py`` imports ``sinks.base``, which imports
        ``models.work_items``, which runs ``models/__init__.py``, which
        imports ``licensing/__init__.py``, which imports ``licensing/gating.py``,
        which imports **fastapi** -- the whole HTTP API stack, pulled in to
        write a row to ClickHouse. That is the CI failure this target closes
        (ModuleNotFoundError: No module named 'fastapi').
      * ``clickhouse/core.py`` imports **clickhouse_connect** at module level
        for its connect path. The sink under test never connects: the oracle
        injects a recording client. Only that name is stubbed, and it is
        stubbed to fail loudly rather than silently return a mock, so an
        oracle that ever reached the real connect path would say so.

    Everything else below is loaded from its real source, in dependency order,
    because ``__init__`` needs each submodule already resolved (its parent
    package stub has an empty __path__, so the import system finds submodules
    only through sys.modules). No projection logic is stubbed: the column
    lists, the coercions, ClickHouseCore._insert_rows' asdict/org_id/datetime
    handling and each mixin's writer are the shipped code.
    """
    _install_module("clickhouse_connect", {"get_client": _unsupported_dependency})
    _load_source_module("dev_health_ops.clickhouse_dedup", _CLICKHOUSE_DEDUP_SOURCE)
    _load_source_module("dev_health_ops.metrics.schemas", _METRICS_SCHEMAS_SOURCE)
    _load_source_module(
        "dev_health_ops.metrics.testops_schemas", _TESTOPS_SCHEMAS_SOURCE
    )
    _load_source_module("dev_health_ops.models.work_items", _WORK_ITEM_MODELS_SOURCE)
    _load_source_module(
        "dev_health_ops.models.ai_attribution", _AI_ATTRIBUTION_MODELS_SOURCE
    )
    _load_source_module("dev_health_ops.models.ai_workflow", _AI_WORKFLOW_MODELS_SOURCE)
    _load_source_module(
        "dev_health_ops.audit.ai_governance.models", _AI_GOVERNANCE_MODELS_SOURCE
    )
    _load_source_module(
        "dev_health_ops.recommendations.snapshot", _RECOMMENDATION_SNAPSHOT_SOURCE
    )
    _load_source_module(
        "dev_health_ops.migrations.clickhouse", _CLICKHOUSE_MIGRATIONS_SOURCE
    )
    _load_source_module("dev_health_ops.metrics.sinks.base", _METRICS_SINK_BASE_SOURCE)
    _load_source_module(
        "dev_health_ops.metrics.sinks.factory", _METRICS_SINK_FACTORY_SOURCE
    )
    _load_source_module(
        "dev_health_ops.metrics.sinks.clickhouse._insert", _CLICKHOUSE_INSERT_SOURCE
    )
    _load_source_module(
        "dev_health_ops.metrics.sinks.clickhouse.connection",
        _CLICKHOUSE_CONNECTION_SOURCE,
    )
    _load_source_module(
        "dev_health_ops.metrics.sinks.clickhouse.core", _CLICKHOUSE_CORE_SOURCE
    )
    for module_name, module_source in _CLICKHOUSE_MIXIN_SOURCES:
        _load_source_module(module_name, module_source)


ALLOWED_MODULES: dict[Path, tuple[str, Path, Callable[[], None]]] = {
    _FETCH_UTILS_SOURCE: (
        "dev_health_ops.processors.fetch_utils",
        _FETCH_UTILS_SOURCE,
        _target_fetch_utils,
    ),
    _STATUS_MAPPING_SOURCE: (
        "dev_health_ops.providers.status_mapping",
        _STATUS_MAPPING_SOURCE,
        _target_status_mapping,
    ),
    _BASE_GIT_SOURCE: (
        "dev_health_ops.processors.base_git",
        _BASE_GIT_SOURCE,
        _target_base_git,
    ),
    _GITHUB_CODE_CLIENT_SOURCE: (
        "dev_health_ops.providers.github.code_client",
        _GITHUB_CODE_CLIENT_SOURCE,
        _target_github_code_client,
    ),
    _GITLAB_CODE_CLIENT_SOURCE: (
        "dev_health_ops.providers.gitlab.code_client",
        _GITLAB_CODE_CLIENT_SOURCE,
        _target_gitlab_code_client,
    ),
    _GITLAB_COMMITS_SOURCE: (
        "dev_health_ops.providers.gitlab.commits",
        _GITLAB_COMMITS_SOURCE,
        lambda: None,
    ),
    _GITLAB_COMMIT_STATS_SOURCE: (
        "dev_health_ops.providers.gitlab.commit_stats",
        _GITLAB_COMMIT_STATS_SOURCE,
        lambda: None,
    ),
    _GITLAB_REPOSITORY_SOURCE: (
        "dev_health_ops.providers.gitlab.repository",
        _GITLAB_REPOSITORY_SOURCE,
        _target_gitlab_repository,
    ),
    _GIT_MODEL_SOURCE: (
        "dev_health_ops.models.git",
        _GIT_MODEL_SOURCE,
        lambda: None,
    ),
    _REPOSITORY_ROWS_SOURCE: (
        "dev_health_ops.storage.repository_rows",
        _REPOSITORY_ROWS_SOURCE,
        lambda: None,
    ),
    _RELEASE_REF_SOURCE: (
        "dev_health_ops.processors.release_ref",
        _RELEASE_REF_SOURCE,
        lambda: None,
    ),
    _GITHUB_PROCESSOR_SOURCE: (
        "dev_health_ops.processors.github",
        _GITHUB_PROCESSOR_SOURCE,
        _target_github_processor,
    ),
    _GITLAB_PROCESSOR_SOURCE: (
        "dev_health_ops.processors.gitlab",
        _GITLAB_PROCESSOR_SOURCE,
        _target_gitlab_processor,
    ),
    _LAUNCHDARKLY_PROCESSOR_SOURCE: (
        "dev_health_ops.processors.launchdarkly",
        _LAUNCHDARKLY_PROCESSOR_SOURCE,
        _target_launchdarkly_processor,
    ),
    _GITHUB_BUDGET_SOURCE: (
        "dev_health_ops.providers.github.budget",
        _GITHUB_BUDGET_SOURCE,
        _target_budget,
    ),
    _GITLAB_BUDGET_SOURCE: (
        "dev_health_ops.providers.gitlab.budget",
        _GITLAB_BUDGET_SOURCE,
        _target_budget,
    ),
    _LINEAR_BUDGET_SOURCE: (
        "dev_health_ops.providers.linear.budget",
        _LINEAR_BUDGET_SOURCE,
        _target_budget,
    ),
    _JIRA_BUDGET_SOURCE: (
        "dev_health_ops.providers.jira.budget",
        _JIRA_BUDGET_SOURCE,
        _target_budget,
    ),
    _LAUNCHDARKLY_BUDGET_SOURCE: (
        "dev_health_ops.providers.launchdarkly.budget",
        _LAUNCHDARKLY_BUDGET_SOURCE,
        _target_budget,
    ),
    _DATASET_ADAPTERS_SOURCE: (
        "dev_health_ops.processors.dataset_adapters",
        _DATASET_ADAPTERS_SOURCE,
        _target_dataset_adapters,
    ),
    _TESTOPS_INGEST_SOURCE: (
        "dev_health_ops.processors.testops_ingest",
        _TESTOPS_INGEST_SOURCE,
        _target_testops_ingest,
    ),
    _GITHUB_TESTOPS_PIPELINE_SOURCE: (
        "dev_health_ops.providers.github.testops_pipeline",
        _GITHUB_TESTOPS_PIPELINE_SOURCE,
        _target_github_testops_pipeline,
    ),
    _GITLAB_TESTOPS_PIPELINE_SOURCE: (
        "dev_health_ops.providers.gitlab.testops_pipeline",
        _GITLAB_TESTOPS_PIPELINE_SOURCE,
        _target_gitlab_testops_pipeline,
    ),
    _JSM_INCIDENTS_SOURCE: (
        "dev_health_ops.providers.jira.jsm_incidents",
        _JSM_INCIDENTS_SOURCE,
        _target_jsm_incidents,
    ),
    _FEATURE_POLICY_SOURCE: (
        "dev_health_ops.licensing.feature_policy",
        _FEATURE_POLICY_SOURCE,
        _target_feature_policy,
    ),
    _CLICKHOUSE_SINK_SOURCE: (
        "dev_health_ops.metrics.sinks.clickhouse",
        _CLICKHOUSE_SINK_SOURCE,
        _target_clickhouse_metrics_sink,
    ),
}


def _purge_dev_health_modules() -> None:
    """Remove cached project modules by key without inspecting hostile values."""
    for name in tuple(sys.modules):
        if name == "dev_health_ops" or name.startswith("dev_health_ops."):
            sys.modules.pop(name, None)


def _install_package(name: str) -> ModuleType:
    module = ModuleType(name)
    module.__package__ = name
    module.__path__ = []
    _register_module(name, module)
    return module


def _register_module(name: str, module: ModuleType) -> None:
    sys.modules[name] = module
    parent_name, _, child_name = name.rpartition(".")
    if parent_name:
        parent = sys.modules[parent_name]
        setattr(parent, child_name, module)


def _install_namespace() -> None:
    for name in (
        "dev_health_ops",
        "dev_health_ops.analytics",
        "dev_health_ops.audit",
        "dev_health_ops.audit.ai_governance",
        "dev_health_ops.connectors",
        "dev_health_ops.credentials",
        "dev_health_ops.licensing",
        "dev_health_ops.migrations",
        "dev_health_ops.models",
        "dev_health_ops.metrics",
        "dev_health_ops.metrics.sinks",
        "dev_health_ops.metrics.sinks.clickhouse",
        "dev_health_ops.recommendations",
        "dev_health_ops.parsers",
        "dev_health_ops.processors",
        "dev_health_ops.providers",
        "dev_health_ops.providers.github",
        "dev_health_ops.providers.gitlab",
        "dev_health_ops.providers.jira",
        "dev_health_ops.providers.launchdarkly",
        "dev_health_ops.providers.linear",
        "dev_health_ops.storage",
        "dev_health_ops.sync",
        "dev_health_ops.workers",
    ):
        _install_package(name)


def _install_module(name: str, values: dict[str, Any]) -> ModuleType:
    module = ModuleType(name)
    module.__package__ = name.rpartition(".")[0]
    module.__dict__.update(values)
    _register_module(name, module)
    return module


def _load_source_module(name: str, source: Path) -> ModuleType:
    """Execute one fixed, resolved source file under its fixed module name."""
    spec = importlib.util.spec_from_file_location(name, source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load fixed oracle module {name}")
    module = importlib.util.module_from_spec(spec)
    _register_module(name, module)
    spec.loader.exec_module(module)
    return module


def _load_safe_source(name: str) -> ModuleType:
    source = _SAFE_SOURCE_MODULES[name]
    return _load_source_module(name, source)


def _force_annotation_evaluation(module_name: str, module: ModuleType) -> None:
    """Force every function/class annotation in a freshly-loaded module to
    evaluate NOW, deterministically, on whichever Python is running this
    loader -- rather than lazily, maybe never, depending on interpreter
    version.

    CHAOS-3162 CI incident (the reason this exists): a stub for
    processors/github.py supplied `None` for a name used in a
    `gate: RateLimitGate | None = None` parameter annotation.
    `None | None` raises `TypeError`. Python 3.14 defaults to PEP 649
    deferred annotation evaluation -- the broken annotation is not actually
    evaluated at `def` time, only the first time something reads
    `__annotations__` -- so `load_live_module` returned successfully on a
    local 3.14 interpreter, and every test built on top of it passed. CI's
    isolated `go-storage-integration` job ran an OLDER Python (pre-3.14,
    eager annotation evaluation by default) and failed immediately on
    import, on a code path no local run had ever exercised. A stub whose
    SHAPE is wrong for an annotation (a bare sentinel standing in for a
    type) is exactly the same class of defect as a stub whose shape is
    wrong for a value CHAOS-3162 spent two review rounds closing -- the fix
    here is the same discipline applied to the loader itself: make the
    broken path the one that always runs, not the one only some future
    Python (or some future CI image) happens to discover.

    CODEX FINDING (CHAOS-3162, fourth adversarial review): the first version
    of this function only touched `member.__annotations__` directly. That
    is sufficient to trigger PEP 649's lazy `__annotate__` call on 3.14+ for
    a module WITHOUT `from __future__ import annotations` (like
    processors/github.py, the RateLimitGate incident) -- but modules WITH
    that future-import (like code_client.py) store STRING literals in
    `__annotations__` unconditionally, on every Python version, regardless
    of PEP 649. Reading `__annotations__` on such a module never evaluates
    anything and never raises -- proven empirically: an httpx stub with NO
    `Response` attribute loads `code_client.py` successfully even though
    `_lowered_github_headers(response: httpx.Response)` requires it, and
    only `typing.get_type_hints()` (which actually RESOLVES a string
    annotation against the function's `__globals__`, string-annotated
    module or not) surfaces `AttributeError: module 'httpx' has no
    attribute 'Response'`. This function now uses `get_type_hints` instead
    of a bare attribute read, closing that gap uniformly for both
    annotation styles.

    One deliberate exception: `NameError` is NOT treated as a failure here.
    Forward-referencing a TYPE_CHECKING-only import in a string annotation
    (e.g. `-> "GitHubCodeClient":` when `GitHubCodeClient` is only imported
    under `if TYPE_CHECKING:`) is a normal, load-bearing Python idiom this
    codebase already uses (processors/github.py's own
    `_github_code_client_from_connector`) -- that name is never meant to
    resolve at runtime, and failing loudly on it would break legitimate
    production code, not catch a stub bug. `TypeError` (an operator applied
    to the wrong shape, e.g. `None | None`) and `AttributeError` (a stub
    missing an attribute real code dereferences) cannot come from a
    deliberately-unresolvable forward reference -- only from a name that DID
    resolve, to something the wrong shape. Those two stay hard failures.
    """
    import inspect
    import typing

    def _check(qualified_name: str, member: object) -> None:
        try:
            typing.get_type_hints(member)
        except NameError:
            return  # a TYPE_CHECKING-only forward reference; expected, not a stub bug.
        except (TypeError, AttributeError) as exc:
            raise RuntimeError(
                f"{qualified_name}'s annotations failed to evaluate: {exc} -- "
                "a stub in this loader's configure() is very likely the wrong "
                "SHAPE for a name used in an annotation (e.g. a bare None "
                "standing in for a type, which does not support `X | None`, "
                "or an empty stub module missing an attribute the real "
                "annotation dereferences). See _force_annotation_evaluation's "
                "docstring."
            ) from exc

    for name, member in vars(module).items():
        if inspect.isclass(member) and member.__module__ != module.__name__:
            continue  # an imported (stub) class, not one this module defines
        if inspect.isfunction(member) and member.__module__ != module.__name__:
            continue
        if inspect.isfunction(member):
            _check(f"{module_name}.{name}", member)
        elif inspect.isclass(member):
            _check(f"{module_name}.{name}", member)
            for method_name, method in vars(member).items():
                if inspect.isfunction(method):
                    _check(f"{module_name}.{name}.{method_name}", method)


def load_live_module(source: Path) -> Any:
    """Execute the canonical production module allowlisted for ``source``."""
    expected = source.resolve(strict=True)
    allowed = ALLOWED_MODULES.get(expected)
    if allowed is None:
        raise ValueError(f"unexpected oracle source: {source}")
    module_name, canonical_source, configure = allowed

    _purge_dev_health_modules()
    _install_namespace()
    importlib.invalidate_caches()
    configure()
    module = _load_source_module(module_name, canonical_source)
    spec = module.__spec__
    if spec is None or spec.origin is None:
        raise RuntimeError(f"oracle module {module_name} has no import origin")
    origin = Path(spec.origin).resolve(strict=True)
    if origin != canonical_source:
        raise RuntimeError(f"oracle imported {origin}, expected {canonical_source}")
    _force_annotation_evaluation(module_name, module)
    return module
