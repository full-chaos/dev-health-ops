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
_LAUNCHDARKLY_PROCESSOR_SOURCE = _source("dev_health_ops/processors/launchdarkly.py")
_LINEAR_BUDGET_SOURCE = _source("dev_health_ops/providers/linear/budget.py")
_JIRA_BUDGET_SOURCE = _source("dev_health_ops/providers/jira/budget.py")
_LAUNCHDARKLY_BUDGET_SOURCE = _source("dev_health_ops/providers/launchdarkly/budget.py")
_DATASET_ADAPTERS_SOURCE = _source("dev_health_ops/processors/dataset_adapters.py")
_BASE_GIT_SOURCE = _source("dev_health_ops/processors/base_git.py")
_GITHUB_CODE_CLIENT_SOURCE = _source("dev_health_ops/providers/github/code_client.py")

_SAFE_SOURCE_MODULES: dict[str, Path] = {
    "dev_health_ops.sync.budget_types": _BUDGET_TYPES_SOURCE,
    "dev_health_ops.sync.datasets": _DATASETS_SOURCE,
    "dev_health_ops.providers.usage": _USAGE_SOURCE,
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


def _target_base_git() -> None:
    """Stub base_git.py's heavy, unrelated imports (CHAOS-3122).

    ``BaseGitProcessor.coerce_created_at`` and the module-level
    ``build_git_pull_request`` are pure functions; everything base_git.py
    imports beyond them (complexity scanning, ORM models, the async batch
    collector) is dead weight for this oracle and, more importantly, is not
    importable under the stock interpreter this loader exists for. Each stub
    only needs to satisfy the *names* base_git.py imports at module-load
    time -- it never calls into any of them.
    """
    _install_module(
        "dev_health_ops.analytics.complexity",
        {"DEFAULT_COMPLEXITY_CONFIG_PATH": None, "ComplexityScanner": object},
    )
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
            "CiPipelineRun": object,
            "Deployment": object,
            "GitBlame": object,
            "GitCommitStat": object,
            "GitFile": object,
            "GitPullRequest": type(
                "GitPullRequest",
                (),
                {"__init__": lambda self, **kwargs: self.__dict__.update(kwargs)},
            ),
        },
    )
    _install_module(
        "dev_health_ops.processors.fetch_utils", {"AsyncBatchCollector": object}
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
    ends up using. `from __future__ import annotations` (present at the top
    of code_client.py) is what makes stubbing httpx ITSELF safe: it defers
    every type annotation (e.g. `transport: httpx.AsyncBaseTransport | None`)
    to a string, so an annotation referencing a stub attribute that doesn't
    exist is never evaluated. Verified empirically: loading the real file
    under this exact stub set and calling `_pull_from_item({"user":
    {"login": True}, ...})` returns `author_login='True'` -- the live
    `str(user["login"])` call, not a re-implementation of it.
    """
    _install_module("httpx", {})
    _install_module(
        "dev_health_ops.connectors.models",
        {"FileBlame": object, "SecurityAlertData": object},
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


ALLOWED_MODULES: dict[Path, tuple[str, Path, Callable[[], None]]] = {
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
    _LAUNCHDARKLY_PROCESSOR_SOURCE: (
        "dev_health_ops.processors.launchdarkly",
        _LAUNCHDARKLY_PROCESSOR_SOURCE,
        _target_launchdarkly_processor,
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
        "dev_health_ops.connectors",
        "dev_health_ops.credentials",
        "dev_health_ops.models",
        "dev_health_ops.metrics",
        "dev_health_ops.metrics.sinks",
        "dev_health_ops.processors",
        "dev_health_ops.providers",
        "dev_health_ops.providers.github",
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
    return module
