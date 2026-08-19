"""Shared case decoding for live GitHub work-item row oracle pairs."""

from __future__ import annotations

import importlib.machinery
import pathlib
import sys
import types
from datetime import datetime
from types import SimpleNamespace
from typing import Any

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_SRC_ROOT = _REPO_ROOT / "src/dev_health_ops"


def _install_namespace_package_stub(module_name: str, source_dir: pathlib.Path) -> None:
    """Install a namespace package that skips ``<package>/__init__.py``.

    Several ``dev_health_ops`` packages have an aggregator ``__init__.py``
    that eagerly imports every sibling submodule (Celery tasks, PyGithub /
    python-gitlab connectors, ClickHouse sinks, ...), pulling in third-party
    dependencies the Go quality image never installs even though the one
    submodule an oracle actually needs has no such dependency itself.
    Registering a plain namespace package under ``module_name`` -- pointing
    ``__path__`` at the real package directory -- lets Python's import
    machinery resolve `module_name.submodule` straight to the real,
    unmodified submodule source without ever executing the aggregator
    ``__init__.py``. Nothing about the submodules themselves is replaced.
    """
    if module_name in sys.modules:
        return
    package = types.ModuleType(module_name)
    package.__path__ = [str(source_dir)]
    package.__package__ = module_name
    # A bare types.ModuleType defaults __spec__ to None. Left unset, any
    # later importlib.util.find_spec(module_name) call -- dev_health_ops
    # legacy code makes exactly this call to probe optional-package
    # availability -- raises ValueError("<module>.__spec__ is None") for an
    # already-imported module with no spec, instead of returning one. A real
    # namespace-package ModuleSpec keeps that probe (and anything else that
    # inspects __spec__) working the same as it would against the genuine
    # package.
    spec = importlib.machinery.ModuleSpec(module_name, loader=None, is_package=True)
    spec.submodule_search_locations = package.__path__
    package.__spec__ = spec
    sys.modules[module_name] = package


def _install_attribute_stub(module_name: str, **attributes: Any) -> None:
    """Install a bare module exposing only the given dummy attributes.

    Used for a heavy sibling import that the target function never calls
    (``dev_health_ops.metrics.dependencies.get_metrics_dependencies`` is
    referenced by other functions in the same file, never by
    ``parse_github_projects_v2_env``) or whose concrete class is only ever
    used as an assignment/``setattr`` target (``ClickHouseStore`` in
    ``dev_health_ops.storage``), never constructed or executed, by the
    production code paths these oracles exercise.
    """
    if module_name in sys.modules:
        return
    stub = types.ModuleType(module_name)
    for name, value in attributes.items():
        setattr(stub, name, value)
    sys.modules[module_name] = stub


def _install_bare_leaf_stub(module_name: str) -> None:
    """Install an empty placeholder for a leaf third-party dependency.

    ``urllib3``/``requests``/``jwt``/``clickhouse_connect`` are real runtime
    dependencies of production provider and metrics-sink code, but every
    reference to them in the import chains these oracles execute is either a
    deferred (in-function) call or a type annotation left unevaluated by
    ``from __future__ import annotations`` -- never a module-level call. An
    empty module therefore satisfies `import` without ever standing in for
    real HTTP/JWT/ClickHouse behavior; if a future change adds a
    module-level use, import fails loudly instead of silently returning
    fake data.
    """
    if module_name in sys.modules:
        return
    sys.modules[module_name] = types.ModuleType(module_name)


def install_minimal_oracle_imports(*, real_client: bool = False) -> None:
    """Load provider/model code without unrelated runtime dependencies.

    The Go quality image intentionally contains only the Python standard
    library plus the few dependencies used directly by provider code (see
    ``ci/requirements-live-python-oracles.txt``). Several aggregator
    ``__init__.py`` modules and a handful of leaf third-party libraries pull
    in FastAPI, ClickHouse, Redis/Valkey, Celery, PyGithub/python-gitlab,
    ``requests``, and JWT machinery that these row/decision oracles never
    exercise. Installing namespace packages, attribute-only stand-ins, and
    empty leaf placeholders for exactly those unreached branches preserves
    every real production module and function the oracles DO call --
    nothing involved in producing a compared value is replaced.

    ``real_client=True`` skips the ``dev_health_ops.providers.github.client``
    Protocol-only stub below so callers that subclass or call the real
    ``GitHubWorkClient`` (pagination, REST selection, the GraphQL
    comment/event adapters) get the genuine module instead of the
    annotation-only stand-in used by pairs that only need it as a type.
    """
    _install_namespace_package_stub("dev_health_ops.models", _SRC_ROOT / "models")
    _install_namespace_package_stub(
        "dev_health_ops.connectors", _SRC_ROOT / "connectors"
    )
    _install_namespace_package_stub(
        "dev_health_ops.connectors.utils", _SRC_ROOT / "connectors/utils"
    )
    _install_namespace_package_stub(
        "dev_health_ops.metrics.sinks", _SRC_ROOT / "metrics/sinks"
    )
    _install_namespace_package_stub("dev_health_ops.workers", _SRC_ROOT / "workers")

    def _metrics_dependencies_unavailable(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError(
            "get_metrics_dependencies is unavailable in the minimal oracle "
            "import environment -- only parse_github_projects_v2_env-style "
            "helpers that never call it are expected to run here"
        )

    _install_attribute_stub(
        "dev_health_ops.metrics.dependencies",
        get_metrics_dependencies=_metrics_dependencies_unavailable,
    )
    _install_attribute_stub(
        "dev_health_ops.storage.clickhouse",
        ClickHouseStore=type("ClickHouseStore", (), {}),
    )

    if real_client:
        # PyGithub imports requests.utils while defining its Auth types. Load
        # the locked real package before the minimal-import branch below can
        # install the annotation-only requests stub used by Protocol-only
        # oracles. real_client=True promises a production client, so stubbing
        # one of that client's import-time dependencies contradicts the mode.
        import requests  # noqa: F401

    if "requests" not in sys.modules:
        requests_stub = types.ModuleType("requests")
        # dev_health_ops.connectors.utils.graphql has no
        # `from __future__ import annotations`, so its
        # `response: requests.Response` parameter annotation is evaluated
        # at function-definition time (import time), not deferred like
        # every other annotation these oracles load -- it needs a real
        # attribute, not just an importable module.
        setattr(requests_stub, "Response", type("Response", (), {}))
        sys.modules["requests"] = requests_stub
    if "jwt" not in sys.modules:
        jwt_stub = types.ModuleType("jwt")
        jwt_exceptions_stub = types.ModuleType("jwt.exceptions")
        # dev_health_ops.api.services.auth does
        # `from jwt.exceptions import InvalidTokenError` at module level,
        # so -- like urllib3.util.retry.Retry above -- `jwt` needs to behave
        # as a real package with a real (if inert) exceptions submodule,
        # not just an importable name.
        setattr(
            jwt_exceptions_stub,
            "InvalidTokenError",
            type("InvalidTokenError", (Exception,), {}),
        )
        setattr(jwt_stub, "exceptions", jwt_exceptions_stub)
        sys.modules["jwt"] = jwt_stub
        sys.modules["jwt.exceptions"] = jwt_exceptions_stub
    _install_bare_leaf_stub("clickhouse_connect")
    if "urllib3" not in sys.modules:
        urllib3_stub = types.ModuleType("urllib3")
        urllib3_util_stub = types.ModuleType("urllib3.util")
        urllib3_retry_stub = types.ModuleType("urllib3.util.retry")
        setattr(urllib3_retry_stub, "Retry", type("Retry", (), {}))
        setattr(urllib3_util_stub, "retry", urllib3_retry_stub)
        setattr(urllib3_stub, "util", urllib3_util_stub)
        sys.modules["urllib3"] = urllib3_stub
        sys.modules["urllib3.util"] = urllib3_util_stub
        sys.modules["urllib3.util.retry"] = urllib3_retry_stub

    if real_client:
        return

    client_name = "dev_health_ops.providers.github.client"
    if client_name not in sys.modules:
        client = types.ModuleType(client_name)
        for name in (
            "_GitHubCommentLike",
            "_GitHubEventLike",
            "_GitHubIssueLike",
            "_GitHubMilestoneLike",
            "_GitHubPullRequestLike",
        ):
            setattr(client, name, type(name, (), {}))
        sys.modules[client_name] = client


def object_from_case(value: Any) -> Any:
    """Build the PyGithub-shaped object consumed by production normalizers."""
    if isinstance(value, dict):
        return SimpleNamespace(
            **{
                key: (
                    datetime.fromisoformat(item.replace("Z", "+00:00"))
                    if key.endswith("_at") and isinstance(item, str)
                    else object_from_case(item)
                )
                for key, item in value.items()
            }
        )
    if isinstance(value, list):
        return [object_from_case(item) for item in value]
    return value
