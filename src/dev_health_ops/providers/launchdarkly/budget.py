from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

from dev_health_ops.providers.usage import OperationResolver, UsageRouteFamily
from dev_health_ops.sync.budget_types import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
)
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

_DEFAULT_HOST = "app.launchdarkly.com"
_DEFAULT_BASE_URL = "https://app.launchdarkly.com"
_CONFIDENCE_MEDIUM = "medium"
_CONFIDENCE_LOW = "low"


@dataclass(frozen=True)
class LaunchDarklyBudgetRouteFamily:
    route_family: str
    dimension: BudgetDimension
    endpoint_patterns: tuple[str, ...]
    cost_drivers: tuple[str, ...]
    confidence: str


LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES: tuple[LaunchDarklyBudgetRouteFamily, ...] = (
    LaunchDarklyBudgetRouteFamily(
        route_family="projects",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=(
            "GET /api/v2/projects",
            "GET /api/v2/projects/{projectKey}/environments",
            "GET /api/v2/projects/{projectKey}/environments/{environmentKey}",
        ),
        cost_drivers=(
            "project count",
            "environments per project",
            "expand=environments nested pagination",
        ),
        confidence="medium",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="flags",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=("GET /api/v2/flags/{projectKey}",),
        cost_drivers=(
            "project count",
            "flag count",
            "environment-scoped filtering",
            "summary and expand options",
        ),
        confidence="medium",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="segments",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=("GET /api/v2/segments/{projectKey}/{environmentKey}",),
        cost_drivers=(
            "project count",
            "environment count",
            "segment count",
            "big and synced segment expansion",
        ),
        confidence="low",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="audit_log",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=("GET /api/v2/auditlog", "POST /api/v2/auditlog"),
        cost_drivers=(
            "incremental window size",
            "backfill span",
            "resource-scoped searches",
            "20-entry page cap",
        ),
        confidence="medium",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="members",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=("GET /api/v2/members",),
        cost_drivers=(
            "member count",
            "custom role expansion",
            "role attribute expansion",
        ),
        confidence="low",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="code_refs",
        dimension=BudgetDimension.REST_CORE,
        endpoint_patterns=("GET /api/v2/code-refs/repositories",),
        cost_drivers=(
            "repository count",
            "branch count",
            "references per flag",
            "default branch reference expansion",
        ),
        confidence="medium",
    ),
    LaunchDarklyBudgetRouteFamily(
        route_family="code_refs",
        dimension=BudgetDimension.SECONDARY_ABUSE_RISK,
        endpoint_patterns=("GET /api/v2/code-refs/repositories",),
        cost_drivers=(
            "repository count",
            "branch count",
            "references per flag",
            "default branch reference expansion",
        ),
        confidence="low",
    ),
)


LAUNCHDARKLY_BUDGET_ROUTE_FAMILY_KEYS = frozenset(
    family.route_family for family in LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES
)


class LaunchDarklyBudgetEstimator:
    def estimate(self, context: SyncTaskContext) -> tuple[BudgetEstimate, ...]:
        if context.provider.lower() != "launchdarkly":
            return ()
        if context.dataset_key != DatasetKey.FEATURE_FLAGS.value:
            return ()

        credential_fingerprint = _credential_fingerprint(
            context.decrypted_credentials,
            credential_id=context.credential_id,
            integration_id=context.integration_id,
        )
        bucket = _bucket_factory(
            org_id=context.org_id,
            host=_host_from_credentials(context.decrypted_credentials),
            credential_fingerprint=credential_fingerprint,
        )
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE), 2, _CONFIDENCE_MEDIUM, "flags"
            ),
            _estimate(
                bucket(BudgetDimension.REST_CORE), 52, _CONFIDENCE_LOW, "audit_log"
            ),
            _estimate(
                bucket(BudgetDimension.REST_CORE), 1, _CONFIDENCE_MEDIUM, "code_refs"
            ),
            _estimate(
                bucket(BudgetDimension.SECONDARY_ABUSE_RISK),
                1,
                _CONFIDENCE_LOW,
                "code_refs",
            ),
        )


def _bucket_factory(
    *, org_id: str, host: str, credential_fingerprint: str
) -> Callable[[BudgetDimension], BudgetBucketKey]:
    def _bucket(dimension: BudgetDimension) -> BudgetBucketKey:
        return BudgetBucketKey(
            provider="launchdarkly",
            org_id=org_id,
            host=host,
            credential_fingerprint=credential_fingerprint,
            dimension=dimension,
        )

    return _bucket


def _estimate(
    bucket: BudgetBucketKey,
    estimated_units: int,
    confidence: str,
    route_family: str,
    *,
    notes: tuple[str, ...] = (),
) -> BudgetEstimate:
    return BudgetEstimate(
        bucket=bucket,
        estimated_units=estimated_units,
        confidence=confidence,
        route_family=route_family,
        notes=notes,
    )


def _host_from_credentials(credentials: object) -> str:
    base_url = _DEFAULT_BASE_URL
    if isinstance(credentials, Mapping):
        raw_base_url = credentials.get("base_url") or credentials.get("baseUrl")
        if raw_base_url:
            base_url = str(raw_base_url)
    host = urlparse(base_url).hostname
    return host or _DEFAULT_HOST


def _credential_fingerprint(
    credentials: object, *, credential_id: str | None, integration_id: str
) -> str:
    safe_credentials = _safe_credential_scope(
        credentials,
        credential_id=credential_id,
        integration_id=integration_id,
    )
    payload = json.dumps(
        safe_credentials, sort_keys=True, default=str, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _safe_credential_scope(
    credentials: object, *, credential_id: str | None, integration_id: str
) -> dict[str, object]:
    scope: dict[str, object] = {
        "credential_id": credential_id or "env",
        "integration_id": integration_id,
    }
    if not isinstance(credentials, Mapping):
        return scope

    for key in ("project_key", "environment"):
        value = credentials.get(key)
        if value is not None:
            scope[key] = value
    base_url = credentials.get("base_url") or credentials.get("baseUrl")
    if base_url is not None:
        scope["base_url"] = base_url
    return scope


# ---------------------------------------------------------------------------
# Actuals recorder route-family registry (CHAOS-2754 / CHAOS-2761)
# ---------------------------------------------------------------------------
# Declares the full budget vocabulary the LaunchDarkly estimator(s) emit so
# recorded actuals key by the same (route_family, dimension) an estimate is
# keyed by. Unlike GitHub's registry, every family the *current*
# feature-flags estimator emits (flags, audit_log, code_refs) now has a live,
# instrumented call site: `providers/launchdarkly/client.py::LaunchDarklyClient`
# (flags, audit_log) and `providers/launchdarkly/code_refs.py::
# LaunchDarklyCodeReferencesClient` (code_refs) both record through this
# resolver (CHAOS-2761 -- the "frozen connectors/ path" gap documented in
# docs/providers/rate-limit-policy.md is closed for LaunchDarkly).
#
# `code_refs` reserves both `rest_core` and `secondary_abuse_risk` for the
# SAME single REST call (see `LaunchDarklyBudgetEstimator.estimate` above); a
# real REST response can only resolve to one dimension per the shared
# resolver, so only the `rest_core` entry ever matches a live operation --
# `secondary_abuse_risk` is declared with no markers purely for
# estimator-coverage parity (mirrors GitHub's `commit_stats`/`files`/`blame`
# `contents_blob` entries, which have the same one-call/two-dimension shape).
#
# `projects`, `segments`, and `members` are *modeled* route families
# (`LAUNCHDARKLY_BUDGET_ROUTE_FAMILIES` above) the feature-flags estimator
# does not yet reserve against and no client fetches yet; declared with no
# markers so they never match a live operation, same as the estimator not
# yet emitting them.
LAUNCHDARKLY_USAGE_ROUTE_FAMILIES: tuple[UsageRouteFamily, ...] = (
    UsageRouteFamily(
        "flags",
        BudgetDimension.REST_CORE,
        transport="rest",
        operation_markers=("/flags/",),
    ),
    UsageRouteFamily(
        "audit_log",
        BudgetDimension.REST_CORE,
        transport="rest",
        operation_markers=("/auditlog",),
    ),
    UsageRouteFamily(
        "code_refs",
        BudgetDimension.REST_CORE,
        transport="rest",
        operation_markers=("/code-refs/",),
    ),
    UsageRouteFamily("code_refs", BudgetDimension.SECONDARY_ABUSE_RISK),
    UsageRouteFamily("projects", BudgetDimension.REST_CORE),
    UsageRouteFamily("segments", BudgetDimension.REST_CORE),
    UsageRouteFamily("members", BudgetDimension.REST_CORE),
)

LAUNCHDARKLY_USAGE_ROUTE_FAMILY_KEYS = frozenset(
    family.route_family for family in LAUNCHDARKLY_USAGE_ROUTE_FAMILIES
)

LAUNCHDARKLY_USAGE_RESOLVER = OperationResolver(
    families=LAUNCHDARKLY_USAGE_ROUTE_FAMILIES,
)
