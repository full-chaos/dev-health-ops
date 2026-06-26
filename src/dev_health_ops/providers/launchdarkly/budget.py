from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

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
