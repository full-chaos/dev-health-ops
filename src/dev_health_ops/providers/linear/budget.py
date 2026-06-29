from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from urllib.parse import urlparse

from dev_health_ops.sync.budget_types import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
    window_span_days,
)
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

_DEFAULT_HOST = "api.linear.app"
_DEFAULT_BASE_URL = "https://api.linear.app/graphql"
_CONFIDENCE_MEDIUM = "medium"
_CONFIDENCE_LOW = "low"


class LinearBudgetEstimator:
    def estimate(self, context: SyncTaskContext) -> tuple[BudgetEstimate, ...]:
        if context.provider.lower() != "linear":
            return ()

        credential_fingerprint = _credential_fingerprint(
            context.decrypted_credentials,
            credential_id=context.credential_id,
            integration_id=context.integration_id,
        )
        host = _host_from_credentials(context.decrypted_credentials)
        return tuple(
            _dataset_estimates(
                dataset_key=context.dataset_key,
                org_id=context.org_id,
                host=host,
                credential_fingerprint=credential_fingerprint,
                span_days=window_span_days(context),
            )
        )


def _dataset_estimates(
    *,
    dataset_key: str,
    org_id: str,
    host: str,
    credential_fingerprint: str,
    span_days: int,
) -> tuple[BudgetEstimate, ...]:
    bucket = _bucket_factory(
        org_id=org_id,
        host=host,
        credential_fingerprint=credential_fingerprint,
    )

    if dataset_key == DatasetKey.WORK_ITEMS.value:
        return (
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST), 1, _CONFIDENCE_MEDIUM, "teams"
            ),
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                _scaled_units(5, span_days, per_day_weight=2),
                _CONFIDENCE_LOW,
                "issues",
                notes=(
                    "Linear issue pages include nested labels, project, comments, attachments, and history edges",
                ),
            ),
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST), 2, _CONFIDENCE_LOW, "cycles"
            ),
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                _scaled_units(2, span_days),
                _CONFIDENCE_LOW,
                "comments",
            ),
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                _scaled_units(1, span_days),
                _CONFIDENCE_LOW,
                "attachments",
            ),
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                _scaled_units(2, span_days),
                _CONFIDENCE_LOW,
                "history",
            ),
        )

    if dataset_key == DatasetKey.WORK_ITEM_LABELS.value:
        return (
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST), 1, _CONFIDENCE_MEDIUM, "teams"
            ),
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                1,
                _CONFIDENCE_MEDIUM,
                "team_members",
                notes=(
                    "Linear team pages include a small member edge; large teams require member pagination",
                ),
            ),
        )

    if dataset_key == DatasetKey.WORK_ITEM_PROJECTS.value:
        return (
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST), 2, _CONFIDENCE_MEDIUM, "projects"
            ),
        )

    if dataset_key == DatasetKey.WORK_ITEM_HISTORY.value:
        return (
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                _scaled_units(3, span_days),
                _CONFIDENCE_LOW,
                "history",
            ),
        )

    if dataset_key == DatasetKey.WORK_ITEM_COMMENTS.value:
        return (
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                _scaled_units(3, span_days),
                _CONFIDENCE_LOW,
                "comments",
            ),
        )

    return ()


def _bucket_factory(
    *, org_id: str, host: str, credential_fingerprint: str
) -> Callable[[BudgetDimension], BudgetBucketKey]:
    def _bucket(dimension: BudgetDimension) -> BudgetBucketKey:
        return BudgetBucketKey(
            provider="linear",
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


def _scaled_units(fixed_floor: int, span_days: int, *, per_day_weight: int = 1) -> int:
    if span_days <= 1:
        return fixed_floor
    return max(fixed_floor, fixed_floor * span_days * max(1, per_day_weight))


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
    if not isinstance(credentials, Mapping):
        return _fallback_credential_scope(
            credential_id=credential_id,
            integration_id=integration_id,
        )
    scope: dict[str, object] = {
        "credential_id": credential_id or "env",
        "integration_id": integration_id,
    }
    for key in ("organization_id", "workspace_id", "team_id"):
        value = credentials.get(key)
        if value is not None:
            scope[key] = value
    base_url = credentials.get("base_url") or credentials.get("baseUrl")
    if base_url is not None:
        scope["base_url"] = base_url
    return scope


def _fallback_credential_scope(
    *, credential_id: str | None, integration_id: str
) -> dict[str, object]:
    return {
        "credential_id": credential_id or "env",
        "integration_id": integration_id,
    }
