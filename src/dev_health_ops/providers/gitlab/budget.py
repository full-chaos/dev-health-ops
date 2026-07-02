from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from urllib.parse import urlparse

from dev_health_ops.providers.usage import OperationResolver, UsageRouteFamily
from dev_health_ops.sync.budget_types import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
    window_span_days,
)
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

_DEFAULT_HOST = "gitlab.com"
_DEFAULT_BASE_URL = "https://gitlab.com"
_CONFIDENCE_HIGH = "high"
_CONFIDENCE_MEDIUM = "medium"
_CONFIDENCE_LOW = "low"


class GitLabBudgetEstimator:
    def estimate(self, context: SyncTaskContext) -> tuple[BudgetEstimate, ...]:
        if context.provider.lower() != "gitlab":
            return ()

        credential_fingerprint = _credential_fingerprint(
            context.decrypted_credentials,
            credential_id=context.credential_id,
            integration_id=context.integration_id,
        )
        host = _host_from_credentials(context.decrypted_credentials)
        dataset_key = context.dataset_key
        flags = {
            str(key): bool(value) for key, value in context.processor_flags.items()
        }

        estimates = list(
            _dataset_estimates(
                dataset_key=dataset_key,
                flags=flags,
                org_id=context.org_id,
                host=host,
                credential_fingerprint=credential_fingerprint,
                span_days=window_span_days(context),
            )
        )
        return tuple(estimates)


def _dataset_estimates(
    *,
    dataset_key: str,
    flags: Mapping[str, bool],
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

    if dataset_key == DatasetKey.REPO_METADATA.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE), 1, _CONFIDENCE_HIGH, "project"
            ),
        )

    if dataset_key == DatasetKey.COMMITS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(2, span_days),
                _CONFIDENCE_MEDIUM,
                "project",
            ),
        )

    if dataset_key == DatasetKey.COMMIT_STATS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(4, span_days),
                _CONFIDENCE_LOW,
                "project",
                notes=("commit detail expansion varies by commit volume",),
            ),
        )

    if dataset_key in {DatasetKey.FILES.value, DatasetKey.BLAME.value}:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(
                    5 if dataset_key == DatasetKey.BLAME.value else 3,
                    span_days,
                ),
                _CONFIDENCE_LOW,
                "project",
                notes=("repository file expansion is high variance",),
            ),
        )

    if dataset_key in {DatasetKey.PRS.value, DatasetKey.PR_REVIEWS.value}:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(4, span_days),
                _CONFIDENCE_MEDIUM,
                "merge_requests",
                notes=("merge request iterators are pagination-heavy",),
            ),
        )

    if dataset_key == DatasetKey.PR_COMMENTS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(4, span_days),
                _CONFIDENCE_MEDIUM,
                "merge_requests",
                notes=("merge request iterators are pagination-heavy",),
            ),
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(3, span_days),
                _CONFIDENCE_LOW,
                "notes",
                notes=("MR note expansion varies by discussion volume",),
            ),
        )

    if dataset_key in {
        DatasetKey.CICD.value,
        DatasetKey.TESTS.value,
        DatasetKey.DEPLOYMENTS.value,
    }:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(6, span_days),
                _CONFIDENCE_LOW,
                "pipelines",
                notes=("pipeline job expansion varies by pipeline volume",),
            ),
        )

    if dataset_key == DatasetKey.SECURITY.value:
        return (
            _estimate(bucket(BudgetDimension.REST_CORE), 2, _CONFIDENCE_LOW, "project"),
        )

    if dataset_key == DatasetKey.WORK_ITEM_LABELS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE), 1, _CONFIDENCE_MEDIUM, "issues"
            ),
        )

    if dataset_key == DatasetKey.WORK_ITEM_PROJECTS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                2,
                _CONFIDENCE_MEDIUM,
                "milestones",
                notes=("project and group milestone iterators may both run",),
            ),
        )

    if dataset_key in {
        DatasetKey.WORK_ITEMS.value,
        DatasetKey.WORK_ITEM_HISTORY.value,
        DatasetKey.WORK_ITEM_COMMENTS.value,
    }:
        estimates = [
            _estimate(
                bucket(BudgetDimension.REST_CORE), 1, _CONFIDENCE_HIGH, "project"
            ),
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                2,
                _CONFIDENCE_MEDIUM,
                "milestones",
                notes=("project milestone iterator runs before work-item fetch",),
            ),
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(4, span_days),
                _CONFIDENCE_LOW,
                "epics",
                notes=("group epic expansion requires premium APIs when available",),
            ),
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(4, span_days),
                _CONFIDENCE_MEDIUM,
                "issues",
                notes=("issue iterator and per-issue events are pagination-heavy",),
            ),
        ]
        if dataset_key in {
            DatasetKey.WORK_ITEMS.value,
            DatasetKey.WORK_ITEM_COMMENTS.value,
            DatasetKey.WORK_ITEM_HISTORY.value,
        }:
            estimates.append(
                _estimate(
                    bucket(BudgetDimension.REST_CORE),
                    _scaled_units(3, span_days),
                    _CONFIDENCE_LOW,
                    "notes",
                    notes=("issue/MR notes and state events vary by activity",),
                )
            )
        if flags.get("sync_prs", True):
            estimates.append(
                _estimate(
                    bucket(BudgetDimension.REST_CORE),
                    _scaled_units(4, span_days),
                    _CONFIDENCE_MEDIUM,
                    "merge_requests",
                    notes=("MR iterator runs alongside issue ingestion by default",),
                )
            )
        return tuple(estimates)

    if dataset_key == DatasetKey.FEATURE_FLAGS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(2, span_days),
                _CONFIDENCE_LOW,
                "project",
                notes=("GitLab feature-flag APIs share the REST core budget",),
            ),
        )

    return ()


def _bucket_factory(
    *, org_id: str, host: str, credential_fingerprint: str
) -> Callable[[BudgetDimension], BudgetBucketKey]:
    def _bucket(dimension: BudgetDimension) -> BudgetBucketKey:
        return BudgetBucketKey(
            provider="gitlab",
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


def _scaled_units(fixed_floor: int, span_days: int) -> int:
    return max(fixed_floor, fixed_floor * max(1, span_days))


def _host_from_credentials(credentials: object) -> str:
    """Resolve the GitLab instance host a budget bucket should key against.

    CHAOS-2785 review finding: this previously checked only ``base_url`` /
    ``baseUrl``, so a self-hosted credential row stored under ``gitlab_url``
    (the key ``credentials/resolver.py::gitlab_credentials_from_mapping`` and
    ``workers/feature_flag_sync.py``'s ``GitLabFeatureFlagsClient``
    construction resolve first) fell through to the ``gitlab.com`` default --
    budget reservation/attribution disagreed with where the physical request
    actually went. Extended to the same precedence
    (``gitlab_url > url > base_url``), preserving the existing ``baseUrl``
    fallback. Host resolution INPUT only -- no change to estimator units,
    confidence, dimensions, or reservation logic below.
    """
    base_url = _DEFAULT_BASE_URL
    if isinstance(credentials, Mapping):
        raw_base_url = (
            credentials.get("gitlab_url")
            or credentials.get("url")
            or credentials.get("base_url")
            or credentials.get("baseUrl")
        )
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
    scope: dict[str, object] = {}
    for key in ("user_id", "username", "group_id", "project_id"):
        value = credentials.get(key)
        if value is not None:
            scope[key] = value
    base_url = credentials.get("base_url") or credentials.get("baseUrl")
    if base_url is not None:
        scope["base_url"] = base_url
    token = (
        credentials.get("token")
        or credentials.get("private_token")
        or credentials.get("access_token")
    )
    if token:
        scope["token_sha256"] = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    if not scope:
        return _fallback_credential_scope(
            credential_id=credential_id,
            integration_id=integration_id,
        )
    return scope


def _fallback_credential_scope(
    *, credential_id: str | None, integration_id: str
) -> dict[str, object]:
    return {
        "credential_id": credential_id or "env",
        "integration_id": integration_id,
    }


# ---------------------------------------------------------------------------
# Actuals recorder route-family registry (CHAOS-2754)
# ---------------------------------------------------------------------------
# The GitLab work client labels most paginated reads identically
# ("GET iterator page"), so per-entity distinctions (issues vs merge_requests vs
# notes vs milestones vs epics) cannot be recovered from the operation label.
# The instrumented resolution therefore maps project-metadata reads to
# ``project`` and every other read to the dominant work-item entity ``issues``
# (both REST_CORE); the remaining families are declared for budget-vocabulary
# coverage but carry no operation markers.
GITLAB_USAGE_ROUTE_FAMILIES: tuple[UsageRouteFamily, ...] = (
    UsageRouteFamily(
        "project",
        BudgetDimension.REST_CORE,
        transport="rest",
        operation_markers=(
            "/projects/:id",
            "/projects/",
        ),
    ),
    UsageRouteFamily("issues", BudgetDimension.REST_CORE),
    UsageRouteFamily("merge_requests", BudgetDimension.REST_CORE),
    UsageRouteFamily("notes", BudgetDimension.REST_CORE),
    UsageRouteFamily("pipelines", BudgetDimension.REST_CORE),
    UsageRouteFamily("milestones", BudgetDimension.REST_CORE),
    UsageRouteFamily("epics", BudgetDimension.REST_CORE),
)

GITLAB_USAGE_ROUTE_FAMILY_KEYS = frozenset(
    family.route_family for family in GITLAB_USAGE_ROUTE_FAMILIES
)

GITLAB_USAGE_RESOLVER = OperationResolver(
    families=GITLAB_USAGE_ROUTE_FAMILIES,
    defaults=(("rest", "issues", BudgetDimension.REST_CORE),),
)
