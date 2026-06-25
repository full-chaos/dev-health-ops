from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from urllib.parse import urlparse

from dev_health_ops.sync.budget import (
    BudgetBucketKey,
    BudgetDimension,
    BudgetEstimate,
)
from dev_health_ops.sync.datasets import DatasetKey
from dev_health_ops.workers.sync_bootstrap import SyncTaskContext

_DEFAULT_HOST = "api.github.com"
_DEFAULT_BASE_URL = "https://api.github.com"
_CONFIDENCE_HIGH = "high"
_CONFIDENCE_MEDIUM = "medium"
_CONFIDENCE_LOW = "low"


class GitHubBudgetEstimator:
    def estimate(self, context: SyncTaskContext) -> tuple[BudgetEstimate, ...]:
        if context.provider.lower() != "github":
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
) -> tuple[BudgetEstimate, ...]:
    bucket = _bucket_factory(
        org_id=org_id,
        host=host,
        credential_fingerprint=credential_fingerprint,
    )

    if dataset_key == DatasetKey.REPO_METADATA.value:
        return (
            _estimate(bucket(BudgetDimension.REST_CORE), 1, _CONFIDENCE_HIGH, "repo"),
        )

    if dataset_key == DatasetKey.COMMITS.value:
        return (
            _estimate(bucket(BudgetDimension.REST_CORE), 2, _CONFIDENCE_MEDIUM, "git"),
        )

    if dataset_key == DatasetKey.COMMIT_STATS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE), 4, _CONFIDENCE_LOW, "commit_stats"
            ),
            _estimate(
                bucket(BudgetDimension.CONTENTS_BLOB),
                2,
                _CONFIDENCE_LOW,
                "commit_stats",
                notes=("commit-file expansion varies by commit volume",),
            ),
        )

    if dataset_key == DatasetKey.FILES.value:
        return (
            _estimate(bucket(BudgetDimension.REST_CORE), 3, _CONFIDENCE_LOW, "files"),
            _estimate(
                bucket(BudgetDimension.CONTENTS_BLOB),
                5,
                _CONFIDENCE_LOW,
                "files",
                notes=("repository tree/blob expansion is high variance",),
            ),
        )

    if dataset_key == DatasetKey.BLAME.value:
        return (
            _estimate(bucket(BudgetDimension.REST_CORE), 3, _CONFIDENCE_LOW, "blame"),
            _estimate(
                bucket(BudgetDimension.CONTENTS_BLOB),
                8,
                _CONFIDENCE_LOW,
                "blame",
                notes=("blame expansion is file-count dependent",),
            ),
        )

    if dataset_key in {
        DatasetKey.PRS.value,
        DatasetKey.PR_REVIEWS.value,
        DatasetKey.PR_COMMENTS.value,
    }:
        return (
            _estimate(bucket(BudgetDimension.REST_CORE), 2, _CONFIDENCE_MEDIUM, "prs"),
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                4,
                _CONFIDENCE_MEDIUM,
                "pr_social",
            ),
            _estimate(
                bucket(BudgetDimension.SECONDARY_ABUSE_RISK),
                1,
                _CONFIDENCE_LOW,
                "pr_social",
                notes=("timeline/social expansion may trigger secondary limits",),
            ),
        )

    if dataset_key in {
        DatasetKey.CICD.value,
        DatasetKey.TESTS.value,
        DatasetKey.DEPLOYMENTS.value,
    }:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE), 4, _CONFIDENCE_LOW, dataset_key
            ),
            _estimate(
                bucket(BudgetDimension.CONTENTS_BLOB),
                2,
                _CONFIDENCE_LOW,
                dataset_key,
                notes=("workflow artifact expansion varies by repository activity",),
            ),
        )

    if dataset_key == DatasetKey.SECURITY.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE), 2, _CONFIDENCE_LOW, "security"
            ),
        )

    if dataset_key in {
        DatasetKey.WORK_ITEMS.value,
        DatasetKey.WORK_ITEM_LABELS.value,
        DatasetKey.WORK_ITEM_PROJECTS.value,
        DatasetKey.WORK_ITEM_HISTORY.value,
        DatasetKey.WORK_ITEM_COMMENTS.value,
    }:
        estimates = [
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                2 if dataset_key == DatasetKey.WORK_ITEMS.value else 1,
                _CONFIDENCE_MEDIUM,
                "work_items",
            )
        ]
        if flags.get("sync_prs", False):
            estimates.append(
                _estimate(
                    bucket(BudgetDimension.GRAPHQL_COST),
                    3,
                    _CONFIDENCE_MEDIUM,
                    "work_item_prs",
                )
            )
            estimates.append(
                _estimate(
                    bucket(BudgetDimension.SECONDARY_ABUSE_RISK),
                    1,
                    _CONFIDENCE_LOW,
                    "work_item_prs",
                    notes=("PR work-item expansion shares social/timeline pressure",),
                )
            )
        return tuple(estimates)

    return ()


def _bucket_factory(
    *, org_id: str, host: str, credential_fingerprint: str
) -> Callable[[BudgetDimension], BudgetBucketKey]:
    def _bucket(dimension: BudgetDimension) -> BudgetBucketKey:
        return BudgetBucketKey(
            provider="github",
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
    if not isinstance(credentials, Mapping):
        return _fallback_credential_scope(
            credential_id=credential_id,
            integration_id=integration_id,
        )
    scope: dict[str, object] = {}
    for key in ("app_id", "installation_id"):
        value = credentials.get(key)
        if value is not None:
            scope[key] = value
    base_url = credentials.get("base_url") or credentials.get("baseUrl")
    if base_url is not None:
        scope["base_url"] = base_url
    token = credentials.get("token")
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
