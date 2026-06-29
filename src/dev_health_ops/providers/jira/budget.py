from __future__ import annotations

import hashlib
import json
import os
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

_DEFAULT_HOST = "atlassian.net"
_DEFAULT_BASE_URL = "https://atlassian.net"
_CONFIDENCE_HIGH = "high"
_CONFIDENCE_MEDIUM = "medium"
_CONFIDENCE_LOW = "low"


class JiraBudgetEstimator:
    def estimate(self, context: SyncTaskContext) -> tuple[BudgetEstimate, ...]:
        if context.provider.lower() != "jira":
            return ()

        credential_fingerprint = _credential_fingerprint(
            context.decrypted_credentials,
            credential_id=context.credential_id,
            integration_id=context.integration_id,
        )
        host = _host_from_credentials(context.decrypted_credentials)
        flags = {
            str(key): bool(value) for key, value in context.processor_flags.items()
        }
        return _dataset_estimates(
            dataset_key=context.dataset_key,
            flags=flags,
            org_id=context.org_id,
            host=host,
            credential_fingerprint=credential_fingerprint,
            span_days=window_span_days(context),
        )


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

    if dataset_key in {
        DatasetKey.WORK_ITEM_LABELS.value,
        DatasetKey.WORK_ITEM_PROJECTS.value,
    }:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                1,
                _CONFIDENCE_HIGH,
                "jira_metadata",
            ),
        )

    if dataset_key not in {
        DatasetKey.WORK_ITEMS.value,
        DatasetKey.WORK_ITEM_HISTORY.value,
        DatasetKey.WORK_ITEM_COMMENTS.value,
    }:
        return ()

    estimates: list[BudgetEstimate] = [
        _estimate(
            bucket(BudgetDimension.SEARCH),
            _scaled_units(2, span_days),
            _CONFIDENCE_MEDIUM,
            "jira_jql",
            notes=("Jira work-item listing uses REST /search/jql pagination",),
        ),
        _estimate(
            bucket(BudgetDimension.REST_CORE),
            _scaled_units(2, span_days),
            _CONFIDENCE_MEDIUM,
            "jira_issue_enrichment",
            notes=(
                "per-issue changelog/comment/sprint enrichment varies by issue count",
            ),
        ),
    ]

    if dataset_key == DatasetKey.WORK_ITEM_COMMENTS.value:
        estimates.append(
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(2, span_days),
                _CONFIDENCE_LOW,
                "jira_comments",
                notes=("comment pagination is issue-activity dependent",),
            )
        )

    if _flag_enabled(flags, "jira_fetch_worklogs", "fetch_worklogs"):
        estimates.append(
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(3, span_days),
                _CONFIDENCE_LOW,
                "jira_worklogs",
                notes=("JIRA_FETCH_WORKLOGS adds per-issue worklog expansion",),
            )
        )

    if _flag_enabled(flags, "atlassian_gql_enabled", "gql_enabled"):
        estimates.append(
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                _scaled_units(3, span_days),
                _CONFIDENCE_MEDIUM,
                "jira_gql_enrichment",
                notes=("ATLASSIAN_GQL_ENABLED routes Jira enrichment through AGG",),
            )
        )

    return tuple(estimates)


def _bucket_factory(
    *, org_id: str, host: str, credential_fingerprint: str
) -> Callable[[BudgetDimension], BudgetBucketKey]:
    def _bucket(dimension: BudgetDimension) -> BudgetBucketKey:
        return BudgetBucketKey(
            provider="jira",
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


def _flag_enabled(flags: Mapping[str, bool], *names: str) -> bool:
    if any(flags.get(name, False) for name in names):
        return True
    env_names = {
        "jira_fetch_worklogs": "JIRA_FETCH_WORKLOGS",
        "fetch_worklogs": "JIRA_FETCH_WORKLOGS",
        "atlassian_gql_enabled": "ATLASSIAN_GQL_ENABLED",
        "gql_enabled": "ATLASSIAN_GQL_ENABLED",
    }
    return any(_env_flag(env_names[name]) for name in names if name in env_names)


def _env_flag(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _host_from_credentials(credentials: object) -> str:
    base_url = os.getenv("ATLASSIAN_JIRA_BASE_URL") or os.getenv("JIRA_BASE_URL")
    if isinstance(credentials, Mapping):
        raw_base_url = (
            credentials.get("base_url")
            or credentials.get("baseUrl")
            or credentials.get("jira_base_url")
            or credentials.get("jiraBaseUrl")
        )
        if raw_base_url:
            base_url = str(raw_base_url)
    host = urlparse(_normalize_base_url(base_url or _DEFAULT_BASE_URL)).hostname
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
    for key in (
        "email",
        "cloud_id",
        "cloudId",
        "client_id",
        "clientId",
    ):
        value = credentials.get(key)
        if value is not None:
            scope[key] = value
    base_url = (
        credentials.get("base_url")
        or credentials.get("baseUrl")
        or credentials.get("jira_base_url")
        or credentials.get("jiraBaseUrl")
    )
    if base_url is not None:
        scope["base_url"] = _normalize_base_url(str(base_url))
    for secret_key in (
        "api_token",
        "apiToken",
        "access_token",
        "accessToken",
        "refresh_token",
        "refreshToken",
    ):
        value = credentials.get(secret_key)
        if value:
            scope[f"{secret_key}_sha256"] = hashlib.sha256(
                str(value).encode("utf-8")
            ).hexdigest()
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


def _normalize_base_url(value: str) -> str:
    url = (value or "").strip().rstrip("/")
    if not url:
        return url
    if url.startswith("http://"):
        return "https://" + url[len("http://") :]
    if url.startswith("https://"):
        return url
    return "https://" + url.lstrip("/")
