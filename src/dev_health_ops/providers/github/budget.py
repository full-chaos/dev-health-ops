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
            _estimate(bucket(BudgetDimension.REST_CORE), 1, _CONFIDENCE_HIGH, "repo"),
        )

    if dataset_key == DatasetKey.COMMITS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(2, span_days),
                _CONFIDENCE_MEDIUM,
                "git",
            ),
        )

    if dataset_key == DatasetKey.COMMIT_STATS.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(4, span_days),
                _CONFIDENCE_LOW,
                "commit_stats",
            ),
            _estimate(
                bucket(BudgetDimension.CONTENTS_BLOB),
                _scaled_units(2, span_days),
                _CONFIDENCE_LOW,
                "commit_stats",
                notes=("commit-file expansion varies by commit volume",),
            ),
        )

    if dataset_key == DatasetKey.FILES.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(3, span_days),
                _CONFIDENCE_LOW,
                "files",
            ),
            _estimate(
                bucket(BudgetDimension.CONTENTS_BLOB),
                _scaled_units(5, span_days),
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
                _scaled_units(8, span_days),
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
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(2, span_days),
                _CONFIDENCE_MEDIUM,
                "prs",
            ),
            _estimate(
                bucket(BudgetDimension.GRAPHQL_COST),
                _scaled_units(4, span_days),
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
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(4, span_days),
                _CONFIDENCE_LOW,
                dataset_key,
            ),
            _estimate(
                bucket(BudgetDimension.CONTENTS_BLOB),
                _scaled_units(2, span_days),
                _CONFIDENCE_LOW,
                dataset_key,
                notes=("workflow artifact expansion varies by repository activity",),
            ),
        )

    if dataset_key == DatasetKey.SECURITY.value:
        return (
            _estimate(
                bucket(BudgetDimension.REST_CORE),
                _scaled_units(2, span_days),
                _CONFIDENCE_LOW,
                "security",
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
                _scaled_units(
                    2 if dataset_key == DatasetKey.WORK_ITEMS.value else 1,
                    span_days,
                ),
                _CONFIDENCE_MEDIUM,
                "work_items",
            )
        ]
        if flags.get("sync_prs", False):
            estimates.append(
                _estimate(
                    bucket(BudgetDimension.GRAPHQL_COST),
                    _scaled_units(3, span_days),
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


def _scaled_units(fixed_floor: int, span_days: int) -> int:
    return max(fixed_floor, fixed_floor * max(1, span_days))


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


# ---------------------------------------------------------------------------
# Actuals recorder route-family registry (CHAOS-2754)
# ---------------------------------------------------------------------------
# Declares the full budget vocabulary the GitHub estimator emits so recorded
# actuals key by the same (route_family, dimension) an estimate is keyed by.
# Some code-dataset families are budgeted but still fetched through frozen
# connector paths with no instrumented client, so they carry no operation
# markers here (documented gap; see docs/providers/rate-limit-policy.md). Every
# canonical code-client migration adds an explicit family prefix marker only when
# the live fetch path has moved.
#
# `pr_social` (GRAPHQL_COST) is the first code-dataset family whose fetch path
# IS instrumented (CHAOS-2803/CS2): the PR review-batch enrichment
# (`processors/github.py::_enrich_prs_with_reviews_batch`) constructs its own
# local `GitHubWorkClient` and labels its GraphQL calls with the explicit
# `"pr_social:"` prefix (CS1's resolver short-circuit, `providers/usage.py`),
# so this marker documents that the family is now live rather than driving
# resolution itself -- the short-circuit matches on `route_family` alone,
# irrespective of `operation_markers`. The `pr_social` SECONDARY_ABUSE_RISK
# dimension remains uninstrumented/estimate-only: no client observes a distinct
# secondary-limit signal on success responses (an abstract reservation, like
# `work_item_prs`' SECONDARY_ABUSE_RISK sibling), so its marker stays empty.
#
# `repo` (REST_CORE), `git` (REST_CORE), `commit_stats` (REST_CORE), `security`
# (REST_CORE), and deployments REST_CORE now fetch through
# `providers/github/code_client.py::GitHubCodeClient`, labeling operations
# with explicit family prefixes so CS1's resolver short-circuit resolves them
# directly. `commit_stats` CONTENTS_BLOB and deployments CONTENTS_BLOB remain
# empty/deferred until a distinct blob/content path is migrated; markers are
# populated only for live instrumented traffic.
#
# `files`/`blame` CONTENTS_BLOB (CHAOS-2808/CS7) now fetch through the SAME
# `GitHubCodeClient`, over its GraphQL support (`providers/github/graphql.py`),
# labeled with explicit `files:`/`blame:` prefixes -- listed BEFORE their
# sibling REST_CORE entry (unlike `commit_stats`, where REST_CORE is the
# live dimension) because `files`/`blame`'s actual traffic is 100% GraphQL
# blob/blame queries, never REST; the resolver's prefix short-circuit matches
# route_family literally (not dimension), so the FIRST family sharing that
# name wins -- ordering CONTENTS_BLOB first is what makes a `files:`/`blame:`
# label resolve to the instrumented dimension instead of the still-frozen
# REST_CORE one (the repository tree listing that discovers candidate paths
# stays on the frozen PyGithub connector, uninstrumented, out of CS7 scope).
# `repo` REST_CORE (metadata/listing), `prs` REST_CORE, and the processor's
# incident-label issue fetches now share the same `GitHubCodeClient` REST core as
# git/commit_stats and emit explicit `repo:`/`prs:`/`incidents:` labels.
# `incidents` is a resolver-only actuals family: the estimator constants remain
# frozen, and incident issue traffic is attached to observed actuals without
# adding a new planned estimate.
GITHUB_USAGE_ROUTE_FAMILIES: tuple[UsageRouteFamily, ...] = (
    UsageRouteFamily("repo", BudgetDimension.REST_CORE, operation_markers=("repo:",)),
    UsageRouteFamily("git", BudgetDimension.REST_CORE, operation_markers=("git:",)),
    UsageRouteFamily(
        "commit_stats", BudgetDimension.REST_CORE, operation_markers=("commit_stats:",)
    ),
    UsageRouteFamily("commit_stats", BudgetDimension.CONTENTS_BLOB),
    UsageRouteFamily(
        "files", BudgetDimension.CONTENTS_BLOB, operation_markers=("files:",)
    ),
    UsageRouteFamily("files", BudgetDimension.REST_CORE),
    UsageRouteFamily(
        "blame", BudgetDimension.CONTENTS_BLOB, operation_markers=("blame:",)
    ),
    UsageRouteFamily("blame", BudgetDimension.REST_CORE),
    UsageRouteFamily("prs", BudgetDimension.REST_CORE, operation_markers=("prs:",)),
    UsageRouteFamily(
        "incidents", BudgetDimension.REST_CORE, operation_markers=("incidents:",)
    ),
    UsageRouteFamily(
        "pr_social", BudgetDimension.GRAPHQL_COST, operation_markers=("pr_social:",)
    ),
    UsageRouteFamily("pr_social", BudgetDimension.SECONDARY_ABUSE_RISK),
    UsageRouteFamily("cicd", BudgetDimension.REST_CORE),
    UsageRouteFamily("cicd", BudgetDimension.CONTENTS_BLOB),
    UsageRouteFamily("tests", BudgetDimension.REST_CORE),
    UsageRouteFamily("tests", BudgetDimension.CONTENTS_BLOB),
    UsageRouteFamily(
        "deployments", BudgetDimension.REST_CORE, operation_markers=("deployments:",)
    ),
    UsageRouteFamily("deployments", BudgetDimension.CONTENTS_BLOB),
    UsageRouteFamily(
        "security", BudgetDimension.REST_CORE, operation_markers=("security:",)
    ),
    UsageRouteFamily("work_items", BudgetDimension.REST_CORE),
    UsageRouteFamily("work_item_prs", BudgetDimension.GRAPHQL_COST),
    UsageRouteFamily("work_item_prs", BudgetDimension.SECONDARY_ABUSE_RISK),
)

GITHUB_USAGE_ROUTE_FAMILY_KEYS = frozenset(
    family.route_family for family in GITHUB_USAGE_ROUTE_FAMILIES
)

GITHUB_USAGE_RESOLVER = OperationResolver(
    families=GITHUB_USAGE_ROUTE_FAMILIES,
    defaults=(
        ("rest", "work_items", BudgetDimension.REST_CORE),
        ("graphql", "work_item_prs", BudgetDimension.GRAPHQL_COST),
    ),
)
