from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dev_health_ops.workers.sync_bootstrap import SyncTaskContext


class BudgetDimension(str, Enum):
    REST_CORE = "rest_core"
    GRAPHQL_COST = "graphql_cost"
    CONTENTS_BLOB = "contents_blob"
    SEARCH = "search"
    SECONDARY_ABUSE_RISK = "secondary_abuse_risk"


@dataclass(frozen=True)
class BudgetBucketKey:
    provider: str
    org_id: str
    host: str
    credential_fingerprint: str
    dimension: BudgetDimension

    def to_dict(self) -> dict[str, str]:
        return {
            "provider": self.provider,
            "org_id": self.org_id,
            "host": self.host,
            "credential_fingerprint": self.credential_fingerprint,
            "dimension": self.dimension.value,
        }


@dataclass(frozen=True)
class BudgetEstimate:
    bucket: BudgetBucketKey
    estimated_units: int
    confidence: str
    route_family: str
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket.to_dict(),
            "estimated_units": self.estimated_units,
            "confidence": self.confidence,
            "route_family": self.route_family,
            "notes": list(self.notes),
        }


class BudgetEstimator(Protocol):
    def estimate(self, context: SyncTaskContext) -> tuple[BudgetEstimate, ...]:
        return ()


def window_span_days(context: SyncTaskContext) -> int:
    start = context.window_start
    end = context.window_end
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return 1
    return max(1, (end - start).days)
