"""CompoundingRiskMixin — write method for ``compounding_risk_daily`` (CHAOS-1641).

Table: ``compounding_risk_daily``
Engine: MergeTree (append-only; read latest with ``argMax(<col>, computed_at)``)

The composite is computed elsewhere in
``dev_health_ops.metrics.compounding_risk``; this mixin only persists rows.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import CompoundingRiskDailyRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class CompoundingRiskMixin(_ClickHouseSinkBase):
    """Write methods for ``compounding_risk_daily``."""

    def write_compounding_risk_daily(
        self, rows: Sequence[CompoundingRiskDailyRecord]
    ) -> None:
        """Append rows to ``compounding_risk_daily``.

        Append-only: re-running for the same ``(org_id, day, scope, scope_id)``
        produces new rows with a newer ``computed_at``. Use
        ``argMax(<col>, computed_at)`` in read queries.
        """
        if not rows:
            return
        self._insert_rows(
            "compounding_risk_daily",
            [
                "org_id",
                "day",
                "scope",
                "scope_id",
                "compounding_risk",
                "severity",
                "churn_norm",
                "complexity_norm",
                "ownership_norm",
                "review_norm",
                "rework_churn",
                "complexity_delta",
                "bus_factor",
                "ownership_gini",
                "single_owner_ratio",
                "review_latency_p90h",
                "w_churn",
                "w_complexity",
                "w_ownership",
                "w_review",
                "threshold_elevated",
                "threshold_high",
                "computed_at",
            ],
            rows,
        )
