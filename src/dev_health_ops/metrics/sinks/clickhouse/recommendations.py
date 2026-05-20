"""
RecommendationsMixin — write methods for recommendations_daily.

Table: recommendations_daily
Engine: ReplacingMergeTree(computed_at)  (append-only; argMax reads)
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.recommendations.snapshot import RecommendationRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class RecommendationsMixin(_ClickHouseSinkBase):
    """Mixin for recommendations write methods."""

    def write_recommendations(self, rows: Sequence[RecommendationRecord]) -> None:
        """Write recommendation evaluation results to ``recommendations_daily``.

        Append-only: re-running for the same window produces new rows with a
        newer ``computed_at``.  Use ``argMax(fired, computed_at)`` in read
        queries to retrieve the latest status per
        ``(org_id, team_id, rule_id, window_end)``.

        Args:
            rows: Sequence of ``RecommendationRecord`` dataclasses.
        """
        if not rows:
            return
        self._insert_rows(
            "recommendations_daily",
            [
                "team_id",
                "org_id",
                "rule_id",
                "rule_version",
                "window_start",
                "window_end",
                "fired",
                "severity",
                "title",
                "rationale",
                "success_criterion",
                "evidence_json",
                "computed_at",
            ],
            rows,
        )
