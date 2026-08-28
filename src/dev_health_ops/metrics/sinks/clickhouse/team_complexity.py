"""TeamComplexityMixin — write method for ``team_complexity_daily``
(CHAOS-4365 item 3 / 4347-C).

Table: ``team_complexity_daily``
Engine: MergeTree (append-only; read latest with ``argMax(<col>, computed_at)``)

The row set is composed elsewhere in ``dev_health_ops.metrics.team_complexity``;
this mixin only persists rows.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import TeamComplexityDailyRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class TeamComplexityMixin(_ClickHouseSinkBase):
    """Write methods for ``team_complexity_daily``."""

    def write_team_complexity_daily(
        self, rows: Sequence[TeamComplexityDailyRecord]
    ) -> None:
        """Append rows to ``team_complexity_daily``.

        Append-only: re-running for the same ``(org_id, team_id, day)``
        produces new rows with a newer ``computed_at``. Use
        ``argMax(<col>, computed_at)`` in read queries.
        """
        if not rows:
            return
        self._insert_rows(
            "team_complexity_daily",
            [
                "org_id",
                "team_id",
                "day",
                "loc_total",
                "cyclomatic_total",
                "cyclomatic_per_kloc",
                "high_complexity_functions",
                "very_high_complexity_functions",
                "contributing_repo_count",
                "computed_at",
            ],
            rows,
        )
