"""TeamCognitiveLoadMixin — write method for ``team_cognitive_load_daily``
(CHAOS-4365 item 2 / 4347-C).

Table: ``team_cognitive_load_daily``
Engine: MergeTree (append-only; read latest with ``argMax(<col>, computed_at)``)

The row set is composed elsewhere in
``dev_health_ops.metrics.team_cognitive_load``; this mixin only persists rows.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import TeamCognitiveLoadDailyRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)


class TeamCognitiveLoadMixin(_ClickHouseSinkBase):
    """Write methods for ``team_cognitive_load_daily``."""

    def write_team_cognitive_load_daily(
        self, rows: Sequence[TeamCognitiveLoadDailyRecord]
    ) -> None:
        """Append rows to ``team_cognitive_load_daily``.

        Append-only: re-running for the same ``(org_id, team_id, day)``
        produces new rows with a newer ``computed_at``. Use
        ``argMax(<col>, computed_at)`` in read queries.
        """
        if not rows:
            return
        self._insert_rows(
            "team_cognitive_load_daily",
            [
                "org_id",
                "team_id",
                "day",
                "pr_interruption_load",
                "context_spread_count",
                "review_request_load",
                "after_hours_commit_ratio",
                "weekend_commit_ratio",
                "contributing_repo_count",
                "sample_author_count",
                "computed_at",
            ],
            rows,
        )
