"""
AIAttributionMixin — ClickHouse write methods for AI attribution records.

Table: ai_attribution (ReplacingMergeTree, ORDER BY (org_id, provider, subject_type, subject_id, source))
View:  ai_attribution_resolved (plain VIEW resolving highest-precedence record per subject)

Write-time: every detected signal is persisted raw; dedup is by the ORDER BY key.
Read-time:  query ai_attribution_resolved for the effective attribution.

Implementation note
-------------------
This mixin calls ``self.client.insert()`` directly rather than going through
``_insert_rows``.  The core ``_insert_rows`` helper calls ``dataclasses.asdict()``
on every row and only converts ``datetime`` values — it cannot handle:
  - ``evidence: dict[str, object]`` → must be a JSON string in ClickHouse
  - ``UUID`` fields → need explicit str() coercion for older clickhouse_connect versions

The pre-conversion in ``_to_row()`` handles those transformations; the
resulting matrix is then inserted via the low-level client API.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import timezone
from typing import TYPE_CHECKING

from dev_health_ops.metrics.sinks.clickhouse._insert import (
    DEFAULT_BATCH_SIZE,
    _chunked,
    _dt_to_clickhouse_datetime,
)
from dev_health_ops.models.ai_attribution import AIAttributionRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


logger = logging.getLogger(__name__)

_COLUMNS = [
    "record_id",
    "org_id",
    "provider",
    "subject_type",
    "subject_id",
    "repo_id",
    "kind",
    "source",
    "confidence",
    "actor",
    "evidence",
    "observed_at",
    "ingested_at",
    "superseded_by",
    "computed_at",
]


def _to_row(record: AIAttributionRecord) -> list[object]:
    """
    Convert an AIAttributionRecord to a ClickHouse value list.

    Returns values in the same order as ``_COLUMNS``.  All type conversions
    (UUID → str, StrEnum → str, dict → JSON str, datetime → naive-UTC) are
    applied here so the insert matrix contains clean primitives.
    """
    from datetime import datetime

    now_utc = datetime.now(timezone.utc)

    return [
        str(record.record_id),
        str(record.org_id),
        str(record.provider),
        str(record.subject_type),
        str(record.subject_id),
        str(record.repo_id) if record.repo_id is not None else None,
        str(record.kind),
        str(record.source),
        float(record.confidence),
        record.actor,
        record.evidence_json(),
        _dt_to_clickhouse_datetime(record.observed_at),
        _dt_to_clickhouse_datetime(record.ingested_at),
        str(record.superseded_by) if record.superseded_by is not None else None,
        _dt_to_clickhouse_datetime(now_utc),
    ]


class AIAttributionMixin(_ClickHouseSinkBase):
    """Mixin for AI attribution write methods."""

    def write_ai_attribution(
        self,
        records: Sequence[AIAttributionRecord],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """
        Persist AI attribution records to ClickHouse.

        Idempotent: re-inserting the same (org_id, provider, subject_type,
        subject_id, source) tuple with a later computed_at will supersede the
        previous row via ReplacingMergeTree(computed_at).

        Args:
            records:    Sequence of AIAttributionRecord to persist.
            batch_size: Number of rows per ClickHouse insert call.
        """
        if not records:
            return

        for chunk in _chunked(list(records), batch_size):
            matrix = [_to_row(r) for r in chunk]
            self.client.insert("ai_attribution", matrix, column_names=_COLUMNS)

        logger.debug(
            "write_ai_attribution: persisted %d record(s)", len(records)
        )
