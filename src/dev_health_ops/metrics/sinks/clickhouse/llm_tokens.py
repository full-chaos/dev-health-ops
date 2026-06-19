from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from dev_health_ops.metrics.schemas import LLMTokenUsageRecord

if TYPE_CHECKING:
    from dev_health_ops.metrics.sinks.clickhouse._insert import _ClickHouseSinkBase
else:

    class _ClickHouseSinkBase:
        pass


class LLMTokenUsageMixin(_ClickHouseSinkBase):
    def write_llm_token_usage(self, rows: Sequence[LLMTokenUsageRecord]) -> None:
        if not rows:
            return
        self._insert_rows(
            "llm_token_usage",
            [
                "org_id",
                "provider",
                "model",
                "source",
                "input_tokens",
                "output_tokens",
                "calls",
                "computed_at",
            ],
            rows,
        )
