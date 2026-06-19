from __future__ import annotations

import logging
from datetime import datetime, timezone

from dev_health_ops.metrics.schemas import LLMTokenUsageRecord
from dev_health_ops.metrics.sinks.base import BaseMetricsSink

logger = logging.getLogger(__name__)


def token_count(value: int | None) -> int:
    return int(value or 0)


def write_llm_token_usage(
    sink: BaseMetricsSink,
    *,
    org_id: str,
    provider: str,
    model: str | None,
    source: str,
    input_tokens: int | None,
    output_tokens: int | None,
    calls: int = 1,
    computed_at: datetime | None = None,
) -> None:
    input_count = token_count(input_tokens)
    output_count = token_count(output_tokens)
    call_count = int(calls or 0)
    if call_count <= 0 and input_count <= 0 and output_count <= 0:
        return
    try:
        sink.write_llm_token_usage(
            [
                LLMTokenUsageRecord(
                    org_id=org_id or "",
                    provider=provider or "unknown",
                    model=model or "unknown",
                    source=source,
                    input_tokens=input_count,
                    output_tokens=output_count,
                    calls=max(0, call_count),
                    computed_at=computed_at or datetime.now(timezone.utc),
                )
            ]
        )
    except Exception:
        logger.debug("Failed to persist LLM token usage", exc_info=True)
