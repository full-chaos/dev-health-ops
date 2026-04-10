"""Base scorer abstraction for platform-health dimensions.

Each concrete dimension scorer queries ClickHouse for the relevant daily
metrics, normalises each signal to 0.0-1.0, and returns a weighted
:class:`DimensionScore`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Protocol

from dev_health_ops.metrics.scoring.schemas import DimensionScore, SignalValue


class ClickHouseClient(Protocol):
    """Minimal subset of the ``clickhouse_connect`` client used by scorers."""

    def query(self, query: str, parameters: dict | None = None) -> QueryResult: ...  # noqa: E704


class QueryResult(Protocol):
    """Minimal result protocol returned by :meth:`ClickHouseClient.query`."""

    @property
    def result_rows(self) -> list[tuple]: ...  # noqa: E704

    @property
    def column_names(self) -> list[str]: ...  # noqa: E704


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    """Clamp *value* between *lo* and *hi*."""
    return max(lo, min(hi, value))


class DimensionScorer(ABC):
    """Abstract base for a single health-dimension scorer.

    Subclasses define *dimension_name* and *signal_definitions* then implement
    :meth:`_fetch_signals` to pull raw metric values from ClickHouse.
    """

    @property
    @abstractmethod
    def dimension_name(self) -> str:
        """Human-readable dimension label (e.g. ``delivery``)."""

    @property
    @abstractmethod
    def signal_definitions(self) -> list[tuple[str, float, str]]:
        """Ordered list of ``(signal_name, weight, source_table)``."""

    @abstractmethod
    def _fetch_signals(
        self,
        client: ClickHouseClient,
        org_id: str,
        day: date,
        team_id: str | None,
    ) -> dict[str, float | None]:
        """Return a mapping of signal_name -> raw metric value.

        Implementations should query ClickHouse and return ``None`` for any
        signal where data is unavailable.
        """

    def compute(
        self,
        client: ClickHouseClient,
        org_id: str,
        day: date,
        team_id: str | None = None,
        computed_at: datetime | None = None,
    ) -> DimensionScore:
        """Score the dimension for a given org/team/day."""
        raw_signals = self._fetch_signals(client, org_id, day, team_id)

        signal_values: list[SignalValue] = []
        weighted_sum = 0.0
        weight_sum = 0.0

        for name, weight, source_table in self.signal_definitions:
            raw = raw_signals.get(name)
            if raw is not None:
                normalized = _clamp(raw)
                weighted_sum += normalized * weight
                weight_sum += weight
            else:
                normalized = None

            signal_values.append(
                SignalValue(
                    name=name,
                    raw_value=raw,
                    normalized_value=normalized,
                    weight=weight,
                    source_table=source_table,
                )
            )

        score = round(weighted_sum / weight_sum, 4) if weight_sum > 0 else None

        return DimensionScore(
            dimension=self.dimension_name,
            score=score,
            signals=signal_values,
            day=day,
            org_id=org_id,
            team_id=team_id,
            computed_at=computed_at or datetime.utcnow(),
        )
