"""CHAOS-3617: the indexed-through watermark and the arm's freshness states.

The issue requires two things that are really one thing: "canonical writes
never wait for graph indexing" and "explicit indexed-through watermark and
stale/truncated states". The first makes the graph *always* potentially
behind; the second is what stops that being invisible.

So the arm never answers "is the graph up to date?" with a boolean. Every
read carries an :class:`IndexWatermark` stating the instant the projection
is complete through, and :meth:`IndexWatermark.freshness_for` turns that
plus the question's own time window into a
``SourceRequirementState`` the packet already knows how to disclose —
reusing the platform's observed-state vocabulary rather than minting a
parallel freshness enum the packet would then have to translate.

The state that matters most is :data:`SourceRequirementState.UNAVAILABLE`
for a never-projected store. A store with no watermark is not "fresh with no
data" — a question asked against it would return an empty, perfectly
well-formed, entirely misleading packet. :meth:`freshness_for` returns
UNAVAILABLE in that case and
``packet_builder`` turns it into a declared missing source, which the
family-obligation validator then requires be accounted for.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from dev_health_ops.api.dev.contracts_v2.base import SourceRequirementState

__all__ = [
    "DEFAULT_STALENESS_TOLERANCE",
    "IndexWatermark",
]

#: How far behind the requested window's end the projection may be and still
#: be called current. One hour: the projection is a batch job, and calling a
#: 40-minute lag "stale" would make every packet disclose staleness and
#: thereby make the disclosure meaningless.
DEFAULT_STALENESS_TOLERANCE = timedelta(hours=1)


@dataclass(frozen=True, slots=True)
class IndexWatermark:
    """How far the graph projection has been completed through.

    ``indexed_through`` is the observation instant the projection has fully
    consumed — not the instant the projection *ran*. Those differ whenever a
    source is itself behind, and reporting the run time would claim
    freshness the data does not have.

    ``partial`` marks a run that stopped early (budget, error, cancelled).
    A partial run's watermark is still honest about how far it got, but the
    flag is what a reader needs to distinguish "nothing happened after this
    instant" from "we stopped looking at this instant".
    """

    indexed_through: datetime | None
    projected_at: datetime | None = None
    records_indexed: int = 0
    partial: bool = False
    failure_detail: str | None = None

    def __post_init__(self) -> None:
        if self.indexed_through is not None and self.indexed_through.tzinfo is None:
            raise ValueError("indexed_through must be timezone-aware")
        if self.projected_at is not None and self.projected_at.tzinfo is None:
            raise ValueError("projected_at must be timezone-aware")
        if self.records_indexed < 0:
            raise ValueError("records_indexed must not be negative")
        if self.indexed_through is None and self.records_indexed:
            raise ValueError(
                "a watermark with no indexed_through instant claims to have "
                "indexed records; one of the two is wrong and a reader cannot "
                "tell which"
            )

    @property
    def never_projected(self) -> bool:
        return self.indexed_through is None

    def freshness_for(
        self,
        window_end: datetime,
        *,
        tolerance: timedelta = DEFAULT_STALENESS_TOLERANCE,
    ) -> SourceRequirementState:
        """The source state to disclose for a question ending at ``window_end``.

        Three outcomes, and the order they are checked in is the point:

        1. never projected -> ``UNAVAILABLE``. Checked first, so an empty
           store can never be reported as merely stale, and never as fresh.
        2. behind the window by more than ``tolerance`` -> ``AVAILABLE_STALE``.
        3. otherwise -> ``AVAILABLE_CURRENT``.

        A partial run is never reported ``AVAILABLE_CURRENT`` even when its
        watermark is recent, because "we indexed up to now but stopped early"
        means records before the watermark may still be missing.
        """

        if window_end.tzinfo is None:
            raise ValueError("window_end must be timezone-aware")
        if self.indexed_through is None:
            return SourceRequirementState.UNAVAILABLE
        if self.partial:
            return SourceRequirementState.AVAILABLE_STALE
        if self.indexed_through + tolerance < window_end:
            return SourceRequirementState.AVAILABLE_STALE
        return SourceRequirementState.AVAILABLE_CURRENT

    def detail_for(self, window_end: datetime) -> str:
        """A content-safe one-line description of the watermark state.

        Content-safe by construction: it contains timestamps, a count and a
        fixed phrase, and never any entity label, title or body. Logs and
        traces use this rather than formatting the watermark themselves.
        """

        if self.indexed_through is None:
            return "graph projection has never run for this partition"
        lag = window_end - self.indexed_through
        suffix = " (partial run)" if self.partial else ""
        return (
            f"indexed through {self.indexed_through.isoformat()}, "
            f"{int(lag.total_seconds())}s behind the requested window end, "
            f"{self.records_indexed} records{suffix}"
        )
