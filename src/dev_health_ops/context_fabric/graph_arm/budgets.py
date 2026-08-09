"""CHAOS-3617: the arm's bounds on rows, nodes, paths, bytes, time and tokens.

Every bound is required by the issue's operational-trial controls, and every
bound that bites has to be *disclosed*: the frozen contract pairs each
truncation flag with a ``TruncationReason`` and rejects a flag without one.
So a budget here is never just a limit — it is a limit plus the reason code
the packet must carry when it is hit, which is why
:class:`BudgetOutcome` carries both and why nothing in this module returns a
bare boolean.

The defaults are deliberately small. A trial arm that can traverse a hundred
thousand nodes will, and the resulting packet would be unreviewable; the
frozen contract's own field bounds (100 entities, 100 paths, 6 hops, 200
evidence entries) are the ceiling these sit under.
"""

from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.api.dev.investigation_contract import TruncationReason

__all__ = [
    "DEFAULT_BUDGETS",
    "BudgetOutcome",
    "TrialBudgets",
]


@dataclass(frozen=True, slots=True)
class BudgetOutcome:
    """What a budget check decided, and the reason the packet must disclose."""

    within_budget: bool
    truncation_reason: TruncationReason | None = None
    detail: str = ""

    def __post_init__(self) -> None:
        if self.within_budget and self.truncation_reason is not None:
            raise ValueError(
                "a within-budget outcome carries a truncation reason; the "
                "contract rejects a reason without a truncation flag"
            )
        if not self.within_budget and self.truncation_reason is None:
            raise ValueError(
                "an over-budget outcome carries no truncation reason; a "
                "partial result that does not say why is indistinguishable "
                "from a complete one"
            )


_WITHIN = BudgetOutcome(within_budget=True)


@dataclass(frozen=True, slots=True)
class TrialBudgets:
    """Hard bounds for one investigation.

    ``max_*`` values are inclusive: a run producing exactly the limit is
    within budget; the limit + 1 is truncated. Stated because an off-by-one
    here would make a "truncated" disclosure appear on complete results and
    train reviewers to ignore it.
    """

    max_ingest_records: int = 50_000
    max_nodes_visited: int = 5_000
    max_paths: int = 100
    #: How many distinct paths may be kept per reached entity. Three, not
    #: one: a single shortest path is a defensible explanation but a poor
    #: measure of relationship recall, and unbounded enumeration produces
    #: dozens of near-identical chains that bury the explanatory ones and
    #: overflow the contract's own 10-citation bound on
    #: ``RelatedEntity.supporting_path_ids``.
    max_paths_per_entity: int = 3
    max_path_hops: int = 6
    max_entities: int = 100
    max_evidence_entries: int = 200
    max_cohort_members: int = 50
    max_result_bytes: int = 2_000_000
    max_wall_seconds: float = 60.0
    max_output_tokens: int = 8_000

    def __post_init__(self) -> None:
        # The contract's own field bounds are the ceiling. A budget above one
        # of them would produce a result the packet cannot hold, and the
        # failure would surface as a pydantic error at emission time with no
        # indication that a budget was misconfigured.
        ceilings = {
            "max_paths": 100,
            "max_path_hops": 6,
            "max_entities": 100,
            "max_evidence_entries": 200,
            "max_cohort_members": 50,
        }
        for name, ceiling in ceilings.items():
            value = getattr(self, name)
            if value > ceiling:
                raise ValueError(
                    f"{name}={value} exceeds the frozen packet contract's "
                    f"own bound of {ceiling}; the packet could not hold the "
                    "result this budget permits"
                )
        for name in (
            "max_ingest_records",
            "max_nodes_visited",
            "max_paths",
            "max_paths_per_entity",
            "max_path_hops",
            "max_entities",
            "max_evidence_entries",
            "max_cohort_members",
            "max_result_bytes",
            "max_output_tokens",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1")
        if self.max_wall_seconds <= 0:
            raise ValueError("max_wall_seconds must be positive")

    def check_ingest_records(self, count: int) -> BudgetOutcome:
        return self._check(
            count,
            self.max_ingest_records,
            TruncationReason.NODE_BUDGET,
            "ingested records",
        )

    def check_paths(self, count: int) -> BudgetOutcome:
        return self._check(count, self.max_paths, TruncationReason.PATH_BUDGET, "paths")

    def check_nodes(self, count: int) -> BudgetOutcome:
        return self._check(
            count, self.max_nodes_visited, TruncationReason.NODE_BUDGET, "nodes"
        )

    def check_entities(self, count: int) -> BudgetOutcome:
        return self._check(
            count, self.max_entities, TruncationReason.NODE_BUDGET, "entities"
        )

    def check_cohort(self, count: int) -> BudgetOutcome:
        return self._check(
            count, self.max_cohort_members, TruncationReason.COHORT_BUDGET, "cohort"
        )

    def check_evidence(self, count: int) -> BudgetOutcome:
        return self._check(
            count,
            self.max_evidence_entries,
            TruncationReason.EVIDENCE_BUDGET,
            "evidence entries",
        )

    def check_bytes(self, size: int) -> BudgetOutcome:
        return self._check(
            size, self.max_result_bytes, TruncationReason.EVIDENCE_BUDGET, "bytes"
        )

    def check_elapsed(self, seconds: float) -> BudgetOutcome:
        if seconds <= self.max_wall_seconds:
            return _WITHIN
        return BudgetOutcome(
            within_budget=False,
            truncation_reason=TruncationReason.TIME_BUDGET,
            detail=f"{seconds:.3f}s elapsed exceeds the {self.max_wall_seconds}s budget",
        )

    def check_output_tokens(self, tokens: int) -> BudgetOutcome:
        return self._check(
            tokens,
            self.max_output_tokens,
            TruncationReason.EVIDENCE_BUDGET,
            "output tokens",
        )

    @staticmethod
    def _check(
        value: int, limit: int, reason: TruncationReason, noun: str
    ) -> BudgetOutcome:
        if value <= limit:
            return _WITHIN
        return BudgetOutcome(
            within_budget=False,
            truncation_reason=reason,
            detail=f"{value} {noun} exceeds the budget of {limit}",
        )


DEFAULT_BUDGETS = TrialBudgets()
