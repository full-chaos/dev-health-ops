"""Rule evaluation engine for the recommendations system (CHAOS-1622).

The ``RuleEngine`` orchestrates metric loading, rule dispatch, and result
collection.  It has NO side-effects itself — persistence is the caller's
responsibility.

Non-negotiable contracts
------------------------
* **Deterministic**: ``evaluate`` is a pure function of its arguments.
* **now parameter**: injected at construction; never read inside rule logic.
* **Hexagonal**: depends on the ``MetricsLoader`` *protocol*, not ClickHouse.

MetricsSnapshot is re-exported here so rule files can write::

    from dev_health_ops.recommendations.engine import MetricsSnapshot
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from dev_health_ops.recommendations.snapshot import (
    MetricsSnapshot,
    RecommendationRecord,
)

if TYPE_CHECKING:
    from dev_health_ops.recommendations.loader import MetricsLoader
    from dev_health_ops.recommendations.schema import Recommendation

logger = logging.getLogger(__name__)

__all__ = ["MetricsSnapshot", "RecommendationRecord", "RuleEngine"]


def _parse_window(window: int | str) -> int:
    """Return window in whole days.  Accepts int, ``"7d"``, ``"2w"``."""
    if isinstance(window, int):
        return window
    s = str(window).strip().lower()
    if s.endswith("d"):
        return int(s[:-1])
    if s.endswith("w"):
        return int(s[:-1]) * 7
    return int(s)


class RuleEngine:
    """Deterministic rule evaluation engine.

    Args:
        loader:     ``MetricsLoader`` implementation (ClickHouse or fake).
        now:        Evaluation instant (UTC, timezone-aware).
        registry:   Optional registry module/object with ``all_rules()``.
                    Used for documentation/validation only.
        evaluators: Optional dict[rule_id → callable] override.
                    Defaults to ``RULE_EVALUATORS`` from ``recommendations.rules``.
                    Inject a custom dict in tests.
    """

    def __init__(
        self,
        loader: MetricsLoader,
        now: datetime,
        registry: Any = None,
        evaluators: dict[str, Any] | None = None,
    ) -> None:
        if now.tzinfo is None:
            raise ValueError("RuleEngine.now must be timezone-aware (UTC).")
        self._loader = loader
        self._now = now
        self._registry = registry

        if evaluators is not None:
            self._evaluators: dict[str, Any] = evaluators
        else:
            from dev_health_ops.recommendations.rules import (
                RULE_EVALUATORS,  # noqa: PLC0415
            )

            self._evaluators = dict(RULE_EVALUATORS)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(
        self,
        team_id: str,
        org_id: str,
        window_start: date,
        window_end: date,
    ) -> list[Recommendation]:
        """Evaluate all registered rules; return fired recommendations only.

        Args:
            team_id:      Team identifier.
            org_id:       Organisation ID for metric scoping.
            window_start: Inclusive start date (UTC).
            window_end:   Exclusive end date (UTC).

        Returns:
            ``list[Recommendation]`` — fired rules only, in evaluator dict order.
            Rules that raise are logged and skipped.
        """
        snapshot = self._loader.load_team_metrics_window(
            team_id=team_id,
            org_id=org_id,
            window_start=window_start,
            window_end=window_end,
        )
        results: list[Any] = []
        for rule_id, fn in self._evaluators.items():
            try:
                rec = fn(snapshot, self._now)
            except Exception:
                logger.exception(
                    "Rule %r raised for team=%r; skipping.", rule_id, team_id
                )
                continue
            if rec is not None:
                results.append(rec)

        logger.debug(
            "evaluate team=%r fired=%d/%d", team_id, len(results), len(self._evaluators)
        )
        return results

    def evaluate_all(
        self,
        team_id: str,
        window: int | str = 7,
        org_id: str = "",
    ) -> list[Recommendation]:
        """Convenience wrapper: derive window dates from ``window`` string.

        Window = ``[now.date() - window_days, now.date())``.
        """
        days = _parse_window(window)
        window_end = self._now.date()
        window_start = window_end - timedelta(days=days)
        return self.evaluate(
            team_id=team_id,
            org_id=org_id,
            window_start=window_start,
            window_end=window_end,
        )

    def evaluate_state(
        self,
        team_id: str,
        window: int | str = 7,
        org_id: str = "",
        rule_version: str = "1.0.0",
    ) -> list[RecommendationRecord]:
        """Evaluate every registered rule and return the *full* state as records.

        Unlike :meth:`evaluate_all` (fired-only), this returns one
        ``RecommendationRecord`` for **every** registered rule:

        * a fired rule yields a ``fired=True`` record carrying its rationale and
          evidence;
        * a rule that does **not** fire yields an explicit ``fired=False``
          tombstone record (from the registry's static ``RuleDef``).

        Persisting the full state is required for correctness: the readers
        ``argMax(fired, computed_at)`` per ``(org_id, team_id, rule_id,
        window_end)`` and keep ``HAVING latest_fired = true``. Without a
        ``fired=False`` row at the new ``window_end``, a rule that fired
        yesterday but has since recovered would keep surfacing stale guidance
        (CHAOS-2373). The records all share the same ``computed_at`` and
        ``window_end`` so one scheduled run replaces the rule state for the
        team in a single, internally-consistent batch.
        """
        from dev_health_ops.recommendations import registry as _registry

        days = _parse_window(window)
        window_end = self._now.date()
        window_start = window_end - timedelta(days=days)

        fired = self.evaluate(
            team_id=team_id,
            org_id=org_id,
            window_start=window_start,
            window_end=window_end,
        )
        fired_by_rule = {rec.rule_id: rec for rec in fired}

        from dev_health_ops.recommendations.loader import recommendation_to_record

        records: list[RecommendationRecord] = []
        for rule_id in self._evaluators:
            rec = fired_by_rule.get(rule_id)
            if rec is not None:
                records.append(recommendation_to_record(rec, rule_version=rule_version))
                continue
            # Non-fired: persist an explicit tombstone so the latest state for
            # this (team, rule, window_end) reads as resolved.
            try:
                rule_def = _registry.get_rule(rule_id)
                title = rule_def.title
                severity = rule_def.severity
                success_criterion = rule_def.success_criterion
            except KeyError:
                title = ""
                severity = "warning"
                success_criterion = ""
            records.append(
                RecommendationRecord(
                    team_id=team_id,
                    org_id=org_id,
                    rule_id=rule_id,
                    rule_version=rule_version,
                    window_start=window_start,
                    window_end=window_end,
                    fired=False,
                    severity=severity,
                    title=title,
                    rationale="",
                    success_criterion=success_criterion,
                    evidence_json="[]",
                    computed_at=self._now,
                )
            )
        return records

    def evaluate_one(
        self,
        rule_id: str,
        team_id: str,
        window: int | str = 7,
        org_id: str = "",
    ) -> Recommendation | None:
        """Evaluate a single rule by *rule_id*.

        Raises:
            KeyError: when *rule_id* is not in the registered evaluators.
        """
        if rule_id not in self._evaluators:
            raise KeyError(
                f"Rule {rule_id!r} not found. Available: {sorted(self._evaluators)}"
            )
        days = _parse_window(window)
        window_end = self._now.date()
        window_start = window_end - timedelta(days=days)
        snapshot = self._loader.load_team_metrics_window(
            team_id=team_id,
            org_id=org_id,
            window_start=window_start,
            window_end=window_end,
        )
        return self._evaluators[rule_id](snapshot, self._now)
