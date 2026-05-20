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

from dev_health_ops.recommendations.snapshot import (  # noqa: F401 (re-export for rules)
    MetricsSnapshot,
    RecommendationRecord,
)

if TYPE_CHECKING:
    from dev_health_ops.recommendations.loader import MetricsLoader
    from dev_health_ops.recommendations.schema import Recommendation

logger = logging.getLogger(__name__)


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
                logger.exception("Rule %r raised for team=%r; skipping.", rule_id, team_id)
                continue
            if rec is not None:
                results.append(rec)

        logger.debug("evaluate team=%r fired=%d/%d", team_id, len(results), len(self._evaluators))
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
        return self.evaluate(team_id=team_id, org_id=org_id,
                             window_start=window_start, window_end=window_end)

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
            team_id=team_id, org_id=org_id,
            window_start=window_start, window_end=window_end,
        )
        return self._evaluators[rule_id](snapshot, self._now)
