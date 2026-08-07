"""Arm-agnostic trial runner and per-class reporting.

Two rules shape this module:

1. **No skip path.** :func:`run_oracle` never omits a result. If an arm cannot
   be invoked -- not configured, stack down, provider disabled -- the runner
   records a ``NOT_MEASURED`` verdict rather than dropping the row. PRD §16
   requires several gates to be 100%, and a 100% computed over the subset that
   happened to run is not a 100%.
2. **Per-class first, aggregate never alone.** :class:`TrialReport` refuses to
   emit an aggregate score without the per-class breakdown beside it (PRD
   §15.2), because an aggregate can hide that the graph only helps on class
   (c) while regressing class (a).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from .contracts import ArmResponse, QuestionClass
from .oracle import Oracle, OracleResult, Verdict

ArmCallable = Callable[[Oracle], ArmResponse]


@dataclass(frozen=True)
class ClassScore:
    question_class: QuestionClass
    passed: int
    failed: int
    not_measured: int

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.not_measured

    @property
    def is_clean(self) -> bool:
        """Clean means every oracle in the class was measured and passed."""
        return self.total > 0 and self.failed == 0 and self.not_measured == 0

    def render(self) -> str:
        return (
            f"class {self.question_class.value}: {self.passed}/{self.total} pass, "
            f"{self.failed} fail, {self.not_measured} NOT MEASURED"
        )


@dataclass(frozen=True)
class TrialReport:
    arm: str
    results: tuple[OracleResult, ...]

    def by_class(self) -> Mapping[QuestionClass, ClassScore]:
        buckets: dict[QuestionClass, list[OracleResult]] = {
            klass: [] for klass in QuestionClass
        }
        for result in self.results:
            buckets[result.question_class].append(result)
        return {
            klass: ClassScore(
                question_class=klass,
                passed=sum(1 for r in rows if r.verdict is Verdict.PASS),
                failed=sum(1 for r in rows if r.verdict is Verdict.FAIL),
                not_measured=sum(1 for r in rows if r.verdict is Verdict.NOT_MEASURED),
            )
            for klass, rows in buckets.items()
        }

    @property
    def not_measured(self) -> tuple[OracleResult, ...]:
        return tuple(r for r in self.results if r.verdict is Verdict.NOT_MEASURED)

    def render(self) -> str:
        """Per-class lines first; the aggregate is only ever a footer.

        Deliberately renders ``NOT MEASURED`` in its own column rather than
        folding it into failures, so a reader can tell "we looked and it was
        wrong" apart from "we never looked" -- the ADR needs that distinction
        to report an honest gap instead of an inflated one.
        """
        lines = [f"arm: {self.arm}"]
        scores = self.by_class()
        for klass in QuestionClass:
            lines.append("  " + scores[klass].render())
        total = len(self.results)
        passed = sum(1 for r in self.results if r.verdict is Verdict.PASS)
        unmeasured = len(self.not_measured)
        footer = f"  aggregate: {passed}/{total}"
        if unmeasured:
            footer += f" -- NOT COMPARABLE: {unmeasured} oracle(s) were never measured"
        lines.append(footer)
        return "\n".join(lines)


def run_oracle(oracle: Oracle, arm_name: str, arm: ArmCallable) -> OracleResult:
    """Invoke one arm against one oracle, converting every failure to a result.

    An arm that raises has still been measured -- the measurement is "it blew
    up" -- so the exception becomes a ``NOT_RUN`` response rather than
    propagating and aborting the sweep. Aborting would leave the remaining
    oracles unrun *and* unreported, which is the exact silent-coverage-loss
    this harness exists to prevent.
    """
    try:
        response = arm(oracle)
    except Exception as exc:  # noqa: BLE001 - the failure IS the measurement
        response = ArmResponse.not_run(arm_name, f"{type(exc).__name__}: {exc}")
    return oracle.evaluate(response)


def run_trial(
    oracles: Sequence[Oracle], arm_name: str, arm: ArmCallable
) -> TrialReport:
    return TrialReport(
        arm=arm_name,
        results=tuple(run_oracle(oracle, arm_name, arm) for oracle in oracles),
    )


@dataclass
class ArmRegistry:
    """Arms registered for a sweep.

    Registration is explicit and total: :func:`sweep` runs *every* registered
    arm against *every* oracle. An arm that is unavailable must still be
    registered with a stub that returns ``NOT_RUN``, so its absence shows up
    as an unmeasured row in the report instead of a missing column nobody
    notices.
    """

    arms: dict[str, ArmCallable] = field(default_factory=dict)

    def register(self, name: str, arm: ArmCallable) -> None:
        if name in self.arms:
            raise ValueError(f"arm {name!r} already registered")
        self.arms[name] = arm

    def register_unavailable(self, name: str, reason: str) -> None:
        def _stub(_: Oracle) -> ArmResponse:
            return ArmResponse.not_run(name, reason)

        self.register(name, _stub)


def sweep(
    oracles: Sequence[Oracle], registry: ArmRegistry
) -> Mapping[str, TrialReport]:
    return {name: run_trial(oracles, name, arm) for name, arm in registry.arms.items()}


def unmeasured_oracle_ids(reports: Iterable[TrialReport]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {result.oracle_id for report in reports for result in report.not_measured}
        )
    )
