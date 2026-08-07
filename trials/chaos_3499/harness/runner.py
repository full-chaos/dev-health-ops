"""Trial runner, and the baseline-versus-arms report shape the ADR needs.

Three rules shape this module:

1. **No skip path.** :func:`run_oracle` never omits a result. If an arm cannot
   be invoked -- not configured, stack down, provider disabled -- the runner
   records a ``NOT_MEASURED`` verdict rather than dropping the row. PRD §16
   requires several gates to be 100%, and a 100% computed over the subset that
   happened to run is not a 100%.

2. **Per-class first, aggregate never alone.** Reports refuse to emit an
   aggregate without the per-class breakdown beside it (PRD §15.2), because an
   aggregate can hide that the graph only helps on class (c) while regressing
   class (a).

3. **Baseline versus arms, not a flat league table.** Amended §14 restructured
   the native work as pre-trial increments feeding a NATIVE BASELINE -- it is
   not a competing entrant. The baseline is native increments plus episode
   readback, composed; the candidates are the Graphiti arm and the
   direct-store arm, each scored as a *delta against that baseline* per
   question class. Ranking four peers side by side would let a candidate
   "win" a class by placing above one baseline component while losing to the
   baseline as a whole, which is precisely the comparison the ADR must not
   make.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum

from .contracts import ArmResponse, QuestionClass
from .oracle import Oracle, OracleResult, Verdict

ArmCallable = Callable[[Oracle], ArmResponse]


class ArmRole(str, Enum):
    """What an arm is *for* in the comparison.

    ``BASELINE_COMPONENT`` members are composed into one baseline before any
    candidate is compared against it. Per amended §14 the native increments
    and episode readback are both baseline components: they are the thing the
    graph must beat, not entrants beside it.
    """

    BASELINE_COMPONENT = "baseline_component"
    CANDIDATE_ARM = "candidate_arm"


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


@dataclass(frozen=True)
class DependencyState:
    """Which state of an in-flight dependency a class of results ran against.

    Class (b) results are uninterpretable without this. "Native scored 0 on
    class (b)" means something entirely different before and after
    CHAOS-3563 lands declared-state retention, and a reader who cannot tell
    which one they are looking at will read a pre-increment baseline as
    evidence that native cannot do it.

    Deliberately has no default. An unrecorded dependency is represented by
    :data:`UNRECORDED_DEPENDENCY`, which reports render as NOT COMPARABLE
    rather than silently omitting.
    """

    issue: str
    #: Free text describing the branch/merge state actually measured against,
    #: e.g. "merged into feature/chaos-3498-context-fabric @ <sha>" or
    #: "not landed -- baseline measured WITHOUT declared-state retention".
    state: str
    recorded: bool = True


#: The value a class-(b) report carries until the orchestrator confirms
#: CHAOS-3563's branch state. Reading the other lane's worktree directly is
#: not a substitute -- an uncommitted working tree is not a state anyone can
#: cite in an ADR.
UNRECORDED_DEPENDENCY = DependencyState(
    issue="CHAOS-3563",
    state="UNRECORDED -- confirm branch state with the orchestrator",
    recorded=False,
)

#: Which question classes depend on an in-flight issue to be interpretable.
CLASS_DEPENDENCIES: Mapping[QuestionClass, str] = {
    QuestionClass.NEEDS_DECLARED_STATE_HISTORY: "CHAOS-3563",
}


@dataclass
class ArmRegistry:
    """Arms registered for a sweep, each with its role in the comparison.

    Registration is explicit and total: :func:`sweep` runs *every* registered
    arm against *every* oracle. An arm that is unavailable must still be
    registered with a stub that returns ``NOT_RUN``, so its absence shows up
    as an unmeasured row in the report instead of a missing column nobody
    notices.
    """

    arms: dict[str, ArmCallable] = field(default_factory=dict)
    roles: dict[str, ArmRole] = field(default_factory=dict)

    def register(self, name: str, arm: ArmCallable, role: ArmRole) -> None:
        if name in self.arms:
            raise ValueError(f"arm {name!r} already registered")
        self.arms[name] = arm
        self.roles[name] = role

    def register_unavailable(self, name: str, role: ArmRole, reason: str) -> None:
        def _stub(_: Oracle) -> ArmResponse:
            return ArmResponse.not_run(name, reason)

        self.register(name, _stub, role)

    def names_with_role(self, role: ArmRole) -> tuple[str, ...]:
        return tuple(n for n, r in self.roles.items() if r is role)


def compose_baseline(
    components: Sequence[TrialReport], name: str = "baseline"
) -> TrialReport:
    """Fold the baseline components into the single baseline the ADR compares against.

    Per-oracle resolution, in order:

    * any component PASSES -> the baseline passes. The baseline is what the
      product can already do, and it can do it if *any* of its parts can.
    * otherwise, any component was NOT MEASURED -> the baseline is NOT
      MEASURED. Recording a FAIL here would assert the baseline cannot answer
      a question nobody asked it, and a candidate would then be credited with
      beating a baseline that was never run.
    * otherwise -> FAIL.
    """
    if not components:
        raise ValueError("a baseline needs at least one component")
    length = len(components[0].results)
    if any(len(c.results) != length for c in components):
        raise ValueError("baseline components disagree on oracle count")

    folded: list[OracleResult] = []
    for position in range(length):
        rows = [component.results[position] for component in components]
        oracle_ids = {row.oracle_id for row in rows}
        if len(oracle_ids) != 1:
            raise ValueError(
                f"baseline components misaligned at {position}: {oracle_ids}"
            )
        winner = next((r for r in rows if r.verdict is Verdict.PASS), None)
        if winner is None:
            winner = next(
                (r for r in rows if r.verdict is Verdict.NOT_MEASURED), rows[0]
            )
        folded.append(winner)
    return TrialReport(arm=name, results=tuple(folded))


@dataclass(frozen=True)
class ClassComparison:
    question_class: QuestionClass
    baseline: ClassScore
    arm: ClassScore
    dependency: DependencyState | None

    @property
    def delta(self) -> int:
        return self.arm.passed - self.baseline.passed

    @property
    def is_comparable(self) -> bool:
        """False whenever the number would mislead rather than inform."""
        if self.baseline.not_measured or self.arm.not_measured:
            return False
        if self.dependency is not None and not self.dependency.recorded:
            return False
        return True

    def render(self) -> str:
        line = (
            f"  class {self.question_class.value}: baseline "
            f"{self.baseline.passed}/{self.baseline.total}, arm "
            f"{self.arm.passed}/{self.arm.total}, delta {self.delta:+d}"
        )
        if self.is_comparable:
            return line
        reasons = []
        if self.baseline.not_measured:
            reasons.append(
                f"{self.baseline.not_measured} baseline oracle(s) NOT MEASURED"
            )
        if self.arm.not_measured:
            reasons.append(f"{self.arm.not_measured} arm oracle(s) NOT MEASURED")
        if self.dependency is not None and not self.dependency.recorded:
            reasons.append(f"{self.dependency.issue} state {self.dependency.state}")
        return f"{line}  -- NOT COMPARABLE: {'; '.join(reasons)}"


@dataclass(frozen=True)
class ComparisonReport:
    """Baseline versus one candidate arm, per question class.

    This is the shape the ADR consumes. It never exposes a single headline
    number, because per amended §15.2 an aggregate over this question set --
    weighted (a)x1, (b)x1, (c)x5 -- would flatter any extraction-capable
    candidate regardless of merit.
    """

    baseline: TrialReport
    arm: TrialReport
    dependencies: Mapping[QuestionClass, DependencyState] = field(default_factory=dict)

    def by_class(self) -> tuple[ClassComparison, ...]:
        baseline_scores = self.baseline.by_class()
        arm_scores = self.arm.by_class()
        return tuple(
            ClassComparison(
                question_class=klass,
                baseline=baseline_scores[klass],
                arm=arm_scores[klass],
                dependency=self.dependencies.get(klass),
            )
            for klass in QuestionClass
        )

    def native_control_holds(self) -> bool:
        """PRD §15.2: on class (a) the baseline must win or tie.

        If it does not, the finding is about the harness, not about the
        baseline -- and no class-(c) result from the same run should be
        believed until that is explained.
        """
        for comparison in self.by_class():
            if comparison.question_class is QuestionClass.NATIVE_ANSWERABLE:
                return comparison.is_comparable and comparison.delta <= 0
        return False

    def render(self) -> str:
        lines = [f"baseline: {self.baseline.arm}   vs   arm: {self.arm.arm}"]
        for comparison in self.by_class():
            lines.append(comparison.render())
        if not self.native_control_holds():
            lines.append(
                "  !! class (a) control did NOT hold: the baseline should win "
                "or tie on natively-answerable questions. Treat every other "
                "row in this report as unexplained until this is resolved."
            )
        return "\n".join(lines)


def sweep(
    oracles: Sequence[Oracle], registry: ArmRegistry
) -> Mapping[str, TrialReport]:
    return {name: run_trial(oracles, name, arm) for name, arm in registry.arms.items()}


def compare(
    oracles: Sequence[Oracle],
    registry: ArmRegistry,
    dependencies: Mapping[QuestionClass, DependencyState] | None = None,
) -> tuple[ComparisonReport, ...]:
    """Run every arm, fold the baseline, and compare each candidate to it.

    ``dependencies`` defaults to marking every dependent class UNRECORDED --
    the safe direction. A class-(b) comparison that nobody has attached a
    CHAOS-3563 branch state to renders NOT COMPARABLE rather than quietly
    reporting a number whose meaning depends on information the report does
    not carry.
    """
    resolved = dict(dependencies or {})
    for klass, issue in CLASS_DEPENDENCIES.items():
        resolved.setdefault(klass, UNRECORDED_DEPENDENCY)
        if resolved[klass].issue != issue:
            raise ValueError(
                f"class {klass.value} depends on {issue}, but a "
                f"{resolved[klass].issue} state was supplied"
            )

    reports = sweep(oracles, registry)
    components = [
        reports[name] for name in registry.names_with_role(ArmRole.BASELINE_COMPONENT)
    ]
    if not components:
        raise ValueError(
            "no baseline components registered; a candidate arm scored "
            "against nothing is not a comparison"
        )
    baseline = compose_baseline(components)
    return tuple(
        ComparisonReport(baseline=baseline, arm=reports[name], dependencies=resolved)
        for name in registry.names_with_role(ArmRole.CANDIDATE_ARM)
    )


def unmeasured_oracle_ids(reports: Iterable[TrialReport]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {result.oracle_id for report in reports for result in report.not_measured}
        )
    )
