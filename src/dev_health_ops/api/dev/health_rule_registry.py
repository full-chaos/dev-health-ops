"""``HealthRuleRegistry`` -- the code-owned, versioned health-rule registry

and evaluation engine required by CHAOS-3302 before Ask Dev may describe a
project/team as healthy, at risk, needing attention, overburdened, or
operationally deficient.

Mirrors ``tool_registry.AskDevToolRegistry``'s construction-time-validated,
exact-allowlist posture: a registry is built once from a fixed tuple of
``HealthRuleDefinition`` instances, duplicate/malformed rule IDs are
rejected at construction (not at lookup time), and lookups are a closed
dict keyed by the validated ID.

Non-goal (explicit, per the CHAOS-3302 plan): this module does not compute
dimension values from raw platform data. That stays with Operating
Review/Diagnose/Bottlenecks/etc., or becomes the job of the Wave 3.1
issues that call this registry (CHAOS-3303/3304/3305). Callers hand this
module already-computed ``DimensionObservation`` values; the registry and
evaluation engine own only the governance layer: which rule fires, under
what evidentiary guardrails, and whether a team qualifies for a
"needs-attention" finding.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

from .contracts_v2.base import SourceClass, SourceRequirementState
from .contracts_v2.health_rules import (
    CalibrationRecord,
    CalibrationState,
    DimensionObservation,
    DimensionState,
    HealthDimension,
    HealthRuleDefinition,
    HealthRuleFinding,
    RuleApplicability,
    RuleDirection,
    TeamQualificationBasis,
    TeamQualificationResult,
)
from .health_rule_calibration_inventory import (
    CALIBRATION_RECORDS,
    CHAOS_3331_ATTRIBUTION_BLOCKED_RULE_IDS,
)

__all__ = [
    "HEALTH_RULE_REGISTRY",
    "RULE_ID_PATTERN",
    "DuplicateRuleError",
    "HealthRuleEvaluationResult",
    "HealthRuleRegistry",
    "HealthRuleRegistryError",
    "InvalidCalibrationEvidenceError",
    "InvalidRuleIDError",
    "UnknownRuleError",
    "evaluate_registry",
    "evaluate_rule",
    "qualify_team_needs_attention",
    "rule_version_fingerprint",
]

#: The closed rule-ID grammar. Prefixed ``health_rule.`` on top of the
#: generic dotted/versioned token family the platform already uses for
#: internal identifiers (``validators._VERSIONED_ID_PATTERN``,
#: ``health_rule.completion.v3`` in that module's own docstring example).
RULE_ID_PATTERN = re.compile(
    r"^health_rule\.[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v\d+$"
)


class HealthRuleRegistryError(RuntimeError):
    """Base class for safe, deterministic registry failures."""


class DuplicateRuleError(HealthRuleRegistryError):
    pass


class InvalidRuleIDError(HealthRuleRegistryError):
    pass


class UnknownRuleError(HealthRuleRegistryError):
    pass


class InvalidCalibrationEvidenceError(HealthRuleRegistryError):
    """A rule claims reviewed authority its evidence does not back.

    Raised when a rule's ``calibration_state`` is a reviewed state (not
    ``provisional``) but its ``calibration_evidence_ref`` does not resolve
    to a genuinely reviewed ``CalibrationRecord`` for that same rule --
    see ``_resolves_against_inventory``.
    """


def rule_version_fingerprint(rule: HealthRuleDefinition) -> str:
    """A deterministic, content-addressed fingerprint for one rule version.

    Deliberately a plain SHA-256 digest over the rule's canonical JSON, not
    an HMAC. ``EvidenceReferenceSigner`` (``evidence_service.py``) is keyed
    because an evidence handle is an *authorization token*: it must be
    unforgeable by a party who does not hold the server secret, since
    possessing a valid handle is what authorizes dereferencing. A rule
    fingerprint has no such authorization role -- ``HealthRuleDefinition``
    is code-owned and public (it ships in the checked-in manifest), so
    there is no secret-holder/verifier distinction to key against. What the
    fingerprint needs is exactly what a content hash gives for free:
    two rule definitions with the same fields produce the same fingerprint,
    and any field change changes it, which is what "persisted rule
    fingerprints" (CHAOS-3302 deliverable) exists to detect. This is a
    deliberate, documented deviation from the pre-implementation plan's
    "reuse the evidence-signer minting pattern" note.
    """

    payload = rule.model_dump(mode="json")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    digest = hashlib.sha256(canonical).hexdigest()
    return f"hrf1_{digest[:40]}"


#: Namespace for folding a finding's identity into a canonical UUID
#: (``ServerHandle``), the same "arbitrary string -> UUID5" pattern already
#: used at the request boundary (``router._storage_uuid``, cited in
#: ``test_contracts_v2._NON_HANDLE_IDENTIFIER_REASONS``). Deterministic --
#: the same rule/subject/window always mints the same finding_id -- while
#: still landing in the platform's one opaque-handle grammar rather than a
#: bespoke prefixed-hex shape.
_FINDING_ID_NAMESPACE = uuid.UUID("6f6e6531-6865-616c-7468-72756c657631")


def _mint_finding_id(
    org_id: str, rule: HealthRuleDefinition, observation: DimensionObservation
) -> str:
    """Mint a deterministic finding id, scoped to tenant and cohort/window identity.

    ``org_id`` is required (Codex-confirmed finding, 2026-08-01): the
    subject id is a provider-scoped key (e.g. a Jira/Linear project key),
    not globally unique, so two different organizations reusing the same
    key at the same timestamp would otherwise mint the same finding id and
    collide cross-tenant -- risking incorrect deduplication, joins, or
    finding references across an organization boundary. ``cohort_size`` and
    ``window_index`` are included as the observation's own cohort/window
    identity: without them, two evaluations of the same subject at the same
    timestamp but over different cohorts (e.g. cohort_size=1 vs. 99) minted
    the identical finding id.

    ``observed_at`` is normalized to UTC with fixed microsecond precision
    before formatting (Codex-confirmed finding, 2026-08-01, round 3):
    ``AwareDatetime.isoformat()`` preserves whatever offset the caller's
    value carried, so ``2026-08-01T12:00:00Z`` and its equal instant
    ``2026-08-01T05:00:00-07:00`` produced two different UUID5 values --
    a genuine identity failure, not merely a formatting quirk, since the
    finding-id contract promises the same rule/subject/window always mints
    the same id. Converting to UTC (and forcing an explicit microseconds
    field, present or not) makes two equal instants always render the
    identical string regardless of the offset a caller happened to supply.
    """

    normalized_observed_at = observation.observed_at.astimezone(UTC).isoformat(
        timespec="microseconds"
    )
    payload = "|".join(
        (
            org_id,
            rule.rule_id,
            rule.rule_version,
            observation.subject_kind.value,
            observation.subject_id,
            str(observation.cohort_size),
            str(observation.window_index),
            normalized_observed_at,
        )
    )
    return str(uuid.uuid5(_FINDING_ID_NAMESPACE, payload))


def _resolves_against_inventory(
    rule: HealthRuleDefinition, records: Mapping[str, CalibrationRecord]
) -> bool:
    """Does this rule's evidence resolve to a genuinely reviewed record?

    A ``provisional`` rule cites no evidence and trivially resolves (there
    is nothing to check). A reviewed rule's ``calibration_evidence_ref`` is
    looked up by exact match against ``CalibrationRecord.calibration_id``
    in ``records`` -- not merely checked for non-emptiness (Codex-confirmed
    finding, 2026-08-01, round 3: "a normally valid second registry with
    caller-asserted calibration evidence passed construction"). The
    matched record must resolve on every one of four independent fields
    (Codex-confirmed finding, round 4 -- checking only ``rule_id`` and
    "not provisional" let a ``product_approved`` v1 rule resolve against a
    ``data_derived`` record for v99: a real record for a *different*
    version, or a record recording a *different* reviewed state than the
    rule itself claims, are each their own way to authorize a rule that
    was never actually reviewed):

    * ``record.calibration_id == ref`` -- the mapping key actually matches
      the record's own identity, not merely however the caller happened to
      key the ``records`` mapping;
    * ``record.rule_id == rule.rule_id`` -- the record is about this rule,
      not an unrelated one;
    * ``record.rule_version == rule.rule_version`` -- the record is about
      *this exact version* of the rule, not a stale or future one (a
      changed threshold bumps the version; a record for the old version
      must not authorize the new one);
    * ``record.calibration_state == rule.calibration_state`` -- the record
      documents the same reviewed state the rule claims (a
      ``data_derived`` review does not make a ``product_approved`` claim
      true, and vice versa);
    * ``record.evidence_ref is not None`` -- the record itself cites real
      evidence, not just a same-changeset placeholder.

    Today every record in the shipped ``health_rule_calibration_inventory``
    is ``provisional`` with ``evidence_ref=None`` (nothing has actually
    been reviewed yet), so this makes it structurally impossible to ship a
    reviewed launch rule until a real calibration decision exists to cite.
    """

    if rule.calibration_state == CalibrationState.PROVISIONAL:
        return True
    ref = rule.calibration_evidence_ref
    if ref is None:
        return False
    record = records.get(ref)
    if record is None:
        return False
    return (
        record.calibration_id == ref
        and record.rule_id == rule.rule_id
        and record.rule_version == rule.rule_version
        and record.calibration_state == rule.calibration_state
        and record.evidence_ref is not None
    )


class HealthRuleRegistry:
    """Exact allowlist of code-owned health rules, validated at construction.

    Immutable after construction, not merely "immutable-shaped": every
    attribute set is routed through ``__setattr__`` below, which raises
    once construction finishes (Codex-confirmed finding, 2026-08-01, round
    3 -- ``MappingProxyType`` blocks item assignment on ``self._rules``,
    but not *rebinding* ``self._rules`` itself to a whole new object;
    ``HEALTH_RULE_REGISTRY._rules = MappingProxyType({...})`` succeeded
    against the round-2 fix). This is a same-process guard, not a security
    boundary: code running in the same interpreter can still reach past it
    with ``object.__setattr__`` directly -- that residual is accepted and
    documented (see this module's closure argument), not defended against,
    because no Python-level guard can stop an attacker who already has
    arbitrary code execution in-process.
    """

    #: Bare annotations, not class-level assignments -- both are actually
    #: set via ``object.__setattr__`` in ``__init__`` (see there). Declared
    #: here only so static type checking knows these instance attributes
    #: exist.
    _rules: Mapping[str, HealthRuleDefinition]
    _initialized: bool

    def __init__(
        self,
        rules: Sequence[HealthRuleDefinition],
        *,
        calibration_records: Mapping[str, CalibrationRecord] | None = None,
    ) -> None:
        ids = [rule.rule_id for rule in rules]
        if len(ids) != len(set(ids)):
            duplicates = sorted({rule_id for rule_id in ids if ids.count(rule_id) > 1})
            raise DuplicateRuleError(f"duplicate rule id(s): {duplicates}")
        validated: dict[str, HealthRuleDefinition] = {}
        for rule in rules:
            if not RULE_ID_PATTERN.match(rule.rule_id):
                raise InvalidRuleIDError(
                    f"rule id {rule.rule_id!r} does not match the closed "
                    "health_rule.<name>.vN grammar"
                )
            # Revalidate every definition here rather than trust the
            # instance handed in. ``HealthRuleDefinition`` is frozen, but
            # frozen only blocks attribute assignment -- ``model_copy``
            # bypasses every validator (Codex-confirmed finding,
            # 2026-08-01), so a caller could otherwise hand this
            # constructor a rule that reports e.g.
            # ``calibration_state=product_approved`` with no
            # ``calibration_evidence_ref``, a combination
            # ``validate_calibration_evidence`` would never allow through
            # ``__init__``. Round-tripping through ``model_validate`` forces
            # every validator to run again; a model-copy-invalid definition
            # is rejected here, not silently admitted.
            revalidated = HealthRuleDefinition.model_validate(
                rule.model_dump(mode="json")
            )
            # A structurally-valid non-empty evidence_ref is not the same
            # claim as "this rule was actually reviewed" -- a caller can
            # construct a whole second HealthRuleRegistry with a rule that
            # asserts calibration_state=product_approved and any string for
            # calibration_evidence_ref (round-3 Codex repro). When a
            # calibration inventory is supplied, cross-check every reviewed
            # rule's evidence against it instead of trusting the string.
            # Registries that omit ``calibration_records`` (e.g. a
            # deliberately test-scoped registry, see
            # ``test_chaos_3302_health_rule_e2e_controls.py``) skip this
            # cross-check and keep only the structural guarantee above --
            # they were never claiming production authority in the first
            # place.
            if calibration_records is not None and not _resolves_against_inventory(
                revalidated, calibration_records
            ):
                raise InvalidCalibrationEvidenceError(
                    f"rule {revalidated.rule_id!r} claims calibration_state="
                    f"{revalidated.calibration_state.value!r} but its "
                    f"calibration_evidence_ref={revalidated.calibration_evidence_ref!r} "
                    "does not resolve to a reviewed calibration record for "
                    "this rule in the supplied inventory"
                )
            validated[revalidated.rule_id] = revalidated
        # An immutable mapping, not a plain dict: the only reference to the
        # underlying mutable dict is local to this constructor, so no
        # caller can reach in and replace a shipped rule post-construction
        # (e.g. ``registry._rules[rule_id] = mutated_rule``) to smuggle a
        # forged rule into the singleton after it has already passed the
        # checks above. Set via ``object.__setattr__`` because this class's
        # own ``__setattr__`` (below) forbids attribute assignment once
        # ``_initialized`` is set -- this is the last write allowed.
        object.__setattr__(self, "_rules", MappingProxyType(validated))
        object.__setattr__(self, "_initialized", True)

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_initialized", False):
            raise AttributeError(
                "HealthRuleRegistry is immutable after construction -- "
                f"cannot set {name!r}. Construct a new HealthRuleRegistry "
                "instead of mutating this one."
            )
        object.__setattr__(self, name, value)

    def rule(self, rule_id: str) -> HealthRuleDefinition:
        try:
            return self._rules[rule_id]
        except KeyError as exc:
            raise UnknownRuleError(f"unknown rule id: {rule_id!r}") from exc

    def __contains__(self, rule_id: object) -> bool:
        return rule_id in self._rules

    def __iter__(self):
        return iter(self._rules)

    def __len__(self) -> int:
        return len(self._rules)

    def items(self):
        return self._rules.items()

    def values(self) -> tuple[HealthRuleDefinition, ...]:
        return tuple(self._rules.values())

    def rules_for_dimension(
        self, dimension: HealthDimension
    ) -> tuple[HealthRuleDefinition, ...]:
        return tuple(
            rule for rule in self._rules.values() if rule.dimension is dimension
        )

    def manifest(self) -> dict[str, object]:
        """The ``health_rule_manifest.v1`` payload -- see ``health_rule_manifest.py``."""

        return {
            "schema_version": "health_rule_manifest.v1",
            "rules": [
                {
                    "rule_id": rule.rule_id,
                    "rule_version": rule.rule_version,
                    "owner": rule.owner,
                    "applicability": [item.value for item in rule.applicability],
                    "dimension": rule.dimension.value,
                    "calibration_state": rule.calibration_state.value,
                    "minimum_sample": rule.minimum_sample,
                    "minimum_cohort_size": rule.minimum_cohort_size,
                    "severity_mapping": rule.triggered_state.value,
                    "fingerprint": rule_version_fingerprint(rule),
                }
                for rule in sorted(self._rules.values(), key=lambda rule: rule.rule_id)
            ],
        }


def _condition_met(
    rule: HealthRuleDefinition, observation: DimensionObservation
) -> bool:
    if rule.direction is RuleDirection.DETERMINISTIC:
        return observation.current_value is not None and observation.current_value != 0
    if observation.current_value is None or rule.threshold is None:
        return False
    if rule.direction is RuleDirection.HIGHER_IS_WORSE:
        return observation.current_value >= rule.threshold
    return observation.current_value <= rule.threshold


def evaluate_rule(
    rule: HealthRuleDefinition,
    observations: Sequence[DimensionObservation],
    *,
    org_id: str,
) -> HealthRuleFinding:
    """Evaluate one rule against its ordered observation windows.

    ``observations`` must include ``window_index=0`` (the current period);
    later windows (1, 2, ...) supply the sustained-window history a rule
    with ``sustained_periods_required > 1`` needs. Guardrails are checked
    in a fixed order -- no-data, cohort, sample, coverage, denominator,
    attribution, then the condition itself -- so exactly one governs any
    given suppressed result.

    ``org_id`` is required, keyword-only: it scopes the minted
    ``finding_id`` to the calling tenant (see ``_mint_finding_id``) and is
    never inferred or defaulted, so a caller cannot accidentally evaluate
    (and mint a finding id for) one organization's observations without
    naming which organization they belong to.
    """

    if not observations:
        raise ValueError("evaluate_rule requires at least one observation")
    ordered = sorted(observations, key=lambda item: item.window_index)
    current = ordered[0]
    if current.window_index != 0:
        raise ValueError("the current observation (window_index=0) is required")

    def _finding(
        state: DimensionState, suppressed_reason: str | None = None
    ) -> HealthRuleFinding:
        return HealthRuleFinding(
            schema_version="health_rule_finding.v1",
            finding_id=_mint_finding_id(org_id, rule, current),
            rule_id=rule.rule_id,
            rule_version=rule.rule_version,
            dimension=rule.dimension,
            subject_kind=current.subject_kind,
            subject_id=current.subject_id,
            state=state,
            fact_kind=rule.fact_kind,
            shadow_only=rule.calibration_state == CalibrationState.PROVISIONAL,
            evidence_source_classes=rule.evidence_source_classes,
            remediation_template=rule.remediation_template,
            calibration_state=rule.calibration_state,
            evaluated_at=current.observed_at,
            suppressed_reason=suppressed_reason,
        )

    # Zero versus no-data: a genuinely unmeasured source reports unknown
    # honestly and is never suppressed for insufficient sample/coverage --
    # there is nothing to have insufficient sample of. "not_measured" is
    # the other valid spelling of "never measured" (DimensionObservation's
    # own validate_zero_semantics accepts either for an unmeasured
    # observed_states set) and must short-circuit identically, or a
    # never-measured source is misreported as insufficient_cohort/sample/
    # coverage instead of the honest reason (Codex-confirmed finding).
    if current.data_semantics in ("no_data", "not_measured"):
        return _finding(DimensionState.UNKNOWN)

    needs_cohort = current.subject_kind in (
        RuleApplicability.TEAM,
        RuleApplicability.PORTFOLIO,
    )
    if needs_cohort and rule.minimum_cohort_size is not None:
        if (current.cohort_size or 0) < rule.minimum_cohort_size:
            return _finding(DimensionState.UNKNOWN, "insufficient_cohort")

    if current.sample_count is not None and current.sample_count < rule.minimum_sample:
        return _finding(DimensionState.UNKNOWN, "insufficient_sample")

    if current.coverage < rule.minimum_coverage:
        return _finding(DimensionState.UNKNOWN, "insufficient_coverage")

    if rule.denominator_required and not current.denominator_present:
        return _finding(DimensionState.UNKNOWN, "missing_denominator")

    if rule.attribution_required and not current.attribution_present:
        return _finding(DimensionState.UNKNOWN, "missing_attribution")

    if not _condition_met(rule, current):
        return _finding(DimensionState.HEALTHY)

    required = rule.sustained_periods_required
    window = ordered[:required]
    if len(window) < required or not all(_condition_met(rule, item) for item in window):
        return _finding(DimensionState.UNKNOWN, "not_sustained")

    return _finding(rule.triggered_state)


@dataclass(frozen=True, slots=True)
class HealthRuleEvaluationResult:
    """The partitioned output of evaluating a batch of rules.

    ``shadow_findings`` (from a ``provisional`` rule) and
    ``suppressed_findings`` (guardrail-suppressed) are computed for
    calibration/observability but are structurally excluded from
    ``launch_findings`` -- the only set ``qualify_team_needs_attention``
    (and any downstream launch surface) may read.
    """

    launch_findings: tuple[HealthRuleFinding, ...]
    shadow_findings: tuple[HealthRuleFinding, ...]
    suppressed_findings: tuple[HealthRuleFinding, ...]


def _evaluate_with_registry(
    registry: HealthRuleRegistry,
    observations_by_rule: Mapping[str, Sequence[DimensionObservation]],
    *,
    org_id: str,
) -> HealthRuleEvaluationResult:
    """Evaluate a batch of rules against an explicitly-supplied registry.

    NOT the production evaluation seam -- see ``evaluate_registry`` below,
    which is the only entry point production code should call. This
    function accepts an arbitrary ``HealthRuleRegistry`` instance, which is
    exactly the shape of the round-3 Codex repro: a caller can construct
    any second registry, including one whose rules assert
    ``calibration_state=product_approved`` with caller-chosen evidence, and
    this function will happily treat its rules as launch-eligible. It
    exists only so tests can prove the evaluation mechanism against a
    registry defined entirely in test scope (see
    ``test_chaos_3302_health_rule_e2e_controls.py``'s
    ``_AUTHORIZED_TEST_REGISTRY``) without that test-only authority ever
    being reachable from the production seam.
    """

    launch: list[HealthRuleFinding] = []
    shadow: list[HealthRuleFinding] = []
    suppressed: list[HealthRuleFinding] = []
    for rule_id, observations in observations_by_rule.items():
        rule = registry.rule(rule_id)
        finding = evaluate_rule(rule, observations, org_id=org_id)
        if finding.shadow_only:
            shadow.append(finding)
        elif finding.suppressed_reason is not None:
            suppressed.append(finding)
        else:
            launch.append(finding)
    return HealthRuleEvaluationResult(
        launch_findings=tuple(launch),
        shadow_findings=tuple(shadow),
        suppressed_findings=tuple(suppressed),
    )


def evaluate_registry(
    observations_by_rule: Mapping[str, Sequence[DimensionObservation]],
    *,
    org_id: str,
) -> HealthRuleEvaluationResult:
    """The production evaluation seam: hard-bound to ``HEALTH_RULE_REGISTRY``.

    Deliberately takes no registry parameter (Codex-confirmed finding,
    2026-08-01, round 3) -- a caller-supplied registry is exactly the
    authority-forging vector this function must not offer. Every rule
    consulted here is a member of the canonical, construction-validated,
    inventory-cross-checked, rebind-resistant module singleton. A caller
    that genuinely needs to evaluate against a different registry (tests
    only) must say so explicitly by calling ``_evaluate_with_registry``.
    """

    return _evaluate_with_registry(
        HEALTH_RULE_REGISTRY, observations_by_rule, org_id=org_id
    )


def _is_canonically_launch_eligible(
    finding: HealthRuleFinding, registry: HealthRuleRegistry
) -> bool:
    """Is this finding backed by the registry's OWN record of its rule?

    Never trusts ``finding.calibration_state`` (Codex-confirmed finding,
    2026-08-01, round 4): a ``HealthRuleFinding`` is a bare value object --
    an entirely ordinary constructor call, no ``model_copy`` or
    ``model_construct`` bypass required, can claim
    ``calibration_state=product_approved`` for a rule that is provisional
    (or does not exist at all) in ``registry``. Launch eligibility is
    re-derived here from the registry's own, construction-time
    inventory-validated record of the named ``rule_id``/``rule_version`` --
    the finding's own ``calibration_state`` field is read nowhere in this
    function.
    """

    try:
        rule = registry.rule(finding.rule_id)
    except UnknownRuleError:
        return False
    return (
        rule.rule_version == finding.rule_version
        and rule.calibration_state != CalibrationState.PROVISIONAL
    )


def _qualify_team_needs_attention_against_registry(
    findings: Sequence[HealthRuleFinding],
    *,
    team_id: str,
    registry: HealthRuleRegistry,
) -> TeamQualificationResult:
    """The team-needs-attention qualification contract (CHAOS-3302), against

    an explicitly supplied registry.

    NOT the production seam -- see ``qualify_team_needs_attention`` below,
    which is the only entry point production code should call. This
    function's launch-eligibility check (``_is_canonically_launch_eligible``)
    resolves every finding against ``registry``, so it is only as
    trustworthy as the registry it is handed; it exists so tests can prove
    the qualification mechanism against a registry defined entirely in test
    scope (``_AUTHORIZED_TEST_REGISTRY``) without that test-only authority
    ever being reachable from the production seam.

    "A team-level finding requires either at least two independent
    applicable dimensions at risk for the sustained window; or one critical
    rule with required evidence and coverage. One metric, one bad week, one
    missing source, or one provisional threshold is insufficient."
    """

    # ``evaluated_at`` must be derived strictly from launch-authorized
    # evidence -- filter non-launch findings out FIRST, then aggregate.
    # Aggregating over the unfiltered ``findings`` (Codex-confirmed finding,
    # 2026-08-01) let a fresher shadow/provisional finding's timestamp make
    # stale launch evidence look current, even though shadow activity never
    # contributes to the qualification decision itself.
    launch_only = [
        finding
        for finding in findings
        if _is_canonically_launch_eligible(finding, registry)
    ]
    evaluated_at = max(
        (finding.evaluated_at for finding in launch_only), default=_utc_now()
    )

    critical = [
        finding
        for finding in launch_only
        if finding.state is DimensionState.CRITICAL
        and finding.suppressed_reason is None
        and finding.evidence_source_classes
    ]
    if critical:
        chosen = critical[0]
        return TeamQualificationResult(
            schema_version="team_qualification_result.v1",
            team_id=team_id,
            qualifies=True,
            basis=TeamQualificationBasis.CRITICAL_RULE,
            contributing_dimensions=(chosen.dimension,),
            contributing_finding_ids=(chosen.finding_id,),
            evaluated_at=evaluated_at,
        )

    at_risk_by_dimension: dict[HealthDimension, HealthRuleFinding] = {}
    for finding in launch_only:
        if (
            finding.state is DimensionState.AT_RISK
            and finding.suppressed_reason is None
        ):
            at_risk_by_dimension.setdefault(finding.dimension, finding)

    if len(at_risk_by_dimension) >= 2:
        chosen_dimensions = tuple(
            sorted(at_risk_by_dimension, key=lambda dimension: dimension.value)
        )
        chosen_finding_ids = tuple(
            at_risk_by_dimension[dimension].finding_id
            for dimension in chosen_dimensions
        )
        return TeamQualificationResult(
            schema_version="team_qualification_result.v1",
            team_id=team_id,
            qualifies=True,
            basis=TeamQualificationBasis.MULTI_DIMENSION,
            contributing_dimensions=chosen_dimensions,
            contributing_finding_ids=chosen_finding_ids,
            evaluated_at=evaluated_at,
        )

    return TeamQualificationResult(
        schema_version="team_qualification_result.v1",
        team_id=team_id,
        qualifies=False,
        basis=None,
        evaluated_at=evaluated_at,
    )


def qualify_team_needs_attention(
    findings: Sequence[HealthRuleFinding], *, team_id: str
) -> TeamQualificationResult:
    """The production qualification seam: hard-bound to ``HEALTH_RULE_REGISTRY``.

    Deliberately takes no registry parameter, mirroring ``evaluate_registry``
    (Codex-confirmed finding, 2026-08-01, round 4) -- every finding passed
    here has its launch eligibility re-derived against the canonical,
    construction-validated, inventory-cross-checked, rebind-resistant
    module singleton, never against what the finding itself claims. A
    caller that genuinely needs to qualify against a different registry
    (tests only) must say so explicitly by calling
    ``_qualify_team_needs_attention_against_registry``.
    """

    return _qualify_team_needs_attention_against_registry(
        findings, team_id=team_id, registry=HEALTH_RULE_REGISTRY
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _rule(
    *,
    rule_id: str,
    owner: str,
    applicability: tuple[RuleApplicability, ...],
    dimension: HealthDimension,
    required_source_classes: tuple[SourceClass, ...],
    required_observed_states: tuple[SourceRequirementState, ...] = (
        SourceRequirementState.AVAILABLE_CURRENT,
    ),
    direction: RuleDirection,
    threshold: float | None,
    comparison_unit: str | None,
    minimum_sample: int,
    minimum_coverage: float,
    current_window_days: int,
    comparison_window_days: int | None = None,
    sustained_periods_required: int,
    denominator_required: bool,
    attribution_required: bool,
    minimum_cohort_size: int | None,
    triggered_state: DimensionState,
    evidence_source_classes: tuple[SourceClass, ...],
    fact_kind: str,
    remediation_template: str,
    calibration_state: CalibrationState,
    calibration_evidence_ref: str | None,
) -> HealthRuleDefinition:
    return HealthRuleDefinition(
        schema_version="health_rule_definition.v1",
        rule_id=rule_id,
        rule_version=rule_id,
        owner=owner,
        applicability=applicability,
        dimension=dimension,
        required_source_classes=required_source_classes,
        required_observed_states=required_observed_states,
        direction=direction,
        threshold=threshold,
        comparison_unit=comparison_unit,
        minimum_sample=minimum_sample,
        minimum_coverage=minimum_coverage,
        current_window_days=current_window_days,
        comparison_window_days=comparison_window_days,
        sustained_periods_required=sustained_periods_required,
        denominator_required=denominator_required,
        attribution_required=attribution_required,
        minimum_cohort_size=minimum_cohort_size,
        triggered_state=triggered_state,
        evidence_source_classes=evidence_source_classes,
        fact_kind=fact_kind,
        remediation_template=remediation_template,
        calibration_state=calibration_state,
        calibration_evidence_ref=calibration_evidence_ref,
    )


# ---------------------------------------------------------------------------
# Launch rule set.
#
# Every rule shipped here is ``provisional`` (shadow-only), with no
# exceptions -- ``test_no_shipped_rule_is_launch_authorized`` enforces this
# as a totality check. A non-provisional calibration_state is a claim that
# a real review (sample sizes, distributions, false-positive/negative
# analysis, small-cohort behavior, owner sign-off) actually happened; none
# has, for anything in this file. The first three rules below
# (completion_stalled/review_latency_sustained/data_trust_broken) were
# originally shipped ``product_approved`` with a same-changeset
# illustrative calibration record as their own "evidence" -- a Codex
# adversarial review (2026-08-01) correctly flagged that as launch-
# authorizing real findings on authority that never existed: any caller of
# ``evaluate_registry`` would receive them in ``launch_findings``, not
# ``shadow_findings``, indistinguishable from a genuinely reviewed rule.
# They are demoted to provisional here; the "approved launch finding" and
# "team qualification" paths are now proven by a registry instance defined
# entirely within the test that needs it
# (``test_chaos_3302_health_rule_e2e_controls.py``'s positive controls),
# which is honest about being test-scoped authority rather than shipping
# fake authority in the production registry. This also, as a consequence,
# already matches the second half of the original comment here: this file
# does NOT retroactively promote any of the thresholds already live in
# Operating Review/forecast/opportunities to canonical status -- per
# CHAOS-3302, "Do not activate provisional thresholds as canonical rules
# without review", and no such review has happened for those either. See
# ``health_rule_calibration_inventory`` for the full citation of where each
# provisional threshold lives today and why it is not yet approved.
# ---------------------------------------------------------------------------

_LAUNCH_RULES: tuple[HealthRuleDefinition, ...] = (
    _rule(
        rule_id="health_rule.completion_stalled.v1",
        owner="ask-dev-governance",
        applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
        dimension=HealthDimension.EXECUTION_COMPLETION,
        required_source_classes=(SourceClass.STATUS_CHANGE, SourceClass.WORK_ITEM),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=0.2,
        comparison_unit="stalled_work_item_ratio",
        minimum_sample=10,
        minimum_coverage=0.6,
        current_window_days=14,
        sustained_periods_required=2,
        denominator_required=True,
        attribution_required=False,
        minimum_cohort_size=5,
        triggered_state=DimensionState.AT_RISK,
        evidence_source_classes=(SourceClass.STATUS_CHANGE, SourceClass.WORK_ITEM),
        fact_kind="observed",
        remediation_template="Review stalled work items with the team before the next planning cycle.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.review_latency_sustained.v1",
        owner="ask-dev-governance",
        applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
        dimension=HealthDimension.REVIEW_CI_PRESSURE,
        required_source_classes=(SourceClass.PULL_REQUEST, SourceClass.REVIEW),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=0.5,
        comparison_unit="p50_review_latency_hours",
        minimum_sample=10,
        minimum_coverage=0.6,
        current_window_days=14,
        sustained_periods_required=2,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=5,
        triggered_state=DimensionState.AT_RISK,
        evidence_source_classes=(SourceClass.PULL_REQUEST, SourceClass.REVIEW),
        fact_kind="observed",
        remediation_template="Review open pull requests aging past the team's usual review latency.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.data_trust_broken.v1",
        owner="ask-dev-governance",
        applicability=(
            RuleApplicability.PROJECT,
            RuleApplicability.TEAM,
            RuleApplicability.PORTFOLIO,
        ),
        dimension=HealthDimension.DATA_TRUST,
        required_source_classes=(SourceClass.SOURCE_HEALTH,),
        required_observed_states=(
            SourceRequirementState.AVAILABLE_CURRENT,
            SourceRequirementState.AVAILABLE_STALE,
        ),
        direction=RuleDirection.DETERMINISTIC,
        threshold=None,
        comparison_unit=None,
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=7,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=1,
        triggered_state=DimensionState.CRITICAL,
        evidence_source_classes=(SourceClass.SOURCE_HEALTH,),
        fact_kind="observed",
        remediation_template="Restore or reconfigure the affected data source before trusting downstream findings.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    # -- Calibration-inventory rules: provisional, shadow-only ------------
    _rule(
        rule_id="health_rule.wip_congestion.v1",
        owner="calibration-inventory",
        applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
        dimension=HealthDimension.DELIVERY_FLOW,
        required_source_classes=(SourceClass.WORK_ITEM,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=1.25,
        comparison_unit="wip_congestion_ratio",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=7,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=1,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.WORK_ITEM,),
        fact_kind="observed",
        remediation_template="Investigate WIP congestion before it affects delivery flow.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.review_bottleneck_hours.v1",
        owner="calibration-inventory",
        applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
        dimension=HealthDimension.REVIEW_CI_PRESSURE,
        required_source_classes=(SourceClass.PULL_REQUEST, SourceClass.REVIEW),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=48.0,
        comparison_unit="review_latency_hours",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=7,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=1,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.PULL_REQUEST, SourceClass.REVIEW),
        fact_kind="observed",
        remediation_template="Investigate review latency before it affects delivery flow.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.incident_load.v1",
        owner="calibration-inventory",
        applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
        dimension=HealthDimension.RELIABILITY_RELEASE,
        required_source_classes=(SourceClass.INCIDENT,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=10.0,
        comparison_unit="incident_count",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=7,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=1,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.INCIDENT,),
        fact_kind="observed",
        remediation_template="Review incident volume before it affects release reliability.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.change_failure_rate.v1",
        owner="calibration-inventory",
        applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
        dimension=HealthDimension.RELIABILITY_RELEASE,
        required_source_classes=(SourceClass.DEPLOYMENT,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=0.15,
        comparison_unit="change_failure_rate",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=30,
        sustained_periods_required=1,
        denominator_required=True,
        attribution_required=False,
        minimum_cohort_size=1,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.DEPLOYMENT,),
        fact_kind="observed",
        remediation_template="Review recent deployment failures before they affect release reliability.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.flaky_test_rate.v1",
        owner="calibration-inventory",
        applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
        dimension=HealthDimension.REVIEW_CI_PRESSURE,
        required_source_classes=(SourceClass.TEST_REPORT,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=0.05,
        comparison_unit="weighted_flake_rate",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=14,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=1,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.TEST_REPORT,),
        fact_kind="observed",
        remediation_template="Review flaky tests before they erode CI trust.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.high_churn.v1",
        owner="calibration-inventory",
        applicability=(RuleApplicability.PROJECT, RuleApplicability.TEAM),
        dimension=HealthDimension.CODE_OWNERSHIP_RISK,
        required_source_classes=(SourceClass.CODE_CHANGE,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=0.30,
        comparison_unit="rework_churn_ratio_30d",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=30,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=False,
        minimum_cohort_size=1,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.CODE_CHANGE,),
        fact_kind="observed",
        remediation_template="Review high-churn areas of the codebase for ownership risk.",
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    # -- CHAOS-3304: team workload pressure / investment-balance rules -----
    # Team-only (Wave 3.1's workload/investment analysis is a team-level
    # question family, per PRD 6.6); provisional/shadow-only like every
    # other rule in this file -- none of these have been through the
    # CHAOS-3302 calibration review either.
    _rule(
        rule_id="health_rule.after_hours_pressure_sustained.v1",
        owner="ask-dev-governance",
        applicability=(RuleApplicability.TEAM,),
        dimension=HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,
        required_source_classes=(SourceClass.COGNITIVE_LOAD,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=0.25,
        comparison_unit="after_hours_commit_ratio",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=14,
        sustained_periods_required=1,
        denominator_required=False,
        attribution_required=True,
        minimum_cohort_size=5,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.COGNITIVE_LOAD,),
        fact_kind="observed",
        remediation_template=(
            "Review recent after-hours commit activity with the team as a "
            "pressure signal -- not a commitment or performance judgment."
        ),
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.review_request_load_pressure.v1",
        owner="ask-dev-governance",
        applicability=(RuleApplicability.TEAM,),
        dimension=HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,
        required_source_classes=(SourceClass.COGNITIVE_LOAD,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=5.0,
        comparison_unit="review_request_load_per_active_contributor",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=14,
        sustained_periods_required=1,
        denominator_required=True,
        attribution_required=True,
        minimum_cohort_size=5,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.COGNITIVE_LOAD,),
        fact_kind="observed",
        remediation_template=(
            "Review distribution of incoming review requests before it "
            "affects the team's delivery pace."
        ),
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.pr_interruption_load_pressure.v1",
        owner="ask-dev-governance",
        applicability=(RuleApplicability.TEAM,),
        dimension=HealthDimension.COGNITIVE_WORKLOAD_PRESSURE,
        required_source_classes=(SourceClass.COGNITIVE_LOAD,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=5.0,
        comparison_unit="pr_interruption_load_per_active_contributor",
        minimum_sample=1,
        minimum_coverage=0.0,
        current_window_days=14,
        sustained_periods_required=1,
        denominator_required=True,
        attribution_required=True,
        minimum_cohort_size=5,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.COGNITIVE_LOAD,),
        fact_kind="observed",
        remediation_template=(
            "Review context-switching load from pull request interruptions "
            "before it affects the team's delivery pace."
        ),
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
    _rule(
        rule_id="health_rule.investment_allocation_shift.v1",
        owner="ask-dev-governance",
        applicability=(RuleApplicability.TEAM,),
        dimension=HealthDimension.INVESTMENT_BALANCE,
        required_source_classes=(SourceClass.INVESTMENT_ALLOCATION,),
        direction=RuleDirection.HIGHER_IS_WORSE,
        threshold=0.25,
        comparison_unit="new_value_share_shift",
        minimum_sample=1,
        minimum_coverage=0.5,
        current_window_days=14,
        comparison_window_days=14,
        sustained_periods_required=1,
        denominator_required=True,
        attribution_required=True,
        minimum_cohort_size=5,
        triggered_state=DimensionState.WATCH,
        evidence_source_classes=(SourceClass.INVESTMENT_ALLOCATION,),
        fact_kind="observed",
        remediation_template=(
            "Review the team's investment mix shift with planning -- a "
            "large swing in either direction, not a value judgment about "
            "the mix itself."
        ),
        calibration_state=CalibrationState.PROVISIONAL,
        calibration_evidence_ref=None,
    ),
)

HEALTH_RULE_REGISTRY = HealthRuleRegistry(
    _LAUNCH_RULES,
    calibration_records={
        record.calibration_id: record for record in CALIBRATION_RECORDS
    },
)

# ---------------------------------------------------------------------------
# CHAOS-3331 promotion guard (disclose-and-defer ruling, 2026-08-02): the
# three cognitive-load-sourced CHAOS-3304 rules cannot be promoted out of
# provisional -- their sole source table's team_id comes from a legacy
# resolver, never canonical primary attribution (see
# native_team_workload.py's module docstring and
# health_rule_calibration_inventory.CHAOS_3331_ATTRIBUTION_BLOCKED_RULE_IDS).
# A future editor who flips ONE rule's calibration_state to promote it
# (e.g. product_approved, with a real CalibrationRecord to back it) breaks
# this import loudly, everywhere, the instant the module loads -- not only
# when a specific test happens to run. Mirrors this module's own
# _undocumented_rule_ids totality-check pattern above (health_profile_
# synthesis.py) and CHAOS-3302's test_no_shipped_rule_is_launch_authorized.
# ---------------------------------------------------------------------------
_chaos_3331_promoted_rule_ids = sorted(
    rule_id
    for rule_id in CHAOS_3331_ATTRIBUTION_BLOCKED_RULE_IDS
    if HEALTH_RULE_REGISTRY.rule(rule_id).calibration_state
    != CalibrationState.PROVISIONAL
)
if _chaos_3331_promoted_rule_ids:
    raise RuntimeError(
        "CHAOS-3331 blocks promotion of the following rule(s) out of "
        f"provisional: {_chaos_3331_promoted_rule_ids} -- their source "
        "table's team_id is not canonically attributed yet (see "
        "native_team_workload.py's module docstring). Revert "
        "calibration_state to provisional, or close CHAOS-3331 first and "
        "remove the affected rule id(s) from "
        "health_rule_calibration_inventory.CHAOS_3331_ATTRIBUTION_BLOCKED_RULE_IDS."
    )
