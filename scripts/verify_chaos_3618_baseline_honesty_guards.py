#!/usr/bin/env python3
"""Fault injection proof: every CHAOS-3618 honesty guard is load-bearing.

``tests/context_fabric/test_chaos_3618_*.py`` and
``tests/api/dev/test_chaos_3618_investigation_shadow.py`` prove that an
honest baseline is what the code produces today. That, on its own, is not
proof that the *named guard* is what keeps it honest — a check could be
passing incidentally, or be satisfied by an unrelated constraint, and the
suite would stay green with the real guard gone. This script closes that
gap the only way it can be closed: it **removes one guard at a time and
watches the dishonest baseline become expressible.**

For each case it runs one subprocess that:

1. runs the named test against the pristine tree and requires it to PASS
   (baseline — a case whose test already fails proves nothing);
2. neutralises exactly one guard via an in-process patch injected through a
   generated ``sitecustomize.py`` on ``PYTHONPATH``;
3. runs the same test again and requires it to FAIL.

A guard whose removal changes nothing is not the guard the docs claim it
is, and this script says so by name.

Subprocess isolation is not decoration. Several plants mutate module-level
tables that other tests read, and a few of the guards are import-time
checks that only run once per interpreter — patching in-process would both
corrupt the session and miss the exact class of defect those guards catch.
No on-disk source file is ever mutated; the generated ``sitecustomize``
lives in a temporary directory and is gone when the run ends.

Run it directly::

    .venv/bin/python scripts/verify_chaos_3618_baseline_honesty_guards.py

Exit code 0 means every named guard was observed failing. Any other exit
code names the case that did not behave as claimed.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import NamedTuple

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

#: Distinct exit code meaning "the plant itself could not run". Anything
#: else would be indistinguishable from the guard failing.
_PLANT_FAILED_EXIT = 97


class GuardCase(NamedTuple):
    """One honesty guard, and the single plant claimed to defeat it."""

    #: Short id, used in output and in the ``--only`` filter.
    case_id: str
    #: What a reader loses if this guard stops working.
    stake: str
    #: ``pytest`` node id (may include a ``-k`` style parametrisation).
    test: str
    #: The text the NAMED assertion must produce when the guard is removed.
    #: Checked because an exit code alone cannot tell "the guard I removed
    #: fired" from "something else broke". The CHAOS-3618 codex review found
    #: exactly that: two plants were reported KILLED while the failure came
    #: from the contract validator downstream and the projection's own
    #: assertion was never reached.
    expected_failure: str
    #: Python source written to a generated ``sitecustomize.py``. Runs at
    #: interpreter startup, before pytest collects anything.
    plant: str
    #: Text that means the WRONG thing failed. A producer-side guard proved
    #: by a contract rejection is proving somebody else's invariant.
    #:
    #: **Mandatory behind a total-containment boundary.** ``expected_failure``
    #: alone is blind there, and the independent verifier proved it: case 12's
    #: plant declared the pre-H4 signature, so production died on
    #: ``TypeError``, the shadow seam's own exception containment converted
    #: that into a ``SEAM_FAULT`` record, the test failed for that reason, and
    #: the harness credited a guard it had never exercised. Anything wrapped
    #: in ``try/except`` can turn a broken plant into a plausible failure, so
    #: every case planting into such a boundary must name the shapes that
    #: mean "the plant broke" rather than "the guard fired".
    forbidden_failure: tuple[str, ...] = ()


CASES: tuple[GuardCase, ...] = (
    GuardCase(
        case_id="overclaimed_relationship",
        stake=(
            "a relationship marked available whose endpoint kinds the native "
            "path cannot express -- inflated lineage recall"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_available_relationship_has_an_expressible_endpoint_pair"
        ),
        expected_failure="GUARD available_needs_expressible_endpoints",
        plant="""
import dataclasses
from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import RelationshipType
from dev_health_ops.context_fabric.native_arm import capabilities as caps

table = dict(caps.NATIVE_RELATIONSHIP_CAPABILITY)
# Every OTHER field is legal -- STATUS_CHANGE really does mint the
# `deployments` slot under the `linked_deployment` token -- so the ONLY
# thing wrong with this row is that `deploys` terminates on a `service`
# the native path cannot name. That isolates the endpoint-pair guard.
table[RelationshipType.DEPLOYS] = dataclasses.replace(
    table[RelationshipType.DEPLOYS],
    state=caps.NativeRelationshipState.AVAILABLE,
    content_slot="deployments",
    source_class=SourceClass.STATUS_CHANGE,
    native_token="linked_deployment",
    gap_mechanism=None,
)
caps.NATIVE_RELATIONSHIP_CAPABILITY = table
""",
    ),
    GuardCase(
        case_id="misattributed_gap_mechanism",
        stake=(
            "a gap blamed on an absent subject kind when the real blocker is a "
            "missing adapter -- the trial report recommends the wrong fix"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_subject_kind_absent_is_only_blamed_when_it_is_actually_the_blocker"
        ),
        expected_failure="GUARD subject_kind_absent_must_be_the_real_blocker",
        plant="""
import dataclasses
from dev_health_ops.api.dev.investigation_contract import RelationshipType
from dev_health_ops.context_fabric.native_arm import capabilities as caps

table = dict(caps.NATIVE_RELATIONSHIP_CAPABILITY)
table[RelationshipType.DEPENDS_ON] = dataclasses.replace(
    table[RelationshipType.DEPENDS_ON],
    gap_mechanism=caps.NativeGapMechanism.SUBJECT_KIND_ABSENT,
)
caps.NATIVE_RELATIONSHIP_CAPABILITY = table
""",
    ),
    GuardCase(
        case_id="capability_drift_from_the_matrix",
        stake=(
            "a relationship marked available from a source class no adapter "
            "mints -- the single judgment table drifting from the vocabulary "
            "it claims to follow"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_every_available_row_is_backed_by_the_landed_matrix"
        ),
        expected_failure="assert 'not_applicable' != 'not_applicable'",
        plant="""
import dataclasses
from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import RelationshipType
from dev_health_ops.context_fabric.native_arm import capabilities as caps

table = dict(caps.NATIVE_RELATIONSHIP_CAPABILITY)
table[RelationshipType.REVIEWS] = dataclasses.replace(
    table[RelationshipType.REVIEWS],
    state=caps.NativeRelationshipState.AVAILABLE,
    content_slot="pull_requests",
    source_class=SourceClass.REVIEW,
    native_token="reviews",
    gap_mechanism=None,
)
caps.NATIVE_RELATIONSHIP_CAPABILITY = table
""",
    ),
    GuardCase(
        case_id="dimension_without_measured_content",
        stake=(
            "a cohort claiming a comparison dimension no query produced -- "
            "review comparability from an observation carrying nothing"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_a_dimension_needs_content_that_actually_measured_it"
        ),
        expected_failure="GUARD dimension_needs_measured_content",
        plant="""
from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import ComparisonDimension
from dev_health_ops.context_fabric.native_arm import projection as proj

_BY_CLASS = {
    SourceClass.DEFICIENCY_INVENTORY: ComparisonDimension.OPEN_DEFICIENCY_COUNT,
    SourceClass.STATUS_CHANGE: ComparisonDimension.STATUS_DECLARATION_GAP,
    SourceClass.SOURCE_HEALTH: ComparisonDimension.DATA_COVERAGE,
    SourceClass.INCIDENT: ComparisonDimension.INCIDENT_LOAD,
    SourceClass.DEPLOYMENT: ComparisonDimension.DEPLOYMENT_FREQUENCY,
    SourceClass.PULL_REQUEST: ComparisonDimension.REVIEW_LOAD,
}


def _supported_dimensions(payload):
    # The pre-fix logic: keyed on the source-class label, blind to content.
    if payload.investigation_result is None:
        return ()
    found = {
        _BY_CLASS[o.source_class]
        for o in payload.investigation_result.observations
        if o.source_class in _BY_CLASS
    }
    return tuple(sorted(found, key=lambda item: item.value))


proj._supported_dimensions = _supported_dimensions
""",
    ),
    GuardCase(
        case_id="corpus_tunable_classifier",
        stake=(
            "a family classifier that can see the question text, which is the "
            "first step of tuning the baseline to the trial corpus"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_classify_question_family_cannot_see_the_question"
        ),
        expected_failure="GUARD classifier_cannot_see_the_question",
        plant="""
from dev_health_ops.context_fabric.native_arm import capabilities as caps

_real = caps.classify_question_family


def classify_question_family(*, intent_id, shape, question=None):
    return _real(intent_id=intent_id, shape=shape)


caps.classify_question_family = classify_question_family
""",
    ),
    GuardCase(
        case_id="unsafe_organization_widening",
        stake=(
            "an unresolved named reference widening into portfolio scope, which "
            "answers a question nobody asked"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_capabilities.py::"
            "test_an_unresolved_reference_is_what_separates_widening_from_portfolio_scope"
        ),
        expected_failure="GUARD widening_is_distinguished_from_portfolio_scope",
        plant="""
from dev_health_ops.api.dev.contracts_v2.base import Cardinality
from dev_health_ops.api.dev.investigation_contract import ComparisonShape
from dev_health_ops.context_fabric.native_arm import capabilities as caps


def comparison_shape_for(*, cardinality, has_unresolved_mentions):
    if cardinality is Cardinality.SINGULAR:
        return ComparisonShape.SINGULAR_SUBJECT
    if cardinality is Cardinality.PLURAL_COHORT:
        return ComparisonShape.EXPLICIT_COHORT
    return ComparisonShape.PORTFOLIO_WIDE


caps.comparison_shape_for = comparison_shape_for
""",
    ),
    GuardCase(
        case_id="unauthorized_evidence_admitted",
        stake=(
            "evidence naming an entity outside the authorized set reaching the "
            "packet's evidence index"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_evidence_outside_the_authorized_set_is_dropped_not_admitted"
        ),
        expected_failure="GUARD unauthorized_evidence_is_dropped",
        plant="""
from dev_health_ops.context_fabric.native_arm import projection as proj


def _restrict_evidence_to_authorized(entries, *, authorized):
    return entries, 0


proj._restrict_evidence_to_authorized = _restrict_evidence_to_authorized
""",
    ),
    GuardCase(
        case_id="driver_asserted_without_lineage",
        stake=(
            "a driver asserted for a family whose related-context section the "
            "native arm cannot fill -- an unsupported attribution"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_a_lineage_requiring_family_forbids_assertion"
        ),
        expected_failure="GUARD lineage_family_forbids_assertion",
        forbidden_failure=("ValidationError",),
        plant="""
from dev_health_ops.context_fabric.native_arm import projection as proj


def _may_assert(*, family, discovery):
    return True


proj._may_assert = _may_assert
""",
    ),
    GuardCase(
        case_id="unresolved_mention_asserted_anyway",
        stake=(
            "a driver asserted for a question whose subject never resolved -- "
            "explaining why a thing is in trouble without knowing what it is"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_an_unresolved_mention_forbids_assertion_even_without_lineage_need"
        ),
        expected_failure="GUARD unresolved_mention_forbids_assertion",
        forbidden_failure=("ValidationError",),
        plant="""
from dev_health_ops.api.dev.investigation_contract import QUESTION_FAMILY_REGISTRY, PacketSection
from dev_health_ops.context_fabric.native_arm import projection as proj


def _may_assert(*, family, discovery):
    # The pre-fix logic: family only, blind to unresolved mentions.
    required = QUESTION_FAMILY_REGISTRY[family].required_packet_sections
    return PacketSection.RELATED_CONTEXT not in required


proj._may_assert = _may_assert
""",
    ),
    GuardCase(
        case_id="driver_on_unindexed_evidence",
        stake=(
            "a driver resting on an evidence handle nobody can dereference -- "
            "an attribution with no closure"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_driver_analysis_drops_evidence_handles_the_packet_never_indexed"
        ),
        expected_failure="GUARD unindexed_handle_never_supports_a_driver",
        forbidden_failure=("ValidationError",),
        plant="""
from dev_health_ops.context_fabric.native_arm import projection as proj

_real = proj._driver_analysis


def _driver_analysis(payload, *, subject_ids, evidence_handles, may_assert, limitations):
    # Treat every cited handle as indexed -- the filter removed.
    everything = frozenset(
        handle
        for observation in (
            payload.investigation_result.observations
            if payload.investigation_result
            else ()
        )
        for finding in (
            observation.content.deficiency_findings if observation.content else ()
        )
        for handle in finding.evidence_ref_ids
    )
    return _real(
        payload,
        subject_ids=subject_ids,
        evidence_handles=evidence_handles | everything,
        may_assert=may_assert,
        limitations=limitations,
    )


proj._driver_analysis = _driver_analysis
""",
    ),
    GuardCase(
        case_id="non_total_projection",
        stake=(
            "a projection that raises instead of returning -- the shadow seam's "
            "containment would record the arm's failure as a seam fault"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_an_empty_organization_wide_cohort_is_a_named_gap_not_a_crash"
        ),
        expected_failure="RuntimeError",
        plant="""
from dev_health_ops.context_fabric.native_arm import projection as proj

_real = proj.project_native_investigation


def project_native_investigation(payload):
    # Remove the totality wrapper: let _project's exceptions escape.
    return proj._project(payload)


proj.project_native_investigation = project_native_investigation
_orig_project = proj._project


def _project(payload):
    raise RuntimeError("planted non-total projection")


proj._project = _project
""",
    ),
    GuardCase(
        case_id="canonical_bypass_allowed",
        stake=(
            "a packet citing evidence no canonical service minted -- the exact "
            "shape a fabricated measurement arrives in"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_a_packet_citing_uncoined_evidence_is_rejected"
        ),
        expected_failure="GUARD uncoined_evidence_is_rejected",
        forbidden_failure=("TypeError",),
        plant="""
from dev_health_ops.api.dev import investigation_shadow as seam


def canonical_bypass_offenders(*, packet_evidence, canonical_evidence):
    # Current signature, guard neutralized. Declaring the PRE-H4 signature
    # here made production die on TypeError, which the seam's containment
    # converted into a SEAM_FAULT -- the test failed and the harness credited
    # a guard it had never exercised.
    return ()


seam.canonical_bypass_offenders = canonical_bypass_offenders
""",
    ),
    GuardCase(
        case_id="missing_result_still_projected",
        stake=(
            "a run that never reached the plan executor emitting a scorable "
            "packet instead of being counted as a projection gap"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_a_run_that_never_reached_the_plan_executor_is_a_gap_not_a_thin_packet"
        ),
        expected_failure="GUARD missing_result_is_a_gap",
        plant="""
from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.api.dev.investigation_contract import (
    InvestigationVersions,
    SourceContractVersion,
    TrialMetadata,
)
from dev_health_ops.context_fabric.native_arm import projection as proj


def _missing_result_gap(payload):
    # The guard removed: a run with no governed result proceeds.
    return None


proj._missing_result_gap = _missing_result_gap


def _versions(payload):
    # ...and the fabricated fallback provenance restored, which is how a
    # never-executed run used to acquire plausible-looking source versions.
    return InvestigationVersions(
        schema_version="ask_dev_investigation_versions.v1",
        packet_schema_version="ask_dev_investigation_packet.v1",
        query_version="ask_dev_native_queries.v1",
        ranking_version="ask_dev_native_ranking.v1",
        projection_version=proj.NATIVE_PROJECTION_VERSION,
        source_contract_versions=(
            SourceContractVersion(
                source_class=SourceClass.WORK_GRAPH,
                contract_version="ask_dev_native_queries.v1",
            ),
        ),
        trial=TrialMetadata(
            arm_id=proj.NATIVE_ARM_ID,
            producer_id=proj.NATIVE_PROJECTION_VERSION,
            run_id=payload.run_id,
        ),
    )


proj._versions = _versions
""",
    ),
    GuardCase(
        case_id="forged_evidence_payload_accepted",
        stake=(
            "a cited evidence record whose payload differs from what a "
            "canonical service minted -- a forgery behind a genuine handle"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_a_forged_evidence_payload_is_rejected_even_with_a_genuine_handle"
        ),
        expected_failure="GUARD forged_evidence_payload_is_rejected",
        forbidden_failure=("TypeError",),
        plant="""
from dev_health_ops.api.dev import investigation_shadow as seam


def canonical_bypass_offenders(*, packet_evidence, canonical_evidence):
    # The pre-fix logic: compare handles, ignore the record itself.
    minted = {ref.evidence_ref_id for ref in canonical_evidence}
    return tuple(
        sorted(
            {
                ref.evidence_ref_id
                for ref in packet_evidence
                if ref.evidence_ref_id not in minted
            }
        )
    )


seam.canonical_bypass_offenders = canonical_bypass_offenders
""",
    ),
    GuardCase(
        case_id="disabled_seam_still_records",
        stake=(
            "a seam whose off switch does nothing -- the advertised flag "
            "would have no runtime effect at all"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_a_disabled_seam_records_that_it_chose_to_do_nothing"
        ),
        expected_failure="GUARD disabled_seam_does_not_record_a_packet",
        forbidden_failure=("TypeError",),
        plant="""
from dev_health_ops.api.dev import investigation_shadow as seam

_real = seam.InvestigationShadow.__init__


def __init__(self, *, enabled):
    # The pre-fix state: the flag is stored and never consulted.
    _real(self, enabled=True)


seam.InvestigationShadow.__init__ = __init__
""",
    ),
    GuardCase(
        case_id="seam_branches_by_arm",
        stake=(
            "a seam that treats one arm differently from the other -- the "
            "comparison stops measuring the arms and starts measuring the seam"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_the_seam_behaves_identically_for_both_arms"
        ),
        expected_failure="GUARD seam_treats_both_arms_alike",
        forbidden_failure=("TypeError",),
        plant="""
import dataclasses

from dev_health_ops.api.dev import investigation_shadow as seam

_real = seam.InvestigationShadow.evaluate


def evaluate(self, *, payload, run_id, organization_id, canonical_evidence):
    record = _real(
        self,
        payload=payload,
        run_id=run_id,
        organization_id=organization_id,
        canonical_evidence=canonical_evidence,
    )
    # The exact violation the structural checks cannot see: a branch on the
    # recorded arm identity, with no arm module imported anywhere.
    if record.arm_id == "graph":
        return dataclasses.replace(record, frame_facts=())
    return record


seam.InvestigationShadow.evaluate = evaluate
""",
    ),
    GuardCase(
        case_id="packet_from_another_run_recorded",
        stake=(
            "a stale or misrouted packet filed under the run evaluating it, "
            "becoming a comparison row for work it never described"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_a_packet_from_another_run_is_rejected"
        ),
        expected_failure="GUARD packet_from_another_run_is_rejected",
        forbidden_failure=("TypeError",),
        plant="""
from dev_health_ops.api.dev import investigation_shadow as seam

_real = seam.InvestigationShadow.evaluate


def evaluate(self, *, payload, run_id, organization_id, canonical_evidence):
    # The pre-fix state: evaluate against the packet's own run, so the
    # mismatch branch can never fire.
    versions = payload.get("versions") if hasattr(payload, "get") else None
    trial = versions.get("trial") if isinstance(versions, dict) else None
    own = trial.get("run_id") if isinstance(trial, dict) else None
    return _real(
        self,
        payload=payload,
        run_id=own or run_id,
        organization_id=organization_id,
        canonical_evidence=canonical_evidence,
    )


seam.InvestigationShadow.evaluate = evaluate
""",
    ),
    GuardCase(
        case_id="unmeasured_source_health_credits_coverage",
        stake=(
            "an unavailable source with zero coverage crediting a cohort "
            "with data-coverage comparability"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_an_unavailable_source_health_observation_measures_no_coverage"
        ),
        expected_failure="GUARD unmeasured_source_health_credits_no_coverage",
        plant="""
from dev_health_ops.context_fabric.native_arm import projection as proj

# The pre-fix set: only not_measured counted as unmeasured.
proj._UNMEASURED_SEMANTICS = frozenset({"not_measured"})
""",
    ),
    GuardCase(
        case_id="unknown_source_system_guessed",
        stake=(
            "an evidence ref from an unknown source system assigned a "
            "plausible class -- smuggling a source past the allowlist"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_the_evidence_source_class_map_returns_none_rather_than_guessing"
        ),
        expected_failure="GUARD unknown_source_system_is_not_guessed",
        plant="""
from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.native_arm import projection as proj

_real = proj._evidence_source_class


def _evidence_source_class(ref):
    # Guess instead of refusing -- the smuggling the docstring names.
    return _real(ref) or SourceClass.WORK_GRAPH


proj._evidence_source_class = _evidence_source_class
""",
    ),
    GuardCase(
        case_id="trial_allowlist_widened",
        stake=(
            "an off-allowlist source class reaching the packet, where the "
            "contract's own allowlist check should have rejected it"
        ),
        test=(
            "tests/context_fabric/test_chaos_3618_projection.py::"
            "test_the_trial_allowlist_excludes_classes_the_contract_never_allowed"
        ),
        expected_failure="GUARD trial_allowlist_matches_the_contract",
        plant="""
from dev_health_ops.api.dev.contracts_v2.base import SourceClass
from dev_health_ops.context_fabric.native_arm import projection as proj

proj._TRIAL_ALLOWLIST = frozenset(SourceClass)
""",
    ),
    GuardCase(
        case_id="canonical_evidence_taken_from_the_packet",
        stake=(
            "the seam handed the arm's OWN packet evidence as canonical, so "
            "the digest compares a value to itself -- every H4 guarantee "
            "becomes a check that cannot fail while looking like one"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_shadow_wiring.py::"
            "test_canonical_evidence_comes_from_the_run_not_the_packet"
        ),
        expected_failure="GUARD canonical_evidence_is_the_frames_own",
        forbidden_failure=("TypeError",),
        plant="""
from dev_health_ops.api.dev import investigation_shadow as seam
from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from dev_health_ops.api.dev.orchestrator import DevOrchestrator

_real = DevOrchestrator._run_investigation_shadow


async def _run_investigation_shadow(
    self, *, run_id, org_id, frame, investigation_result, preflight_result
):
    shadow = self._investigation_shadow
    producer = self._investigation_packet_producer
    if shadow is None or producer is None or not shadow.enabled:
        return None
    context = seam.FinishedRunContext(
        run_id=run_id,
        organization_id=org_id,
        frame=frame,
        investigation_result=investigation_result,
        interpretation=None,
        ledger=None,
        subject_set=None,
        committed_subject=None,
        window_start=None,
        window_end=None,
        canonical_evidence=(),
    )
    payload = producer.build_packet(context)
    if payload is None:
        return None
    # The defect: canonical evidence sourced FROM THE PACKET.
    cited = tuple(
        DevEvidenceRefV2.model_validate(entry["evidence"])
        for entry in payload["evidence_coverage"]["evidence_index"]
    )
    record = shadow.evaluate(
        payload=payload,
        run_id=run_id,
        organization_id=org_id,
        canonical_evidence=cited,
    )
    await self._recorder.record_investigation_shadow(record)
    return None


DevOrchestrator._run_investigation_shadow = _run_investigation_shadow
""",
    ),
    GuardCase(
        case_id="shadow_fault_reaches_the_run",
        stake=(
            "a failing arm failing the run it shadows -- the single thing a "
            "shadow seam must never do"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_shadow_wiring.py::"
            "test_an_always_raising_producer_leaves_the_run_byte_identical"
        ),
        expected_failure="RuntimeError",
        plant="""
from dev_health_ops.api.dev.orchestrator import DevOrchestrator


async def _run_investigation_shadow(
    self, *, run_id, org_id, frame, investigation_result, preflight_result
):
    # Containment removed: the producer's exception escapes into the run.
    shadow = self._investigation_shadow
    producer = self._investigation_packet_producer
    if shadow is None or producer is None or not shadow.enabled:
        return None
    producer.build_packet(None)
    return None


DevOrchestrator._run_investigation_shadow = _run_investigation_shadow
""",
    ),
    GuardCase(
        case_id="unattributed_comparison_record",
        stake=(
            "a persisted comparison record that cannot say which arm produced "
            "its packet -- the differential becomes unreadable"
        ),
        test=(
            "tests/api/dev/test_chaos_3618_investigation_shadow.py::"
            "test_a_recorded_evaluation_without_an_arm_is_unconstructable"
        ),
        expected_failure="DID NOT RAISE",
        plant="""
from dev_health_ops.api.dev import investigation_shadow as seam


def _post_init(self):
    return None


seam.InvestigationShadowRecord.__post_init__ = _post_init
""",
    ),
)


def _run_pytest(
    test: str, *, plant_dir: Path | None
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        "-m",
        "pytest",
        test,
        "-q",
        "-p",
        "no:randomly",
        "--no-header",
    ]
    env_overlay = {}
    if plant_dir is not None:
        # sitecustomize is imported during interpreter startup, before pytest
        # collects anything -- which is what lets a plant land before the
        # module under test binds any name it reads at import time.
        env_overlay["PYTHONPATH"] = str(plant_dir)
    return subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        env={**_base_env(), **env_overlay},
    )


def _base_env() -> dict[str, str]:
    import os

    env = dict(os.environ)
    # Keep the plants deterministic and the output quiet.
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    return env


def _verify(case: GuardCase, *, verbose: bool) -> str | None:
    """Return an error string, or ``None`` when the guard behaved as claimed."""

    baseline = _run_pytest(case.test, plant_dir=None)
    if baseline.returncode != 0:
        return (
            f"{case.case_id}: the named test does not pass against the pristine "
            f"tree, so the plant would prove nothing\n{baseline.stdout[-2000:]}"
        )

    workspace = Path(tempfile.mkdtemp(prefix=f"chaos3618-{case.case_id}-"))
    try:
        # The plant must land before the test module imports anything. A
        # conftest.py beside the tests would do it, but writing into the repo
        # is exactly what this script promises not to do -- so the plant is
        # installed as a sitecustomize on PYTHONPATH instead, which Python
        # imports during interpreter startup, before pytest collects anything.
        # sitecustomize swallows exceptions by design, so a plant with a
        # bad import silently becomes a no-op and the case reports "not
        # load-bearing" -- blaming the guard for the harness's mistake.
        # This session hit exactly that. Wrap it so a broken plant exits
        # with a distinct code instead.
        (workspace / "sitecustomize.py").write_text(
            "try:\n"
            + "".join(f"    {line}\n" for line in case.plant.splitlines())
            + "except BaseException as _plant_exc:\n"
            "    import os, sys\n"
            "    sys.stderr.write(\n"
            "        f'PLANT-FAILED: {type(_plant_exc).__name__}: {_plant_exc}\\n'\n"
            "    )\n"
            f"    os._exit({_PLANT_FAILED_EXIT})\n"
        )
        planted = _run_pytest(case.test, plant_dir=workspace)
    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    output = planted.stdout + planted.stderr
    if planted.returncode == _PLANT_FAILED_EXIT or "PLANT-FAILED" in output:
        return (
            f"{case.case_id}: the PLANT could not run, so this case proved "
            "nothing about the guard.\n"
            f"  {[line for line in output.splitlines() if 'PLANT-FAILED' in line]}"
        )
    if planted.returncode == 0:
        return (
            f"{case.case_id}: removing the guard changed nothing -- the test "
            "still passes, so it is not the guard that keeps this honest\n"
            f"  stake: {case.stake}"
        )
    # An exit code alone cannot distinguish "the guard I removed fired" from
    # "something else broke". This is the exact hole the CHAOS-3618 codex
    # review found: two plants were reported KILLED while the failure came
    # from the contract validator downstream, so the projection's own
    # assertion was never reached and was never proven load-bearing.
    for forbidden in case.forbidden_failure:
        if forbidden in output:
            return (
                f"{case.case_id}: the test failed, but on {forbidden!r} -- not "
                f"the named assertion {case.expected_failure!r}. A "
                "producer-side guard proved by a downstream rejection is "
                "proving somebody else's invariant.\n"
                f"  stake: {case.stake}\n"
                f"  output tail: {output[-800:]}"
            )
    if case.expected_failure not in output:
        return (
            f"{case.case_id}: the test failed, but {case.expected_failure!r} "
            "never appeared in the output, so the failure is not the one this "
            "case claims to prove.\n"
            f"  stake: {case.stake}\n"
            f"  output tail: {output[-800:]}"
        )
    if verbose:
        print(output[-1500:])
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", help="run a single case by id")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    cases = CASES
    if args.only:
        cases = tuple(case for case in CASES if case.case_id == args.only)
        if not cases:
            print(f"no such case: {args.only}", file=sys.stderr)
            print(f"known: {[case.case_id for case in CASES]}", file=sys.stderr)
            return 2

    failures: list[str] = []
    for index, case in enumerate(cases, start=1):
        print(f"[{index}/{len(cases)}] {case.case_id} ... ", end="", flush=True)
        error = _verify(case, verbose=args.verbose)
        if error is None:
            print("guard observed failing")
        else:
            print("NOT LOAD-BEARING")
            failures.append(error)

    print()
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
            print(file=sys.stderr)
        print(
            f"{len(failures)} of {len(cases)} guards are not load-bearing.",
            file=sys.stderr,
        )
        return 1
    print(f"All {len(cases)} guards observed failing when removed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
