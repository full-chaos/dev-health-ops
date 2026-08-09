"""CHAOS-3618: the shadow seam must be inert, isolated and arm-neutral.

The three properties this file exists to prove, in the order they matter:

1. **Isolation.** A shadow that can fail a run it shadows is worse than no
   shadow. Every fault path returns a record instead of raising.
2. **Canonical authority.** A packet may reference what canonical services
   measured; it may not introduce measurements of its own. The negative
   test plants exactly the shape a fabricated measurement arrives in — a
   citation to a handle no canonical service minted — and requires
   rejection.
3. **Arm attribution.** A comparison record that cannot say which arm
   produced its packet is not comparable, so the seam refuses to write one.

The seam is deliberately given the *native* arm's packet here, but nothing
in the module under test can tell which arm it is holding: it reads
``versions.trial.arm_id`` and records it. That is asserted directly rather
than assumed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from dev_health_ops.api.dev import investigation_shadow as seam
from dev_health_ops.api.dev.contracts import (
    DevEvidenceFlags,
    DevEvidenceRef,
    FreshnessState,
)
from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from dev_health_ops.api.dev.investigation_contract.fixtures import (
    positive_variant_fixtures,
)

_RUN = "run-3618"
_OBSERVED_AT = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)


def _packet_payload() -> dict[str, Any]:
    """The contract's own positive golden — not a hand-rolled payload.

    Using the frozen golden means this suite exercises the same bytes the
    contract's parity tests do, so a contract change that would break a
    real producer breaks here too.
    """

    variants = dict(positive_variant_fixtures()["ask_dev_investigation_packet.v1"])
    payload = variants["trial_metadata_present"]
    assert isinstance(payload, dict)
    return dict(payload)


def _handles(payload: dict[str, Any]) -> list[str]:
    index = payload["evidence_coverage"]["evidence_index"]
    return [entry["evidence"]["evidence_ref_id"] for entry in index]


def _canonical(payload: dict[str, Any]) -> list[DevEvidenceRefV2]:
    """Canonical evidence covering exactly what the golden packet cites."""

    return [
        DevEvidenceRefV2.model_validate(
            DevEvidenceRef(
                schema_version="dev_evidence_ref.v1",
                evidence_ref_id=handle,
                source_system="work_items",
                source_version="status.entity.v2",
                entity_type="project",
                entity_id="proj-1",
                display_label="canonical",
                observed_at=_OBSERVED_AT,
                freshness=FreshnessState.FRESH,
                provenance="persisted",
                confidence=1.0,
                flags=DevEvidenceFlags(),
            ).model_dump()
        )
        for handle in _handles(payload)
    ]


# --------------------------------------------------------------------------
# Flag
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [("1", True), ("0", False), ("true", False), ("", False)],
)
def test_only_an_explicit_one_switches_the_seam_on(value: str, expected: bool) -> None:
    """ "Unset" is off, and so is every value that merely looks truthy."""

    assert seam.shadow_enabled({seam.INVESTIGATION_SHADOW_FLAG: value}) is expected


def test_an_absent_flag_is_off() -> None:
    assert seam.shadow_enabled({}) is False


# --------------------------------------------------------------------------
# Isolation — the seam cannot fail the run it shadows
# --------------------------------------------------------------------------


def test_an_invalid_packet_is_recorded_not_raised() -> None:
    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload={"schema_version": "ask_dev_investigation_packet.v1"},
        run_id=_RUN,
        canonical_evidence=[],
    )
    assert record.status is seam.InvestigationShadowStatus.PACKET_INVALID
    assert record.detail is not None


def test_a_hostile_payload_is_recorded_not_raised() -> None:
    """Nothing about an arbitrary mapping's shape may reach the caller."""

    for payload in ({}, {"versions": "not-a-mapping"}, {"versions": {"trial": 7}}):
        record = seam.InvestigationShadow(enabled=True).evaluate(
            payload=payload,
            run_id=_RUN,
            canonical_evidence=[],
        )
        assert record.status is seam.InvestigationShadowStatus.PACKET_INVALID


def test_a_faulting_canonical_evidence_sequence_is_contained() -> None:
    """A bug in the caller's own material must not escape the seam either."""

    class Exploding(list[DevEvidenceRefV2]):
        def __iter__(self) -> Any:
            raise RuntimeError("canonical evidence blew up")

    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload=_packet_payload(),
        run_id=_RUN,
        canonical_evidence=Exploding(),
    )
    assert record.status is seam.InvestigationShadowStatus.SEAM_FAULT
    assert record.detail == "RuntimeError"


def test_an_invalid_packet_still_records_the_arm_that_sent_it() -> None:
    """A trial that cannot attribute its rejections learns nothing from them."""

    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload={"versions": {"trial": {"arm_id": "graph"}}},
        run_id=_RUN,
        canonical_evidence=[],
    )
    assert record.status is seam.InvestigationShadowStatus.PACKET_INVALID
    assert record.arm_id == "graph"


# --------------------------------------------------------------------------
# Canonical authority
# --------------------------------------------------------------------------


def test_a_packet_citing_uncoined_evidence_is_rejected() -> None:
    """The exact shape a fabricated measurement arrives in.

    The packet contract has nowhere to put a number, so a graph arm cannot
    assert a wrong metric value directly. What it can do is cite an
    evidence handle no canonical service ever minted, and have the reader
    infer a measurement behind it. That is what this rejects.
    """

    payload = _packet_payload()
    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload=payload,
        run_id=_RUN,
        canonical_evidence=[],  # canonical services minted nothing
    )
    assert record.status is seam.InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED, (
        "GUARD uncoined_evidence_is_rejected"
    )
    assert record.detail is not None
    assert "never minted" in record.detail


def test_a_packet_within_canonical_evidence_is_accepted() -> None:
    payload = _packet_payload()
    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload=payload,
        run_id=_RUN,
        canonical_evidence=_canonical(payload),
    )
    assert record.status is seam.InvestigationShadowStatus.RECORDED


def test_one_uncoined_handle_among_many_is_enough_to_reject() -> None:
    """A partial forgery is a forgery; the check is not a majority vote."""

    payload = _packet_payload()
    canonical = _canonical(payload)
    assert canonical, "the golden packet must cite evidence for this to mean anything"
    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload=payload,
        run_id=_RUN,
        canonical_evidence=canonical[:-1],
    )
    assert record.status is seam.InvestigationShadowStatus.CANONICAL_BYPASS_REJECTED


def test_the_bypass_check_names_offenders_rather_than_returning_a_boolean() -> None:
    offenders = seam.canonical_bypass_offenders(
        packet_evidence_handles=["ev1_" + "a" * 40, "ev1_" + "b" * 40],
        canonical_evidence=[],
    )
    assert offenders == ("ev1_" + "a" * 40, "ev1_" + "b" * 40)


def test_the_bypass_check_is_not_vacuous() -> None:
    """It must actually pass when the packet stays inside canonical evidence."""

    payload = _packet_payload()
    assert (
        seam.canonical_bypass_offenders(
            packet_evidence_handles=_handles(payload),
            canonical_evidence=_canonical(payload),
        )
        == ()
    )


# --------------------------------------------------------------------------
# Arm attribution and neutrality
# --------------------------------------------------------------------------


def test_a_recorded_evaluation_preserves_arm_packet_version_and_lineage() -> None:
    payload = _packet_payload()
    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload=payload,
        run_id=_RUN,
        canonical_evidence=_canonical(payload),
    )
    assert record.arm_id
    assert record.packet_schema_version == "ask_dev_investigation_packet.v1"
    assert record.projection_version
    assert record.evidence_handles == tuple(_handles(payload))


def test_arm_identity_is_read_off_the_packet_not_supplied_by_the_caller() -> None:
    """A caller cannot mislabel another arm's packet as its own.

    ``evaluate`` has no ``arm_id`` parameter, which is the guarantee. If one
    is ever added, this test says so.
    """

    import inspect

    parameters = set(
        inspect.signature(seam.InvestigationShadow.evaluate).parameters
    ) - {"self"}
    assert parameters == {"payload", "run_id", "canonical_evidence"}


def _module_dependencies() -> set[str]:
    """Modules the seam actually imported, read off the module object.

    Scanning source text was the first version of this check and it was
    vacuous in the worst way: the module's own docstring says the word
    "provider" while explaining that no provider is reachable, so the scan
    failed on prose and would equally have passed on a real import hidden
    behind an alias. Reading ``__dict__`` asks what the module *has*.
    """

    dependencies: set[str] = set()
    for value in vars(seam).values():
        module_name = getattr(value, "__module__", None) or getattr(
            value, "__name__", None
        )
        if isinstance(module_name, str):
            dependencies.add(module_name)
    return dependencies


def test_the_seam_holds_no_reference_into_either_arm() -> None:
    """Neutrality is structural: the seam cannot see either arm's code."""

    dependencies = _module_dependencies()
    assert not [name for name in dependencies if "graph_arm" in name]
    assert not [name for name in dependencies if "native_arm" in name]


def test_the_neutrality_check_can_actually_fail() -> None:
    """Anti-vacuity: prove the scan sees a dependency when there is one."""

    dependencies = _module_dependencies()
    assert [name for name in dependencies if "investigation_contract" in name], (
        "the seam genuinely imports the contract package, so a check that "
        "sees nothing here is not looking at dependencies at all"
    )


def test_no_narrative_provider_is_reachable_from_the_seam() -> None:
    """No live model call, guaranteed by what the module does not hold."""

    dependencies = _module_dependencies()
    for forbidden in ("narrative", "agent_provider", "llm", "openai"):
        assert not [name for name in dependencies if forbidden in name.lower()], (
            forbidden
        )
    assert not [name for name in vars(seam) if "provider" in name.lower()]


def test_a_recorded_evaluation_without_an_arm_is_unconstructable() -> None:
    """The invariant is enforced, not merely satisfied by today's callers."""

    with pytest.raises(
        ValueError, match="must name the arm"
    ):  # GUARD arm_attribution_required
        seam.InvestigationShadowRecord(
            run_id=_RUN,
            status=seam.InvestigationShadowStatus.RECORDED,
            arm_id=None,
            packet_schema_version="ask_dev_investigation_packet.v1",
            projection_version="p.v1",
            packet_id="packet-1",
            outcome="supported",
            evidence_handles=(),
            latency_ms=1,
        )


def test_a_packet_without_trial_metadata_is_not_recorded_as_comparable() -> None:
    """Arm identity is optional on the contract; a comparison record needs it."""

    payload = _packet_payload()
    versions = dict(payload["versions"])
    versions.pop("trial", None)
    payload["versions"] = versions
    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload=payload,
        run_id=_RUN,
        canonical_evidence=_canonical(payload),
    )
    assert record.status is seam.InvestigationShadowStatus.PACKET_INVALID
    assert record.detail is not None
    assert "no arm owns it" in record.detail


# --------------------------------------------------------------------------
# No prose, no model
# --------------------------------------------------------------------------


def test_the_shadow_frame_carries_only_server_assembled_structure() -> None:
    """Every fact is built here from structural claims, never producer text."""

    payload = _packet_payload()
    record = seam.InvestigationShadow(enabled=True).evaluate(
        payload=payload,
        run_id=_RUN,
        canonical_evidence=_canonical(payload),
    )
    assert record.frame_facts
    for fact in record.frame_facts:
        key, _, value = fact.partition(":")
        assert key
        assert value
        # A structural fact is a token or a count -- never a sentence.
        assert " " not in value
