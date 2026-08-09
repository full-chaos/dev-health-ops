"""Dimensions whose inputs are known-defective, and may not be published.

A dimension can be *computable* and still be *unsound*: the oracle runs, a
verdict comes out, and the verdict is a function of a defect rather than of
the arm's behaviour. Publishing such a value — even tagged provisional — is
worse than publishing nothing, because a number in a cell invites being
quoted and a tag does not travel with a quotation.

**The case that created this registry.** PR #1617's verifier measured that
115 of 291 graph-arm packets carry an evidence entry whose ``entity_id``
contradicts the world record its handle names: an un-reached backing record
gets described by the citing observation. ``entity_sightings()`` treats an
indexed evidence item's ``entity_id`` as a *sighting of that entity*, and
sightings feed ``unauthorized_disclosures`` and ``fabricated_entities``, which
feed ``ZERO_UNAUTHORIZED_RESULTS`` — a ``MUST_BE_ZERO`` safety dimension.

So the authorization oracle is currently shown sightings the packet does not
genuinely make, and it cuts both ways. It can **manufacture** a false
unauthorized-disclosure finding, or — the direction that makes the fix
release-blocking — it can **mask a real leak**, by attributing a genuinely
leaked entity's evidence to a permitted entity instead. A safety column built
on that is not stale, it is unsound.

**Why a registry rather than a remembered rule.** The instruction "do not
publish pre-fix safety numbers" is exactly the kind of rule that survives
until the person holding it hands over. Encoded here it is applied by the
renderer, checked by a test, and — the property that matters most — it is
**removable by deletion**: when the fix lands, the entry goes, and every
affected cell starts rendering its real verdict with no other edit anywhere.
An empty registry is the healthy state and the tests assert what that means.
"""

from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.api.dev.investigation_contract import ScoringDimensionID

__all__ = [
    "UNSOUND_DIMENSIONS",
    "UnsoundDimension",
    "is_unsound",
    "unsound_for",
]


@dataclass(frozen=True, slots=True)
class UnsoundDimension:
    """One dimension that must render NOT MEASURED, and who owns the fix.

    ``owner`` is required and is a ticket id: an unsound dimension with no
    named owner is indistinguishable from an untested one, which is the same
    rule ``ArmResult`` enforces for expected limitations. ``arm_ids`` scopes
    the suppression, because a defect in one arm's emitter must not blank the
    other arm's column — that would hide a real result behind someone else's
    bug.
    """

    dimension_id: ScoringDimensionID
    owner: str
    arm_ids: frozenset[str]
    reason: str


#: Currently-unsound dimensions. **Empty is the healthy state.**
#:
#: Delete an entry the moment its owning ticket's fix merges — and re-measure
#: rather than assuming the verdicts that then appear are correct.
UNSOUND_DIMENSIONS: tuple[UnsoundDimension, ...] = (
    UnsoundDimension(
        dimension_id=ScoringDimensionID.ZERO_UNAUTHORIZED_RESULTS,
        owner="CHAOS-3627",
        arm_ids=frozenset({"graph_assisted_shadow_arm"}),
        reason=(
            "115 of 291 graph-arm packets (measured by PR #1617's "
            "verifier) carry an evidence entry whose entity_id contradicts "
            "the world record its handle names. entity_sightings() reads that "
            "field as a sighting of the entity, and sightings feed "
            "unauthorized_disclosures and fabricated_entities. The verdict is "
            "therefore a function of the mis-attribution rather than of the "
            "arm's disclosure behaviour, and it can mask a real leak by "
            "attributing a leaked entity's evidence to a permitted one"
        ),
    ),
)


def unsound_for(dimension_id: str, arm_id: str) -> UnsoundDimension | None:
    """The registry entry suppressing this cell, if any."""

    for entry in UNSOUND_DIMENSIONS:
        if entry.dimension_id.value == dimension_id and arm_id in entry.arm_ids:
            return entry
    return None


def is_unsound(dimension_id: str, arm_id: str) -> bool:
    return unsound_for(dimension_id, arm_id) is not None
