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
    "DEFERRED_DEFECTS",
    "DeferredDefect",
    "deferred_for",
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
#:
#: CHAOS-3627's entry was deleted here when its fix merged (squash
#: ``2c74c5767``, PR #1617). Its dimension —
#: ``ZERO_UNAUTHORIZED_RESULTS`` for the graph arm — was NOT carried
#: forward from the pre-fix run; it was re-derived from scratch, because a
#: mis-attribution that can mask a leak makes the old column unsound rather
#: than merely stale.
UNSOUND_DIMENSIONS: tuple[UnsoundDimension, ...] = ()


def unsound_for(dimension_id: str, arm_id: str) -> UnsoundDimension | None:
    """The registry entry suppressing this cell, if any."""

    for entry in UNSOUND_DIMENSIONS:
        if entry.dimension_id.value == dimension_id and arm_id in entry.arm_ids:
            return entry
    return None


def is_unsound(dimension_id: str, arm_id: str) -> bool:
    return unsound_for(dimension_id, arm_id) is not None


# ---------------------------------------------------------------------------
# Defects that persist into the measured results
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DeferredDefect:
    """A defect this trial observed, ticketed, and did NOT get fixed.

    From the descoping of CHAOS-3634 onward the standing rule is that a
    defect this trial finds gets a disposition and a ticket, not a fix,
    unless it blocks measurement itself. H3 was fixed because without it the
    trial measured nothing; CHAOS-3634 is not, because the trial can measure
    and report it.

    That makes attribution load-bearing rather than tidy. **A fault row
    carrying no owner is indistinguishable from an unexplained crash**, and a
    reader of the artifact cannot tell a known, accepted boundary from a
    surprise. This registry is what lets the runner attach the ticket to a
    fault it already expects, so "observed and deferred" and "observed and
    unexplained" stay different facts.

    ``signature`` is matched as a substring of the recorded fault detail.
    Deliberately not a case id: the same defect can surface on more than one
    case (CHAOS-3634 was found from two directions on two different subjects),
    and keying on the case would attribute it in one place and leave it
    anonymous in the other.
    """

    signature: str
    owner: str
    note: str


#: Defects known to persist into the measured sweep. **Empty is the healthy
#: state**, and an entry here is a claim that the defect was seen, ticketed
#: and deliberately not fixed -- never that it was tolerated silently.
DEFERRED_DEFECTS: tuple[DeferredDefect, ...] = (
    DeferredDefect(
        signature="staffing_qualification",
        owner="CHAOS-3634",
        note=(
            "the arm builds a capacity/staffing driver with no staffing "
            "qualification and the frozen contract refuses it. Descoped from "
            "the fix train, so it persists in these results: the bait "
            "WORKED on the arm, and that is measured rather than transient. "
            "Whoever implements the staffing-qualification derivation on the "
            "path CHAOS-3621 chooses must re-verify BOTH directions -- "
            "denial-of-packet and bait-acceptance -- because a fix for one "
            "can leave the other intact"
        ),
    ),
)


def deferred_for(detail: str) -> DeferredDefect | None:
    """The ticket owning a fault detail, if this trial already expects it.

    Returns ``None`` for anything unrecognised, and that is the important
    half: an unexpected fault must stay unattributed so it reads as the
    surprise it is, rather than being quietly absorbed into a known one.
    """

    for entry in DEFERRED_DEFECTS:
        if entry.signature in detail:
            return entry
    return None
