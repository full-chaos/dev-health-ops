"""CHAOS-3646: the arm's half of canonical evidence admission.

The CHAOS-3619 trial recorded a **canonical bypass** as an architectural
fact: the graph arm mints its own evidence refs, so a packet citing them is
rejected at the Ask Dev frame boundary even when the evidence is authentic.
Two independent columns, and no path across them.

This module is the arm's side of the path, and it is deliberately tiny. It
turns a traversal readout into a list of **pointers** and nothing else. The
canonical service (``api.dev.evidence_service.EvidenceService.admit``)
resolves each pointer against the source, authorizes it for the requesting
principal, and mints the handle. Nothing in this module mints, signs,
authorizes, or describes a record's content.

Default OFF, twice over: the flag below is off unless explicitly set, and
``EvidenceService`` admits nothing at all unless a caller supplies a
candidate resolver, which no shipped construction does.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence

from dev_health_ops.api.dev.contracts_v2.embedded import DevEvidenceRefV2
from dev_health_ops.api.dev.evidence_service import EvidenceCandidate

from .readback import DiscoveredObservation, InvestigationReadout

# ``SOURCE_EVIDENCE_HANDLE_ATTRIBUTE`` is deliberately NOT imported. The
# handle an observation carries is precisely the thing admission must not
# propagate: a candidate carrying it would let a source-issued -- or
# arm-minted -- identity cross the boundary and come back as though the
# canonical service had chosen it.
from .vocabulary import (
    SOURCE_EVIDENCE_ENTITY_ATTRIBUTE,
    SOURCE_EVIDENCE_ID_ATTRIBUTE,
)

__all__ = [
    "EVIDENCE_ADMISSION_FLAG",
    "ARM_SOURCE_SYSTEM",
    "AdmittedEvidence",
    "candidate_locator",
    "candidates_from_readout",
    "evidence_admission_enabled",
]

#: Same convention as the other two arm flags (``CONTEXT_FABRIC_*``,
#: ``== "1"``), so "unset" is off and every other value is off too. Must also
#: appear in ``tests/_env_isolation.py``'s scrub list -- a new ``os.getenv``
#: name in ``src/`` that is not registered there fails a drift guard that no
#: test subset reaches.
EVIDENCE_ADMISSION_FLAG = "CONTEXT_FABRIC_GRAPH_EVIDENCE_ADMISSION_ENABLED"

#: The ``source_system`` a candidate from this arm declares. It names the
#: *discovery* layer, not a store of record: a resolver registered under this
#: name is asserting "I can look up what the graph arm points at", and the
#: trial's resolver answers that by reading the world, never the projection.
ARM_SOURCE_SYSTEM = "context_fabric_graph_arm"

#: Locator -> the canonical ref the evidence service admitted for it.
AdmittedEvidence = Mapping[str, DevEvidenceRefV2]


def evidence_admission_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Whether graph-discovered evidence may be submitted for admission.

    Default off. Injectable ``environ`` follows the native arm's flag rather
    than this package's older bare-``os.getenv`` style, because a trial
    harness has to be able to turn this on for one run without mutating the
    process it shares with everything else.
    """

    source = os.environ if environ is None else environ
    return source.get(EVIDENCE_ADMISSION_FLAG) == "1"


def candidate_locator(observation: DiscoveredObservation) -> str:
    """The SOURCE's own identity for the record this observation is of.

    ``source_evidence_id`` where the source declared one, the observation's
    own canonical id otherwise. Not the entity the record is about, and not
    the handle: an admission that keyed on the entity would collapse two
    records of one kind about one entity into one (CHAOS-3633), and one that
    keyed on the handle would be carrying the arm's mint across a boundary
    that exists to stop exactly that.
    """

    return observation.attributes.get(
        SOURCE_EVIDENCE_ID_ATTRIBUTE, observation.canonical_id
    )


def candidates_from_readout(
    readout: InvestigationReadout,
) -> tuple[EvidenceCandidate, ...]:
    """Every source record this readout reached, as pointers.

    Pure: no I/O, no store, no signer, no clock. One candidate per distinct
    locator, in locator order, so a caller submitting them twice submits the
    same list -- an admission round that varied with traversal order would
    make a refusal impossible to attribute.

    An observation with no subject in the readout's authorized set is skipped
    rather than submitted. The evidence service would refuse it, and that is
    the right outcome, but sending it would spend a resolution round asking a
    source about a record this principal has already been shown not to reach.
    The refusal that matters -- an entity the *service's* own resolution
    withholds -- is still exercised, because the two authorized sets are
    resolved independently and this one is the arm's belief, not the grant.
    """

    authorized = set(readout.authorized_entity_ids)
    by_locator: dict[str, EvidenceCandidate] = {}
    for observation in readout.observations:
        if not any(
            subject in authorized for subject in observation.subject_canonical_ids
        ):
            continue
        declared = observation.attributes.get(SOURCE_EVIDENCE_ENTITY_ATTRIBUTE)
        entity_id = declared if declared is not None else _first_subject(observation)
        if entity_id is None:
            continue
        locator = candidate_locator(observation)
        by_locator.setdefault(
            locator,
            EvidenceCandidate(
                source_system=ARM_SOURCE_SYSTEM,
                entity_type=observation.kind.value,
                entity_id=entity_id,
                locator=locator,
                repository_ids=tuple(observation.repository_ids),
            ),
        )
    return tuple(by_locator[key] for key in sorted(by_locator))


def _first_subject(observation: DiscoveredObservation) -> str | None:
    subjects: Sequence[str] = observation.subject_canonical_ids
    return subjects[0] if subjects else None
