"""CHAOS-3525: when a QUA proposal may become the run's committed subject.

``qua_shadow.py`` produces a proposal and records it. This module decides
whether that proposal is allowed to *matter*, and re-verifies it against the
authorized catalog at the moment it would. The split is deliberate: the
shadow's job is to observe, and it must stay independently testable as a
thing that observes; promotion is a separate policy with a separate flag and
separate failure modes.

Three properties this module exists to guarantee:

**It only ever fires where the deterministic layer declined.** A run that
resolved its subject deterministically is never second-guessed by a model --
``promotable_selection`` returns ``None`` for it before looking at anything
else. That is what keeps CHAOS-3388's deterministic layer the fast path
rather than a suggestion.

**It fails closed on every ambiguity.** Wrong outcome, missing entity, a
rejected proposal, confidence under the floor, more than one mention, a
cohort, an uncorroborated organization-wide cardinality -- each returns
``None``, and ``None`` means the run proceeds exactly as it would have
without the seam. There is no branch here that repairs a partial proposal
into a usable one.

**It re-authorizes at commit time.** The shadow does bound proposals two ways
before promotion sees them -- but read ``qua_shadow``'s module docstring for
what each one covers, because this paragraph once asserted the pair
uncritically while one of them was absent. CHAOS-3536 found the wire-level
bound stripped in transit (``minimum``/``maximum`` are not in
``_STRUCTURAL_SCHEMA_KEYS``); CHAOS-3537 restored it as an ``enum``, which
survives the projection. So today the wire schema bounds indices to what the
CALL authorized, and ``qua_shadow._verify`` bounds them to what the MENTION
authorized. Those are different STAGES rather than different coverage --
``_verify``'s rejection set subsumes the schema's, and the schema's value is
that an unauthorized index is never generated in the first place.

Promotion's ``verify_still_authorized`` is the third check, and on the
singular path it is not redundancy -- it is the ONLY receipt.
``committed_resolution_for``
mints no ``subject_set_fingerprint``, and the executor's fingerprint
cross-check (``investigation_plans/executor.py``) only covers *set* batches,
so nothing downstream re-verifies a singular committed entity. See
``verify_still_authorized``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .contracts import ScopeResolutionOutcome
from .contracts_v2.base import Cardinality
from .contracts_v2.question_understanding import QUAOutcome
from .contracts_v2.subject import DevResolutionEntry, ResolutionOutcome
from .qua_shadow import QUAShadowRecord, QUAShadowStatus
from .scope_service import (
    SEARCHABLE_ENTITY_KINDS,
    AuthorizedEntity,
    EntityKind,
    ScopeRef,
    ScopeResolutionService,
    ScopeResolveRequest,
)

# The single producer of the AuthorizedEntity -> DevEntityRefV2 projection.
# Imported rather than re-implemented: see ``qua_committed_entry``.
from .subject_preflight import _entity_ref_v2

__all__ = [
    "QUA_COMMIT_DIAGNOSTIC",
    "qua_committed_entry",
    "QUA_COMMIT_MIN_CONFIDENCE",
    "QUA_COMMIT_RESOLVER_VERSION",
    "QUAPromotion",
    "promotable_selection",
    "verify_still_authorized",
]


#: Recorded on ``dev_runs.preflight_outcome`` when a QUA proposal committed
#: the subject. A member of ``subject_preflight.PREFLIGHT_DIAGNOSTICS``,
#: whose tuple is closed AND length-bounded by a ``String(32)`` column -- 21
#: characters, checked by that module's own totality test.
QUA_COMMIT_DIAGNOSTIC = "committed_qua_subject"

#: Stamped on the QUA-committed ledger entry's ``resolver_version``, where
#: every deterministic entry carries the catalog resolver's own version.
#:
#: This is the audit seam that keeps a model-committed subject distinguishable
#: from a deterministically-committed one after the fact. Without it the
#: ledger shape of the two is identical, and the acceptance corpus's
#: ``derive_resolution_path`` would classify an LLM commit as
#: ``deterministic-exact``/``deterministic-alias`` -- a receipt that reads as
#: stronger provenance than the run actually had.
QUA_COMMIT_RESOLVER_VERSION = "qua.v1"

#: The confidence floor a proposal must clear to commit.
#:
#: PROVISIONAL, and deliberately conservative (team-lead ruling, 2026-08-07):
#: pinned as a constant now rather than blocking the capability on a
#: calibration study, with every commit-mode decision recording the observed
#: confidence so the data to justify or move this number accumulates from the
#: first run. It is a floor, not a threshold to tune upward casually -- the
#: cost of a wrong commit is a confidently wrong subject, which is the exact
#: failure CHAOS-3289 and ``alias_matching``'s never-auto-commit rule were
#: written about.
QUA_COMMIT_MIN_CONFIDENCE = 0.85


@dataclass(frozen=True, slots=True)
class QUAPromotion:
    """One promotable selection, with the evidence that justified it."""

    mention_id: str
    text_span: str
    entity: AuthorizedEntity
    confidence: float


def promotable_selection(
    record: QUAShadowRecord,
    *,
    deterministic_declined: bool,
    min_confidence: float = QUA_COMMIT_MIN_CONFIDENCE,
) -> QUAPromotion | None:
    """The single selection this record may commit, or ``None``.

    ``deterministic_declined`` is supplied by the caller rather than derived
    here: the shape of "declined" belongs to the preflight's own vocabulary
    (a TERMINATE on an unresolved mention, or the bare-name PROCEED that
    widens to organization scope), and duplicating that classification in a
    second module is how the two drift apart.
    """

    if not deterministic_declined:
        return None
    if record.status is not QUAShadowStatus.EVALUATED:
        return None
    # Ruling 3 (team-lead, 2026-08-07): a cohort never auto-commits. The
    # executor requires a subject set homogeneous in kind and there is no
    # faithful v1 rendering of several subjects at once, so a multi-mention
    # proposal falls through to ranked clarification instead. Recorded as a
    # limitation on CHAOS-3525 rather than silently narrowed to the first
    # mention -- picking one of several named subjects would be a wrong-
    # subject answer wearing a confident label.
    if len(record.mentions) != 1:
        return None
    # Stated rather than left to be discovered: the shortlist is capped at
    # ``max_total_candidates`` (50) across the WHOLE call, and a mention past
    # that bound gets an EMPTY slice -- never a partial one straddling the
    # truncation. So for a question with many mentions, a later mention
    # cannot carry a selection that survives verification.
    #
    # CHAOS-3536 corrected the mechanism claimed here, and the claim was
    # wrong in TWO ways. It used to say the per-call JSON Schema bounded
    # such a mention's index range to ``[0, -1]``, "which no integer
    # satisfies".
    #
    # First: as written, those bounds never reached the provider at all --
    # ``_structural_schema`` strips ``minimum``/``maximum`` before dispatch.
    # CHAOS-3537 fixed that by re-expressing the bound as an ``enum``, which
    # survives, so a call-wide bound now genuinely reaches the decoder. That
    # repair does NOT rescue the claim below.
    #
    # Second, and the reason CHAOS-3537 changes nothing here: THERE IS NO
    # PER-MENTION
    # INDEX RANGE IN THE SCHEMA. ``_response_schema`` is built once per call
    # from ``candidate_count=len(combined)`` -- the COMBINED, call-wide
    # shortlist -- so every mention shares one index space. A mention past
    # ``max_total_candidates`` gets an empty SLICE from
    # ``_combine_shortlists``, but ``candidate_count`` is still 50, so the
    # schema's enum keeps admitting 0..49 for it. The zero-candidate encoding
    # fires only when the WHOLE call authorized nothing, which is not this
    # case.
    #
    # So for a truncated mention the schema offers nothing whatsoever, and
    # ``_verify`` -- which checks each index against that mention's OWN
    # ``[start, end)`` slice, not the call-wide range -- is the entire
    # reason a truncated mention cannot carry a selection. That is still
    # fail-closed, and it is one more reason a multi-mention question falls
    # through to clarification above.
    # An organization-wide proposal can name no entity to verify, so there is
    # nothing here to commit even when corroborated; a singular commit is the
    # only shape this promotion has. Checked explicitly rather than left to
    # fall out of the per-mention checks below, because "the model asked for
    # org-wide" is exactly the widening channel the CHAOS-3389 adversarial
    # critique named, and it should fail on its own name.
    if record.cardinality is Cardinality.ORGANIZATION_WIDE:
        return None
    assessment = record.mentions[0]
    if assessment.outcome is not QUAOutcome.RESOLVED:
        return None
    if assessment.rejected_reason is not None:
        return None
    if assessment.selected_entity is None:
        return None
    if assessment.confidence < min_confidence:
        return None
    return QUAPromotion(
        mention_id=assessment.mention_id,
        text_span=assessment.text_span,
        entity=assessment.selected_entity,
        confidence=assessment.confidence,
    )


async def verify_still_authorized(
    promotion: QUAPromotion,
    *,
    scope_service: ScopeResolutionService,
    org_id: str,
    permission_fingerprint: str,
    limit: int,
) -> bool:
    """Re-resolve the entity by IDENTITY and confirm this caller may see it.

    Deliberately an identity resolution, not a re-read of the record's own
    ``candidate_entities`` and not a fuzzy re-search:

    * re-reading the record would only prove the record is self-consistent --
      it would pass just as happily if the shortlist that produced it had
      been built for the wrong tenant;
    * re-searching by the user's text makes authorization depend on how the
      entity ranked against everything else matching that text. Adversarial
      review could not rule out a legitimate entity sitting below the
      candidate cap among enough same-kind matches and being refused. Asking
      for the entity BY ITS OWN ID removes the question rather than bounding
      it.

    ``resolve`` is the same authorization boundary the deterministic resolver
    uses; it rejects an empty ``org_id``/``permission_fingerprint`` outright,
    and every catalog query behind it is ``WHERE org_id = ...``, so an entity
    belonging to another tenant resolves as unresolved rather than exact.
    ``allow_organization_fallback=False`` so an unresolvable ref can never
    quietly become an organization-scoped "success".

    On the singular path this is the ONLY receipt -- ``committed_resolution_for``
    mints no ``subject_set_fingerprint`` and the executor's fingerprint
    cross-check covers only set batches, so nothing downstream re-verifies a
    singular committed entity.

    Returns ``False`` on ANY failure, including an exception from the catalog:
    a verification that could not be performed is not a verification, and the
    run must proceed as though the seam had never spoken.
    """

    kind = promotion.entity.kind
    if kind not in SEARCHABLE_ENTITY_KINDS:
        return False
    try:
        resolution = await scope_service.resolve(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            request=ScopeResolveRequest(
                explicit_refs=(ScopeRef(kind, promotion.entity.canonical_id),),
                allow_organization_fallback=False,
            ),
        )
    except Exception:
        return False
    if resolution.outcome is not ScopeResolutionOutcome.EXACT:
        return False
    return any(
        entity.kind is kind and entity.canonical_id == promotion.entity.canonical_id
        for entity in resolution.entities
    )


def qua_committed_entry(
    promotion: QUAPromotion,
    *,
    entry_ordinal: int,
    query_version: str,
    resolved_at: datetime,
) -> DevResolutionEntry:
    """The ledger row a QUA commit appends, stamped with its own provenance.

    Appending is not optional bookkeeping. ``derive_resolution_path`` reads
    the ledger's LAST entry per mention; without this row the mention's last
    entry is still the deterministic decline, so a run that answered about a
    committed subject would produce a receipt saying it never resolved one --
    the same class of defect as CHAOS-3497's, where the wire disagreed with
    what the run actually did.

    ``resolver_version`` is what keeps the row honest in the other direction:
    the shape of an ``exact_match`` entry is identical whichever layer
    produced it, so without a distinct version an LLM commit would read as
    ``deterministic-exact``. Built through ``subject_preflight._entity_ref_v2``
    rather than a second local projection of the same thing -- that function
    is the one place that knows ``DevEntityRefV2`` is team-capable where the
    v1 enums are not, and duplicating it here is how the two drift.
    """

    return DevResolutionEntry(
        entry_ordinal=entry_ordinal,
        mention_id=promotion.mention_id,
        outcome=ResolutionOutcome.EXACT_MATCH,
        committed_entity_ref=_entity_ref_v2(promotion.entity),
        candidates=(),
        repository_attribution=promotion.entity.repository_id,
        team_attribution=(
            promotion.entity.canonical_id
            if promotion.entity.kind is EntityKind.TEAM
            else None
        ),
        resolver_version=QUA_COMMIT_RESOLVER_VERSION,
        query_version=query_version,
        resolved_at=resolved_at,
    )
