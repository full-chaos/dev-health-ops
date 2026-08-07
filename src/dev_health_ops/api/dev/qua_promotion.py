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

**It re-authorizes at commit time.** The shadow already bounds proposals two
ways (a per-call JSON Schema that makes an out-of-range index inexpressible,
and a runtime verifier re-checking each index against that mention's own
slice). Promotion adds a third check, and on the singular path that third
check is not redundancy -- it is the ONLY receipt. ``committed_resolution_for``
mints no ``subject_set_fingerprint``, and the executor's fingerprint
cross-check (``investigation_plans/executor.py``) only covers *set* batches,
so nothing downstream re-verifies a singular committed entity. See
``verify_still_authorized``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .contracts_v2.base import Cardinality
from .contracts_v2.question_understanding import QUAOutcome
from .contracts_v2.subject import DevResolutionEntry, ResolutionOutcome
from .qua_shadow import QUAShadowRecord, QUAShadowStatus
from .scope_service import (
    SEARCHABLE_ENTITY_KINDS,
    AuthorizedEntity,
    EntityKind,
    ScopeResolutionService,
    ScopeSearchRequest,
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
    # truncation. So for a question with many mentions, a later mention is
    # structurally unable to carry a selection: the per-call JSON Schema
    # bounds its index range to ``[0, -1]``, which no integer satisfies.
    # That is the correct fail-closed direction (no proposal rather than a
    # proposal over a truncated list), and it is one more reason a
    # multi-mention question falls through to clarification above.
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
    """Re-fetch the authorized shortlist and confirm the entity is in it.

    Deliberately a fresh authorized lookup rather than a re-read of the
    record's own ``candidate_entities``. Re-reading the record would only
    prove the record is self-consistent -- it would pass just as happily if
    the shortlist that produced it had been built for the wrong tenant, or if
    a later change let a proposal carry an entity its slice never contained.
    The question this must answer is "is this entity authorized for THIS
    caller, right now", and only asking the authorization boundary again
    answers it.

    ``search`` is the same boundary the deterministic resolver uses and it
    rejects an empty ``org_id``/``permission_fingerprint`` outright, so the
    tenancy filter cannot be bypassed by an empty argument. The call is
    served from the request-scoped, ``permission_fingerprint``-keyed cache in
    the common case, so this costs a dictionary lookup rather than a second
    catalog round trip.

    Returns ``False`` on ANY failure, including an exception from the catalog:
    a verification that could not be performed is not a verification, and the
    run must proceed as though the seam had never spoken.
    """

    kind = promotion.entity.kind
    if kind not in SEARCHABLE_ENTITY_KINDS:
        return False
    try:
        result = await scope_service.search(
            org_id,
            permission_fingerprint,
            ScopeSearchRequest(
                query=promotion.text_span,
                kinds=(kind,),
                limit=limit,
                allowed_kinds=SEARCHABLE_ENTITY_KINDS,
                include_alias_matches=True,
            ),
        )
    except Exception:
        return False
    return any(
        candidate.kind is kind
        and candidate.canonical_id == promotion.entity.canonical_id
        for candidate in result.candidates
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
