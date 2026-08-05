"""Deny-by-default subject preflight for Ask Dev (CHAOS-3292).

Runs between scope authorization and the first model round, so **no
evidence-bearing tool executes before an exact, committed subject exists**.
Every named subject in the question is resolved against the authorized
catalog first; the resolution is recorded on an append-only
``dev_resolution_ledger.v1``; and the run either proceeds with a
server-committed scope or terminates with a public outcome that names nothing
the catalog did not confirm.

Why the placement matters
-------------------------

Today the model is *told* to call ``resolve_scope.v1`` before any status tool
(``prompts/composer``'s ``named_entity_resolution`` section) and is *judged*
afterwards by a prose-matching backstop. Both are post-hoc. Committing the
subject here means the model is **given** a project scope in its prompt rather
than asked to earn one — which is what closes the residual failure rate the
CHAOS-3289 verification measured.

What this module never does
---------------------------

* It never constructs ``FORBIDDEN_OR_NOT_FOUND`` — see ``preflight_outcomes``.
* It never answers about one entity under another's name. A *typed* named
  subject that does not resolve terminates the run. A **bare** name — one the
  kind-noun grammar could not type, which may not be a subject at all — widens
  the run to organization scope instead, precisely so a page-derived subject
  cannot be narrated as the named one, and re-arms the legacy backstop to catch
  an answer that narrates the name anyway.
* It never lets a later resolution erase an earlier unresolved one: every
  update appends to the ledger and is checked with ``validate_ledger_extends``.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from .contracts import (
    DevContractVersions,
    DevScope,
    DevScopeResolution,
    DirectScope,
    ScopeResolutionOutcome,
    ToolID,
)
from .contracts_v2 import (
    Cardinality,
    DevAnswerV2,
    DevEntityRefV2,
    DevResolutionCandidate,
    DevResolutionEntry,
    DevResolutionLedger,
    DevSubjectMention,
    DevSubjectSet,
    PublicOutcome,
    QuestionIntentID,
    ResolutionOutcome,
    validate_ledger_extends,
)
from .contracts_v2 import (
    EntityKind as ContractEntityKind,
)
from .contracts_v2.subject import UNRESOLVED_OUTCOMES
from .orchestrator_states import RunState
from .portfolio_status_service import MAX_PORTFOLIO_PROJECTS
from .preflight_outcomes import (
    NOT_FOUND_CLOSE_MATCHES_KEY,
    PREFLIGHT_OUTCOME_BY_RESOLUTION,
    build_preflight_answer,
)
from .question_interpreter import MAX_MENTIONS, InterpretedQuestion, QuestionInterpreter
from .scope_service import (
    DIRECT_SCOPE_KINDS,
    SEARCHABLE_ENTITY_KINDS,
    AuthorizedEntity,
    EntityKind,
    MentionResolution,
    ScopeResolutionService,
    ScopeSearchRequest,
)

__all__ = [
    "NOT_FOUND_FALLBACK_LIMIT",
    "PREFLIGHT_DIAGNOSTICS",
    "SUBJECT_BEARING_TOOLS",
    "CommittedSubjects",
    "PreflightDecision",
    "SubjectPreflight",
    "SubjectPreflightResult",
]

#: Tools whose results are *about a subject*. None of these may execute while
#: any mention lacks a committed exact match.
#:
#: ``resolve_scope.v1`` and ``list_metrics.v1`` are exempt for different
#: reasons: the former is how a subject would be resolved at all, and the
#: latter is an organization catalog answer — CHAOS-3295 states explicitly that
#: "a correct catalog answer does not fail merely because metric definitions do
#: not carry material evidence refs".
SUBJECT_BEARING_TOOLS = frozenset(
    {
        ToolID.STATUS_SNAPSHOT,
        ToolID.CHANGE_SUMMARY,
        ToolID.QUERY_METRIC,
        ToolID.WORK_GRAPH_NEIGHBORS,
        ToolID.SEARCH_EVIDENCE,
        ToolID.GET_EVIDENCE,
        ToolID.DATA_HEALTH,
    }
)

ALL_TOOLS = frozenset(ToolID)


class PreflightDecision(StrEnum):
    PROCEED = "proceed"
    TERMINATE = "terminate"


#: Every diagnostic this module can emit — content-free codes recorded on
#: ``dev_runs.preflight_outcome``, a ``String(32)`` column with no CHECK
#: constraint. A diagnostic over 32 characters is a real insert-time failure
#: in production and passes every unit test that never persists a run
#: (CHAOS-3292's "interpreter_clarification_required", 34 chars, was exactly
#: that — fixed alongside this closed tuple). Kept literal, not derived from
#: an f-string template, so a new diagnostic is always visible here and the
#: length test below is exhaustive.
PREFLIGHT_DIAGNOSTICS: tuple[str, ...] = (
    "clarification_required",
    "proceeded_organization_wide",
    "unresolved_ambiguous_candidates",
    "unresolved_no_authorized_match",
    "unresolved_catalog_unavailable",
    "unresolved_unsupported_kind",
    "proceeded_unresolved_bare_name",
    "committed_kind_unsupported_in_v1",
    "oversized_mention_set_in_v1",
    "cohort_unsupported_in_v1",
    "committed_cohort_v1_only",
    "proceeded_committed_subject",
    "unresolved_close_matches",
    # CHAOS-3393: status.portfolio.v1's PLURAL_COHORT/ORGANIZATION_WIDE
    # PROCEED diagnostics -- see the two call sites' own comments.
    "committed_cohort_portfolio_v1",
    "committed_portfolio_org_wide",
)

#: CHAOS-3366: how many closest matches one not-found fallback may offer.
#:
#: A bound, not a page size — the list is read by a person deciding which thing
#: they meant, and a twenty-five-entry list of substring matches is not a
#: decision aid. Small enough that the search costs one bounded catalog round
#: trip per unresolved mention and no more.
NOT_FOUND_FALLBACK_LIMIT = 5

#: ``DevResolutionEntry.query_version`` is a non-empty ``Version``. A catalog
#: that returned candidates always returns a watermark too; this is the same
#: content-free placeholder ``scope_service`` uses, kept local so a defensive
#: empty value can never fail contract validation on the fallback path.
_UNAVAILABLE_WATERMARK = "catalog-watermark-unavailable"

#: Why a fallback candidate is being offered. Distinct from the same-name
#: ambiguity reason: nothing carried the typed name exactly, so what is being
#: offered is a near miss rather than one of several equal readings.
#:
#: Deliberately silent about entity *kind* (Codex review, low). The obvious
#: phrasing — "under a different kind" — is false on the context-ref path,
#: where the mention's own kind was never fuzzy-searched and the closest match
#: can legitimately be the same kind the user named. One reason string covers
#: every candidate on the entry, so it must be true for all of them.
_CLOSE_MATCH_REASON = "This is a close match to the name in the question."


@dataclass(frozen=True, slots=True)
class CommittedSubjects:
    """What the preflight committed for one run: a scope, a subject set, or both.

    CHAOS-3301. Exactly one of ``resolution``/``subject_set`` is set for a
    singular commit or a cohort respectively, except the "duplicate aliases
    collapse to one subject" case (N4), where both are set: the run proceeds
    on ``resolution`` (there is only one *distinct* committed entity), and
    ``subject_set`` still records that the question named it more than once
    (``original_mention_count`` on the persisted ``dev_subject_set.v1``).
    """

    resolution: DevScopeResolution | None
    subject_set: DevSubjectSet | None

    def __post_init__(self) -> None:
        if self.resolution is None and self.subject_set is None:
            raise ValueError(
                "CommittedSubjects requires a resolution, a subject set, or both"
            )


@dataclass(frozen=True, slots=True)
class SubjectPreflightResult:
    decision: PreflightDecision
    interpretation: InterpretedQuestion
    #: ``None`` only when the question named no subject at all.
    ledger: DevResolutionLedger | None
    #: The server-committed scope, when exactly one *distinct* subject
    #: resolved exactly. ``None`` for a cohort (CHAOS-3301, D1): a committed
    #: cohort never proceeds to tool execution on the v1 surface, so it never
    #: has a single scope to bind — see ``subject_set`` and
    #: ``committed_subjects`` instead.
    committed_resolution: DevScopeResolution | None
    #: CHAOS-3301: the full committed-subject outcome (scope and/or subject
    #: set). ``committed_resolution``/``subject_set`` are equivalent
    #: shorthands over this, kept as separate fields because the orchestrator
    #: and persistence each only need one half.
    committed_subjects: CommittedSubjects | None = None
    #: The persisted ``dev_subject_set.v1``, present whenever the question
    #: named more than one mention (a cohort, complete or partial; or
    #: duplicate aliases of one entity) — regardless of whether the run then
    #: proceeds (N4) or terminates unsupported (D1).
    subject_set: DevSubjectSet | None = None
    #: Present only for ``TERMINATE``.
    answer: DevAnswerV2 | None = None
    outcome: PublicOutcome | None = None
    #: The per-run tool allowlist the model round is held to.
    allowed_tools: frozenset[ToolID] = frozenset()
    #: Content-free code recorded on the run row. Never question or entity
    #: text. Always a member of ``PREFLIGHT_DIAGNOSTICS``.
    diagnostic: str = ""
    #: Mention IDs whose lack of a committed subject must block a
    #: subject-bearing tool. Excludes unresolved *untyped* mentions, which we
    #: are not confident were subjects at all.
    blocking_mention_ids: frozenset[str] = frozenset()
    #: True when a bare name went unresolved. The run proceeds organization-
    #: wide exactly as it does today, but the legacy CHAOS-3289 backstop is
    #: re-armed as terminal for it: that check judges the model's own answer
    #: text, so it fires only if the answer actually narrates the name.
    legacy_guard_required: bool = False
    #: The normalized bare names that went unresolved. The legacy backstop's own
    #: grammar only recognizes a name adjacent to an entity noun, so without
    #: these it cannot see the very names re-arming it is meant to guard.
    unresolved_name_spans: frozenset[str] = frozenset()
    #: CHAOS-3325: the ledger entry that produced ``answer.frame.
    #: clarification_candidates``, when the run terminated on an ambiguous
    #: mention -- ``None`` for every other termination reason (nothing on
    #: the frame to authorize) and for a non-terminal result. The caller
    #: (``orchestrator.run``) persists this via ``recorder.append_resolution``
    #: immediately before ``record_frame``, so the persistence layer has a
    #: real ledger row to cross-check the frame's candidates against
    #: (Codex review, CHAOS-3325 NO-SHIP finding).
    terminating_resolution_entry: DevResolutionEntry | None = None

    @property
    def has_committed_subject(self) -> bool:
        """Whether a *named subject* was committed, not merely a scope.

        The unresolved-bare-name path commits an organization scope to strip a
        page-derived subject, which is a scope but emphatically not a subject:
        telling the model "resolution is already complete" there would be the
        same false claim the v1/v2 prompt split exists to prevent.
        """

        return self.committed_resolution is not None and not self.legacy_guard_required

    @property
    def all_subjects_committed(self) -> bool:
        """Whether every *blocking* mention currently holds an exact match."""

        if self.ledger is None or not self.blocking_mention_ids:
            return True
        latest = self.ledger.latest_by_mention()
        return all(
            latest[mention_id].outcome is ResolutionOutcome.EXACT_MATCH
            for mention_id in self.blocking_mention_ids
            if mention_id in latest
        )


def _entity_ref_v2(entity: AuthorizedEntity) -> DevEntityRefV2:
    """Build the v2 ref directly from the authorized entity.

    Deliberately **not** via ``_contract_entity_ref`` / ``_resolved_scope`` /
    ``resolve_query_contract``: all three call ``EntityType(...)`` or
    ``DirectScope(...)``, which raise for ``team``. ``DevEntityRefV2`` is
    team-capable at the contract layer, which is what lets a resolved team be
    recorded honestly as ``exact_match`` before CHAOS-3301 gives it v1 scope
    semantics.
    """

    return DevEntityRefV2(
        entity_kind=ContractEntityKind(entity.kind.value),
        entity_id=entity.canonical_id,
        display_label=entity.label,
        repository_id=entity.repository_id,
        team_id=entity.canonical_id if entity.kind is EntityKind.TEAM else None,
    )


class SubjectPreflight:
    """Interpret one question, resolve its subjects, and gate the run."""

    def __init__(
        self,
        *,
        interpreter: QuestionInterpreter,
        scope_service: ScopeResolutionService,
        versions: DevContractVersions,
        mint_id: Callable[[], str] = lambda: str(uuid.uuid4()),
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._interpreter = interpreter
        self._scope_service = scope_service
        self._versions = versions
        self._mint_id = mint_id
        self._now = now

    async def run(
        self,
        *,
        request,
        org_id: str,
        permission_fingerprint: str,
        authorized_scope: DevScope,
        run_id: str,
        answer_id: str,
        conversation_id: str,
        on_phase: Callable[[RunState], Awaitable[None]] | None = None,
    ) -> SubjectPreflightResult:
        async def phase(state: RunState) -> None:
            if on_phase is not None:
                await on_phase(state)

        await phase(RunState.INTERPRETING)
        interpretation = await self._interpreter.interpret(request)
        intent = interpretation.intent
        mentions = interpretation.mentions
        generated_at = self._now()

        if intent.requires_clarification:
            return self._terminate(
                interpretation=interpretation,
                ledger=None,
                outcome=PublicOutcome.NEEDS_CLARIFICATION,
                # CHAOS-3301 fix, pre-existing CHAOS-3292 bug: the previous
                # value ("interpreter_clarification_required", 34 chars)
                # exceeded dev_runs.preflight_outcome's String(32) column
                # with no CHECK constraint -- a real insert-time failure for
                # every clarification-required run, caught by the new closed
                # PREFLIGHT_DIAGNOSTICS length test below.
                diagnostic="clarification_required",
                run_id=run_id,
                answer_id=answer_id,
                conversation_id=conversation_id,
                generated_at=generated_at,
                clarification_key="uninterpretable",
            )

        if not mentions:
            # CHAOS-3393: an ORGANIZATION_WIDE status.portfolio.v1 question
            # (no named subjects -- "what's the portfolio status?") gets a
            # bounded, deterministic project enumeration committed as its
            # subject set, so the plan executor has something to batch
            # over. Every other organization-wide question is unaffected --
            # see the branch below.
            if (
                intent.intent_id is QuestionIntentID.PORTFOLIO_STATUS
                and intent.cardinality is Cardinality.ORGANIZATION_WIDE
            ):
                return await self._organization_wide_portfolio_result(
                    interpretation=interpretation,
                    org_id=org_id,
                    permission_fingerprint=permission_fingerprint,
                    generated_at=generated_at,
                    run_id=run_id,
                    answer_id=answer_id,
                    conversation_id=conversation_id,
                )
            # Organization-wide by derivation, not by fallback: the question
            # named nothing, so there is no subject to get wrong. This is the
            # branch that keeps org-wide questions working exactly as today.
            return SubjectPreflightResult(
                decision=PreflightDecision.PROCEED,
                interpretation=interpretation,
                ledger=None,
                committed_resolution=None,
                answer=None,
                outcome=None,
                allowed_tools=ALL_TOOLS,
                diagnostic="proceeded_organization_wide",
            )

        if interpretation.total_named_mention_count > MAX_MENTIONS:
            # Bounds are rejections, never truncations (CHAOS-3301). Without
            # this check, `mentions` (already capped by extract_mentions) is
            # indistinguishable from a genuinely complete cohort at exactly
            # the bound -- a 26-subject question would silently narrate the
            # first 25 as though nothing were omitted. Rejected before any
            # catalog round trip.
            return self._terminate(
                interpretation=interpretation,
                ledger=None,
                outcome=PublicOutcome.UNSUPPORTED,
                diagnostic="oversized_mention_set_in_v1",
                run_id=run_id,
                answer_id=answer_id,
                conversation_id=conversation_id,
                generated_at=generated_at,
            )

        await phase(RunState.RESOLVING_SUBJECTS)
        context_ref_ids = self._context_ref_ids(request)
        untyped_ids = interpretation.untyped_mention_ids
        all_kinds = tuple(sorted(SEARCHABLE_ENTITY_KINDS, key=lambda kind: kind.value))
        resolutions = await asyncio.gather(
            *(
                self._scope_service.resolve_mention(
                    org_id,
                    permission_fingerprint,
                    lookup_text=mention.normalized_lookup_text,
                    kinds=(
                        # A bare name states no kind, so every searchable kind
                        # is a legitimate reading of it.
                        all_kinds
                        if mention.mention_id in untyped_ids
                        else (EntityKind(mention.requested_entity_kind.value),)
                    ),
                    exact=mention.normalized_lookup_text in context_ref_ids,
                )
                for mention in mentions
            )
        )

        ledger = self._build_ledger(
            mentions=mentions, resolutions=resolutions, resolved_at=generated_at
        )
        ledger = self._apply_context_tiebreaker(
            ledger=ledger,
            mentions=mentions,
            resolutions=resolutions,
            context_ref_ids=context_ref_ids,
            resolved_at=generated_at,
        )
        ledger = self._apply_catalog_reuse(
            ledger=ledger,
            mentions=mentions,
            resolutions=resolutions,
            authorized_scope=authorized_scope,
            resolved_at=generated_at,
        )
        latest = ledger.latest_by_mention()
        blocking_ids = frozenset(
            mention.mention_id
            for mention in mentions
            if mention.mention_id not in untyped_ids
        )
        unresolved_untyped = any(
            latest[mention.mention_id].outcome in UNRESOLVED_OUTCOMES
            for mention in mentions
            if mention.mention_id in untyped_ids
        )
        # D2 (Amendment TRD v2 line 154, CHAOS-3301): for a plural/cohort
        # question with at least two *distinct* exactly-resolved mentions, an
        # unresolved typed mention no longer terminates the run — it is
        # recorded as omitted on the eventual subject set instead. Singular
        # behavior (one named subject) is unchanged: the lowest-ordinal
        # unresolved mention still terminates immediately, exactly as A6
        # established. Stable and explainable ("the first thing you named"),
        # and independent of catalog latency — a severity ordering would let
        # a slow catalog change the reported outcome between runs, which
        # directly breaks determinism.
        #
        # The threshold counts UNIQUE committed entities, deduped by (kind,
        # canonical id) here rather than raw exact-match mentions (CHAOS-3301
        # review fix): two aliases of the same entity plus one unresolved
        # mention previously satisfied ">= 2 exact matches" and skipped the
        # termination loop below even though only one distinct subject had
        # resolved, letting the unresolved mention slip past without being
        # accounted for anywhere. With one distinct entity, D2 must not
        # activate — the pre-D2 lowest-ordinal termination applies instead.
        blocking_committed_entities: dict[tuple[EntityKind, str], AuthorizedEntity] = {}
        for mention in mentions:
            if mention.mention_id not in blocking_ids:
                continue
            entry = latest[mention.mention_id]
            if entry.outcome is ResolutionOutcome.EXACT_MATCH:
                entity = self._authorized_entity_for(entry)
                blocking_committed_entities.setdefault(
                    (entity.kind, entity.canonical_id), entity
                )
        cohort_may_proceed_partial = (
            intent.cardinality is Cardinality.PLURAL_COHORT
            # CHAOS-3393 codex MED-1: D2's own kind-blind ">=2 distinct
            # resolved entities" relaxation must never apply to
            # PORTFOLIO_STATUS -- status.portfolio.v1 can only ever batch
            # PROJECT subjects (see the homogeneous-cohort gate below), so
            # an omitted mention here could be a TEAM (or any other kind)
            # D2 has no way to notice, silently dropped while the run
            # proceeds to REAL EXECUTION on the mentions that did resolve.
            # Every other CHAOS-3301 cohort intent still terminates
            # UNSUPPORTED regardless of this flag (D2 only ever changes
            # whether an omission is disclosed-and-committed vs.
            # terminal, never whether the v1 surface can render it), so
            # this is safe to tighten for PORTFOLIO_STATUS alone: portfolio
            # execution requires every typed mention to resolve exactly,
            # full stop.
            and intent.intent_id is not QuestionIntentID.PORTFOLIO_STATUS
            and len(blocking_committed_entities) >= 2
        )
        if not cohort_may_proceed_partial:
            # The lowest-ordinal unresolved blocking mention — "the first
            # thing you named" — is the one and only mention that can
            # terminate this run. Selecting it *before* the CHAOS-3366
            # fallback, rather than searching every unresolved mention and
            # then discovering which one mattered, is what keeps the fallback
            # at exactly one bounded catalog round trip per question instead
            # of up to MAX_MENTIONS of them (Codex review, medium: a
            # 25-mention question issued 25 serialized wide searches, 24 of
            # whose results were then discarded unread).
            #
            # Selecting it here is safe because none of the three amenders can
            # change *which* mention it is: the first two only ever produce
            # EXACT_MATCH (removing a mention from contention, never adding
            # one) and they have already run; the fallback only ever rewrites
            # NO_AUTHORIZED_MATCH to AMBIGUOUS_CANDIDATES, and both are
            # unresolved outcomes, so the selected mention stays selected.
            terminating = next(
                (
                    mention
                    for mention in mentions
                    if mention.mention_id in blocking_ids
                    and latest[mention.mention_id].outcome in UNRESOLVED_OUTCOMES
                ),
                None,
            )
            if terminating is not None:
                is_close_match = False
                if (
                    latest[terminating.mention_id].outcome
                    is ResolutionOutcome.NO_AUTHORIZED_MATCH
                ):
                    # Last of the three ledger amenders, and the only one that
                    # adds candidates rather than commits. Gated on
                    # NO_AUTHORIZED_MATCH specifically: a mention that is
                    # already ambiguous has real candidates, and one whose
                    # catalog read failed has no successful answer to enrich.
                    ledger, is_close_match = await self._apply_not_found_fallback(
                        ledger=ledger,
                        mention=terminating,
                        org_id=org_id,
                        permission_fingerprint=permission_fingerprint,
                        resolved_at=generated_at,
                    )
                    latest = ledger.latest_by_mention()
                entry = latest[terminating.mention_id]
                # CHAOS-3366: an amended mention reaches this point as
                # AMBIGUOUS_CANDIDATES like any other, so the outcome mapping
                # and the clarification channel are untouched -- only the
                # diagnostic and the copy key distinguish "we found nothing
                # under that name, here is what is close" from genuine
                # same-name ambiguity.
                return self._terminate(
                    interpretation=interpretation,
                    ledger=ledger,
                    outcome=PREFLIGHT_OUTCOME_BY_RESOLUTION[entry.outcome],
                    diagnostic=(
                        "unresolved_close_matches"
                        if is_close_match
                        else f"unresolved_{entry.outcome.value}"
                    ),
                    run_id=run_id,
                    answer_id=answer_id,
                    conversation_id=conversation_id,
                    generated_at=generated_at,
                    clarification_key=(
                        NOT_FOUND_CLOSE_MATCHES_KEY if is_close_match else "ambiguous"
                    ),
                    # CHAOS-3325: only AMBIGUOUS_CANDIDATES carries real
                    # candidates to persist/authorize (the other three
                    # UNRESOLVED_OUTCOMES map to no-answer outcomes,
                    # where clarification_candidates is ABSENT-forced
                    # regardless) -- passing the whole entry, not just
                    # its candidates, is what lets the orchestrator
                    # persist the exact ledger row record_frame's new
                    # cross-check authorizes against.
                    terminating_resolution_entry=(
                        entry
                        if entry.outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES
                        else None
                    ),
                )

        if unresolved_untyped:
            # A bare name we could not resolve is not proof of a subject, so
            # blocking here would break questions like "what is our DORA
            # score?". The run continues organization-wide — today's behaviour
            # — with the legacy backstop re-armed as terminal, which judges the
            # model's own answer text and so fires only if the answer actually
            # narrates the unresolved name.
            #
            # It must continue **organization-wide specifically**. Simply not
            # committing anything leaves the run holding whatever scope it
            # arrived with, and on a page-scoped request that is a concrete
            # subject: "How is Nightfall doing?" asked from an Ask Dev page
            # would execute status against Ask Dev and answer under the name
            # Nightfall — the same misattribution one layer down. Widening to
            # the organization is not a privilege change (the org is the
            # authenticated tenant, and every native query is bound by org_id
            # anyway); it removes the specific entity that would otherwise be
            # silently narrated as the named one.
            return SubjectPreflightResult(
                decision=PreflightDecision.PROCEED,
                interpretation=interpretation,
                ledger=ledger,
                committed_resolution=self._organization_resolution(
                    authorized_scope, resolved_at=generated_at
                ),
                answer=None,
                outcome=None,
                allowed_tools=ALL_TOOLS,
                blocking_mention_ids=blocking_ids,
                legacy_guard_required=True,
                unresolved_name_spans=frozenset(
                    mention.normalized_lookup_text
                    for mention in mentions
                    if mention.mention_id in untyped_ids
                    and latest[mention.mention_id].outcome in UNRESOLVED_OUTCOMES
                ),
                diagnostic="proceeded_unresolved_bare_name",
            )

        # All exactly-resolved mentions, not only blocking (typed) ones: by
        # this point any untyped mention is either exact_match or absent
        # entirely (`unresolved_untyped` already returned above otherwise),
        # so a resolved bare name commits exactly as a resolved typed one
        # does — matching pre-CHAOS-3301 behavior (A-series "resolvable bare
        # name commits a subject").
        committed_entries = [
            (mention, latest[mention.mention_id])
            for mention in mentions
            if latest[mention.mention_id].outcome is ResolutionOutcome.EXACT_MATCH
        ]
        kinds: set[ContractEntityKind] = set()
        for _mention, entry in committed_entries:
            assert entry.committed_entity_ref is not None  # exact_match guarantees this
            kinds.add(entry.committed_entity_ref.entity_kind)
        if any(EntityKind(kind.value) not in DIRECT_SCOPE_KINDS for kind in kinds):
            # A defensive totality check, not a live branch today: every
            # SEARCHABLE_ENTITY_KINDS member (including TEAM, CHAOS-3301) has
            # a v1 DevScope representation. Guards against a future v2-only
            # kind reaching here before it gets one.
            return self._terminate(
                interpretation=interpretation,
                ledger=ledger,
                outcome=PublicOutcome.UNSUPPORTED,
                diagnostic="committed_kind_unsupported_in_v1",
                run_id=run_id,
                answer_id=answer_id,
                conversation_id=conversation_id,
                generated_at=generated_at,
            )

        # Dedup by (kind, canonical id): duplicate aliases of the same entity
        # collapse to one committed subject (N4) — the *distinct* subject
        # count is what determines singular-vs-cohort, not the raw mention
        # count. Insertion order is preserved (first-seen entity wins the
        # position), which keeps this deterministic across runs.
        unique_by_id: dict[tuple[EntityKind, str], AuthorizedEntity] = {}
        for _mention, entry in committed_entries:
            entity = self._authorized_entity_for(entry)
            unique_by_id.setdefault((entity.kind, entity.canonical_id), entity)
        unique_entities = list(unique_by_id.values())
        unique_kinds = {entity.kind for entity in unique_entities}

        if len(unique_entities) > 1:
            if len(unique_kinds) > 1:
                # A heterogeneous set has no faithful v1 representation and
                # is a non-goal outright (never even a committed set): a
                # ``dev_subject_set.v1`` must be homogeneous in entity kind.
                # The landed v2-to-v1 projector reaches the same conclusion
                # independently for ``subject_set_ref`` ("Cohort-scoped Ask
                # Dev answers require a newer client").
                return self._terminate(
                    interpretation=interpretation,
                    ledger=ledger,
                    outcome=PublicOutcome.UNSUPPORTED,
                    diagnostic="cohort_unsupported_in_v1",
                    run_id=run_id,
                    answer_id=answer_id,
                    conversation_id=conversation_id,
                    generated_at=generated_at,
                )
            # A homogeneous, bounded cohort: commit and persist the subject
            # set, but the v1 surface still returns unsupported (D1) —
            # rendering a cohort answer is CHAOS-3297/3298's job, not this
            # issue's. "committed" (not "cohort_unsupported") is the honest
            # distinction: we did commit every resolvable member, and cannot
            # render it here, rather than refusing outright.
            #
            # Omissions are partitioned by outcome (CHAOS-3301 review fix):
            # AMBIGUOUS_CANDIDATES is a distinct outcome from no-match/
            # unavailable/unsupported-kind, and the set contract keeps them
            # in separate fields (`ambiguous_mention_ids` vs
            # `unresolved_mention_ids`) so a reader — and any later
            # ambiguity-disambiguation flow — can tell "we found nothing" from
            # "we found more than one" without re-deriving it from the ledger.
            omitted_blocking_entries = [
                (mention.mention_id, latest[mention.mention_id].outcome)
                for mention in mentions
                if mention.mention_id in blocking_ids
                and latest[mention.mention_id].outcome in UNRESOLVED_OUTCOMES
            ]
            unresolved_ids = tuple(
                mention_id
                for mention_id, outcome in omitted_blocking_entries
                if outcome is not ResolutionOutcome.AMBIGUOUS_CANDIDATES
            )
            ambiguous_ids = tuple(
                mention_id
                for mention_id, outcome in omitted_blocking_entries
                if outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES
            )
            warnings: tuple[str, ...] = ()
            if unresolved_ids:
                warnings += (
                    "one or more named subjects could not be resolved and "
                    "were omitted from this set",
                )
            if ambiguous_ids:
                warnings += (
                    "one or more named subjects were ambiguous and were "
                    "omitted from this set",
                )
            subject_set = self._scope_service.committed_subject_set_for(
                unique_entities,
                set_id=self._mint_id(),
                original_mention_count=len(mentions),
                unresolved_mention_ids=unresolved_ids,
                ambiguous_mention_ids=ambiguous_ids,
                warnings=warnings,
            )
            # CHAOS-3393: a homogeneous PROJECT cohort named under the
            # PORTFOLIO_STATUS intent PROCEEDs -- status.portfolio.v1 can
            # render it. Gated on BOTH intent and kind: every other
            # homogeneous cohort (a different intent's plural mention, or a
            # PORTFOLIO_STATUS-recognized question naming only teams) still
            # terminates UNSUPPORTED below, unchanged. `unique_kinds` is a
            # single-member set by construction here (the `len(unique_kinds)
            # > 1` heterogeneous branch above already returned otherwise).
            if (
                intent.intent_id is QuestionIntentID.PORTFOLIO_STATUS
                and next(iter(unique_kinds)) is EntityKind.PROJECT
            ):
                return SubjectPreflightResult(
                    decision=PreflightDecision.PROCEED,
                    interpretation=interpretation,
                    ledger=ledger,
                    committed_resolution=None,
                    committed_subjects=CommittedSubjects(
                        resolution=None, subject_set=subject_set
                    ),
                    subject_set=subject_set,
                    answer=None,
                    outcome=None,
                    allowed_tools=ALL_TOOLS,
                    blocking_mention_ids=blocking_ids,
                    diagnostic="committed_cohort_portfolio_v1",
                )
            return self._terminate(
                interpretation=interpretation,
                ledger=ledger,
                outcome=PublicOutcome.UNSUPPORTED,
                diagnostic="committed_cohort_v1_only",
                run_id=run_id,
                answer_id=answer_id,
                conversation_id=conversation_id,
                generated_at=generated_at,
                subject_set=subject_set,
            )

        # Exactly one *distinct* committed subject — a singular commit, even
        # if the question named it more than once (N4: duplicate aliases).
        entity = unique_entities[0]
        committed_resolution = self._scope_service.committed_resolution_for(
            entity,
            org_id=org_id,
            base_scope=authorized_scope,
            resolved_at=generated_at,
        )
        # The subject set here is audit-only: it exists so a duplicate-alias
        # question's original mention count is not lost, not because this run
        # is a cohort — it never blocks execution, unlike the >1-distinct
        # branch above.
        audit_subject_set: DevSubjectSet | None = (
            self._scope_service.committed_subject_set_for(
                unique_entities,
                set_id=self._mint_id(),
                original_mention_count=len(mentions),
            )
            if len(mentions) > 1
            else None
        )
        return SubjectPreflightResult(
            decision=PreflightDecision.PROCEED,
            interpretation=interpretation,
            ledger=ledger,
            committed_resolution=committed_resolution,
            committed_subjects=CommittedSubjects(
                resolution=committed_resolution, subject_set=audit_subject_set
            ),
            subject_set=audit_subject_set,
            answer=None,
            outcome=None,
            # With a subject already committed there is nothing left for the
            # model to resolve, so ``resolve_scope.v1`` is withheld — which is
            # also what makes the ``forbidden_or_not_found`` tool-result leak
            # channel unreachable on this path, without renaming a token that
            # five published v1 schemas carry.
            allowed_tools=ALL_TOOLS - {ToolID.RESOLVE_SCOPE},
            blocking_mention_ids=frozenset(mention.mention_id for mention in mentions),
            diagnostic="proceeded_committed_subject",
        )

    # -- construction helpers -------------------------------------------------

    @staticmethod
    def _organization_resolution(
        authorized_scope: DevScope, *, resolved_at: datetime
    ) -> DevScopeResolution:
        """The run's own organization, with any page-derived subject removed.

        Keeps the request's time window (the question is still about that
        period) and drops the entity refs, repositories, team filters and
        surface context that name a specific subject.
        """

        scope = DevScope(
            schema_version="dev_scope.v1",
            organization_id=authorized_scope.organization_id,
            direct_scope=DirectScope.ORGANIZATION,
            repositories=[],
            entity_refs=[],
            team_ids=[],
            time_range=authorized_scope.time_range,
            comparison_range=authorized_scope.comparison_range,
            surface_context=None,
        )
        return DevScopeResolution(
            schema_version="dev_scope_resolution.v1",
            requested_scope=authorized_scope,
            resolved_scope=scope,
            outcome=ScopeResolutionOutcome.ORGANIZATION_FALLBACK,
            authorized_repository_ids=[],
            authorized_entity_ids=[],
            candidates=[],
            fallbacks=["organization"],
            warnings=[],
            resolved_at=resolved_at,
        )

    async def _organization_wide_portfolio_result(
        self,
        *,
        interpretation: InterpretedQuestion,
        org_id: str,
        permission_fingerprint: str,
        generated_at: datetime,
        run_id: str,
        answer_id: str,
        conversation_id: str,
    ) -> SubjectPreflightResult:
        """CHAOS-3393: an ORGANIZATION_WIDE ``status.portfolio.v1`` question
        named no subjects at all, so there is nothing for the usual mention-
        resolution machinery to resolve. A bounded, deterministic project
        enumeration (label then id, capped at
        ``portfolio_status_service.MAX_PORTFOLIO_PROJECTS``) stands in for
        it instead, committed as a real ``dev_subject_set.v1`` -- the same
        contract a named cohort commits -- so the plan executor has
        something to batch over.

        Zero authorized projects falls back to the ordinary organization-
        wide PROCEED (no committed subject at all) rather than failing: a
        ``DevSubjectSet`` requires at least one committed entity, so there
        is nothing valid to commit, and "no projects to report on" is not
        a preflight failure.

        A truncated enumeration (more authorized projects than the cap)
        is disclosed via the subject set's own ``warnings`` -- never a
        silent sample: ``DevSubjectSet.cohort_complete`` cannot itself
        express "capped, not omitted" (that field's own invariant is about
        omitted *mentions*, and an org-wide enumeration names none), so
        cap truncation rides the warnings channel instead, exactly like a
        partial cohort's own unresolved-mention disclosure does.

        CHAOS-3393 codex MED-3: a catalog OUTAGE (``catalog_available``
        False) is never treated as "zero authorized projects" -- that
        conflation used to fall back to the SAME ordinary organization-wide
        PROCEED as a confirmed-empty catalog, silently granting the
        unrestricted ``ALL_TOOLS`` legacy loop during a transient failure
        the caller had no way to detect. An outage instead terminates,
        fails closed (zero tools), and discloses a bounded, honest
        "temporarily unavailable" -- never a fabricated or substituted
        organization-wide answer.
        """

        (
            entities,
            total,
            catalog_available,
        ) = await self._scope_service.organization_committed_projects(
            org_id, permission_fingerprint, limit=MAX_PORTFOLIO_PROJECTS
        )
        if not catalog_available:
            return self._terminate(
                interpretation=interpretation,
                ledger=None,
                outcome=PublicOutcome.TEMPORARILY_UNAVAILABLE,
                diagnostic="portfolio_catalog_unavailable",
                run_id=run_id,
                answer_id=answer_id,
                conversation_id=conversation_id,
                generated_at=generated_at,
            )
        if not entities:
            return SubjectPreflightResult(
                decision=PreflightDecision.PROCEED,
                interpretation=interpretation,
                ledger=None,
                committed_resolution=None,
                answer=None,
                outcome=None,
                allowed_tools=ALL_TOOLS,
                diagnostic="proceeded_organization_wide",
            )
        truncated = total > len(entities)
        warnings = (
            (
                f"the organization has {total} authorized projects; only the "
                f"first {len(entities)} (by name, then id) are included in "
                "this portfolio",
            )
            if truncated
            else ()
        )
        subject_set = self._scope_service.committed_subject_set_for(
            entities,
            set_id=self._mint_id(),
            original_mention_count=len(entities),
            warnings=warnings,
        )
        return SubjectPreflightResult(
            decision=PreflightDecision.PROCEED,
            interpretation=interpretation,
            ledger=None,
            committed_resolution=None,
            committed_subjects=CommittedSubjects(
                resolution=None, subject_set=subject_set
            ),
            subject_set=subject_set,
            answer=None,
            outcome=None,
            allowed_tools=ALL_TOOLS,
            diagnostic="committed_portfolio_org_wide",
        )

    @staticmethod
    def _context_ref_ids(request) -> frozenset[str]:
        refs = list(request.scope.entity_refs)
        if request.scope.surface_context is not None:
            refs.extend(request.scope.surface_context.entity_refs)
        return frozenset(ref.entity_id for ref in refs)

    def _build_ledger(
        self,
        *,
        mentions: Sequence[DevSubjectMention],
        resolutions: Sequence[MentionResolution],
        resolved_at: datetime,
    ) -> DevResolutionLedger:
        entries = tuple(
            self._entry(
                ordinal=ordinal,
                mention=mention,
                resolution=resolution,
                resolved_at=resolved_at,
            )
            for ordinal, (mention, resolution) in enumerate(
                zip(mentions, resolutions, strict=True)
            )
        )
        return DevResolutionLedger(
            schema_version="dev_resolution_ledger.v1",
            ledger_id=self._mint_id(),
            mention_ids=tuple(mention.mention_id for mention in mentions),
            entries=entries,
            updated_at=resolved_at,
        )

    def _entry(
        self,
        *,
        ordinal: int,
        mention: DevSubjectMention,
        resolution: MentionResolution,
        resolved_at: datetime,
        candidate_reason: str = "Multiple authorized entities match this name.",
    ) -> DevResolutionEntry:
        committed = (
            _entity_ref_v2(resolution.entity) if resolution.entity is not None else None
        )
        candidates = tuple(
            DevResolutionCandidate(
                entity_ref=_entity_ref_v2(candidate),
                reason=candidate_reason,
            )
            for candidate in resolution.candidates
        )
        return DevResolutionEntry(
            entry_ordinal=ordinal,
            mention_id=mention.mention_id,
            outcome=resolution.outcome,
            committed_entity_ref=committed,
            candidates=candidates,
            repository_attribution=(
                resolution.entity.repository_id if resolution.entity else None
            ),
            team_attribution=(
                resolution.entity.canonical_id
                if resolution.entity is not None
                and resolution.entity.kind is EntityKind.TEAM
                else None
            ),
            resolver_version=resolution.query_version,
            query_version=resolution.catalog_watermark,
            resolved_at=resolved_at,
        )

    def _append(
        self,
        ledger: DevResolutionLedger,
        entries: Sequence[DevResolutionEntry],
        *,
        resolved_at: datetime,
    ) -> DevResolutionLedger:
        """Append entries and prove the result extends the previous snapshot.

        ``validate_ledger_extends`` is called on every update, not only at the
        persistence boundary: it is the mechanical proof that a later success
        cannot erase an earlier unresolved mention, and it is cheap enough to
        run at the one place that constructs a successor ledger.
        """

        if not entries:
            return ledger
        candidate = DevResolutionLedger(
            schema_version="dev_resolution_ledger.v1",
            ledger_id=ledger.ledger_id,
            mention_ids=ledger.mention_ids,
            entries=ledger.entries + tuple(entries),
            updated_at=resolved_at,
        )
        validate_ledger_extends(ledger, candidate)
        return candidate

    def _apply_context_tiebreaker(
        self,
        *,
        ledger: DevResolutionLedger,
        mentions: Sequence[DevSubjectMention],
        resolutions: Sequence[MentionResolution],
        context_ref_ids: frozenset[str],
        resolved_at: datetime,
    ) -> DevResolutionLedger:
        """Break an ambiguity with the page's own typed refs — never override one.

        A context ref may only pick between candidates the catalog itself
        returned for the name the user typed. It can never introduce an entity
        the user did not name, and it can never overturn an exact match.
        """

        appended: list[DevResolutionEntry] = []
        next_ordinal = len(ledger.entries)
        for mention, resolution in zip(mentions, resolutions, strict=True):
            if resolution.outcome is not ResolutionOutcome.AMBIGUOUS_CANDIDATES:
                continue
            matches = [
                candidate
                for candidate in resolution.candidates
                if candidate.canonical_id in context_ref_ids
            ]
            if len(matches) != 1:
                continue
            appended.append(
                self._entry(
                    ordinal=next_ordinal,
                    mention=mention,
                    resolution=MentionResolution(
                        outcome=ResolutionOutcome.EXACT_MATCH,
                        entity=matches[0],
                        candidates=(),
                        catalog_watermark=resolution.catalog_watermark,
                        query_version=resolution.query_version,
                    ),
                    resolved_at=resolved_at,
                )
            )
            next_ordinal += 1
        return self._append(ledger, appended, resolved_at=resolved_at)

    def _apply_catalog_reuse(
        self,
        *,
        ledger: DevResolutionLedger,
        mentions: Sequence[DevSubjectMention],
        resolutions: Sequence[MentionResolution],
        authorized_scope: DevScope,
        resolved_at: datetime,
    ) -> DevResolutionLedger:
        """A catalog outage downgrades to "proceed" only on an exact prior commit.

        The reuse is admissible only when this run already holds a committed,
        same-kind direct scope whose entity *is* the named one. Anything else
        stays ``catalog_unavailable`` and terminates as temporarily
        unavailable — a stale guess about a named subject is the same defect as
        an unresolved one.
        """

        appended: list[DevResolutionEntry] = []
        next_ordinal = len(ledger.entries)
        for mention, resolution in zip(mentions, resolutions, strict=True):
            if resolution.outcome is not ResolutionOutcome.CATALOG_UNAVAILABLE:
                continue
            reusable = self._reusable_entity(mention, authorized_scope)
            if reusable is None:
                continue
            appended.append(
                self._entry(
                    ordinal=next_ordinal,
                    mention=mention,
                    resolution=MentionResolution(
                        outcome=ResolutionOutcome.EXACT_MATCH,
                        entity=reusable,
                        candidates=(),
                        catalog_watermark=resolution.catalog_watermark,
                        query_version=resolution.query_version,
                    ),
                    resolved_at=resolved_at,
                )
            )
            next_ordinal += 1
        return self._append(ledger, appended, resolved_at=resolved_at)

    async def _apply_not_found_fallback(
        self,
        *,
        ledger: DevResolutionLedger,
        mention: DevSubjectMention,
        org_id: str,
        permission_fingerprint: str,
        resolved_at: datetime,
    ) -> tuple[DevResolutionLedger, bool]:
        """Offer the closest authorized matches instead of a bare not-found.

        CHAOS-3366. A *typed* mention that matched nothing under the kind the
        user named is the one case where the catalog may still hold the thing
        they meant, one kind over: the live organization has 23 work items
        titled ``Go workers…`` and no project of that name, so "the Go workers
        project" dead-ends today with nothing to act on.

        Four properties are load-bearing, and each has its own test:

        * **Never a commit.** The appended entry is always
          ``AMBIGUOUS_CANDIDATES`` — including for a *sole* result whose label
          equals the typed name outright. That result is still not what was
          asked for (the user named a kind it does not have), and
          auto-committing it would be exactly the "answer about one entity
          under another's name" defect this module exists to prevent. The
          invariant is structural: there is no branch here that constructs
          ``EXACT_MATCH``.
        * **Bounded and tenant-scoped.** Exactly one search, for exactly one
          mention — the caller has already established that this is the
          mention that terminates the run — capped at
          ``NOT_FOUND_FALLBACK_LIMIT`` and issued through the same
          ``ScopeResolutionService.search`` seam every other caller uses, so
          it inherits the ``org_id`` filter every ``scope_catalog`` query
          applies in SQL. No organization fallback, no widening.
        * **Never worse than today.** No candidates, or a catalog that fails
          under us, leaves the mention exactly as ``NO_AUTHORIZED_MATCH`` and
          the run terminates ``not_found`` unchanged.
        * **Append-only.** Like both amenders above it, the new entry goes
          through ``_append``/``validate_ledger_extends``, so the original
          ``no_authorized_match`` entry survives as history.

        The search covers *every* searchable kind, not only the other five.
        For an ordinary mention the named kind is provably empty already (it
        is why we are here), so re-including it costs one no-op query; for a
        context-ref mention, which is resolved with ``exact=True`` and so
        never ran a fuzzy search at all, it is the only way its own kind gets
        searched — and that path can legitimately return a *same-kind*
        candidate, which is why the offered reason below says nothing about
        kinds.
        """

        resolution = await self._close_matches(
            org_id=org_id,
            permission_fingerprint=permission_fingerprint,
            lookup_text=mention.normalized_lookup_text,
        )
        if resolution is None:
            return ledger, False
        if resolution.outcome is not ResolutionOutcome.AMBIGUOUS_CANDIDATES:
            # The ratified invariant, stated where it can be violated rather
            # than left to be discovered three layers down. Unreachable today
            # -- ``_close_matches`` has no branch that builds anything else --
            # but "never auto-commit a fuzzy match" is the whole reason this
            # amender is allowed to exist, and an unreachable state that is
            # merely *implied* by construction stops being implied the moment
            # someone edits the constructor.
            raise RuntimeError(  # pragma: no cover - guarded by _close_matches
                "the not-found fallback may never commit a subject"
            )
        entry = self._entry(
            ordinal=len(ledger.entries),
            mention=mention,
            resolution=resolution,
            resolved_at=resolved_at,
            candidate_reason=_CLOSE_MATCH_REASON,
        )
        return self._append(ledger, [entry], resolved_at=resolved_at), True

    async def _close_matches(
        self,
        *,
        org_id: str,
        permission_fingerprint: str,
        lookup_text: str,
    ) -> MentionResolution | None:
        """One bounded tenant-scoped search, or ``None`` for "nothing to offer".

        A catalog failure here is deliberately *not* typed as
        ``CATALOG_UNAVAILABLE``: the mention already has a real, successful
        ``NO_AUTHORIZED_MATCH`` outcome from the resolution round, and
        downgrading a definite answer to "temporarily unavailable" because an
        optional enrichment failed would make the run's reported outcome worse
        than it was before this method existed.
        """

        query = lookup_text.strip()[:256]
        if not query:
            return None
        # Built outside the try, exactly as ``resolve_mention`` does: a
        # malformed request is a caller defect, not a catalog outage.
        #
        # ``limit`` is the *only* place the bound is applied on this path, on
        # purpose. ``search`` already truncates its own result to
        # ``request.limit`` and the catalog applies it again in SQL, so a
        # second slice here would be redundant defence that also makes the
        # real clause unkillable: mutate the bound and an output-only
        # assertion still passes. One clause, one witness.
        request = ScopeSearchRequest(
            query=query,
            kinds=tuple(sorted(SEARCHABLE_ENTITY_KINDS, key=lambda kind: kind.value)),
            limit=NOT_FOUND_FALLBACK_LIMIT,
            allowed_kinds=SEARCHABLE_ENTITY_KINDS,
            # CHAOS-3388: this is the one seam allowed to widen the search to
            # acronym/parenthetical-alias matches -- the mention is already
            # confirmed unresolved under its literal name, so an acronym hit
            # here is additional "closest matches" material, never a
            # commit-eligible primary resolution (see ScopeSearchRequest.
            # include_alias_matches).
            include_alias_matches=True,
        )
        try:
            result = await self._scope_service.search(
                org_id, permission_fingerprint, request
            )
        except Exception:
            return None
        candidates = result.candidates
        if not candidates:
            return None
        return MentionResolution(
            outcome=ResolutionOutcome.AMBIGUOUS_CANDIDATES,
            entity=None,
            candidates=candidates,
            catalog_watermark=result.catalog_watermark or _UNAVAILABLE_WATERMARK,
            query_version=result.query_version,
        )

    @staticmethod
    def _reusable_entity(
        mention: DevSubjectMention, authorized_scope: DevScope
    ) -> AuthorizedEntity | None:
        wanted = mention.normalized_lookup_text.casefold()
        if authorized_scope.direct_scope.value != mention.requested_entity_kind.value:
            return None
        for ref in authorized_scope.entity_refs:
            if wanted in {ref.entity_id.casefold(), ref.display_label.casefold()}:
                return AuthorizedEntity(
                    kind=EntityKind(ref.entity_type.value),
                    canonical_id=ref.entity_id,
                    label=ref.display_label,
                    repository_id=ref.repository_id,
                )
        for repository_id in authorized_scope.repositories:
            if wanted == repository_id.casefold():
                return AuthorizedEntity(
                    kind=EntityKind.REPOSITORY,
                    canonical_id=repository_id,
                    label=repository_id,
                    repository_id=repository_id,
                )
        return None

    @staticmethod
    def _authorized_entity_for(entry: DevResolutionEntry) -> AuthorizedEntity:
        ref = entry.committed_entity_ref
        if ref is None:  # pragma: no cover - guarded by the caller
            raise RuntimeError("an exact match must carry a committed entity ref")
        return AuthorizedEntity(
            kind=EntityKind(ref.entity_kind.value),
            canonical_id=ref.entity_id,
            label=ref.display_label,
            repository_id=ref.repository_id,
        )

    def _terminate(
        self,
        *,
        interpretation: InterpretedQuestion,
        ledger: DevResolutionLedger | None,
        outcome: PublicOutcome,
        diagnostic: str,
        run_id: str,
        answer_id: str,
        conversation_id: str,
        generated_at: datetime,
        clarification_key: str = "ambiguous",
        subject_set: DevSubjectSet | None = None,
        terminating_resolution_entry: DevResolutionEntry | None = None,
    ) -> SubjectPreflightResult:
        # CHAOS-3325: the frame's clarification_candidates and the result's
        # terminating_resolution_entry are always derived from the same
        # single entry -- never set independently -- so a caller cannot
        # attach candidates to the frame without also giving the
        # orchestrator the ledger row that authorizes them.
        answer = build_preflight_answer(
            outcome=outcome,
            intent_id=interpretation.intent.intent_id,
            versions=self._versions,
            run_id=run_id,
            answer_id=answer_id,
            conversation_id=conversation_id,
            generated_at=generated_at,
            clarification_key=clarification_key,
            clarification_candidates=(
                terminating_resolution_entry.candidates
                if terminating_resolution_entry is not None
                else ()
            ),
        )
        return SubjectPreflightResult(
            decision=PreflightDecision.TERMINATE,
            interpretation=interpretation,
            ledger=ledger,
            committed_resolution=None,
            committed_subjects=(
                CommittedSubjects(resolution=None, subject_set=subject_set)
                if subject_set is not None
                else None
            ),
            subject_set=subject_set,
            answer=answer,
            outcome=outcome,
            allowed_tools=frozenset(),
            diagnostic=diagnostic,
            terminating_resolution_entry=terminating_resolution_entry,
        )
