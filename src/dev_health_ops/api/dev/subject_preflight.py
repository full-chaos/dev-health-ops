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
    ResolutionOutcome,
    validate_ledger_extends,
)
from .contracts_v2 import (
    EntityKind as ContractEntityKind,
)
from .contracts_v2.subject import UNRESOLVED_OUTCOMES
from .orchestrator_states import RunState
from .preflight_outcomes import (
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
)

__all__ = [
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
)


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
            and len(blocking_committed_entities) >= 2
        )
        if not cohort_may_proceed_partial:
            for mention in mentions:
                if mention.mention_id not in blocking_ids:
                    continue
                entry = latest[mention.mention_id]
                if entry.outcome in UNRESOLVED_OUTCOMES:
                    return self._terminate(
                        interpretation=interpretation,
                        ledger=ledger,
                        outcome=PREFLIGHT_OUTCOME_BY_RESOLUTION[entry.outcome],
                        diagnostic=f"unresolved_{entry.outcome.value}",
                        run_id=run_id,
                        answer_id=answer_id,
                        conversation_id=conversation_id,
                        generated_at=generated_at,
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
    ) -> DevResolutionEntry:
        committed = (
            _entity_ref_v2(resolution.entity) if resolution.entity is not None else None
        )
        candidates = tuple(
            DevResolutionCandidate(
                entity_ref=_entity_ref_v2(candidate),
                reason="Multiple authorized entities match this name.",
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
