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
* It never falls back to organization scope for an unresolved named subject.
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

from .contracts import DevContractVersions, DevScope, DevScopeResolution, ToolID
from .contracts_v2 import (
    DevAnswerV2,
    DevEntityRefV2,
    DevResolutionCandidate,
    DevResolutionEntry,
    DevResolutionLedger,
    DevSubjectMention,
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
from .question_interpreter import InterpretedQuestion, QuestionInterpreter
from .scope_service import (
    V1_SEARCHABLE_ENTITY_KINDS,
    AuthorizedEntity,
    EntityKind,
    MentionResolution,
    ScopeResolutionService,
)

__all__ = [
    "SUBJECT_BEARING_TOOLS",
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


@dataclass(frozen=True, slots=True)
class SubjectPreflightResult:
    decision: PreflightDecision
    interpretation: InterpretedQuestion
    #: ``None`` only when the question named no subject at all.
    ledger: DevResolutionLedger | None
    #: The server-committed scope, when exactly one subject resolved exactly.
    committed_resolution: DevScopeResolution | None
    #: Present only for ``TERMINATE``.
    answer: DevAnswerV2 | None
    outcome: PublicOutcome | None
    #: The per-run tool allowlist the model round is held to.
    allowed_tools: frozenset[ToolID]
    #: Content-free code recorded on the run row. Never question or entity text.
    diagnostic: str

    @property
    def all_subjects_committed(self) -> bool:
        """Whether every mention currently holds an ``exact_match``."""

        if self.ledger is None:
            return True
        return all(
            entry.outcome is ResolutionOutcome.EXACT_MATCH
            for entry in self.ledger.latest_by_mention().values()
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
                diagnostic="interpreter_clarification_required",
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

        await phase(RunState.RESOLVING_SUBJECTS)
        context_ref_ids = self._context_ref_ids(request)
        resolutions = await asyncio.gather(
            *(
                self._scope_service.resolve_mention(
                    org_id,
                    permission_fingerprint,
                    lookup_text=mention.normalized_lookup_text,
                    kinds=(EntityKind(mention.requested_entity_kind.value),),
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
        # Precedence: the lowest-ordinal unresolved mention wins. Stable and
        # explainable ("the first thing you named"), and independent of catalog
        # latency — a severity ordering would let a slow catalog change the
        # reported outcome between runs, which directly breaks determinism.
        for mention in mentions:
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
                )

        committed = [
            latest[mention.mention_id].committed_entity_ref for mention in mentions
        ]
        kinds = {ref.entity_kind for ref in committed if ref is not None}
        if any(
            EntityKind(kind.value) not in V1_SEARCHABLE_ENTITY_KINDS for kind in kinds
        ):
            # Interim TEAM semantics (CHAOS-3301 owns the rest): the ledger
            # already records exact_match with a team ref — the team
            # demonstrably exists — but no v1 DevScope can carry a team
            # subject. Never not_found, never organization fallback.
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
        if len(committed) > 1:
            # A cohort has no faithful v1 representation either: a v1 DevScope
            # names one direct subject. The landed v2-to-v1 projector reaches
            # the same conclusion independently for ``subject_set_ref``
            # ("Cohort-scoped Ask Dev answers require a newer client"), and
            # committing only the first of several named subjects is precisely
            # the fabricated-premise shape this issue exists to close.
            # CHAOS-3301 owns subject sets and batch execution.
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

        entry = latest[mentions[0].mention_id]
        entity = self._authorized_entity_for(entry)
        committed_resolution = self._scope_service.committed_resolution_for(
            entity,
            org_id=org_id,
            base_scope=authorized_scope,
            resolved_at=generated_at,
        )
        return SubjectPreflightResult(
            decision=PreflightDecision.PROCEED,
            interpretation=interpretation,
            ledger=ledger,
            committed_resolution=committed_resolution,
            answer=None,
            outcome=None,
            # With a subject already committed there is nothing left for the
            # model to resolve, so ``resolve_scope.v1`` is withheld — which is
            # also what makes the ``forbidden_or_not_found`` tool-result leak
            # channel unreachable on this path, without renaming a token that
            # five published v1 schemas carry.
            allowed_tools=ALL_TOOLS - {ToolID.RESOLVE_SCOPE},
            diagnostic="proceeded_committed_subject",
        )

    # -- construction helpers -------------------------------------------------

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
    ) -> SubjectPreflightResult:
        answer = build_preflight_answer(
            outcome=outcome,
            intent_id=interpretation.intent.intent_id,
            versions=self._versions,
            run_id=run_id,
            answer_id=answer_id,
            conversation_id=conversation_id,
            generated_at=generated_at,
            clarification_key=clarification_key,
        )
        return SubjectPreflightResult(
            decision=PreflightDecision.TERMINATE,
            interpretation=interpretation,
            ledger=ledger,
            committed_resolution=None,
            answer=answer,
            outcome=outcome,
            allowed_tools=frozenset(),
            diagnostic=diagnostic,
        )
