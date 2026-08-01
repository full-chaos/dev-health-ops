"""Shared deterministic harness for the CHAOS-3301 team-scope/cohort suites.

Clones the shape of ``tests/_chaos_3292_preflight`` (same underscore-prefixed
placement, same ``SeededCatalog``/``RecordingProvider``/``recording_registry``/
``run_preflight_orchestrator`` primitives) and imports the org/user/catalog
constants from it directly rather than redefining them, so both suites stay
consistent about what "the organization" and "the catalog" mean.

New entities and case factories here are specific to CHAOS-3301: a named team
as a direct subject, bounded multi-subject (cohort) sets, and the structural
regression that a legacy ``team_ids`` filter must never become a committed
subject.
"""

from __future__ import annotations

from dev_health_ops.api.dev.contracts_v2 import DevSubjectSet
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from tests._chaos_3292_preflight import (
    ANSWER_ID,
    ASK_DEV_PROJECT,
    ATLAS_PROJECT_ONE,
    ATLAS_PROJECT_TWO,
    CONVERSATION_ID,
    ORG_ID,
    OTHER_ORG_ID,
    PERMISSION_FINGERPRINT,
    PLATFORM_TEAM,
    RUN_ID,
    USER_ID,
    Recorder,
    RecordingProvider,
    RunOutput,
    SeededCatalog,
    answer_payload,
    fixed_now,
    organization_resolution,
    recording_registry,
    request_for,
    run_preflight_orchestrator,
    scope_dict,
    sequential_ids,
    status_then_answer,
    versions,
)

__all__ = [
    "ANSWER_ID",
    "ASK_DEV_PROJECT",
    "CONVERSATION_ID",
    "HALO_PROJECT",
    "NOVA_PROJECT",
    "ORG_ID",
    "OTHER_ORG_ID",
    "PERMISSION_FINGERPRINT",
    "PLATFORM_TEAM",
    "RIVAL_TEAM",
    "RUN_ID",
    "USER_ID",
    "VESTA_PROJECT_NAME",
    "Recorder",
    "RecordingProvider",
    "RunOutput",
    "SeededCatalog",
    "SubjectSetRecorder",
    "answer_payload",
    "case_n0_team_facts_or_typed_not_applicable",
    "case_n1_cross_tenant_team",
    "case_n2_mixed_kind_set",
    "case_n3_oversized_project_set",
    "case_n4_duplicate_aliases_one_entity",
    "case_n5_team_filter_is_not_a_subject",
    "case_p1_known_team_singular",
    "case_p2_two_project_cohort",
    "case_p3_partial_project_cohort",
    "case_r1_duplicate_alias_with_unresolved_mention",
    "case_r2_twentyfive_typed_plus_resolvable_bare_name",
    "case_r3_ambiguous_mention_in_cohort",
    "fixed_now",
    "organization_resolution",
    "recording_registry",
    "request_for",
    "run_preflight_orchestrator",
    "scope_dict",
    "sequential_ids",
    "status_then_answer",
    "versions",
]

#: Owned by ORG_ID. Two homogeneous projects for the bounded-cohort controls.
HALO_PROJECT = AuthorizedEntity(EntityKind.PROJECT, "project-halo", "Halo")
NOVA_PROJECT = AuthorizedEntity(EntityKind.PROJECT, "project-nova", "Nova")
#: Named in a question but never seeded for ORG_ID — the partial cohort's
#: unresolved third mention.
VESTA_PROJECT_NAME = "Vesta"

#: Owned by OTHER_ORG_ID only — the cross-tenant team control (N1). Distinct
#: from PLATFORM_TEAM (which lives under ORG_ID) so a catalog bug that leaked
#: cross-tenant would be caught by name, not just by id.
RIVAL_TEAM = AuthorizedEntity(EntityKind.TEAM, "team-rival", "Rival")


class SubjectSetRecorder(Recorder):
    """``Recorder`` plus a capture point for ``record_subject_set``.

    The orchestrator does not call this today (CHAOS-3301 wires it); the
    capture list stays empty until that wiring lands, which is itself part of
    what the cohort controls (P2/P3/N4) are RED against.
    """

    def __init__(self) -> None:
        super().__init__()
        self.subject_sets: list[DevSubjectSet] = []

    async def record_subject_set(self, subject_set: DevSubjectSet) -> None:
        self.subject_sets.append(subject_set)


def _oversized_project_question(count: int) -> str:
    """A question naming ``count`` distinct capitalized project mentions.

    Each ``project AlphaNN`` is an independent noun-leading match in the
    question-interpreter grammar (see ``question_interpreter._NOUN_LEADING``),
    so this reliably produces ``count`` candidate mentions before any
    extraction-time cap is applied.
    """

    names = [f"project Alpha{index:02d}" for index in range(count)]
    return "What is the status of " + " and ".join(names) + "?"


def _oversized_project_entities(
    count: int,
) -> list[tuple[str, AuthorizedEntity]]:
    return [
        (
            ORG_ID,
            AuthorizedEntity(
                EntityKind.PROJECT, f"project-alpha-{index:02d}", f"Alpha{index:02d}"
            ),
        )
        for index in range(count)
    ]


# ---------------------------------------------------------------------------
# The named CHAOS-3301 case factories
# ---------------------------------------------------------------------------


async def case_p1_known_team_singular() -> RunOutput:
    """A known team, asked about on its own — a direct singular subject."""

    return await run_preflight_orchestrator(
        question="How is the Platform team doing?",
        entities=[(ORG_ID, PLATFORM_TEAM)],
        script_id="p1",
    )


async def case_n1_cross_tenant_team() -> RunOutput:
    """A real team that exists, but only for a different tenant."""

    return await run_preflight_orchestrator(
        question="How is the Rival team doing?",
        entities=[(OTHER_ORG_ID, RIVAL_TEAM), (ORG_ID, PLATFORM_TEAM)],
        script_id="n1",
    )


async def case_p2_two_project_cohort() -> RunOutput:
    """Two known projects, named together — a complete bounded cohort."""

    return await run_preflight_orchestrator(
        question="Compare project Halo and project Nova",
        entities=[(ORG_ID, HALO_PROJECT), (ORG_ID, NOVA_PROJECT)],
        script_id="p2",
        recorder_factory=SubjectSetRecorder,
    )


async def case_p3_partial_project_cohort() -> RunOutput:
    """Two resolve, one does not — a partial cohort, disclosed not widened."""

    return await run_preflight_orchestrator(
        question=(
            f"Compare project Halo and project Nova and project {VESTA_PROJECT_NAME}"
        ),
        entities=[(ORG_ID, HALO_PROJECT), (ORG_ID, NOVA_PROJECT)],
        script_id="p3",
        recorder_factory=SubjectSetRecorder,
    )


async def case_n2_mixed_kind_set() -> RunOutput:
    """One project and one team, named together — never a supported set."""

    return await run_preflight_orchestrator(
        question="Compare project Halo and the Platform team",
        entities=[(ORG_ID, HALO_PROJECT), (ORG_ID, PLATFORM_TEAM)],
        script_id="n2",
        recorder_factory=SubjectSetRecorder,
    )


async def case_n3_oversized_project_set(count: int = 26) -> RunOutput:
    """More projects than the ``dev_subject_set.v1`` bound admits."""

    return await run_preflight_orchestrator(
        question=_oversized_project_question(count),
        entities=_oversized_project_entities(count),
        script_id="n3",
        recorder_factory=SubjectSetRecorder,
    )


async def case_n4_duplicate_aliases_one_entity() -> RunOutput:
    """The same project named twice, by two different spans."""

    return await run_preflight_orchestrator(
        question='Compare project Halo and project "project-halo"',
        entities=[(ORG_ID, HALO_PROJECT)],
        script_id="n4",
        recorder_factory=SubjectSetRecorder,
    )


async def case_n5_team_filter_is_not_a_subject() -> RunOutput:
    """A legacy ``team_ids`` filter alongside an organization-wide question."""

    return await run_preflight_orchestrator(
        question="How are we doing on delivery this month?",
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, PLATFORM_TEAM)],
        scope_overrides={
            "direct_scope": "organization",
            "team_ids": [PLATFORM_TEAM.canonical_id],
        },
        script_id="n5",
    )


async def case_n0_team_facts_or_typed_not_applicable() -> RunOutput:
    """A committed team subject must never answer from silent zero rows."""

    return await run_preflight_orchestrator(
        question="What is the status of the Platform team?",
        entities=[(ORG_ID, PLATFORM_TEAM)],
        script_id="n0",
    )


# ---------------------------------------------------------------------------
# Codex adversarial-review regression cases (CHAOS-3301 review findings)
# ---------------------------------------------------------------------------


async def case_r1_duplicate_alias_with_unresolved_mention() -> RunOutput:
    """Two aliases of one entity, plus a third mention that never resolves.

    Regression for the review's HIGH finding: D2's ">=2" threshold counted
    raw exact-match *mentions*, not distinct committed *entities*, so the two
    Halo aliases alone satisfied it and let the run skip past the unresolved
    ``Vesta`` mention instead of terminating on it -- then the post-dedup
    singular branch (only one distinct entity survives dedup) committed Halo
    as though nothing had been omitted. With exactly one distinct entity
    here, D2 must not activate at all; the pre-D2 lowest-ordinal termination
    rule applies, exactly as it would with no aliasing.
    """

    return await run_preflight_orchestrator(
        question=(
            'Compare project Halo and project "project-halo" and project '
            f"{VESTA_PROJECT_NAME}"
        ),
        entities=[(ORG_ID, HALO_PROJECT)],
        script_id="r1",
        recorder_factory=SubjectSetRecorder,
    )


async def case_r2_twentyfive_typed_plus_resolvable_bare_name() -> RunOutput:
    """25 typed subjects plus one resolvable bare name -- 26 total.

    Regression for the review's HIGH finding: the uncapped mention count only
    summed the *typed* grammar candidates, so ``_add_untyped_mentions``
    silently dropping the 26th (bare-name) candidate at the ``MAX_MENTIONS``
    merge cap left the oversized-rejection check looking at 25, not 26, and
    the run reported a "complete" 25-subject cohort instead of rejecting an
    oversized one.
    """

    entities = _oversized_project_entities(25)
    entities.append(
        (ORG_ID, AuthorizedEntity(EntityKind.PROJECT, "project-nightfall", "Nightfall"))
    )
    names = [f"project Alpha{index:02d}" for index in range(25)]
    question = "Compare " + " and ".join(names) + " and Nightfall"
    return await run_preflight_orchestrator(
        question=question,
        entities=entities,
        script_id="r2",
        recorder_factory=SubjectSetRecorder,
    )


async def case_r3_ambiguous_mention_in_cohort() -> RunOutput:
    """A cohort with one committed pair and one ambiguous (not unresolved) mention.

    Regression for the review's MEDIUM finding: every ``UNRESOLVED_OUTCOMES``
    member (including ``AMBIGUOUS_CANDIDATES``) was copied into the set's
    ``unresolved_mention_ids`` with the generic "could not be resolved"
    warning. Ambiguity is a distinct, separately-fielded outcome
    (``ambiguous_mention_ids`` plus its own warning) that this must not
    collapse into.
    """

    return await run_preflight_orchestrator(
        question="Compare project Halo and project Nova and project Atlas",
        entities=[
            (ORG_ID, HALO_PROJECT),
            (ORG_ID, NOVA_PROJECT),
            (ORG_ID, ATLAS_PROJECT_ONE),
            (ORG_ID, ATLAS_PROJECT_TWO),
        ],
        script_id="r3",
        recorder_factory=SubjectSetRecorder,
    )
