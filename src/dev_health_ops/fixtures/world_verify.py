"""CHAOS-3219 Codex adversarial review (HIGH-2, 2026-08-05): post-generation
verification of ``subjects.json``/``sources.json`` claims through PRODUCTION
code paths, not the fixture generator's own bookkeeping.

Before this module, ``validate_world_manifest`` (``world.py``) only checked
that a subject/source entry's ``class``/``state`` string appeared in a
required-value set -- a bare ``{"class": "deleted"}`` row with no
realization fields at all would pass. This module closes two separate gaps:

1. **Typed per-entry schema** (:func:`validate_subject_entry_schema`,
   :func:`validate_source_entry_schema`): every subject class and source
   state has a concrete required-field shape now, checked structurally,
   not just "the class name is spelled right".
2. **Live production-path verification** (:func:`verify_world_against_production`):
   after generation, re-derive what a real request would observe --
   ``alias_matching.alias_forms`` (the ACTUAL CHAOS-3388 alias matcher, a
   pure function) for acronym-alias subjects, direct catalog-row
   existence/absence for the other subject classes, and
   ``NativeDataHealthReader`` + ``DataHealthService.inspect`` (the ACTUAL
   production data-health classification) for every sources.json state
   that maps onto a ``DataHealthState`` value -- and raise loudly on the
   first claim that isn't actually observed. States this module cannot
   verify through ``DataHealthState`` (``truncated``/``conflicting``/
   ``not-applicable``/``measured-zero``) are verified elsewhere (measured-
   zero has its own dedicated postcondition check -- see
   ``world.write_and_verify_measured_zero_metric``; conflicting and
   truncated are verified here via direct raw-row checks; not-applicable
   has no fixture row to check per its own sources.json documentation) --
   named explicitly in :data:`STATES_VERIFIED_ELSEWHERE`, never silently
   dropped.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dev_health_ops.api.dev.alias_matching import alias_forms
from dev_health_ops.api.dev.contracts import ScopeResolutionOutcome
from dev_health_ops.api.dev.data_health_service import (
    NATIVE_EVIDENCE_SOURCES,
    DataHealthService,
    DataHealthState,
    NativeDataHealthReader,
)
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ResolvedTimeRange,
    ScopeResolution,
    ScopeResolveRequest,
)

if TYPE_CHECKING:
    from dev_health_ops.fixtures.world import WorldManifest

NATIVE_EVIDENCE_SOURCE_KEYS = frozenset(NATIVE_EVIDENCE_SOURCES)

FIXTURE_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


class WorldSchemaError(ValueError):
    """A subjects.json/sources.json entry does not match its class/state's
    required-field schema -- raised by the STATIC (no-DB) validators."""


class WorldVerificationError(RuntimeError):
    """A subjects.json/sources.json claim was NOT observed through the
    real production resolution/data-health code path after generation."""


# ---------------------------------------------------------------------------
# Part 1: typed per-entry schema (static -- no DB, no generation required)
# ---------------------------------------------------------------------------

#: Required fields per subjects.json ``class``, beyond the always-required
#: ``id``/``class``/``org_alias``/``mentions``/``expected_terminal_resolution``
#: (validated generically). A bare ``{"class": "deleted"}`` row now fails
#: here instead of only failing (or not) much later when generation tries
#: and silently no-ops on a missing field.
_SUBJECT_CLASS_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "exact": ("repo_full_name",),
    "ambiguous": ("candidates",),
    "acronym-alias": ("alias_form",),
    "no-match": (),
    "deleted": (),
    "stale-context": ("repo_full_name", "realizing_conversation_id_seed"),
    "partially-resolved": ("team_id",),
    "bounded-set": ("members",),
}

_GENERIC_SUBJECT_REQUIRED_FIELDS = (
    "id",
    "class",
    "mentions",
    "expected_terminal_resolution",
)


def validate_subject_entry_schema(entry: dict[str, Any]) -> None:
    """Raise :class:`WorldSchemaError` unless ``entry`` has every field its
    own ``class`` requires, with the right shape (not just presence)."""

    for field in _GENERIC_SUBJECT_REQUIRED_FIELDS:
        if not entry.get(field):
            raise WorldSchemaError(
                f"subjects.json entry {entry!r} is missing required field {field!r}"
            )
    entry_class = entry["class"]
    if entry_class not in _SUBJECT_CLASS_REQUIRED_FIELDS:
        raise WorldSchemaError(
            f"subjects.json entry {entry['id']!r} has unknown class {entry_class!r}"
        )
    if entry_class != "no-match" and not entry.get("org_alias"):
        raise WorldSchemaError(
            f"subjects.json entry {entry['id']!r} (class={entry_class!r}) is "
            "missing required field 'org_alias'"
        )
    for field in _SUBJECT_CLASS_REQUIRED_FIELDS[entry_class]:
        if not entry.get(field):
            raise WorldSchemaError(
                f"subjects.json entry {entry['id']!r} (class={entry_class!r}) "
                f"is missing required field {field!r}"
            )
    if entry_class == "ambiguous":
        candidates = entry.get("candidates") or []
        if not isinstance(candidates, list) or len(candidates) < 2:
            raise WorldSchemaError(
                f"subjects.json entry {entry['id']!r}: class=ambiguous requires "
                "candidates to be a list of >= 2 repo names, "
                f"got {candidates!r}"
            )
    if entry_class == "bounded-set":
        members = entry.get("members") or []
        if not isinstance(members, list) or len(members) < 2:
            raise WorldSchemaError(
                f"subjects.json entry {entry['id']!r}: class=bounded-set requires "
                f"members to be a list of >= 2 repo names, got {members!r}"
            )
    if entry_class == "acronym-alias":
        has_project = bool(entry.get("project_id")) and bool(
            entry.get("project_display_name")
        )
        has_team = bool(entry.get("team_id")) and bool(entry.get("team_display_name"))
        if not (has_project or has_team):
            raise WorldSchemaError(
                f"subjects.json entry {entry['id']!r}: class=acronym-alias "
                "requires either (project_id AND project_display_name) or "
                "(team_id AND team_display_name)"
            )
    verification_status = entry.get("verification_status")
    if (
        verification_status is not None
        and verification_status != REALIZED_UNVERIFIED_LIVE_STATUS
    ):
        raise WorldSchemaError(
            f"subjects.json entry {entry['id']!r} has unknown "
            f"verification_status {verification_status!r} -- the only "
            f"recognized non-default value is {REALIZED_UNVERIFIED_LIVE_STATUS!r}"
        )
    if verification_status == REALIZED_UNVERIFIED_LIVE_STATUS and not entry.get(
        "tracked_by"
    ):
        raise WorldSchemaError(
            f"subjects.json entry {entry['id']!r} has "
            f"verification_status={REALIZED_UNVERIFIED_LIVE_STATUS!r} but no "
            "non-empty 'tracked_by' ticket reference -- an unverified claim "
            "must name what is tracking it, or it is indistinguishable from "
            "one nobody noticed."
        )


#: Required fields per sources.json ``state``, beyond ``state``/
#: ``data_health_state``/``mechanism`` (validated generically). Every entry
#: must name what realizes it (or explicitly declare no fixture is needed,
#: via ``realized_by: {"org_alias": null, "repo_full_name": null}`` --
#: still two present keys, just null-valued, so a genuinely-empty
#: ``realized_by: {}`` still fails this check).
_GENERIC_SOURCE_REQUIRED_FIELDS = (
    "state",
    "data_health_state",
    "mechanism",
    "realized_by",
)

#: A sources.json entry's honest third option, alongside "realized and
#: live-verified" and "verified elsewhere" (STATES_VERIFIED_ELSEWHERE):
#: "known NOT realized right now, and that is recorded, not hidden."
#: Added 2026-08-05 after the "truncated" entry's own mechanism text
#: turned out to be a false "verified empirically" claim -- the first-ever
#: end-to-end run of verify_truncated_work_graph (this round's HIGH-2 work)
#: found the current SyntheticDataGenerator has no mechanism that clusters
#: many PRs onto one work item, so the >MAX_NEIGHBORS fan-out this state
#: requires cannot currently be produced by any volume knob. Per the
#: CLAUDE.md rule "an inaccurate coverage claim is worse than an admitted
#: gap": the fix is not to retune the claim down to whatever the generator
#: happens to produce (that would fake the state -- "truncated" specifically
#: means exceeding the limit) and not to silently drop the entry (that
#: un-claims coverage without a trace) -- it is to mark the entry
#: ``"status": "declared-blocked"`` with a ``"blocked_by"`` ticket
#: reference, so ``validate_world_manifest``/schema validation still passes
#: (the entry is honestly typed, not silently absent), the live check is
#: skipped with a loud, visible log line rather than either a false pass or
#: a hard failure, and the ticket is the traceable path back to closing it.
DECLARED_BLOCKED_STATUS = "declared-blocked"


def is_declared_blocked(entry: dict[str, Any]) -> bool:
    return entry.get("status") == DECLARED_BLOCKED_STATUS


#: subjects.json's analogous honest marker (CHAOS-3429, 2026-08-05): unlike
#: DECLARED_BLOCKED_STATUS (the claim CANNOT currently be realized),
#: ``partially-resolved`` subjects have no repo/candidates/members field for
#: ``verify_subjects_against_production`` to check at all (its only checks
#: are against the "repos" catalog table -- there is no team-catalog-row
#: verification path). The fixture generation itself is believed correct
#: (a team-kind subject with no addressable sub-team scope); what is
#: missing is a LIVE CHECK, not a working claim. Building the team-catalog
#: verification path is CHAOS-3429's scope, not done here -- this marker
#: only makes the gap loud and traceable instead of a silent ``continue``.
REALIZED_UNVERIFIED_LIVE_STATUS = "realized-but-unverified-live"


def is_realized_unverified_live(entry: dict[str, Any]) -> bool:
    return entry.get("verification_status") == REALIZED_UNVERIFIED_LIVE_STATUS


def validate_source_entry_schema(entry: dict[str, Any]) -> None:
    """Raise :class:`WorldSchemaError` unless ``entry`` has every generic
    required field, with ``realized_by`` shaped as a dict carrying both
    keys (even if null-valued for the documented no-fixture-needed case).

    An entry may additionally carry ``"status": "declared-blocked"`` (see
    :data:`DECLARED_BLOCKED_STATUS`) -- when present, ``"blocked_by"`` is
    also required, as a non-empty ticket reference explaining why.
    """

    for field in _GENERIC_SOURCE_REQUIRED_FIELDS:
        if field not in entry or entry[field] in (None, ""):
            if field == "realized_by":
                continue  # realized_by itself is checked structurally below
            raise WorldSchemaError(
                f"sources.json entry {entry!r} is missing required field {field!r}"
            )
    realized_by = entry.get("realized_by")
    if not isinstance(realized_by, dict) or not (
        "org_alias" in realized_by and "repo_full_name" in realized_by
    ):
        raise WorldSchemaError(
            f"sources.json entry state={entry.get('state')!r} must carry a "
            "'realized_by' object with both 'org_alias' and 'repo_full_name' "
            "keys (null-valued is fine for the documented no-fixture-needed "
            "case, e.g. not-applicable) -- got "
            f"{realized_by!r}"
        )
    if not entry.get("source_classes"):
        raise WorldSchemaError(
            f"sources.json entry state={entry.get('state')!r} is missing "
            "required non-empty field 'source_classes'"
        )
    status = entry.get("status")
    if status is not None and status != DECLARED_BLOCKED_STATUS:
        raise WorldSchemaError(
            f"sources.json entry state={entry.get('state')!r} has unknown "
            f"status {status!r} -- the only recognized non-default value is "
            f"{DECLARED_BLOCKED_STATUS!r}"
        )
    if status == DECLARED_BLOCKED_STATUS and not entry.get("blocked_by"):
        raise WorldSchemaError(
            f"sources.json entry state={entry.get('state')!r} has "
            f"status={DECLARED_BLOCKED_STATUS!r} but no non-empty "
            "'blocked_by' ticket reference -- a declared-blocked claim "
            "must name what is tracking it, or it is indistinguishable "
            "from a silently dropped one."
        )


# ---------------------------------------------------------------------------
# Part 2: live production-path verification
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _StubEntitlement:
    """Always-allow entitlement stub. Verification here checks the REAL
    data-health classification (NativeDataHealthReader + DataHealthService
    .inspect's state-selection logic) for a KNOWN, already-generated
    subject; it deliberately does not re-derive entitlement or re-resolve
    mention text -- that is Phase 2's corpus-runner concern, exercised
    through the real HTTP/SSE API with a real user, not this fixture-world
    generator."""

    async def require(self, org_id: str) -> None:
        return None


@dataclass(frozen=True, slots=True)
class _FixedScopeAuthorizer:
    """Returns a pre-built ``ScopeResolution`` regardless of the request."""

    resolution: ScopeResolution

    async def resolve(self, org_id: str, permission_fingerprint: str, request: Any):
        return self.resolution


def _dummy_time_range(now: datetime) -> ResolvedTimeRange:
    iso = now.isoformat()
    return ResolvedTimeRange(
        timezone="UTC",
        utc_start=now,
        utc_end=now,
        local_start=iso,
        local_end=iso,
        comparison_utc_start=now,
        comparison_utc_end=now,
        comparison_local_start=iso,
        comparison_local_end=iso,
    )


def _exact_repository_scope(repo_id: str, *, now: datetime) -> ScopeResolution:
    entity = AuthorizedEntity(
        kind=EntityKind.REPOSITORY,
        canonical_id=repo_id,
        label=repo_id,
        repository_id=repo_id,
    )
    return ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(entity,),
        team_filters=(),
        candidates=(),
        time_range=_dummy_time_range(now),
    )


def _unauthorized_scope(now: datetime) -> ScopeResolution:
    return ScopeResolution(
        outcome=ScopeResolutionOutcome.FORBIDDEN_OR_NOT_FOUND,
        entities=(),
        team_filters=(),
        candidates=(),
        time_range=_dummy_time_range(now),
    )


def _organization_scope(now: datetime) -> ScopeResolution:
    """An org-wide direct scope: no entities at all, so
    ``data_health_service._repository_ids`` (which only ever collects from
    ``scope.entities``) is empty -- the shape ``NativeDataHealthReader.read``
    requires to answer an org-wide "is this source configured at all"
    question, as opposed to "for this one repository". Needed specifically
    for the ``incidents`` source key (live-verification correction,
    2026-08-05): production's own reader special-cases ``source ==
    "incidents" and repository_ids`` to unconditionally report
    ``DataHealthState.UNAVAILABLE`` for ANY narrower-than-org scope
    (``operational_incidents`` carries no ``repo_id`` column, so a
    repository-scoped incidents query cannot be answered correctly and the
    reader refuses to guess -- see its own docstring). Verifying an
    "incidents reads UNCONFIGURED org-wide, regardless of which repo is
    asked about" claim (sources.json's own wording) through a
    repository-scoped ``_exact_repository_scope`` therefore always observed
    UNAVAILABLE instead, regardless of the actual provider configuration --
    caught live the first time this exact check ran end to end."""

    return ScopeResolution(
        outcome=ScopeResolutionOutcome.EXACT,
        entities=(),
        team_filters=(),
        candidates=(),
        time_range=_dummy_time_range(now),
    )


def repo_id_for(repo_full_name: str) -> uuid.UUID:
    return uuid.uuid5(FIXTURE_NAMESPACE, repo_full_name)


#: sources.json ``state`` -> the ``DataHealthState`` a real
#: ``DataHealthService.inspect`` call must return for this module to
#: consider the claim verified.
STATE_TO_DATA_HEALTH: dict[str, DataHealthState] = {
    "current": DataHealthState.COMPLETE,
    "stale": DataHealthState.STALE,
    "unavailable": DataHealthState.UNAVAILABLE,
    "unconfigured": DataHealthState.UNCONFIGURED,
    "no-data": DataHealthState.NO_DATA,
    "unauthorized": DataHealthState.UNAUTHORIZED,
}

#: States NOT verified via DataHealthState in this function -- documented
#: explicitly, never silently dropped. Each is verified elsewhere:
#: - measured-zero: world.write_and_verify_measured_zero_metric's own
#:   live read-back postcondition (HIGH-1 fix).
#: - truncated: verify_truncated_work_graph (below), a raw edge-count check.
#: - conflicting: verify_conflicting_ci_runs (below), a raw two-opposite-
#:   status-row check.
#: - not-applicable: sources.json's own entry documents "no fixture rows
#:   are needed" (the reader hardcodes the acr special-case regardless of
#:   fixture state) -- nothing to verify against a live database.
STATES_VERIFIED_ELSEWHERE = frozenset(
    {"measured-zero", "truncated", "conflicting", "not-applicable"}
)


async def verify_sources_against_production_data_health(
    *,
    client: Any,
    session: Any,
    manifest: WorldManifest,
) -> list[str]:
    """For every sources.json matrix entry mapped in
    :data:`STATE_TO_DATA_HEALTH`, run the REAL ``NativeDataHealthReader`` +
    ``DataHealthService.inspect`` against the live database and assert the
    observed ``DataHealthState`` matches the claim. Returns the list of
    ``(state, source_class)`` ids actually verified; raises
    :class:`WorldVerificationError` on the first mismatch.
    """

    reader = NativeDataHealthReader(client, session)
    verified: list[str] = []
    for entry in manifest.sources["matrix"]:
        state = entry["state"]
        if state in STATES_VERIFIED_ELSEWHERE:
            continue
        expected = STATE_TO_DATA_HEALTH.get(state)
        if expected is None:
            raise WorldVerificationError(
                f"sources.json state {state!r} has no DataHealthState mapping "
                "to verify against, and is not listed in STATES_VERIFIED_ELSEWHERE "
                "-- add one or the other, do not leave a state silently unverified."
            )

        realized_by = entry.get("realized_by") or {}
        org_alias = realized_by.get("org_alias")
        repo_full_name = realized_by.get("repo_full_name")
        if not org_alias or not repo_full_name:
            raise WorldVerificationError(
                f"sources.json state {state!r} has no realized_by.org_alias/"
                "repo_full_name to verify against (and is not in "
                "STATES_VERIFIED_ELSEWHERE) -- this claim cannot be checked."
            )

        asked_from_alias = realized_by.get("asked_from_org_alias")
        repo_id = str(repo_id_for(repo_full_name))
        if asked_from_alias:
            scope = _unauthorized_scope(manifest.pinned_now)
            verify_org_id = str(manifest.org_id(asked_from_alias))
        else:
            scope = _exact_repository_scope(repo_id, now=manifest.pinned_now)
            verify_org_id = str(manifest.org_id(org_alias))

        source_keys = sorted(
            {
                token.split(" ")[0]
                for token in entry.get("source_classes", [])
                if token.split(" ")[0] in NATIVE_EVIDENCE_SOURCE_KEYS
            }
        )
        if not source_keys:
            continue

        by_source: dict[str, Any] = {}
        # "incidents" has no repository dimension in production
        # (``operational_incidents`` carries no ``repo_id`` column) --
        # NativeDataHealthReader unconditionally reports it UNAVAILABLE for
        # any narrower-than-org scope, regardless of provider configuration
        # (see ``_organization_scope``'s own docstring). A repo-scoped
        # verification scope (the ``_exact_repository_scope`` branch above,
        # used whenever this isn't a cross-tenant ``asked_from_alias``
        # check) is therefore the WRONG question to ask for an "incidents"
        # claim -- split it out to its own org-wide-scoped inspect call so
        # every other source_key in this entry keeps its normal,
        # repo-scoped verification unaffected.
        incidents_keys = [k for k in source_keys if k == "incidents"]
        other_keys = [k for k in source_keys if k != "incidents"]

        if other_keys:
            service = DataHealthService(
                entitlement=_StubEntitlement(),
                authorizer=_FixedScopeAuthorizer(scope),
                reader=reader,
                now=manifest.pinned_now,
            )
            result = await service.inspect(
                org_id=verify_org_id,
                permission_fingerprint="fixture-world-verify",
                scope_request=ScopeResolveRequest(),  # unused: _FixedScopeAuthorizer ignores it
                required_sources=other_keys,
            )
            by_source.update({row.source_system: row.state for row in result.sources})

        if incidents_keys:
            incidents_scope = (
                scope
                if asked_from_alias
                # already org-wide/empty-entities for the unauthorized path
                else _organization_scope(manifest.pinned_now)
            )
            incidents_service = DataHealthService(
                entitlement=_StubEntitlement(),
                authorizer=_FixedScopeAuthorizer(incidents_scope),
                reader=reader,
                now=manifest.pinned_now,
            )
            result = await incidents_service.inspect(
                org_id=verify_org_id,
                permission_fingerprint="fixture-world-verify",
                scope_request=ScopeResolveRequest(),
                required_sources=incidents_keys,
            )
            by_source.update({row.source_system: row.state for row in result.sources})

        for source_key in source_keys:
            observed = by_source.get(source_key)
            if observed != expected:
                raise WorldVerificationError(
                    f"sources.json state {state!r} (source={source_key!r}, "
                    f"repo={repo_full_name!r}, org={org_alias!r}) claims "
                    f"{expected.value!r} but the REAL DataHealthService.inspect "
                    f"observed {observed.value if observed else observed!r} -- "
                    "the claim is not realized."
                )
            verified.append(f"{state}:{source_key}")
    return verified


async def verify_conflicting_ci_runs(client: Any, *, org_id: str, repo_id: str) -> None:
    """Direct raw-row check for the ``conflicting`` sources.json state:
    at least two ``ci_pipeline_runs`` rows for this repo with different
    ``status`` values."""

    result = await _query(
        client,
        "SELECT DISTINCT status FROM ci_pipeline_runs FINAL "
        "WHERE org_id = {org_id:String} AND repo_id = {repo_id:UUID}",
        {"org_id": org_id, "repo_id": repo_id},
    )
    distinct_statuses = {row[0] for row in result.result_rows}
    if len(distinct_statuses) < 2:
        raise WorldVerificationError(
            f"sources.json state='conflicting' claims repo_id={repo_id!r} has "
            "disagreeing CI signals, but only found status(es) "
            f"{sorted(distinct_statuses)!r} -- the claim is not realized."
        )


async def verify_truncated_work_graph(
    client: Any, *, org_id: str, max_neighbors: int
) -> None:
    """Direct raw-row check for the ``truncated`` sources.json state: at
    least one work_graph_issue_pr/work_graph_pr_commit fan-out for the
    truncated-probe org exceeds ``max_neighbors`` (WorkGraphNeighborsService
    .MAX_NEIGHBORS) so a real query would observe truncation."""

    result = await _query(
        client,
        "SELECT max(cnt) FROM ("
        "  SELECT work_item_id, count() AS cnt FROM work_graph_issue_pr FINAL "
        "  WHERE org_id = {org_id:String} GROUP BY work_item_id"
        ")",
        {"org_id": org_id},
    )
    rows = result.result_rows
    max_fanout = int(rows[0][0]) if rows and rows[0][0] is not None else 0
    if max_fanout <= max_neighbors:
        raise WorldVerificationError(
            f"sources.json state='truncated' claims org_id={org_id!r} has a "
            f"work-graph fan-out exceeding MAX_NEIGHBORS={max_neighbors}, but "
            f"the largest observed fan-out is {max_fanout} -- the claim is "
            "not realized."
        )


async def _query(client: Any, query: str, parameters: dict[str, Any]) -> Any:
    return await asyncio.to_thread(client.query, query, parameters=parameters)


# ---------------------------------------------------------------------------
# Part 3: subject verification (alias matcher + direct catalog existence)
# ---------------------------------------------------------------------------


def verify_acronym_alias_subject(entry: dict[str, Any]) -> None:
    """Run the REAL CHAOS-3388 alias matcher (``alias_matching.alias_forms``,
    a pure function -- no DB needed) against the subject's own claimed
    display name and assert every claimed mention actually resolves as an
    alias/acronym of it. This is the exact function
    ``scope_catalog.ClickHouseAuthorizedEntityCatalog._alias_matches`` calls
    in production."""

    display_name = entry.get("project_display_name") or entry.get("team_display_name")
    if not display_name:
        raise WorldVerificationError(
            f"subject {entry['id']!r}: class=acronym-alias has no "
            "project_display_name/team_display_name to verify against"
        )
    forms = alias_forms(display_name)
    for mention in entry.get("mentions", []):
        normalized = mention.strip().casefold()
        if normalized not in forms.literal_aliases and normalized not in forms.acronyms:
            raise WorldVerificationError(
                f"subject {entry['id']!r}: mention {mention!r} does not "
                f"resolve as a literal alias or acronym of display_name="
                f"{display_name!r} via the REAL alias_matching.alias_forms "
                f"(literal_aliases={sorted(forms.literal_aliases)!r}, "
                f"acronyms={sorted(forms.acronyms)!r}) -- the claim is not "
                "realized."
            )


async def _table_has_row(
    client: Any, table: str, *, org_id: str, id_column: str, id_value: str
) -> bool:
    result = await _query(
        client,
        f"SELECT count() FROM {table} FINAL "  # noqa: S608
        f"WHERE org_id = {{org_id:String}} AND {id_column} = {{id_value:String}}",
        {"org_id": org_id, "id_value": id_value},
    )
    return int(result.result_rows[0][0]) > 0


async def verify_subjects_against_production(
    *, client: Any, manifest: WorldManifest
) -> list[str]:
    """Verify every subjects.json entry's claimed realization actually
    exists (or, for no-match, deliberately does not exist) in the live
    catalog tables. Raises :class:`WorldVerificationError` on the first
    unobserved claim; returns the list of subject ids verified.
    """

    verified: list[str] = []
    for entry in manifest.subjects["subjects"]:
        entry_class = entry["class"]
        subject_id = entry["id"]
        if entry_class == "acronym-alias":
            verify_acronym_alias_subject(entry)
            verified.append(subject_id)
            continue
        if entry_class == "no-match":
            # Absence IS the fixture -- nothing to positively verify beyond
            # what collect_repo_roster's own guard test already proves
            # (test_no_match_subject_has_no_realizing_repo).
            verified.append(subject_id)
            continue
        org_alias = entry.get("org_alias")
        if not org_alias:
            continue
        org_id = str(manifest.org_id(org_alias))
        repo_names: list[str] = []
        if entry.get("repo_full_name"):
            repo_names.append(entry["repo_full_name"])
        repo_names.extend(entry.get("candidates") or [])
        repo_names.extend(entry.get("members") or [])
        if not repo_names:
            # CHAOS-3429 (2026-08-05): this function's only verification
            # path is against the "repos" catalog table -- a subject with
            # no repo_full_name/candidates/members (e.g. a team-only
            # `partially-resolved` claim) has nothing here to check. That
            # used to be a bare, silent `continue` -- indistinguishable
            # from an entry nobody remembered to verify. Now it is loud
            # either way: an entry that explicitly marks itself
            # verification_status=REALIZED_UNVERIFIED_LIVE_STATUS logs a
            # named, ticketed warning (still NOT added to `verified` --
            # this is honestly "not checked", not "checked and passed");
            # anything else reaching this point is a genuinely new,
            # unmarked gap and must fail loudly rather than silently join
            # the same blind spot.
            if is_realized_unverified_live(entry):
                logging.warning(
                    "world_verify: SKIPPING live check for subject %r "
                    "(class=%r) -- verification_status=%r, tracked_by=%s. "
                    "This claim is NOT verified and NOT counted as checked.",
                    subject_id,
                    entry_class,
                    REALIZED_UNVERIFIED_LIVE_STATUS,
                    entry.get("tracked_by"),
                )
                continue
            raise WorldVerificationError(
                f"subject {subject_id!r}: class={entry_class!r} has no "
                "repo_full_name/candidates/members for "
                "verify_subjects_against_production to check against the "
                "live 'repos' table, and does not declare "
                f"verification_status={REALIZED_UNVERIFIED_LIVE_STATUS!r} "
                "with a 'tracked_by' ticket -- this is an unmarked "
                "verification gap, not a documented one. Either add the "
                "fields this check needs, or mark it explicitly (see "
                "CHAOS-3429)."
            )
        for repo_full_name in repo_names:
            repo_id = str(repo_id_for(repo_full_name))
            exists = await _table_has_row(
                client, "repos", org_id=org_id, id_column="id", id_value=repo_id
            )
            if entry_class == "deleted":
                # The repo row itself may still exist (git history is
                # untouched) -- what must be verified is that the PROJECT
                # catalog row is retired, not the repo's mere existence.
                project_id = entry.get("project_id") or repo_full_name
                active = await _query(
                    client,
                    "SELECT is_active FROM projects FINAL "
                    "WHERE org_id = {org_id:String} AND id = {project_id:String}",
                    {"org_id": org_id, "project_id": project_id},
                )
                rows = active.result_rows
                if not rows or int(rows[0][0]) != 0:
                    raise WorldVerificationError(
                        f"subject {subject_id!r}: class=deleted claims project "
                        f"{project_id!r} is retired (is_active=0), but the live "
                        f"catalog shows {rows!r} -- the claim is not realized."
                    )
            elif not exists:
                raise WorldVerificationError(
                    f"subject {subject_id!r}: class={entry_class!r} claims repo "
                    f"{repo_full_name!r} exists in org {org_alias!r}, but no "
                    "matching row was found in the live 'repos' table -- the "
                    "claim is not realized."
                )
        verified.append(subject_id)
    return verified
