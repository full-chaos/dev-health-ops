"""CHAOS-3675 PR 1/3 (review) and PR 2/3 (incident): production resolvers,
and the invariant they exist to enforce -- the entity a record is about is
derived from the canonical row, never from ``candidate.entity_id``.

Every test here uses a fake ClickHouse-shaped sink whose ``query_dicts``
call simulates the real SQL's own filtering predicates, so an org-mismatch
test exercises the same filtering discipline the live SQL performs, not
merely that Python happens to pass the right parameter through. The
tenant-isolation behaviour of the ACTUAL SQL text is separately proven live
in ``test_native_evidence_resolver_clickhouse_live.py``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from dev_health_ops.api.dev.evidence_service import EvidenceCandidate
from dev_health_ops.api.dev.native_evidence import SourceFreshnessPolicy
from dev_health_ops.api.dev.native_evidence_resolver import (
    NativeEvidenceCandidateResolver,
    _review_pr_entity_id,
)
from dev_health_ops.api.dev.scope_service import ScopeResolution
from dev_health_ops.context_fabric.graph_arm.admission import ARM_SOURCE_SYSTEM

NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)
ORG_A = "org-a"
ORG_B = "org-b"
#: This resolver never reads ``scope`` (see the module docstring: the
#: entity comes from the row, never from ambient scope either) -- ``None``
#: cast to the parameter's real type keeps every call site honestly typed
#: instead of scattering per-call ``# type: ignore``s.
_UNUSED_SCOPE = cast(ScopeResolution, None)


@dataclass(frozen=True)
class _Row:
    org_id: str
    locator: str
    repo_id: str
    number: int
    review_id: str
    reviewer: str
    state: str
    submitted_at: datetime
    last_synced: datetime


def _row(
    *,
    org_id: str = ORG_A,
    repo_id: str = "repo-1",
    number: int = 42,
    review_id: str = "rev-9",
    reviewer: str = "octocat",
    state: str = "approved",
) -> _Row:
    locator = f"{repo_id}#pr{number}#review{review_id}"
    return _Row(
        org_id=org_id,
        locator=locator,
        repo_id=repo_id,
        number=number,
        review_id=review_id,
        reviewer=reviewer,
        state=state,
        submitted_at=NOW - timedelta(hours=1),
        last_synced=NOW - timedelta(minutes=5),
    )


class _FakeSink:
    """Simulates the review-resolve SQL's own WHERE clause: a row is
    returned only when BOTH ``org_id`` and the composite locator match --
    the same two predicates ``_REVIEW_RESOLVE_SQL`` binds."""

    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows
        self.calls: list[dict[str, Any]] = []

    async def query_dicts(
        self, query: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.calls.append(params)
        for row in self._rows:
            if row.org_id == params["org_id"] and row.locator == params["locator"]:
                return [
                    {
                        "repo_id": row.repo_id,
                        "number": row.number,
                        "review_id": row.review_id,
                        "reviewer": row.reviewer,
                        "state": row.state,
                        "submitted_at": row.submitted_at,
                        "last_synced": row.last_synced,
                    }
                ]
        return []


def _monkeypatch_query_dicts(monkeypatch: pytest.MonkeyPatch, sink: _FakeSink) -> None:
    async def _fake_query_dicts(client: Any, query: str, params: dict[str, Any]):
        assert client is sink
        return await sink.query_dicts(query, params)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_evidence_resolver.query_dicts",
        _fake_query_dicts,
    )


def _candidate(
    locator: str, *, claimed_entity_id: str = "issue-999"
) -> EvidenceCandidate:
    return EvidenceCandidate(
        source_system=ARM_SOURCE_SYSTEM,
        entity_type="review",
        # Deliberately a claim the row will NOT corroborate -- every test
        # below asserts the resolver never trusts this value.
        entity_id=claimed_entity_id,
        locator=locator,
    )


def _resolve(resolver: NativeEvidenceCandidateResolver, candidate: EvidenceCandidate):
    return asyncio.run(
        resolver.resolve(org_id=ORG_A, scope=_UNUSED_SCOPE, candidate=candidate)
    )


def test_source_system_is_the_arm() -> None:
    # Instance attribute, not class-level: the arm import is lazy (CHAOS-3617
    # containment -- production may not import context_fabric at module
    # scope), set at construction time.
    assert NativeEvidenceCandidateResolver(_FakeSink([])).source_system == (
        ARM_SOURCE_SYSTEM
    )


def test_review_pr_entity_id_is_derived_from_repo_and_number() -> None:
    assert _review_pr_entity_id({"repo_id": "repo-1", "number": 42}) == "repo-1#pr42"


def test_resolve_derives_the_pr_entity_from_the_row_never_the_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The core invariant. The candidate CLAIMS ``entity_id="issue-999"``;
    the row is genuinely about a completely different PR. The resolved
    record must reflect the ROW's truth, and the claim must not leak
    through anywhere.
    """

    row = _row(repo_id="repo-1", number=42)
    sink = _FakeSink([row])
    _monkeypatch_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    record = _resolve(resolver, _candidate(row.locator, claimed_entity_id="issue-999"))

    assert record is not None
    assert record.entity_id == "repo-1#pr42"
    assert record.entity_id != "issue-999"
    assert "issue-999" not in (record.display_label, record.raw_excerpt or "")


def test_resolve_is_keyed_by_locator_not_by_the_candidates_claimed_entity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second row exists whose locator matches the candidate's CLAIMED
    entity_id string by coincidence -- the resolver must still look up by
    ``candidate.locator`` and never fall back to matching on entity_id."""

    real_row = _row(repo_id="repo-1", number=42, review_id="rev-9")
    decoy_row = _row(repo_id="repo-1", number=999, review_id="rev-decoy")
    sink = _FakeSink([real_row, decoy_row])
    _monkeypatch_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    record = _resolve(
        resolver, _candidate(real_row.locator, claimed_entity_id=decoy_row.locator)
    )

    assert record is not None
    assert record.entity_id == "repo-1#pr42"


def test_an_unknown_locator_is_refused_as_no_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sink = _FakeSink([])
    _monkeypatch_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    assert _resolve(resolver, _candidate("repo-1#pr1#review1")) is None


def test_a_locator_that_exists_only_in_a_different_org_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Contract-owner requirement: a candidate claiming a locator that is
    REAL, but in a different org, must die at resolution -- not just an
    outright-nonexistent locator. The fake sink filters on org_id exactly
    as the real SQL's ``WHERE org_id = {org_id:String}`` does, so this
    proves the resolver actually threads the CALLER's org_id into the
    lookup rather than trusting/ignoring it.
    """

    other_org_row = _row(org_id=ORG_B, repo_id="repo-1", number=42)
    sink = _FakeSink([other_org_row])
    _monkeypatch_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    # Same locator that IS real -- just in ORG_B, not the ORG_A this
    # resolver is asked to resolve for.
    assert (
        asyncio.run(
            resolver.resolve(
                org_id=ORG_A,
                scope=_UNUSED_SCOPE,
                candidate=_candidate(other_org_row.locator),
            )
        )
        is None
    )
    # And the SAME locator resolves cleanly for the org it actually
    # belongs to -- proves the refusal above is the org check, not a
    # broken lookup.
    assert (
        asyncio.run(
            resolver.resolve(
                org_id=ORG_B,
                scope=_UNUSED_SCOPE,
                candidate=_candidate(other_org_row.locator),
            )
        )
        is not None
    )


def test_unimplemented_observation_kinds_are_refused_without_querying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``deployment``/``commit``/``ci_run``/``incident`` are deliberately
    unimplemented in PR 1/3 (see the class docstring) -- refused cleanly,
    never a crash, and never a query issued for a kind this resolver
    cannot yet verify safely."""

    sink = _FakeSink([_row()])
    _monkeypatch_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    candidate = EvidenceCandidate(
        source_system=ARM_SOURCE_SYSTEM,
        entity_type="deployment",
        entity_id="issue-999",
        locator="repo-1#deployment1",
    )
    assert _resolve(resolver, candidate) is None
    assert sink.calls == []


def test_resolved_record_carries_the_repository_for_the_separate_repo_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = _row(repo_id="repo-77")
    sink = _FakeSink([row])
    _monkeypatch_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    record = _resolve(resolver, _candidate(row.locator))

    assert record is not None
    assert record.repository_ids == ("repo-77",)


def test_freshness_policy_governs_staleness(monkeypatch: pytest.MonkeyPatch) -> None:
    row = _row()
    sink = _FakeSink([row])
    _monkeypatch_query_dicts(monkeypatch, sink)
    always_stale = SourceFreshnessPolicy(
        source_system="reviews",
        policy_version="test-always-stale.v1",
        maximum_age=timedelta(0),
    )
    resolver = NativeEvidenceCandidateResolver(sink, policies={"reviews": always_stale})

    record = _resolve(resolver, _candidate(row.locator))

    assert record is not None
    assert record.stale is True


# ---------------------------------------------------------------------------
# CHAOS-3675 PR 2/3: the incidents resolver. Incidents have NO
# directly-authorizable entity in the current canonical schema at all --
# only ever a repository, via a LEFT JOIN that can miss entirely (not just
# carry a null repo_id) -- so every resolved incident is
# no_authorizable_entity=True, and a repository-less incident is refused by
# the resolver itself rather than emitted and left to admit()'s anchor
# guard alone.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _IncidentRow:
    org_id: str
    id: str
    title: str
    normalized_status: str
    observed_at: datetime
    last_synced: datetime


@dataclass(frozen=True)
class _EdgeRow:
    org_id: str
    incident_id: str
    repo_id: str | None


class _IncidentFakeSink:
    """Simulates ``operational_incidents FINAL LEFT JOIN
    work_graph_deployment_incident_edges FINAL ON (org_id, incident_id)``:
    an incident row is returned only when ``org_id``+``id`` match (the
    ``WHERE`` clause); ``repo_id`` is populated only when a matching edge
    row exists for the SAME org (the ``LEFT JOIN``'s own predicate) --
    absent edge, not just a null column, models the real "join misses
    entirely" case.
    """

    def __init__(
        self, incidents: list[_IncidentRow], edges: list[_EdgeRow] | None = None
    ) -> None:
        self._incidents = incidents
        self._edges = edges or []
        self.calls: list[dict[str, Any]] = []

    async def query_dicts(
        self, query: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.calls.append(params)
        for incident in self._incidents:
            if incident.org_id != params["org_id"] or incident.id != params["locator"]:
                continue
            edge = next(
                (
                    e
                    for e in self._edges
                    if e.org_id == incident.org_id and e.incident_id == incident.id
                ),
                None,
            )
            return [
                {
                    "id": incident.id,
                    "title": incident.title,
                    "normalized_status": incident.normalized_status,
                    "observed_at": incident.observed_at,
                    "last_synced": incident.last_synced,
                    "repo_id": edge.repo_id if edge else None,
                }
            ]
        return []


def _monkeypatch_incident_query_dicts(
    monkeypatch: pytest.MonkeyPatch, sink: _IncidentFakeSink
) -> None:
    async def _fake_query_dicts(client: Any, query: str, params: dict[str, Any]):
        assert client is sink
        return await sink.query_dicts(query, params)

    monkeypatch.setattr(
        "dev_health_ops.api.dev.native_evidence_resolver.query_dicts",
        _fake_query_dicts,
    )


def _incident_candidate(
    *, locator: str = "inc-1", claimed_entity_id: str = "issue-999"
) -> EvidenceCandidate:
    return EvidenceCandidate(
        source_system=ARM_SOURCE_SYSTEM,
        entity_type="incident",
        # A claim this resolver must never consult -- incidents never
        # produce an entity-based authorization at all.
        entity_id=claimed_entity_id,
        locator=locator,
    )


def test_incident_with_a_linked_repository_is_admitted_repository_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RED-first: before PR 2/3, ``incident``-kind candidates dispatch to
    nothing and refuse as ``UNCONFIGURED``. With a real linked repository,
    resolution now succeeds -- always via the entity-less/repository-only
    mechanism (CHAOS-3675 PR1's prereq), never a directly-authorized
    entity, because incidents have none in the current schema."""

    sink = _IncidentFakeSink(
        incidents=[
            _IncidentRow(
                org_id=ORG_A,
                id="inc-1",
                title="Checkout latency spike",
                normalized_status="resolved",
                observed_at=NOW,
                last_synced=NOW,
            )
        ],
        edges=[_EdgeRow(org_id=ORG_A, incident_id="inc-1", repo_id="repo-1")],
    )
    _monkeypatch_incident_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    record = asyncio.run(
        resolver.resolve(
            org_id=ORG_A, scope=_UNUSED_SCOPE, candidate=_incident_candidate()
        )
    )

    assert record is not None
    assert record.no_authorizable_entity is True
    assert record.repository_ids == ("repo-1",)
    assert record.entity_id == "inc-1"
    # The candidate's claim never leaks anywhere on the resolved record.
    assert "issue-999" not in (record.display_label, record.raw_excerpt or "")


def test_an_incident_with_no_linked_repository_at_all_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No edge row at all (not merely a null ``repo_id`` on an existing
    edge) -- the resolver refuses outright rather than emitting a record
    with no authorization anchor at all."""

    sink = _IncidentFakeSink(
        incidents=[
            _IncidentRow(
                org_id=ORG_A,
                id="inc-2",
                title="Unlinked incident",
                normalized_status="open",
                observed_at=NOW,
                last_synced=NOW,
            )
        ],
        edges=[],
    )
    _monkeypatch_incident_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    record = asyncio.run(
        resolver.resolve(
            org_id=ORG_A,
            scope=_UNUSED_SCOPE,
            candidate=_incident_candidate(locator="inc-2"),
        )
    )

    assert record is None


def test_an_incident_with_a_null_repo_id_on_its_edge_also_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The edge row EXISTS but its own ``repo_id`` is null -- distinct from
    the no-edge-at-all case above, and refused for the same reason: no
    repository to anchor authorization to."""

    sink = _IncidentFakeSink(
        incidents=[
            _IncidentRow(
                org_id=ORG_A,
                id="inc-3",
                title="Edge with no repo",
                normalized_status="open",
                observed_at=NOW,
                last_synced=NOW,
            )
        ],
        edges=[_EdgeRow(org_id=ORG_A, incident_id="inc-3", repo_id=None)],
    )
    _monkeypatch_incident_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    record = asyncio.run(
        resolver.resolve(
            org_id=ORG_A,
            scope=_UNUSED_SCOPE,
            candidate=_incident_candidate(locator="inc-3"),
        )
    )

    assert record is None


def test_an_incident_locator_that_exists_only_in_a_different_org_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same cross-tenant control as the review resolver: a real incident
    id, in a different org, must not resolve."""

    sink = _IncidentFakeSink(
        incidents=[
            _IncidentRow(
                org_id=ORG_B,
                id="inc-4",
                title="Org B's incident",
                normalized_status="open",
                observed_at=NOW,
                last_synced=NOW,
            )
        ],
        edges=[_EdgeRow(org_id=ORG_B, incident_id="inc-4", repo_id="repo-1")],
    )
    _monkeypatch_incident_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    refused = asyncio.run(
        resolver.resolve(
            org_id=ORG_A,
            scope=_UNUSED_SCOPE,
            candidate=_incident_candidate(locator="inc-4"),
        )
    )
    admitted = asyncio.run(
        resolver.resolve(
            org_id=ORG_B,
            scope=_UNUSED_SCOPE,
            candidate=_incident_candidate(locator="inc-4"),
        )
    )

    assert refused is None
    assert admitted is not None


def test_an_edge_belonging_to_a_different_org_does_not_leak_its_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The incident itself is genuinely in ORG_A, but the only matching
    edge row (by incident_id alone) belongs to ORG_B -- the join's own
    ``org_id`` predicate must keep them apart, so ORG_A's incident resolves
    with NO repository (refused), never ORG_B's repo_id smuggled across."""

    sink = _IncidentFakeSink(
        incidents=[
            _IncidentRow(
                org_id=ORG_A,
                id="inc-5",
                title="Org A's incident",
                normalized_status="open",
                observed_at=NOW,
                last_synced=NOW,
            )
        ],
        edges=[_EdgeRow(org_id=ORG_B, incident_id="inc-5", repo_id="org-b-repo")],
    )
    _monkeypatch_incident_query_dicts(monkeypatch, sink)
    resolver = NativeEvidenceCandidateResolver(sink)

    record = asyncio.run(
        resolver.resolve(
            org_id=ORG_A,
            scope=_UNUSED_SCOPE,
            candidate=_incident_candidate(locator="inc-5"),
        )
    )

    assert record is None
