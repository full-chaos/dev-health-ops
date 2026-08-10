"""CHAOS-3664/CHAOS-3675: real ClickHouse-backed candidate resolution for
graph-discovered evidence admission.

The CHAOS-3616 world resolver (``trials/chaos_3646/canonical.py``) reads a
frozen fixture corpus and is test-only. This module is its production
replacement: an :class:`~.evidence_service.EvidenceCandidateResolver`
registered under ``context_fabric_graph_arm`` that resolves a graph-arm
candidate against real ``native_evidence.py``-adjacent ClickHouse tables.

**Structural finding this module is built on (CHAOS-3675 recon).**
``EvidenceCandidate.entity_type`` is always a
:class:`~context_fabric.graph_arm.vocabulary.GraphObservationKind` value --
what KIND OF RECORD the arm observed (a commit, a review, a CI run, ...),
never an :class:`~.scope_service.EntityKind` (what the record is ABOUT: a
pull request, a project, ...). The two vocabularies are disjoint by
construction -- an observation is always ``OBSERVED_ON`` an entity, never
itself one -- and no reachable observation kind (``commit``, ``review``,
``ci_run``, ``deployment``, ``incident``; every other
``GraphObservationKind`` has no native-evidence source at all yet) has a
directly-authorizable identity of its own.

That makes the one invariant every resolver here exists to enforce:
**the entity a record is about is derived from the canonical row, never
from ``candidate.entity_id``.** ``EvidenceCandidate`` carries the arm's
*belief* about what a record is about; a resolver that echoed it back
unverified would let the arm mint authority through a canonical-looking
door, which is exactly what CHAOS-3646's admission seam exists to prevent
("the graph never mints authority" landing at the resolver layer, not just
at the signer). Every ``_resolve_*`` method below re-derives the linked
entity from the row it reads, and the candidate's own claim is consulted
for nothing but the locator used to look the row up.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from dev_health_ops.api.queries.client import query_dicts

from .contracts import FreshnessState
from .evidence_service import EvidenceCandidate, EvidenceRecord
from .native_evidence import (
    SourceFreshnessPolicy,
    _datetime,
    default_native_freshness_policies,
)
from .scope_service import ScopeResolution

RESOLVER_SOURCE_VERSION = "native-evidence-admission.v1"

#: One row, keyed by the review's OWN composite locator -- never by the PR
#: it belongs to, which is exactly the distinction CHAOS-3633 exists to
#: preserve. ``number``/``repo_id`` are read back so the linked PR can be
#: derived from THIS row, not trusted from the candidate.
_REVIEW_RESOLVE_SQL = """
SELECT repo_id, number, review_id, reviewer, state, submitted_at, last_synced
FROM git_pull_request_reviews FINAL
WHERE org_id = {org_id:String}
  AND concat(toString(repo_id), '#pr', toString(number), '#review', review_id) = {locator:String}
LIMIT 1
"""


def _review_pr_entity_id(row: Mapping[str, Any]) -> str:
    return f"{row['repo_id']}#pr{row['number']}"


#: CHAOS-3675 PR2. ``operational_incidents`` has no PR/work_item/project
#: link column at all -- the only repository linkage is this LEFT JOIN,
#: which can miss entirely (no edge row), not just carry a null
#: ``repo_id``. ``org_id`` types differ across the two tables
#: (``operational_incidents.org_id`` is ``String``,
#: ``work_graph_deployment_incident_edges.org_id`` is ``UUID``) --
#: ``toUUIDOrZero`` matches the cast ``native_evidence.py``'s own
#: ``incidents`` ``_SourceSpec`` already uses for this exact join.
_INCIDENT_RESOLVE_SQL = """
SELECT i.id AS id, i.title AS title, i.normalized_status AS normalized_status,
       coalesce(i.source_event_at, i.observed_at) AS observed_at,
       i.last_synced AS last_synced, e.repo_id AS repo_id
FROM operational_incidents AS i FINAL
LEFT JOIN work_graph_deployment_incident_edges AS e FINAL
  ON e.org_id = toUUIDOrZero(i.org_id) AND e.incident_id = i.id
WHERE i.org_id = {org_id:String} AND i.id = {locator:String}
LIMIT 1
"""


async def _resolve_incident(
    client: Any,
    *,
    org_id: str,
    candidate: EvidenceCandidate,
    policy: SourceFreshnessPolicy,
    source_system: str,
) -> EvidenceRecord | None:
    rows = await query_dicts(
        client, _INCIDENT_RESOLVE_SQL, {"org_id": org_id, "locator": candidate.locator}
    )
    if not rows:
        return None
    row = rows[0]
    repo_id = row.get("repo_id")
    if not repo_id:
        # No linked repository at all (missing edge row, or a null
        # repo_id on the edge that exists) -- EntityKind has no INCIDENT
        # member, and a repository is never entity-check-able (that's
        # what no_authorizable_entity is for), so with no repository
        # either there is NO authorization anchor whatsoever. Refused
        # here, not emitted and left to admit()'s own anchor guard alone.
        return None
    observed = _datetime(row.get("observed_at")) or datetime.now(UTC)
    last_synced = _datetime(row.get("last_synced"))
    freshness = policy.classify(last_synced, now=datetime.now(UTC))
    return EvidenceRecord(
        source_system=source_system,
        source_version=RESOLVER_SOURCE_VERSION,
        entity_type="incident",
        # Descriptive only -- see the module docstring. Incidents have no
        # directly-authorizable entity in the current canonical schema at
        # all (only ever a repository), so no_authorizable_entity is
        # ALWAYS True whenever this resolver succeeds; there is no
        # "sometimes derive an entity" branch to launder past, unlike the
        # review/deployment resolvers.
        entity_id=str(row["id"]),
        display_label=f"Incident: {row.get('title') or row['id']}",
        observed_at=observed,
        freshness=freshness,
        provenance="native",
        confidence=1.0,
        repository_ids=(str(repo_id),),
        raw_excerpt=f"Status: {row.get('normalized_status') or 'unknown'}",
        stale=freshness is FreshnessState.STALE,
        no_authorizable_entity=True,
    )


#: CHAOS-3675 PR3. ``deployments.repo_id`` is NOT nullable -- a repository
#: anchor always exists -- but ``pull_request_number`` IS, so unlike
#: ``review``/``incident`` this row genuinely sometimes has a
#: directly-authorizable PR entity and sometimes does not. Both branches
#: are derived from THIS row, never from the candidate.
_DEPLOYMENT_RESOLVE_SQL = """
SELECT repo_id, deployment_id, status, environment, pull_request_number,
       coalesce(deployed_at, finished_at, started_at, last_synced) AS observed_at,
       last_synced
FROM deployments FINAL
WHERE org_id = {org_id:String}
  AND concat(toString(repo_id), '#deployment', deployment_id) = {locator:String}
LIMIT 1
"""


async def _resolve_deployment(
    client: Any,
    *,
    org_id: str,
    candidate: EvidenceCandidate,
    policy: SourceFreshnessPolicy,
    source_system: str,
) -> EvidenceRecord | None:
    rows = await query_dicts(
        client,
        _DEPLOYMENT_RESOLVE_SQL,
        {"org_id": org_id, "locator": candidate.locator},
    )
    if not rows:
        return None
    row = rows[0]
    observed = _datetime(row.get("observed_at")) or datetime.now(UTC)
    last_synced = _datetime(row.get("last_synced"))
    freshness = policy.classify(last_synced, now=datetime.now(UTC))
    pr_number = row.get("pull_request_number")
    display_label = f"Deployment {row['deployment_id']}"
    raw_excerpt = (
        f"Status: {row.get('status') or 'unknown'}. "
        f"Environment: {row.get('environment') or 'unknown'}"
    )
    repository_ids = (str(row["repo_id"]),)
    stale = freshness is FreshnessState.STALE
    if pr_number:
        # A real PR link exists on THIS row -- always derived when present,
        # never skipped for the cheaper anchor-only path (CHAOS-3675 PR3
        # scope-lock condition: anti-laundering, the first resolver here
        # with a genuine per-row choice between the two).
        return EvidenceRecord(
            source_system=source_system,
            source_version=RESOLVER_SOURCE_VERSION,
            entity_type="deployment",
            entity_id=f"{row['repo_id']}#pr{pr_number}",
            display_label=display_label,
            observed_at=observed,
            freshness=freshness,
            provenance="native",
            confidence=1.0,
            repository_ids=repository_ids,
            raw_excerpt=raw_excerpt,
            stale=stale,
        )
    # No PR link on this row -- repo_id is schema-guaranteed non-null, so
    # the repository-only path always has a real anchor to attach to; this
    # never reaches #1648's no-anchor refusal the way a repo-less incident
    # can.
    return EvidenceRecord(
        source_system=source_system,
        source_version=RESOLVER_SOURCE_VERSION,
        entity_type="deployment",
        entity_id=str(row["deployment_id"]),
        display_label=display_label,
        observed_at=observed,
        freshness=freshness,
        provenance="native",
        confidence=1.0,
        repository_ids=repository_ids,
        raw_excerpt=raw_excerpt,
        stale=stale,
        no_authorizable_entity=True,
    )


async def _resolve_review(
    client: Any,
    *,
    org_id: str,
    candidate: EvidenceCandidate,
    policy: SourceFreshnessPolicy,
    source_system: str,
) -> EvidenceRecord | None:
    rows = await query_dicts(
        client, _REVIEW_RESOLVE_SQL, {"org_id": org_id, "locator": candidate.locator}
    )
    if not rows:
        return None
    row = rows[0]
    observed = _datetime(row.get("submitted_at")) or datetime.now(UTC)
    last_synced = _datetime(row.get("last_synced"))
    freshness = policy.classify(last_synced, now=datetime.now(UTC))
    return EvidenceRecord(
        source_system=source_system,
        source_version=RESOLVER_SOURCE_VERSION,
        entity_type="review",
        # Derived from the row's own repo_id/number, never from
        # candidate.entity_id -- see the module docstring.
        entity_id=_review_pr_entity_id(row),
        display_label=f"Review by {row.get('reviewer') or 'unknown'}",
        observed_at=observed,
        freshness=freshness,
        provenance="native",
        confidence=1.0,
        repository_ids=(str(row["repo_id"]),),
        raw_excerpt=f"Review state: {row.get('state') or 'unknown'}",
        stale=freshness is FreshnessState.STALE,
    )


class NativeEvidenceCandidateResolver:
    """Resolves ``context_fabric_graph_arm`` candidates against real
    ClickHouse native evidence tables, one ``GraphObservationKind`` at a
    time.

    Additive: only observation kinds with an implemented ``_resolve_*``
    below are handled; every other kind returns ``None`` (refused as
    ``NO_MATCHES``), never an error. CHAOS-3675 PR 1/3 implements
    ``review`` (always a directly-authorizable PR entity); PR 2/3 adds
    ``incident`` (never a directly-authorizable entity in the schema at
    all -- always repository-only, via CHAOS-3675's entity-less admission
    mechanism); PR 3/3 adds ``deployment`` (SOMETIMES a directly-
    authorizable PR entity -- ``pull_request_number`` is nullable but
    ``repo_id`` is not, so this is the first resolver with a genuine
    per-row choice between the two paths).

    ``commit``/``ci_run`` are a deliberate, recorded gap, not an oversight:
    neither ``git_commits`` nor ``ci_pipeline_runs`` has any PR/issue link
    column, and the only possible linkage -- the generic ``work_graph_edges``
    table -- carries a ``provenance``/``confidence`` spread (native,
    explicit_text, heuristic) that needs its own trust-threshold design
    decision before it can safely grant entity-level authorization. Per
    the CHAOS-3675 PR 3/3 scope-lock: refusing these two with the gap
    recorded beats a speculative join.
    """

    def __init__(
        self,
        client: Any,
        *,
        policies: Mapping[str, SourceFreshnessPolicy] | None = None,
    ) -> None:
        # CHAOS-3617 containment: the production tree may not import the
        # arm at module scope (``derived_store_registry`` is the one
        # existing lazy-import exception; this is the second). Deferred to
        # construction time, not module import time.
        from dev_health_ops.context_fabric.graph_arm.admission import (
            ARM_SOURCE_SYSTEM,
        )

        self.source_system = ARM_SOURCE_SYSTEM
        self._client = client
        self._policies = dict(policies or default_native_freshness_policies())

    async def resolve(
        self,
        *,
        org_id: str,
        scope: ScopeResolution,
        candidate: EvidenceCandidate,
    ) -> EvidenceRecord | None:
        if candidate.entity_type == "review":
            return await _resolve_review(
                self._client,
                org_id=org_id,
                candidate=candidate,
                policy=self._policies["reviews"],
                source_system=self.source_system,
            )
        if candidate.entity_type == "incident":
            return await _resolve_incident(
                self._client,
                org_id=org_id,
                candidate=candidate,
                policy=self._policies["incidents"],
                source_system=self.source_system,
            )
        if candidate.entity_type == "deployment":
            return await _resolve_deployment(
                self._client,
                org_id=org_id,
                candidate=candidate,
                policy=self._policies["deployments"],
                source_system=self.source_system,
            )
        return None
