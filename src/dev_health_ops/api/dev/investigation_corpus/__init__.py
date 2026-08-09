"""CHAOS-3616: the Ask Dev intelligence corpus and its independent oracles.

The corrected CHAOS-3614 trial needs expectations that were authored **before
any arm output existed** and cannot be adjusted after one does. That is what
this package is: a pinned synthetic world, a machine-readable case registry,
and per-case oracles that derive what a correct investigation must find from
the world alone.

The division of labour with the frozen CHAOS-3615 contract is deliberate and
worth stating once, because the two are easy to confuse.

``investigation_contract`` answers **"is this packet well formed?"** — it
rejects shapes an arm must not emit: a symptom promoted to principal driver, a
cohort member with no stated basis, a path whose hops do not join, an
authorization envelope the packet's own contents violate.

``investigation_corpus`` answers **"is this packet right?"** — it knows what
is actually true in the world, so it can say that a well-formed packet named
the wrong subject, missed the driver, cited evidence that does not exist, or
declared an authorized set that is false.

Neither subsumes the other, and two of this package's obligations exist
precisely because the contract cannot discharge them:

* :mod:`.authorization` holds the world's **true** per-principal grants. The
  contract can only check a packet against the ``authorized_entity_ids`` the
  producer itself declared (``packet.py:843-878`` says so in its own
  docstring), so an arm that declared the whole organization authorized would
  pass every contract check. This package catches that.
* :mod:`.evaluate` always runs the canonical Pydantic validator over a packet
  before scoring it. The artifact manifest's ``validation_policy`` records
  that schema-only validation is insufficient; an oracle layer that
  schema-validated and stopped would inherit every gap that policy warns
  about.
"""

from __future__ import annotations

from .authorization import AuthorizationAudit, audit_authorization, entity_sightings
from .cases import (
    ALL_CASE_IDS,
    CASE_REGISTRY,
    REQUIRED_CORPUS_TOPICS,
    AnswerDisposition,
    CaseDisposition,
    CorpusCase,
    CorpusFamily,
    authored_cases,
    validate_case_registry,
)
from .coverage import (
    CellStatus,
    CoverageCell,
    coverage_matrix,
    dispositions_table,
    render_dispositions,
    render_matrix,
    validate_coverage,
)
from .evaluate import (
    CaseEvaluation,
    DimensionResult,
    Verdict,
    evaluate_case,
    evaluate_payload,
)
from .export import ARTIFACT_ROOT, check_artifacts, expected_artifacts, write_artifacts
from .oracles import (
    CASE_ORACLES,
    CaseOracle,
    DriverExpectation,
    ForbiddenEvidence,
    ForbiddenReason,
    PathExpectation,
    oracle_for,
    required_evidence_handles,
    validate_oracles,
)
from .reference import reference_packet
from .world import (
    AS_OF_JUL_15,
    AS_OF_JUN_15,
    CORPUS_VERSION,
    ENTITIES_BY_ID,
    EVIDENCE_BY_HANDLE,
    EVIDENCE_BY_SLUG,
    ORG_HELIO,
    ORG_LUMEN,
    PRINCIPAL_ANALYST,
    PRINCIPAL_COMPLIANCE,
    PRINCIPALS,
    RELATIONSHIPS_BY_KEY,
    SOURCE_MANIFEST,
    TRIAL_NOW,
    WINDOW_END,
    WINDOW_START,
    WORLD_DOCUMENTS,
    WORLD_ENTITIES,
    WORLD_EPISODES,
    WORLD_EVIDENCE,
    WORLD_MEASUREMENTS,
    WORLD_RELATIONSHIPS,
    Alias,
    EntityState,
    EvidenceState,
    MeasurementBasis,
    Principal,
    SourceFeed,
    TrustLevel,
    WorldDocument,
    WorldEntity,
    WorldEpisode,
    WorldEvidence,
    WorldMeasurement,
    WorldRelationship,
    authorized_entity_ids,
    evidence_handle,
    validate_world,
)

__all__ = [
    "ALL_CASE_IDS",
    "ARTIFACT_ROOT",
    "check_artifacts",
    "expected_artifacts",
    "write_artifacts",
    "CASE_ORACLES",
    "AuthorizationAudit",
    "CaseEvaluation",
    "CaseOracle",
    "CellStatus",
    "CoverageCell",
    "DimensionResult",
    "DriverExpectation",
    "ForbiddenEvidence",
    "ForbiddenReason",
    "PathExpectation",
    "Verdict",
    "audit_authorization",
    "coverage_matrix",
    "dispositions_table",
    "entity_sightings",
    "evaluate_case",
    "evaluate_payload",
    "oracle_for",
    "reference_packet",
    "render_dispositions",
    "render_matrix",
    "required_evidence_handles",
    "validate_coverage",
    "validate_oracles",
    "AS_OF_JUL_15",
    "AS_OF_JUN_15",
    "CASE_REGISTRY",
    "CORPUS_VERSION",
    "ENTITIES_BY_ID",
    "EVIDENCE_BY_HANDLE",
    "EVIDENCE_BY_SLUG",
    "ORG_HELIO",
    "ORG_LUMEN",
    "PRINCIPALS",
    "PRINCIPAL_ANALYST",
    "PRINCIPAL_COMPLIANCE",
    "RELATIONSHIPS_BY_KEY",
    "REQUIRED_CORPUS_TOPICS",
    "SOURCE_MANIFEST",
    "TRIAL_NOW",
    "WINDOW_END",
    "WINDOW_START",
    "WORLD_DOCUMENTS",
    "WORLD_ENTITIES",
    "WORLD_EPISODES",
    "WORLD_EVIDENCE",
    "WORLD_MEASUREMENTS",
    "WORLD_RELATIONSHIPS",
    "Alias",
    "AnswerDisposition",
    "CaseDisposition",
    "CorpusCase",
    "CorpusFamily",
    "EntityState",
    "EvidenceState",
    "MeasurementBasis",
    "Principal",
    "SourceFeed",
    "TrustLevel",
    "WorldDocument",
    "WorldEntity",
    "WorldEpisode",
    "WorldEvidence",
    "WorldMeasurement",
    "WorldRelationship",
    "authored_cases",
    "authorized_entity_ids",
    "evidence_handle",
    "validate_case_registry",
    "validate_world",
]
