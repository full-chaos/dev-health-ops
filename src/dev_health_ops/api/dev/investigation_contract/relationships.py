"""The trial's closed technical relationship allowlist (CHAOS-3615).

Two things this allowlist is, and one thing it deliberately is not.

**It is a traversal bound.** An investigation arm may only emit hops whose
relationship type appears here, which keeps traversal finite, reviewable and
authorization-checkable. Twelve types is the whole vocabulary.

**It is an orientation registry.** Each type declares the ``(source_kind,
target_kind)`` pairs that are legal in its *canonical* direction. That turns
"a relationship is reversed" — one of the named fault modes — from a
judgment call into a validation error: a hop that claims ``FORWARD`` while
its endpoints only match the reverse orientation is rejected by
``packet.LineageHop.validate_direction_matches_allowlist``.

**It is not a requirement to pre-model every possible human question.** The
correction addendum is explicit about that, and the distinction is worth
stating precisely: these are *technical* edges between canonical entities
(a PR implements an issue; a project depends on a service). The human
question — "why is ACR still not finished?" — is answered by *composing*
these edges with canonical measurements, not by adding a
``why_is_it_not_finished`` edge type. Adding a type here should be rare and
should follow a new class of entity, never a new class of question.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum

from dev_health_ops.api.dev.contracts import Label, ShortText
from dev_health_ops.api.dev.contracts_v2.base import ContractModelV2

from .vocabulary import InvestigationSubjectKind

__all__ = [
    "ALL_RELATIONSHIP_TYPES",
    "RELATIONSHIP_ALLOWLIST",
    "EntityPair",
    "RelationshipOrientation",
    "RelationshipType",
    "validate_relationship_allowlist",
]

_KIND = InvestigationSubjectKind


class RelationshipType(StrEnum):
    """The closed technical relationship vocabulary for the trial."""

    DEPENDS_ON = "depends_on"
    BLOCKED_BY = "blocked_by"
    OWNED_BY_TEAM = "owned_by_team"
    CONTRIBUTES_TO = "contributes_to"
    PARENT_OF = "parent_of"
    IMPLEMENTED_BY = "implemented_by"
    REVIEWS = "reviews"
    DEPLOYS = "deploys"
    OPERATES = "operates"
    REFERENCES = "references"
    BELONGS_TO_PORTFOLIO = "belongs_to_portfolio"
    SHARES_DEPENDENCY_WITH = "shares_dependency_with"


ALL_RELATIONSHIP_TYPES: tuple[RelationshipType, ...] = (
    RelationshipType.DEPENDS_ON,
    RelationshipType.BLOCKED_BY,
    RelationshipType.OWNED_BY_TEAM,
    RelationshipType.CONTRIBUTES_TO,
    RelationshipType.PARENT_OF,
    RelationshipType.IMPLEMENTED_BY,
    RelationshipType.REVIEWS,
    RelationshipType.DEPLOYS,
    RelationshipType.OPERATES,
    RelationshipType.REFERENCES,
    RelationshipType.BELONGS_TO_PORTFOLIO,
    RelationshipType.SHARES_DEPENDENCY_WITH,
)


class EntityPair(ContractModelV2):
    """One legal ``(source_kind, target_kind)`` ordering for a relationship."""

    source_kind: InvestigationSubjectKind
    target_kind: InvestigationSubjectKind


class RelationshipOrientation(ContractModelV2):
    """The canonical orientation and endpoint kinds of one relationship type.

    ``symmetric`` types (only ``SHARES_DEPENDENCY_WITH`` today) accept either
    ordering; every other type has a single canonical reading, and a hop must
    say which way it was traversed.
    """

    relationship: RelationshipType
    title: Label
    canonical_reading: ShortText
    forward_pairs: tuple[EntityPair, ...]
    symmetric: bool

    def permits(
        self,
        source_kind: InvestigationSubjectKind,
        target_kind: InvestigationSubjectKind,
    ) -> bool:
        """Whether ``source_kind -> target_kind`` is the canonical ordering."""

        if self.symmetric:
            return any(
                (pair.source_kind, pair.target_kind)
                in {(source_kind, target_kind), (target_kind, source_kind)}
                for pair in self.forward_pairs
            )
        return any(
            pair.source_kind == source_kind and pair.target_kind == target_kind
            for pair in self.forward_pairs
        )


def _orientation(
    relationship: RelationshipType,
    title: str,
    canonical_reading: str,
    pairs: tuple[tuple[InvestigationSubjectKind, InvestigationSubjectKind], ...],
    *,
    symmetric: bool = False,
) -> RelationshipOrientation:
    return RelationshipOrientation(
        relationship=relationship,
        title=title,
        canonical_reading=canonical_reading,
        forward_pairs=tuple(
            EntityPair(source_kind=source, target_kind=target)
            for source, target in pairs
        ),
        symmetric=symmetric,
    )


RELATIONSHIP_ALLOWLIST: Mapping[RelationshipType, RelationshipOrientation] = {
    RelationshipType.DEPENDS_ON: _orientation(
        RelationshipType.DEPENDS_ON,
        "Depends on",
        "the dependent entity is the source; the thing depended upon is the target",
        (
            (_KIND.PROJECT, _KIND.SERVICE),
            (_KIND.PROJECT, _KIND.DEPENDENCY),
            (_KIND.PROJECT, _KIND.PROJECT),
            (_KIND.SERVICE, _KIND.SERVICE),
            (_KIND.SERVICE, _KIND.DEPENDENCY),
            (_KIND.REPOSITORY, _KIND.DEPENDENCY),
            (_KIND.INITIATIVE, _KIND.PROJECT),
        ),
    ),
    RelationshipType.BLOCKED_BY: _orientation(
        RelationshipType.BLOCKED_BY,
        "Blocked by",
        "the blocked entity is the source; the blocker is the target",
        (
            (_KIND.WORK_UNIT, _KIND.WORK_UNIT),
            (_KIND.WORK_UNIT, _KIND.ISSUE),
            (_KIND.ISSUE, _KIND.ISSUE),
            (_KIND.PROJECT, _KIND.WORK_UNIT),
            (_KIND.PROJECT, _KIND.DEPENDENCY),
            (_KIND.PULL_REQUEST, _KIND.PULL_REQUEST),
        ),
    ),
    RelationshipType.OWNED_BY_TEAM: _orientation(
        RelationshipType.OWNED_BY_TEAM,
        "Owned by team",
        "the owned entity is the source; the owning team is the target",
        (
            (_KIND.PROJECT, _KIND.TEAM),
            (_KIND.REPOSITORY, _KIND.TEAM),
            (_KIND.SERVICE, _KIND.TEAM),
            (_KIND.WORK_UNIT, _KIND.TEAM),
            (_KIND.INITIATIVE, _KIND.TEAM),
        ),
    ),
    RelationshipType.CONTRIBUTES_TO: _orientation(
        RelationshipType.CONTRIBUTES_TO,
        "Contributes to",
        "the contributing artifact is the source; the larger unit is the target",
        (
            (_KIND.WORK_UNIT, _KIND.PROJECT),
            (_KIND.ISSUE, _KIND.PROJECT),
            (_KIND.PULL_REQUEST, _KIND.PROJECT),
            (_KIND.PROJECT, _KIND.INITIATIVE),
            (_KIND.REPOSITORY, _KIND.PROJECT),
        ),
    ),
    RelationshipType.PARENT_OF: _orientation(
        RelationshipType.PARENT_OF,
        "Parent of",
        "the parent is the source; the child is the target",
        (
            (_KIND.INITIATIVE, _KIND.PROJECT),
            (_KIND.PORTFOLIO, _KIND.INITIATIVE),
            (_KIND.WORK_UNIT, _KIND.WORK_UNIT),
            (_KIND.ISSUE, _KIND.ISSUE),
            (_KIND.TEAM, _KIND.TEAM),
        ),
    ),
    RelationshipType.IMPLEMENTED_BY: _orientation(
        RelationshipType.IMPLEMENTED_BY,
        "Implemented by",
        "the unit of intent is the source; the implementing change is the target",
        (
            (_KIND.WORK_UNIT, _KIND.PULL_REQUEST),
            (_KIND.ISSUE, _KIND.PULL_REQUEST),
            (_KIND.PROJECT, _KIND.PULL_REQUEST),
        ),
    ),
    RelationshipType.REVIEWS: _orientation(
        RelationshipType.REVIEWS,
        "Reviews",
        "the reviewing team is the source; the reviewed change is the target",
        (
            (_KIND.TEAM, _KIND.PULL_REQUEST),
            (_KIND.TEAM, _KIND.REPOSITORY),
        ),
    ),
    RelationshipType.DEPLOYS: _orientation(
        RelationshipType.DEPLOYS,
        "Deploys",
        "the deploying change is the source; the deployed service is the target",
        (
            (_KIND.PULL_REQUEST, _KIND.SERVICE),
            (_KIND.REPOSITORY, _KIND.SERVICE),
            (_KIND.PROJECT, _KIND.SERVICE),
        ),
    ),
    RelationshipType.OPERATES: _orientation(
        RelationshipType.OPERATES,
        "Operates",
        "the operating team is the source; the operated service is the target",
        (
            (_KIND.TEAM, _KIND.SERVICE),
            (_KIND.TEAM, _KIND.REPOSITORY),
        ),
    ),
    RelationshipType.REFERENCES: _orientation(
        RelationshipType.REFERENCES,
        "References",
        "the referring artifact is the source; the referenced artifact is the target",
        (
            (_KIND.PULL_REQUEST, _KIND.ISSUE),
            (_KIND.ISSUE, _KIND.WORK_UNIT),
            (_KIND.WORK_UNIT, _KIND.ISSUE),
            (_KIND.PULL_REQUEST, _KIND.WORK_UNIT),
        ),
    ),
    RelationshipType.BELONGS_TO_PORTFOLIO: _orientation(
        RelationshipType.BELONGS_TO_PORTFOLIO,
        "Belongs to portfolio",
        "the member entity is the source; the portfolio is the target",
        (
            (_KIND.PROJECT, _KIND.PORTFOLIO),
            (_KIND.INITIATIVE, _KIND.PORTFOLIO),
            (_KIND.TEAM, _KIND.PORTFOLIO),
        ),
    ),
    RelationshipType.SHARES_DEPENDENCY_WITH: _orientation(
        RelationshipType.SHARES_DEPENDENCY_WITH,
        "Shares dependency with",
        "both endpoints depend on a common third entity; the ordering is arbitrary",
        (
            (_KIND.PROJECT, _KIND.PROJECT),
            (_KIND.SERVICE, _KIND.SERVICE),
            (_KIND.PROJECT, _KIND.SERVICE),
            (_KIND.TEAM, _KIND.TEAM),
        ),
        symmetric=True,
    ),
}


def validate_relationship_allowlist() -> None:
    """Raise unless the allowlist is total, consistent and non-vacuous.

    Called at import time by :mod:`.packet` (which validates hops against
    it), so a malformed allowlist is an import-time failure rather than a
    silently permissive traversal bound.
    """

    declared = tuple(RELATIONSHIP_ALLOWLIST)
    if set(declared) != set(ALL_RELATIONSHIP_TYPES):
        missing = sorted(set(ALL_RELATIONSHIP_TYPES) - set(declared))
        extra = sorted(set(declared) - set(ALL_RELATIONSHIP_TYPES))
        raise RuntimeError(
            "relationship allowlist is not total; "
            f"missing={[str(item) for item in missing]}, "
            f"extra={[str(item) for item in extra]}"
        )
    for relationship, orientation in RELATIONSHIP_ALLOWLIST.items():
        if orientation.relationship is not relationship:
            raise RuntimeError(
                f"relationship allowlist key {relationship} is filed under "
                f"orientation for {orientation.relationship}"
            )
        if not orientation.forward_pairs:
            # A type with no declared endpoint pairs would permit nothing --
            # and, worse, would make every direction check on it vacuous.
            raise RuntimeError(
                f"relationship {relationship} declares no endpoint pairs; "
                "a type that permits no ordering makes its own direction "
                "check vacuous"
            )
        seen: set[tuple[str, str]] = set()
        for pair in orientation.forward_pairs:
            key = (str(pair.source_kind), str(pair.target_kind))
            if key in seen:
                raise RuntimeError(
                    f"relationship {relationship} repeats endpoint pair {key}"
                )
            seen.add(key)


validate_relationship_allowlist()
