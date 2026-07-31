"""``dev_subject_mention.v1``, ``dev_resolution_ledger.v1``, ``dev_subject_set.v1``.

Amendment TRD v2 §4.2. The resolution ledger is the load-bearing type here:
it must make it structurally impossible for a later resolution to erase an
earlier unresolved mention. Two mechanisms enforce that jointly:

1. ``ContractModelV2`` is frozen — an existing ``DevResolutionLedger`` can
   never be mutated in place, only superseded by constructing a new one.
2. ``DevResolutionLedger`` itself requires ``entries`` to carry contiguous,
   strictly increasing ``entry_ordinal`` values starting at zero. Because a
   ledger is validated as a whole, dropping or rewriting any existing entry
   (to "erase" it) breaks contiguity and fails validation; the only valid
   way to add a new resolution is to construct a new ledger whose entries
   list is the old one plus more appended at the end.
   ``validate_ledger_extends`` enforces that append-only relationship
   between two ledger snapshots explicitly, for callers (e.g. persistence)
   that hold a previous ledger and a candidate replacement.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from .base import ContractModelV2, EntityKind, Label, OpaqueID, ShortText, Version

__all__ = [
    "DevEntityRefV2",
    "DevResolutionCandidate",
    "DevResolutionEntry",
    "DevResolutionLedger",
    "DevSubjectMention",
    "DevSubjectSet",
    "ResolutionOutcome",
    "validate_ledger_extends",
]


class DevEntityRefV2(ContractModelV2):
    """Embedded-only entity reference (no standalone schema_version, matching
    v1's ``DevEntityRef`` convention — this is a nested block, not one of the
    top-level manifest contracts)."""

    entity_kind: EntityKind
    entity_id: OpaqueID
    display_label: Label
    repository_id: OpaqueID | None = None
    team_id: OpaqueID | None = None


class DevSubjectMention(ContractModelV2):
    schema_version: Literal["dev_subject_mention.v1"]
    mention_id: OpaqueID
    mention_ordinal: int = Field(ge=0, le=24)
    original_text_span: ShortText
    requested_entity_kind: EntityKind
    normalized_lookup_text: ShortText


class ResolutionOutcome(StrEnum):
    EXACT_MATCH = "exact_match"
    AMBIGUOUS_CANDIDATES = "ambiguous_candidates"
    NO_AUTHORIZED_MATCH = "no_authorized_match"
    CATALOG_UNAVAILABLE = "catalog_unavailable"
    UNSUPPORTED_KIND = "unsupported_kind"


#: Outcomes that leave a mention without a committed subject.
UNRESOLVED_OUTCOMES = frozenset(
    {
        ResolutionOutcome.AMBIGUOUS_CANDIDATES,
        ResolutionOutcome.NO_AUTHORIZED_MATCH,
        ResolutionOutcome.CATALOG_UNAVAILABLE,
        ResolutionOutcome.UNSUPPORTED_KIND,
    }
)


class DevResolutionCandidate(ContractModelV2):
    entity_ref: DevEntityRefV2
    reason: ShortText


class DevResolutionEntry(ContractModelV2):
    """One immutable ledger entry: the resolver's outcome for one mention.

    Never edited after being appended to a ledger — see module docstring.
    """

    entry_ordinal: int = Field(ge=0, le=99)
    mention_id: OpaqueID
    outcome: ResolutionOutcome
    committed_entity_ref: DevEntityRefV2 | None = None
    candidates: tuple[DevResolutionCandidate, ...] = Field(
        default_factory=tuple, max_length=25
    )
    repository_attribution: OpaqueID | None = None
    team_attribution: OpaqueID | None = None
    resolver_version: Version
    query_version: Version
    resolved_at: AwareDatetime

    @model_validator(mode="after")
    def validate_outcome_payload(self) -> Self:
        if self.outcome is ResolutionOutcome.EXACT_MATCH:
            if self.committed_entity_ref is None:
                raise ValueError("exact_match requires a committed entity reference")
            if self.candidates:
                raise ValueError("exact_match cannot carry candidates")
        elif self.outcome is ResolutionOutcome.AMBIGUOUS_CANDIDATES:
            if self.committed_entity_ref is not None:
                raise ValueError("ambiguous_candidates cannot commit an entity")
            if not self.candidates:
                raise ValueError("ambiguous_candidates requires candidates")
        else:
            if self.committed_entity_ref is not None:
                raise ValueError(f"{self.outcome} cannot commit an entity")
            if self.candidates:
                raise ValueError(f"{self.outcome} cannot carry candidates")
        return self


class DevResolutionLedger(ContractModelV2):
    schema_version: Literal["dev_resolution_ledger.v1"]
    ledger_id: OpaqueID
    mention_ids: tuple[OpaqueID, ...] = Field(min_length=1, max_length=25)
    entries: tuple[DevResolutionEntry, ...] = Field(min_length=1, max_length=100)
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def validate_append_only_structure(self) -> Self:
        if len(set(self.mention_ids)) != len(self.mention_ids):
            raise ValueError("mention ids must be unique")
        ordinals = [entry.entry_ordinal for entry in self.entries]
        if ordinals != list(range(len(self.entries))):
            raise ValueError(
                "ledger entries must carry contiguous ordinals starting at zero "
                "(append-only; an existing entry cannot be dropped or reordered)"
            )
        known_mentions = set(self.mention_ids)
        for entry in self.entries:
            if entry.mention_id not in known_mentions:
                raise ValueError("ledger entry references an unknown mention id")
        covered = {entry.mention_id for entry in self.entries}
        if covered != known_mentions:
            raise ValueError("every mention must have at least one ledger entry")
        return self

    def latest_by_mention(self) -> dict[str, DevResolutionEntry]:
        """The current (highest-ordinal) entry per mention.

        A read helper only — it never removes history from ``entries``.
        """

        latest: dict[str, DevResolutionEntry] = {}
        for entry in self.entries:
            latest[entry.mention_id] = entry
        return latest


def validate_ledger_extends(
    previous: DevResolutionLedger, candidate: DevResolutionLedger
) -> None:
    """Raise unless ``candidate`` is ``previous`` plus zero or more appended entries.

    This is the explicit cross-snapshot half of the append-only guarantee:
    a persistence layer holding the previous ledger can use this to reject
    any candidate replacement that rewrites or drops an already-recorded
    entry, including one that only *appears* to still contain the earlier
    entries but has mutated their content.
    """

    if candidate.ledger_id != previous.ledger_id:
        raise ValueError("ledger id must be stable across resolution updates")
    if len(candidate.entries) < len(previous.entries):
        raise ValueError("resolution ledger cannot shrink")
    prefix = candidate.entries[: len(previous.entries)]
    if prefix != previous.entries:
        raise ValueError(
            "resolution ledger cannot rewrite or erase a prior entry; "
            "later resolutions must be appended, not substituted"
        )


class DevSubjectSet(ContractModelV2):
    """A homogeneous, bounded, authorization-safe committed subject cohort."""

    schema_version: Literal["dev_subject_set.v1"]
    set_id: OpaqueID
    entity_kind: EntityKind
    committed_entity_refs: tuple[DevEntityRefV2, ...] = Field(
        min_length=1, max_length=25
    )
    original_mention_count: int = Field(ge=0, le=100)
    unresolved_mention_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=100
    )
    ambiguous_mention_ids: tuple[OpaqueID, ...] = Field(
        default_factory=tuple, max_length=100
    )
    cohort_complete: bool
    warnings: tuple[ShortText, ...] = Field(default_factory=tuple, max_length=20)
    fingerprint: OpaqueID

    @model_validator(mode="after")
    def validate_set_invariants(self) -> Self:
        kinds = {ref.entity_kind for ref in self.committed_entity_refs}
        if kinds and kinds != {self.entity_kind}:
            raise ValueError("subject set must be homogeneous in entity kind")
        committed_ids = [ref.entity_id for ref in self.committed_entity_refs]
        if len(committed_ids) != len(set(committed_ids)):
            raise ValueError("committed entity references must be unique")
        if set(self.unresolved_mention_ids) & set(self.ambiguous_mention_ids):
            raise ValueError("a mention cannot be both unresolved and ambiguous")
        omitted = set(self.unresolved_mention_ids) | set(self.ambiguous_mention_ids)
        accounted = len(self.committed_entity_refs) + len(omitted)
        if accounted > self.original_mention_count:
            raise ValueError(
                "committed and omitted mentions cannot exceed the original mention count"
            )
        if omitted and self.cohort_complete:
            raise ValueError("a cohort with omitted mentions cannot be marked complete")
        if not omitted and not self.cohort_complete:
            raise ValueError(
                "a cohort with no omitted mentions must be marked complete"
            )
        if omitted and not self.warnings:
            raise ValueError("omitted mentions must be disclosed via warnings")
        return self
