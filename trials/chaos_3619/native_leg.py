"""The native baseline, driven through the REAL orchestrator on corpus data.

Why the orchestrator rather than a hand-built ``NativeProjectionInput``: the
native arm's interpretation, resolution ledger, committed subject and subject
set are exactly what is being baselined, and hand-building them would be the
trial author baselining themselves. Subject discovery and ambiguity are half
this corpus. So the leg seeds the production catalog with corpus entities and
drives ``run_preflight_orchestrator``, which runs the real interpreter, the
real subject preflight and the real scope service; the native producer then
projects whatever that run actually produced.

**The catalog is the principal's GRANT, not the tenant.** The corpus plants a
restricted project inside the analyst's own tenant precisely so that a
tenant-scoped catalog looks correct and is not. Seeding the grant also keeps
the two arms symmetric: the graph leg reads with the same grant, so neither
arm is answering from a wider world than the other.

**A structural limitation this leg cannot paper over, and does not.** The
production subject vocabulary (``EntityKind``) has six members. The corpus
world models ten kinds, so ``service``, ``portfolio``, ``dependency`` and
``initiative`` entities **cannot enter the native catalog at all** -- there is
no ``EntityKind`` to carry them. That is the same gap the arm already names
as ``capabilities.UNREACHABLE_SUBJECT_KINDS``, reached from the other side:
that constant says which subject kinds a native packet can never *emit*, and
this module measures how many corpus entities can never be *seen*.

:func:`catalog_entities` therefore returns the skipped entities alongside the
seeded ones rather than filtering silently. A catalog that quietly dropped a
tenth of the world would make every native miss on those subjects look like a
resolution failure, when it is a vocabulary gap -- and the trial would report
a capability difference that is really a type-system difference.
"""

from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.api.dev.investigation_corpus import world
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind

__all__ = [
    "NATIVE_CATALOG_KIND",
    "CatalogSeeding",
    "catalog_entities",
    "unrepresentable_corpus_kinds",
]

#: Corpus entity kind -> the production CATALOG subject vocabulary.
#:
#: ``scope_service.EntityKind``, NOT ``contracts_v2.base.EntityKind``. There
#: are two enums of that name and they are not the same type: the scope one
#: carries an extra ``organization`` member. The first version of this module
#: imported the contract enum, and the probe ran fine -- both are ``StrEnum``
#: with identical values for the six kinds used here, so nothing failed at
#: runtime. mypy caught it. Left as a comment rather than silently corrected
#: because "these two same-named enums are interchangeable" is exactly the
#: assumption that stops being true the day one of them gains a member.
#:
#: Deliberately partial, and the partiality is the finding. Every key here is
#: an exact counterpart, never an approximation: mapping ``service`` onto
#: ``project`` would let the native arm "resolve" a service by silently
#: answering about something else, which is a wrong-but-confident subject --
#: the exact fault mode the frozen registry names
#: ``wrong_but_similar_subject_ranked_first``.
NATIVE_CATALOG_KIND: dict[str, EntityKind] = {
    "project": EntityKind.PROJECT,
    "team": EntityKind.TEAM,
    "repository": EntityKind.REPOSITORY,
    "work_unit": EntityKind.WORK_UNIT,
    "issue": EntityKind.ISSUE,
    "pull_request": EntityKind.PULL_REQUEST,
}


@dataclass(frozen=True, slots=True)
class CatalogSeeding:
    """What the native catalog could and could not be given.

    ``skipped`` is not an error list. It is a measured statement of how much
    of the authorized world the production subject vocabulary cannot express,
    and it belongs in the fairness table beside the graph arm's own figures.
    """

    entities: tuple[tuple[str, AuthorizedEntity], ...]
    skipped: tuple[tuple[str, str], ...]

    @property
    def skipped_kinds(self) -> frozenset[str]:
        return frozenset(kind for _id, kind in self.skipped)


def unrepresentable_corpus_kinds() -> frozenset[str]:
    """Corpus entity kinds with no ``EntityKind`` counterpart.

    Derived by difference rather than listed, so a new corpus kind -- or a
    new ``EntityKind`` -- changes this without anyone remembering to.
    """

    corpus_kinds = {
        entity.kind.value if hasattr(entity.kind, "value") else str(entity.kind)
        for entity in world.WORLD_ENTITIES
    }
    return frozenset(corpus_kinds - set(NATIVE_CATALOG_KIND))


def catalog_entities(principal_id: str) -> CatalogSeeding:
    """Seed the production catalog from one principal's true grant.

    Grant-scoped, not tenant-scoped: see the module docstring. Entities whose
    kind the production vocabulary cannot carry are reported in ``skipped``
    rather than dropped, because a silent drop turns a vocabulary gap into an
    apparent resolution failure.
    """

    grant = world.authorized_entity_ids(principal_id)
    tenant = world.PRINCIPALS[principal_id].tenant_id
    seeded: list[tuple[str, AuthorizedEntity]] = []
    skipped: list[tuple[str, str]] = []
    for entity in world.WORLD_ENTITIES:
        if entity.entity_id not in grant:
            continue
        if entity.tenant_id != tenant:
            # A grant naming another tenant's entity would be a corpus
            # defect, not something to seed around. Skipped and counted so
            # it cannot pass unnoticed.
            skipped.append((entity.entity_id, "cross_tenant_grant"))
            continue
        kind_value = (
            entity.kind.value if hasattr(entity.kind, "value") else str(entity.kind)
        )
        kind = NATIVE_CATALOG_KIND.get(kind_value)
        if kind is None:
            skipped.append((entity.entity_id, kind_value))
            continue
        seeded.append(
            (
                tenant,
                AuthorizedEntity(
                    kind=kind,
                    canonical_id=entity.entity_id,
                    label=entity.display_label,
                ),
            )
        )
    return CatalogSeeding(entities=tuple(seeded), skipped=tuple(skipped))
