"""ClickHouse-backed authorized entity catalog for Ask Dev scope resolution."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.api.queries.scopes import resolve_repo_id
from dev_health_ops.api.services.identity import resolve_scope_display_names

from .alias_matching import alias_forms
from .scope_service import AuthorizedEntity, EntityKind, ScopeRef

#: CHAOS-3388. Kinds whose display name is a stable proper noun an acronym or
#: parenthetical alias can meaningfully apply to. Deliberately narrow and
#: explicit rather than "every searchable kind": an issue/PR/work-unit
#: "title" is prose, not a name, and taking word-initials of prose produces
#: noise, not a plausible shorthand.
ALIAS_AWARE_ENTITY_KINDS = (EntityKind.PROJECT, EntityKind.TEAM)

#: Bound on the full active-roster fetch the alias/acronym pass runs when
#: the ordinary substring search finds nothing to offer. An acronym cannot be
#: matched by ``LIKE``, so there is no SQL predicate to push this into; the
#: roster is org-scoped and, unlike the unbounded catalog, actually small
#: enough in practice for a bounded in-Python scan to be cheap. This only
#: runs from the already-terminal CHAOS-3366 closest-matches fallback (one
#: bounded call per unresolved named subject), never from the primary
#: resolution path.
_ALIAS_ROSTER_LIMIT = 1000

_ALIAS_ROSTER_SQL: dict[EntityKind, str] = {
    EntityKind.PROJECT: """
        SELECT id AS canonical_id, name AS label
        FROM projects FINAL
        WHERE org_id = {org_id:String} AND is_active = 1
        ORDER BY lowerUTF8(name), canonical_id
        LIMIT {limit:UInt32}
    """,
    EntityKind.TEAM: """
        SELECT id AS canonical_id, name AS label
        FROM teams FINAL
        WHERE org_id = {org_id:String}
        ORDER BY lowerUTF8(name), canonical_id
        LIMIT {limit:UInt32}
    """,
}

_ORGANIZATION_REPOSITORY_IDS_SQL = """
    SELECT toString(id) AS repository_id, count() OVER () AS total_authorized
    FROM repos FINAL
    WHERE org_id = {org_id:String}
    ORDER BY repository_id
    LIMIT {limit:UInt32}
"""


def _entity_sort_key(entity: AuthorizedEntity) -> tuple[str, str, str]:
    return (entity.label.casefold(), entity.kind.value, entity.canonical_id)


_WATERMARK_TABLES: dict[EntityKind, tuple[str, str]] = {
    EntityKind.REPOSITORY: ("repos", "last_synced"),
    EntityKind.PROJECT: ("projects", "updated_at"),
    EntityKind.WORK_UNIT: ("work_unit_investments", "computed_at"),
    EntityKind.ISSUE: ("work_items", "last_synced"),
    EntityKind.PULL_REQUEST: ("git_pull_requests", "last_synced"),
    EntityKind.TEAM: ("teams", "updated_at"),
}


class ClickHouseAuthorizedEntityCatalog:
    """Resolve entities only through tenant-filtered canonical ClickHouse tables."""

    def __init__(self, client: Any) -> None:
        if client is None:
            raise RuntimeError("Database client is required for scope resolution")
        self._client = client

    async def watermark(self, org_id: str, kinds: tuple[EntityKind, ...]) -> str:
        tables = sorted(
            {_WATERMARK_TABLES[kind] for kind in kinds if kind in _WATERMARK_TABLES}
        )
        if not tables:
            return "organization"
        selects = [
            (
                "SELECT maxOrNull(" + column + ") AS watermark "
                f"FROM {table} WHERE org_id = {{org_id:String}}"
            )
            for table, column in tables
        ]
        rows = await query_dicts(
            self._client,
            " UNION ALL ".join(selects),
            {"org_id": org_id},
        )
        values = [self._watermark_value(row.get("watermark")) for row in rows]
        return max(values, default="empty")

    async def exact(
        self, org_id: str, ref: ScopeRef, *, limit: int
    ) -> list[AuthorizedEntity]:
        sql = self._query_for(ref.kind, exact=True)
        rows = await query_dicts(
            self._client,
            sql,
            {"org_id": org_id, "query": ref.value, "limit": limit},
        )
        entities = self._entities(rows, expected_kind=ref.kind)
        if ref.kind is EntityKind.REPOSITORY and len(entities) == 1:
            # Reuse the production org-scoped repository choke point instead
            # of establishing an Ask Dev-specific authorization interpretation.
            verified_id = await resolve_repo_id(self._client, ref.value, org_id=org_id)
            if verified_id != entities[0].canonical_id:
                return []
            entities = await self._with_repository_display_names(org_id, entities)
        return entities

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
        include_alias_matches: bool = False,
    ) -> list[AuthorizedEntity]:
        rows_by_kind = await asyncio.gather(
            *(
                query_dicts(
                    self._client,
                    self._query_for(kind, exact=False),
                    {"org_id": org_id, "query": query, "limit": limit},
                )
                for kind in kinds
            )
        )
        entities: list[AuthorizedEntity] = []
        for kind, rows in zip(kinds, rows_by_kind, strict=True):
            kind_entities = self._entities(rows, expected_kind=kind)
            if kind is EntityKind.REPOSITORY:
                kind_entities = await self._with_repository_display_names(
                    org_id, kind_entities
                )
            entities.extend(kind_entities)
        alias_entities: list[AuthorizedEntity] = []
        if include_alias_matches:
            alias_hits = await asyncio.gather(
                *(
                    self._alias_matches(org_id, kind, query, limit=limit)
                    for kind in kinds
                    if kind in ALIAS_AWARE_ENTITY_KINDS
                )
            )
            for kind_hits in alias_hits:
                alias_entities.extend(kind_hits)
        # Alias/acronym hits are ordered AHEAD of plain substring hits, before
        # either is truncated to `limit` -- never merged into one alphabetical
        # sort. CHAOS-3388 live finding: a real organization can hold far more
        # than `limit` incidental substring hits for a short query
        # (issue/work-unit titles that happen to contain "acr" as a literal
        # substring, e.g. "Harden ACR runtime client boundary") than it holds
        # true acronym matches. A single combined alphabetical sort let that
        # noise crowd the one real acronym match for the named project out of
        # the returned page entirely -- the closest-matches offer named
        # everything BUT the thing the acronym actually stood for. An
        # acronym/alias equality is a strictly more precise signal than an
        # incidental substring containment, so it always survives the bound.
        alias_by_key: dict[tuple[EntityKind, str], AuthorizedEntity] = {}
        for entity in alias_entities:
            alias_by_key.setdefault((entity.kind, entity.canonical_id), entity)
        ordered_alias = sorted(alias_by_key.values(), key=_entity_sort_key)
        substring_by_key: dict[tuple[EntityKind, str], AuthorizedEntity] = {}
        for entity in entities:
            key = (entity.kind, entity.canonical_id)
            if key not in alias_by_key:
                substring_by_key.setdefault(key, entity)
        ordered_substring = sorted(substring_by_key.values(), key=_entity_sort_key)
        return (ordered_alias + ordered_substring)[:limit]

    async def _alias_matches(
        self, org_id: str, kind: EntityKind, query: str, *, limit: int
    ) -> list[AuthorizedEntity]:
        """CHAOS-3388: entities of ``kind`` whose acronym or parenthetical
        alias equals ``query`` outright, found via a bounded roster scan.

        Never called for a kind outside ``ALIAS_AWARE_ENTITY_KINDS``. Every hit is
        returned regardless of provenance (acronym vs. literal alias) --
        ``scope_service.resolve_mention`` is the layer that decides which of
        those is auto-commit eligible; this method only reports "matches
        something", the same contract ``search()``'s substring pass already
        has.
        """

        normalized_query = query.strip().casefold()
        roster_sql = _ALIAS_ROSTER_SQL.get(kind)
        if not normalized_query or roster_sql is None:
            return []
        rows = await query_dicts(
            self._client,
            roster_sql,
            {"org_id": org_id, "limit": _ALIAS_ROSTER_LIMIT},
        )
        matches: list[AuthorizedEntity] = []
        for row in self._entities(rows, expected_kind=kind):
            forms = alias_forms(row.label)
            if (
                normalized_query in forms.literal_aliases
                or normalized_query in forms.acronyms
            ):
                matches.append(row)
        return matches[:limit]

    async def organization_repository_ids(
        self, org_id: str, *, limit: int
    ) -> tuple[list[str], int]:
        # A single query with a window function, not two sequential queries:
        # `count() OVER ()` reflects every matching row before LIMIT clips the
        # returned page, so the id page and the true total are one consistent
        # snapshot. Two separate queries could race a concurrent repository
        # insert/delete into reporting a truncated page as the complete set.
        rows = await query_dicts(
            self._client,
            _ORGANIZATION_REPOSITORY_IDS_SQL,
            {"org_id": org_id, "limit": limit},
        )
        if not rows:
            return [], 0
        ids = sorted(
            {str(row["repository_id"]) for row in rows if row.get("repository_id")}
        )
        total = int(rows[0]["total_authorized"])
        return ids, total

    async def _with_repository_display_names(
        self, org_id: str, entities: list[AuthorizedEntity]
    ) -> list[AuthorizedEntity]:
        names = await resolve_scope_display_names(
            self._client,
            org_id=org_id,
            scope="repo",
            ids=[entity.canonical_id for entity in entities],
        )
        return [
            AuthorizedEntity(
                kind=entity.kind,
                canonical_id=entity.canonical_id,
                label=names.get(entity.canonical_id, entity.label),
                repository_id=entity.repository_id,
            )
            for entity in entities
        ]

    @staticmethod
    def _watermark_value(value: object) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value) if value is not None else "empty"

    @staticmethod
    def _entities(
        rows: Iterable[dict[str, Any]], *, expected_kind: EntityKind
    ) -> list[AuthorizedEntity]:
        entities: list[AuthorizedEntity] = []
        for row in rows:
            canonical_id = str(row.get("canonical_id") or "").strip()
            label = str(row.get("label") or "").strip()
            if not canonical_id or not label:
                continue
            entities.append(
                AuthorizedEntity(
                    kind=expected_kind,
                    canonical_id=canonical_id,
                    label=label,
                    repository_id=str(row["repository_id"])
                    if row.get("repository_id")
                    else None,
                )
            )
        return entities

    @staticmethod
    def _query_for(kind: EntityKind, *, exact: bool) -> str:
        comparison = "=" if exact else "LIKE"
        query_value = (
            "lowerUTF8({query:String})"
            if exact
            else "concat('%', lowerUTF8({query:String}), '%')"
        )

        if kind is EntityKind.REPOSITORY:
            match = (
                "toString(id) = {query:String} OR "
                f"lowerUTF8(repo) {comparison} {query_value}"
            )
            return f"""
                SELECT toString(id) AS canonical_id, repo AS label,
                       toString(id) AS repository_id
                FROM repos FINAL
                WHERE org_id = {{org_id:String}} AND ({match})
                ORDER BY lowerUTF8(label), canonical_id
                LIMIT {{limit:UInt32}}
            """
        if kind is EntityKind.PROJECT:
            exact_ids = (
                "id = {query:String} OR ifNull(project_key, '') = {query:String} OR "
            )
            match = exact_ids + f"lowerUTF8(name) {comparison} {query_value}"
            return f"""
                SELECT id AS canonical_id, name AS label, NULL AS repository_id
                FROM projects FINAL
                WHERE org_id = {{org_id:String}} AND is_active = 1 AND ({match})
                ORDER BY lowerUTF8(label), canonical_id
                LIMIT {{limit:UInt32}}
            """
        if kind is EntityKind.WORK_UNIT:
            match = (
                "work_unit_id = {query:String} OR "
                f"lowerUTF8(ifNull(work_unit_name, '')) {comparison} {query_value}"
            )
            return f"""
                SELECT work_unit_id AS canonical_id,
                       if(empty(ifNull(work_unit_name, '')), 'Work unit', work_unit_name) AS label,
                       toString(repo_id) AS repository_id
                FROM work_unit_investments FINAL
                WHERE org_id = {{org_id:String}} AND ({match})
                ORDER BY lowerUTF8(label), canonical_id
                LIMIT {{limit:UInt32}}
            """
        if kind is EntityKind.ISSUE:
            match = (
                "work_item_id = {query:String} OR "
                f"lowerUTF8(title) {comparison} {query_value}"
            )
            return f"""
                SELECT work_item_id AS canonical_id, title AS label,
                       toString(repo_id) AS repository_id
                FROM work_items FINAL
                WHERE org_id = {{org_id:String}} AND ({match})
                ORDER BY lowerUTF8(label), canonical_id
                LIMIT {{limit:UInt32}}
            """
        if kind is EntityKind.PULL_REQUEST:
            canonical = "concat(toString(repo_id), '#pr', toString(number))"
            match = (
                f"{canonical} = {{query:String}} OR "
                f"lowerUTF8(ifNull(title, '')) {comparison} {query_value}"
            )
            return f"""
                SELECT {canonical} AS canonical_id,
                       if(empty(ifNull(title, '')), concat('Pull request #', toString(number)), title) AS label,
                       toString(repo_id) AS repository_id
                FROM git_pull_requests FINAL
                WHERE org_id = {{org_id:String}} AND ({match})
                ORDER BY lowerUTF8(label), canonical_id
                LIMIT {{limit:UInt32}}
            """
        if kind is EntityKind.TEAM:
            match = (
                "id = {query:String} OR "
                + f"lowerUTF8(name) {comparison} {query_value}"
            )
            return f"""
                SELECT id AS canonical_id, name AS label, NULL AS repository_id
                FROM teams FINAL
                WHERE org_id = {{org_id:String}} AND ({match})
                ORDER BY lowerUTF8(label), canonical_id
                LIMIT {{limit:UInt32}}
            """
        kind_value = kind.value if isinstance(kind, EntityKind) else str(kind)
        raise ValueError(f"Entity kind {kind_value!r} is not catalog-searchable")
