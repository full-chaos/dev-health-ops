"""ClickHouse-backed authorized entity catalog for Ask Dev scope resolution."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.api.queries.scopes import resolve_repo_id
from dev_health_ops.api.services.identity import resolve_scope_display_names

from .scope_service import AuthorizedEntity, EntityKind, ScopeRef

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
        entities.sort(
            key=lambda entity: (
                entity.label.casefold(),
                entity.kind.value,
                entity.canonical_id,
            )
        )
        return entities[:limit]

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
