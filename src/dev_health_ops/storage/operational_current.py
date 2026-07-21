from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from dev_health_ops.models.operational import OPERATIONAL_ENTITY_TABLES

from .operational_ordering_guard import (
    OperationalOrderingContract,
    configured_operational_ordering_contract,
)

_CANONICAL_TABLES = frozenset(OPERATIONAL_ENTITY_TABLES.values())
_ORG_SCOPES = frozenset({"org_id = {org_id:String}", "org_id = %(org_id)s"})


@dataclass(frozen=True, slots=True)
class OperationalCurrentReadError(ValueError):
    value: str

    def __str__(self) -> str:
        return f"invalid canonical current-row query input: {self.value}"


def current_operational_rows_sql(
    table: str,
    post_selection_filters: Sequence[str] = (),
    org_scope: str = "org_id = {org_id:String}",
    ordering_contract: OperationalOrderingContract | None = None,
) -> str:
    if table not in _CANONICAL_TABLES:
        raise OperationalCurrentReadError(table)
    if org_scope not in _ORG_SCOPES:
        raise OperationalCurrentReadError(org_scope)
    if any(
        not predicate.strip() or ";" in predicate
        for predicate in post_selection_filters
    ):
        raise OperationalCurrentReadError("invalid post-selection filter")
    outer_filter = (
        f"WHERE {' AND '.join(post_selection_filters)}"
        if post_selection_filters
        else ""
    )
    contract = ordering_contract or configured_operational_ordering_contract()
    if contract is OperationalOrderingContract.LEGACY:
        return f"""(
        SELECT *
        FROM (
            SELECT *
            FROM {table} FINAL
            WHERE {org_scope}
        )
        {outer_filter}
    )"""
    return f"""(
        SELECT *
        FROM (
            SELECT *
            FROM {table}
            WHERE {org_scope}
            ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC
            LIMIT 1 BY org_id, id
        )
        {outer_filter}
    )"""
