"""Query-fragment builder used by ClickHouse data loaders.

Encapsulates the "AND org_id = {org_id:String}" filter and parameter
injection so individual loader methods don't repeat the same branch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = ["OrgScopedQuery"]


@dataclass(frozen=True)
class OrgScopedQuery:
    """Immutable helper for scoping ClickHouse queries to an org."""

    org_id: str = ""

    def __bool__(self) -> bool:
        return bool(self.org_id)

    def filter(self, *, alias: str = "") -> str:
        """Return ``" AND {alias?.}org_id = {org_id:String}"`` or ``""``.

        ``alias`` is interpolated into SQL directly and MUST be a valid SQL
        identifier (ASCII letters/digits/underscore, non-digit first char).
        Defense in depth: all known call sites pass hardcoded literals, but
        a stray user-supplied value would be a SQL-injection vector without
        this check.
        """
        if alias and not alias.isidentifier():
            raise ValueError(
                "OrgScopedQuery.filter: alias must be a valid identifier, "
                f"got {alias!r}"
            )
        if not self.org_id:
            return ""
        col = f"{alias}.org_id" if alias else "org_id"
        return f" AND {col} = {{org_id:String}}"

    def expression(self, *, alias: str = "") -> str:
        """Return ``"{alias?.}org_id = {org_id:String}"`` or ``""``.

        Companion to :meth:`filter` for callers that build a list of filter
        clauses and join them with ``" AND "``. Using :meth:`filter` in that
        context would produce ``"... AND  AND org_id = ..."`` because
        :meth:`filter` carries the connecting ``" AND "`` prefix.

        Same identifier safety check as :meth:`filter`.
        """
        if alias and not alias.isidentifier():
            raise ValueError(
                "OrgScopedQuery.expression: alias must be a valid identifier, "
                f"got {alias!r}"
            )
        if not self.org_id:
            return ""
        col = f"{alias}.org_id" if alias else "org_id"
        return f"{col} = {{org_id:String}}"

    def filter_uuid(self, *, alias: str = "") -> str:
        """Return ``" AND toString({alias?.}org_id) = {org_id:String}"`` or ``""``.

        UUID-safe variant of :meth:`filter` for tables whose ``org_id`` column
        is typed ``UUID`` (e.g. ``ai_attribution_resolved``, ``ai_attribution``,
        ``ai_workgraph``).  Casting the column to String via ``toString()`` is
        always valid; casting the String constant ``'default'`` to UUID is not.

        Same identifier safety check and empty-org short-circuit as
        :meth:`filter`.  Callers do NOT need to change their :meth:`inject`
        calls — the param binding name ``org_id`` is unchanged.
        """
        if alias and not alias.isidentifier():
            raise ValueError(
                "OrgScopedQuery.filter_uuid: alias must be a valid identifier, "
                f"got {alias!r}"
            )
        if not self.org_id:
            return ""
        col = f"{alias}.org_id" if alias else "org_id"
        return f" AND toString({col}) = {{org_id:String}}"

    def expression_uuid(self, *, alias: str = "") -> str:
        """Return ``"toString({alias?.}org_id) = {org_id:String}"`` or ``""``.

        UUID-safe companion to :meth:`expression` for tables whose ``org_id``
        column is typed ``UUID``.  Use in filter-list callers (join with
        ``" AND "``) targeting ``ai_attribution_resolved``, ``ai_attribution``,
        or ``ai_workgraph`` tables.

        Same identifier safety check as :meth:`expression`.  The param binding
        name ``org_id`` is unchanged so :meth:`inject` calls are unaffected.
        """
        if alias and not alias.isidentifier():
            raise ValueError(
                "OrgScopedQuery.expression_uuid: alias must be a valid identifier, "
                f"got {alias!r}"
            )
        if not self.org_id:
            return ""
        col = f"{alias}.org_id" if alias else "org_id"
        return f"toString({col}) = {{org_id:String}}"

    def inject(self, params: dict[str, Any]) -> dict[str, Any]:
        """Return a new params dict with ``org_id`` added when set.

        Non-mutating: the original ``params`` dict is untouched.
        """
        if not self.org_id:
            return params
        merged = dict(params)
        merged["org_id"] = self.org_id
        return merged
