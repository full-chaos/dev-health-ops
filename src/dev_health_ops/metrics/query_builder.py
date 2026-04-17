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

    def inject(self, params: dict[str, Any]) -> dict[str, Any]:
        """Return a new params dict with ``org_id`` added when set.

        Non-mutating: the original ``params`` dict is untouched.
        """
        if not self.org_id:
            return params
        merged = dict(params)
        merged["org_id"] = self.org_id
        return merged
