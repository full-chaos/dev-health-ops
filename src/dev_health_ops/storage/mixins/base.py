from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class SQLAlchemyStoreMixinProtocol(Protocol):
    """Protocol for SQLAlchemy store mixins."""

    session: AsyncSession | None

    def _insert_for_dialect(self, model: Any) -> Any:
        """Return dialect-specific insert statement."""

    async def _upsert_many(
        self,
        model: Any,
        rows: list[dict[str, Any]],
        conflict_columns: list[str],
        update_columns: list[str],
    ) -> None:
        """Upsert multiple rows."""
