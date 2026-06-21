"""Shared test helpers.

Centralizes patterns that mypy flags repeatedly across the suite, so individual
tests stay focused on their assertions instead of working around library
quirks.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, cast

from sqlalchemy import Table


def tables_of(*models: Any) -> list[Table]:
    """Return ``[Model.__table__, ...]`` typed as ``list[Table]``.

    SQLAlchemy 2's ``DeclarativeBase`` declares ``__table__`` as
    ``FromClause`` (broader, to accommodate inheritance), which makes mypy
    reject the value when callers pass it to APIs typed as
    ``Sequence[Table]`` (e.g. :meth:`MetaData.create_all`).

    Concrete mapped classes always expose a real :class:`Table` at runtime,
    so this helper performs the narrowing once instead of forcing every
    test fixture to repeat ``cast(Table, Model.__table__)``.

    The ``Any`` parameter type is intentional: SQLAlchemy mapped classes use
    ``DeclarativeAttributeIntercept`` as their metaclass, which doesn't
    surface ``__table__`` to a plain ``type[...]`` view.
    """
    return [cast(Table, model.__table__) for model in models]


def closing_coroutine_runner(
    return_value: Any = None, *, raises: BaseException | None = None
) -> Callable[..., Any]:
    """Build a ``side_effect`` for a mocked coroutine runner that closes the coroutine.

    Mocking a coroutine runner (``asyncio.run``, ``workers.async_runner.run_async``,
    etc.) leaves the coroutine argument un-awaited, which emits a
    ``RuntimeWarning: coroutine '...' was never awaited`` at garbage-collection
    time. This side_effect closes the coroutine so it is consumed cleanly, while
    the mock still records the call and honours the requested return/raise
    behaviour (CHAOS-2586).
    """

    def _run(coro: Any = None, *args: Any, **kwargs: Any) -> Any:
        if asyncio.iscoroutine(coro):
            coro.close()
        if raises is not None:
            raise raises
        return return_value

    return _run
