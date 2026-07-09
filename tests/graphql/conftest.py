from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _passthrough_resolve_repo_ids():
    """Stub work-graph repo-ref resolution with an identity pass-through.

    The work-graph resolvers pre-resolve ``filters.repo_ids`` (repo slugs OR
    UUID strings) to catalog UUIDs via ``resolve_repo_ids`` — a real ClickHouse
    round-trip against the ``repos`` table. Unit tests here mock only
    ``query_dicts``, so without this the resolver would hit a live DB.

    The identity stub returns the refs unchanged, so ``repo_ids`` reach the SQL
    exactly as passed (tests assert the resulting param list) and no DB call is
    made. Tests that need real slug→UUID mapping or the empty-scope
    short-circuit patch ``resolve_repo_ids`` themselves; that inner patch
    overrides this fixture for the duration of the test.
    """

    async def _identity(
        sink: Any, repo_refs: Iterable[str], *, org_id: str = ""
    ) -> list[str]:
        return [str(ref) for ref in repo_refs if ref]

    with patch(
        "dev_health_ops.api.graphql.resolvers.work_graph.resolve_repo_ids",
        side_effect=_identity,
    ):
        yield
