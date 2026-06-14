"""CHAOS-2397: flame.fetch_deployment must scope on deployments.org_id.

The previous query joined ``repos`` and filtered ``repos.org_id``. Because
``repos.id`` is duplicated across orgs (a known fixtures/sync artifact), a shared
``repo_id`` let the join cross the requester's repos row with ANOTHER tenant's
deployment row, and ``LIMIT 1`` (no ORDER BY) could return that other tenant's
deployment on an authenticated ``GET /api/v1/flame?entity_type=deployment``.
The fix filters ``deployments.org_id`` directly (added to the table + sort key
in migration 027) and orders by ``last_synced`` for a deterministic latest row.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.queries import flame


@pytest.mark.asyncio
async def test_fetch_deployment_scopes_on_deployments_org_id(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_query_dicts(client, query, params):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(flame, "query_dicts", _fake_query_dicts)

    await flame.fetch_deployment(
        object(), repo_id="r1", deployment_id="d1", org_id="org-A"
    )

    query = str(captured["query"])
    # Scopes directly on the deployments.org_id column (leak-proof against the
    # duplicate-repos.id-across-orgs artifact)...
    assert "org_id = %(org_id)s" in query
    # ...and no longer routes the tenant check through the repos join.
    assert "INNER JOIN repos" not in query
    assert "repos.org_id" not in query
    # Deterministic latest-version read from the ReplacingMergeTree.
    assert "ORDER BY last_synced DESC" in query
    assert captured["params"] == {  # type: ignore[comparison-overlap]
        "repo_id": "r1",
        "deployment_id": "d1",
        "org_id": "org-A",
    }
