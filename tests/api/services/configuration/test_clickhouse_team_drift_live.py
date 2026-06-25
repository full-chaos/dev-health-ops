"""Live-ClickHouse regression for the drift pending-changes query.

The service-level tests in ``test_clickhouse_team_drift_service.py`` back the
store with an in-memory fake whose ``client.query`` *substring-matches* the SQL
and returns canned rows — so a syntax error in the query string is invisible to
them. That blind spot shipped a broken ``GET /admin/teams/pending-changes``: the
``_pending_rows`` SELECT placed ``FINAL`` *before* the table alias
(``team_drift_changes FINAL AS c`` / ``teams FINAL AS t``), which ClickHouse
rejects with ``SYNTAX_ERROR`` (``FINAL`` must follow the alias:
``team_drift_changes AS c FINAL``).

This test executes the real query against a live engine so the grammar is
actually parsed — the only thing that catches a ``FINAL``/alias ordering slip.

Opt-in (filtered from unit/CI): ``pytest -m clickhouse`` with ``CLICKHOUSE_URI``
pointing at a SCRATCH db (never the dev ``default``).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate)",
    ),
]

_NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_get_pending_changes_executes_against_live_engine() -> None:
    from dev_health_ops.api.services.configuration.clickhouse_team_drift import (
        ClickHouseTeamDriftService,
    )
    from dev_health_ops.storage.clickhouse import ClickHouseStore

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    org_id = f"test-drift-final-{uuid.uuid4()}"
    team_id = "team-platform"

    async with ClickHouseStore(CLICKHOUSE_URI) as store:
        store.org_id = org_id
        # Seed the catalog team so the LEFT JOIN ... FINAL leg actually matches
        # (exercises coalesce(t.name, c.entity_id) -> the joined name).
        await store.insert_teams(
            [
                {
                    "id": team_id,
                    "name": "Platform",
                    "org_id": org_id,
                    "provider": "linear",
                    "is_active": 1,
                    "updated_at": _NOW,
                }
            ]
        )
        await store.insert_team_drift_changes(
            [
                {
                    "org_id": org_id,
                    "change_id": "chg-name",
                    "entity_type": "team",
                    "entity_id": team_id,
                    "provider": "linear",
                    "native_team_key": "PLATFORM",
                    "change_type": "field_changed",
                    "field": "name",
                    "old_value_json": '"Old"',
                    "new_value_json": '"Platform"',
                    "status": "pending",
                    "first_seen_at": _NOW,
                    "last_seen_at": _NOW,
                }
            ]
        )

        service = ClickHouseTeamDriftService(store, org_id)
        # Real engine parses + executes the FROM ... AS c FINAL / LEFT JOIN ... AS
        # t FINAL query here; a FINAL/alias ordering regression raises
        # clickhouse_connect.driver.exceptions.DatabaseError (SYNTAX_ERROR).
        changes = await service.get_pending_changes()

    matching = [c for c in changes if c.change_id == "chg-name"]
    assert len(matching) == 1
    change = matching[0]
    assert change.team_id == team_id
    assert change.team_name == "Platform"  # came through the LEFT JOIN ... FINAL
    assert change.field == "name"
    assert change.new_value == "Platform"
