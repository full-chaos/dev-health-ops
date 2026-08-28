"""Live-ClickHouse readback proof for CHAOS-4365.

``load_team_repo_ownership_map`` exists to make compounding-risk's
team-scope resolution see ``team_repo_ownership`` rows written by native
GitHub team auto-import. Codex (round 1 HIGH, round 2 P1/P2) caught defects a
mocked-sink unit test cannot see -- a real producer/consumer shape mismatch
and non-deterministic ranking -- so this file proves the query against a
real ClickHouse instead.

1. ``test_resolves_repo_id_when_writer_left_it_null_like_real_github_autoimport``:
   the ONLY writer of this table for a real org,
   ``team_autoimport_github.py``'s ``_repo_rows`` (``source="provider_access"``),
   constructs every ``TeamRepoOwnershipRecord`` WITHOUT ``repo_id`` --
   ``schemas.py`` defaults it to ``None`` -- and populates only
   ``repo_full_name``. A first version of the query filtered
   ``WHERE repo_id IS NOT NULL``, which silently returned ``{}`` for every
   row this real writer produces: it would have shipped looking correct
   (green unit tests, green fixtures-org CH readback, since fixtures DO set
   ``repo_id``) while remaining unable to fix the exact real-org gap
   CHAOS-4365 exists to close.
2. ``test_expired_ownership_row_is_excluded``: a retired (``valid_to`` in the
   past) row must not resolve.
3. ``test_tie_between_same_rank_co_owners_is_deterministic``: GitHub
   auto-import writes EVERY row ``is_primary=0`` (see above), so a repo
   granted to two GitHub teams produces two identical-rank rows. The pick
   must be stable (same winner every call), never arbitrary/flapping.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.metrics.schemas import TeamRepoOwnershipRecord
from dev_health_ops.providers.teams import load_team_repo_ownership_map

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]

# Comfortably in the past (matches fixtures/generators/teams.py's own
# convention) so every row's `valid_from <= now64()` bound sees it as already
# valid regardless of exact wall-clock skew between this process and the
# ClickHouse server at query time.
_PAST = timedelta(days=1)


def _sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_tables()
    return sink


def _write_repo(sink, *, org_id: str, repo_id: uuid.UUID, full_name: str) -> None:
    now = datetime.now(timezone.utc)
    sink.client.insert(
        "repos",
        [[repo_id, full_name, "main", now, None, None, now, org_id, "github", None]],
        column_names=[
            "id",
            "repo",
            "ref",
            "created_at",
            "settings",
            "tags",
            "last_synced",
            "org_id",
            "provider",
            "source_id",
        ],
    )


def test_resolves_repo_id_when_writer_left_it_null_like_real_github_autoimport() -> (
    None
):
    sink = _sink()
    org_id = str(uuid.uuid4())  # throwaway random org (isolated, no cleanup needed)
    sink.org_id = org_id
    repo_id = uuid.uuid4()
    repo_full_name = "chaos4365/repo-owned-by-native-import"
    now = datetime.now(timezone.utc)

    _write_repo(sink, org_id=org_id, repo_id=repo_id, full_name=repo_full_name)

    # The real production write shape: team_autoimport_github.py's
    # _repo_rows never sets repo_id (schemas.py defaults it to None) --
    # only repo_full_name identifies the repo.
    sink.write_team_repo_ownership(
        [
            TeamRepoOwnershipRecord(
                org_id=org_id,
                provider="github",
                team_id="gh:platform",
                repo_full_name=repo_full_name,
                match_type="exact",
                source="provider_access",
                is_primary=1,
                specificity=100,
                priority=0,
                valid_from=now - _PAST,
                updated_at=now,
                repo_id=None,
            )
        ]
    )

    mapping = load_team_repo_ownership_map(sink, org_id)

    assert mapping == {str(repo_id): "gh:platform"}


def test_expired_ownership_row_is_excluded() -> None:
    sink = _sink()
    org_id = str(uuid.uuid4())
    sink.org_id = org_id
    repo_id = uuid.uuid4()
    repo_full_name = "chaos4365/repo-retired-ownership"
    now = datetime.now(timezone.utc)

    _write_repo(sink, org_id=org_id, repo_id=repo_id, full_name=repo_full_name)

    sink.write_team_repo_ownership(
        [
            TeamRepoOwnershipRecord(
                org_id=org_id,
                provider="github",
                team_id="gh:retired-owner",
                repo_full_name=repo_full_name,
                match_type="exact",
                source="provider_access",
                is_primary=1,
                specificity=100,
                priority=0,
                valid_from=now - (2 * _PAST),
                valid_to=now - _PAST,  # retired yesterday
                updated_at=now - _PAST,
                repo_id=None,
            )
        ]
    )

    mapping = load_team_repo_ownership_map(sink, org_id)

    assert str(repo_id) not in mapping


def test_tie_between_same_rank_co_owners_is_deterministic() -> None:
    """GitHub auto-import writes every row ``is_primary=0`` with the same
    ``specificity`` -- a repo granted to two teams produces two
    identical-rank siblings. The pick must be the same every call (the
    query's ``updated_at DESC, team_id ASC`` tiebreak), not arbitrary.
    """
    sink = _sink()
    org_id = str(uuid.uuid4())
    sink.org_id = org_id
    repo_id = uuid.uuid4()
    repo_full_name = "chaos4365/repo-two-owning-teams"
    now = datetime.now(timezone.utc)

    _write_repo(sink, org_id=org_id, repo_id=repo_id, full_name=repo_full_name)

    def _sibling_row(team_id: str) -> TeamRepoOwnershipRecord:
        return TeamRepoOwnershipRecord(
            org_id=org_id,
            provider="github",
            team_id=team_id,
            repo_full_name=repo_full_name,
            match_type="exact",
            source="provider_access",
            is_primary=0,
            specificity=50,
            priority=300,
            valid_from=now - _PAST,
            updated_at=now - _PAST,
            repo_id=None,
        )

    sink.write_team_repo_ownership(
        [_sibling_row("gh:team-b"), _sibling_row("gh:team-a")]
    )

    first = load_team_repo_ownership_map(sink, org_id)
    second = load_team_repo_ownership_map(sink, org_id)

    assert first == second
    assert str(repo_id) in first
    # Deterministic tiebreak with identical rank and updated_at: team_id ASC.
    assert first[str(repo_id)] == "gh:team-a"
