"""Live-ClickHouse readback proof for CHAOS-4365.

``load_team_repo_ownership_map`` exists to make compounding-risk's
team-scope resolution see ``team_repo_ownership`` rows written by native
GitHub team auto-import. Three codex rounds (1 HIGH, 2 P1/P2, 3 P1/P2) caught
defects a mocked-sink unit test cannot see -- real producer/consumer shape
mismatches, non-deterministic ranking, and a ClickHouse JOIN-null default
trap -- so this file proves the query against a real ClickHouse instead.

1. ``test_resolves_repo_id_when_writer_left_it_null_like_real_github_autoimport``
   (round 1 HIGH): the ONLY writer of this table for a real org,
   ``team_autoimport_github.py``'s ``_repo_rows`` (``source="provider_access"``),
   constructs every ``TeamRepoOwnershipRecord`` WITHOUT ``repo_id`` --
   ``schemas.py`` defaults it to ``None`` -- and populates only
   ``repo_full_name``. A first version of the query filtered
   ``WHERE repo_id IS NOT NULL``, which silently returned ``{}`` for every
   row this real writer produces.
2. ``test_expired_ownership_row_is_excluded`` (round 2 P2): a retired
   (``valid_to`` in the past) row must not resolve.
3. ``test_tie_between_same_rank_co_owners_is_deterministic`` (round 2 P1):
   GitHub auto-import writes EVERY row ``is_primary=0``, so a repo granted to
   two GitHub teams produces two identical-rank rows. The pick must be
   stable, never arbitrary.
4. ``test_ownership_validity_is_evaluated_as_of_the_caller_supplied_instant``
   (round 3 P1): a row valid on a historical day but expired by "now" must
   still resolve when ``as_of`` is that historical day; a row that only
   starts being valid AFTER the target day must not resolve for that day.
5. ``test_unmatched_ownership_row_does_not_resolve_to_the_zero_uuid``
   (round 3 P2): an ownership row whose ``repo_full_name`` matches no real
   ``repos`` row must be excluded, not resolve to ClickHouse's zero-UUID
   default for an unmatched LEFT JOIN column (this sink does not enable
   ``join_use_nulls``, confirmed live in the docstring of
   ``load_team_repo_ownership_map``).
6. ``test_provider_mismatch_does_not_join_a_same_named_repo_from_another_provider``
   (round 3 P1): a mixed-provider org with two identically-named repos (one
   per provider) must resolve the ownership row to the SAME provider's repo,
   never an arbitrary one.
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
# convention) so every row's `valid_from <= as_of` bound sees it as already
# valid regardless of exact wall-clock skew between this process and the
# ClickHouse server at query time.
_PAST = timedelta(days=1)


def _sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    sink.ensure_tables()
    return sink


def _write_repo(
    sink,
    *,
    org_id: str,
    repo_id: uuid.UUID,
    full_name: str,
    provider: str = "github",
) -> None:
    now = datetime.now(timezone.utc)
    sink.client.insert(
        "repos",
        [
            [
                repo_id,
                full_name,
                "main",
                now,
                None,
                None,
                now,
                org_id,
                provider,
                None,
            ]
        ],
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


def _ownership_row(
    *,
    org_id: str,
    team_id: str,
    repo_full_name: str,
    provider: str = "github",
    is_primary: int = 1,
    specificity: int = 100,
    valid_from: datetime,
    valid_to: datetime | None = None,
    updated_at: datetime,
) -> TeamRepoOwnershipRecord:
    return TeamRepoOwnershipRecord(
        org_id=org_id,
        provider=provider,
        team_id=team_id,
        repo_full_name=repo_full_name,
        match_type="exact",
        source="provider_access",
        is_primary=is_primary,
        specificity=specificity,
        priority=0,
        valid_from=valid_from,
        valid_to=valid_to,
        updated_at=updated_at,
        repo_id=None,
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
            _ownership_row(
                org_id=org_id,
                team_id="gh:platform",
                repo_full_name=repo_full_name,
                valid_from=now - _PAST,
                updated_at=now,
            )
        ]
    )

    mapping = load_team_repo_ownership_map(sink, org_id, as_of=now)

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
            _ownership_row(
                org_id=org_id,
                team_id="gh:retired-owner",
                repo_full_name=repo_full_name,
                valid_from=now - (2 * _PAST),
                valid_to=now - _PAST,  # retired yesterday
                updated_at=now - _PAST,
            )
        ]
    )

    mapping = load_team_repo_ownership_map(sink, org_id, as_of=now)

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
        return _ownership_row(
            org_id=org_id,
            team_id=team_id,
            repo_full_name=repo_full_name,
            is_primary=0,
            specificity=50,
            valid_from=now - _PAST,
            updated_at=now - _PAST,
        )

    sink.write_team_repo_ownership(
        [_sibling_row("gh:team-b"), _sibling_row("gh:team-a")]
    )

    first = load_team_repo_ownership_map(sink, org_id, as_of=now)
    second = load_team_repo_ownership_map(sink, org_id, as_of=now)

    assert first == second
    assert str(repo_id) in first
    # Deterministic tiebreak with identical rank and updated_at: team_id ASC.
    assert first[str(repo_id)] == "gh:team-a"


def test_ownership_validity_is_evaluated_as_of_the_caller_supplied_instant() -> None:
    """CHAOS-4365 codex round 3 (P1): a backfilled day's team-scope
    attribution must reflect ownership validity ON THAT DAY, not at compute
    time. Two rows for the SAME repo, different teams, non-overlapping
    validity windows -- the day-1 instant must resolve the day-1 owner, the
    day-3 instant must resolve the day-3 owner, and NEITHER instant may see
    the other owner.
    """
    sink = _sink()
    org_id = str(uuid.uuid4())
    sink.org_id = org_id
    repo_id = uuid.uuid4()
    repo_full_name = "chaos4365/repo-reassigned-mid-history"
    now = datetime.now(timezone.utc)
    day1 = now - timedelta(days=10)
    day2_reassignment = now - timedelta(days=5)

    _write_repo(sink, org_id=org_id, repo_id=repo_id, full_name=repo_full_name)
    sink.write_team_repo_ownership(
        [
            _ownership_row(
                org_id=org_id,
                team_id="gh:original-owner",
                repo_full_name=repo_full_name,
                valid_from=day1,
                valid_to=day2_reassignment,
                updated_at=day1,
            ),
            _ownership_row(
                org_id=org_id,
                team_id="gh:new-owner",
                repo_full_name=repo_full_name,
                valid_from=day2_reassignment,
                updated_at=day2_reassignment,
            ),
        ]
    )

    # CHAOS-4391: the write path can mis-serialize a tz-aware datetime on a
    # non-UTC host (a separate, filed bug -- see providers/teams.py's own
    # docstring). Deriving `as_of` from the STORED valid_from/valid_to
    # (whatever they actually landed as) rather than the original Python
    # `day1`/`day2_reassignment` keeps this test's own comparisons
    # internally consistent regardless of that write-path bug -- it is
    # exercising `load_team_repo_ownership_map`'s query-parameter `as_of`
    # semantics, not re-testing CHAOS-4391 by accident.
    stored = sink.query_dicts(
        """
        SELECT team_id, valid_from, valid_to FROM team_repo_ownership FINAL
        WHERE org_id = {org_id:String}
        """,
        {"org_id": org_id},
    )
    stored_by_team = {row["team_id"]: row for row in stored}
    as_of_day1 = stored_by_team["gh:original-owner"]["valid_from"] + timedelta(hours=1)
    as_of_day3 = stored_by_team["gh:new-owner"]["valid_from"] + timedelta(hours=1)

    mapping_day1 = load_team_repo_ownership_map(sink, org_id, as_of=as_of_day1)
    mapping_day3 = load_team_repo_ownership_map(sink, org_id, as_of=as_of_day3)

    assert mapping_day1[str(repo_id)] == "gh:original-owner"
    assert mapping_day3[str(repo_id)] == "gh:new-owner"


def test_unmatched_ownership_row_does_not_resolve_to_the_zero_uuid() -> None:
    """CHAOS-4365 codex round 3 (P2): this sink does not enable
    ``join_use_nulls``, so an unmatched LEFT JOIN column defaults to the
    type's zero value (the zero UUID), not NULL. An ownership row whose
    ``repo_full_name`` matches no real ``repos`` row must not silently
    resolve to that zero UUID.
    """
    sink = _sink()
    org_id = str(uuid.uuid4())
    sink.org_id = org_id
    now = datetime.now(timezone.utc)

    # Deliberately no _write_repo call -- this repo_full_name never existed.
    sink.write_team_repo_ownership(
        [
            _ownership_row(
                org_id=org_id,
                team_id="gh:orphaned-grant",
                repo_full_name="chaos4365/repo-that-was-never-synced",
                valid_from=now - _PAST,
                updated_at=now,
            )
        ]
    )

    mapping = load_team_repo_ownership_map(sink, org_id, as_of=now)

    assert "00000000-0000-0000-0000-000000000000" not in mapping
    assert mapping == {}


def test_provider_mismatch_does_not_join_a_same_named_repo_from_another_provider() -> (
    None
):
    """CHAOS-4365 codex round 3 (P1): the join must match on provider too --
    both ``team_repo_ownership`` and ``repos`` carry it. A mixed-provider org
    with two identically-named repos (mirrored on GitHub and GitLab) must
    resolve a GitHub ownership row to the GitHub repo's id, never GitLab's.
    """
    sink = _sink()
    org_id = str(uuid.uuid4())
    sink.org_id = org_id
    repo_full_name = "chaos4365/mirrored-repo"
    github_repo_id = uuid.uuid4()
    gitlab_repo_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    _write_repo(
        sink,
        org_id=org_id,
        repo_id=github_repo_id,
        full_name=repo_full_name,
        provider="github",
    )
    _write_repo(
        sink,
        org_id=org_id,
        repo_id=gitlab_repo_id,
        full_name=repo_full_name,
        provider="gitlab",
    )
    sink.write_team_repo_ownership(
        [
            _ownership_row(
                org_id=org_id,
                team_id="gh:platform",
                repo_full_name=repo_full_name,
                provider="github",
                valid_from=now - _PAST,
                updated_at=now,
            )
        ]
    )

    mapping = load_team_repo_ownership_map(sink, org_id, as_of=now)

    assert mapping == {str(github_repo_id): "gh:platform"}
    assert str(gitlab_repo_id) not in mapping
