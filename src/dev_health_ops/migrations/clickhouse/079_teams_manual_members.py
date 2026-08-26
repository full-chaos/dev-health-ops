"""Migration 079: teams.manual_members -- CHAOS-4321 admin-override provenance.

chris (2026-08-26 10:39 PT): "Admin is an override, not a default. It's the
sync config [mapping] but admin can override it in the panel. It's just a
mapping." teams.members is written by BOTH the admin Identities screen AND
provider auto-import (team_autoimport_github.py and its gitlab/jira siblings
write unreviewed provider-discovered rosters straight into it for any row
that does not conflict with an existing manual override) -- confirmed via a
codex adversarial review round 1 HIGH finding: treating ALL of
teams.members as an authoritative, no-fallthrough admin override let a
provider-imported roster entry from one provider become authoritative for a
DIFFERENT provider's work item sharing the same identity string.

manual_members is the provenance marker this ticket adds to close that gap:
written ONLY by ClickHouseTeamAdminService.add_members/remove_members/
set_members -- the functions the admin Identities screen and the
drift-approval flow (clickhouse_identity_drift.py) call. Provider
auto-import (ClickHouseTeamDriftProjector.project_team's AUTO_APPLY_POLICY
branch) carries the EXISTING value forward unchanged on every sync write
and never sets it, per chris's "sync must not clear it."

BACKFILL (team-lead, 2026-08-26): identities.team_ids is confirmed
admin-only (every writer traced -- only /org/admin/identities and the
drift-approval flow ever touch it, both genuinely admin actions). Seed
manual_members for every team from the UNION of member_facets(...) --
the SAME facet shape ClickHouseTeamAdminService.member_facets computes at
request time -- over every active identity whose team_ids includes that
team. Mirrored into legacy teams.members too (union, never removed), so the
pre-CHAOS-4321 TeamResolver reader (providers/teams.py) keeps working
unchanged.

KNOWN GAP (documented, not fixed here): drift-approved memberships
(apply_identity_membership_change writes teams.members/team_memberships
directly, WITHOUT updating identities.team_ids) carry no admin-only signal
this backfill can use -- they stay in the provider-fallback tier (same
attribution as before this PR, no worse) until an admin re-approves that
drift change or re-saves the identity from the panel, both of which now
write manual_members going forward.

Idempotent: reruns recompute the same facet sets and skip any team whose
members/manual_members already contain them (a no-op INSERT of an
unchanged row would just add a wasted ReplacingMergeTree version).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
    member_facets,
)

log = logging.getLogger(__name__)

# Full row shape teams.insert requires (a ReplacingMergeTree new-version
# write must carry every column forward, not just the ones changing) --
# mirrors storage/clickhouse.py's insert_teams column list exactly.
_TEAMS_COLUMNS = (
    "id",
    "team_uuid",
    "name",
    "description",
    "members",
    "manual_members",
    "project_keys",
    "repo_patterns",
    "is_active",
    "updated_at",
    "last_synced",
    "org_id",
    "provider",
    "native_team_key",
    "parent_team_id",
    "source_id",
)


def _decode_provider_identities(value: object) -> dict[str, list[str]]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (ValueError, TypeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(k): [str(v) for v in (vals or [])] for k, vals in parsed.items()}


def upgrade(client) -> None:
    log.info("=== Migration 079: teams.manual_members (CHAOS-4321) ===")
    client.command(
        "ALTER TABLE teams ADD COLUMN IF NOT EXISTS manual_members "
        "Array(String) DEFAULT [] AFTER members"
    )

    identity_rows = (
        client.query(
            "SELECT org_id, canonical_id, email, display_name, "
            "provider_identities, team_ids "
            "FROM identities FINAL WHERE is_active = 1"
        ).result_rows
        or []
    )

    facets_by_org_team: dict[tuple[str, str], set[str]] = {}
    for row in identity_rows:
        # Defensive: a migration-runner test harness (or any other caller
        # whose fake client doesn't discriminate by query text) can hand
        # back a row shape that doesn't match this SELECT's 6 columns --
        # skip it rather than crash the whole migration chain over what is,
        # in every real environment, optional enrichment data (a clean
        # install's identities table is empty anyway).
        if len(row) != 6:
            continue
        (
            org_id,
            canonical_id,
            email,
            display_name,
            provider_identities_raw,
            team_ids,
        ) = row
        if not team_ids:
            continue
        facets = member_facets(
            canonical_id=canonical_id,
            email=email,
            display_name=display_name,
            provider_identities=_decode_provider_identities(provider_identities_raw),
        )
        if not facets:
            continue
        for team_id in team_ids:
            key = (str(org_id), str(team_id))
            facets_by_org_team.setdefault(key, set()).update(facets)

    if not facets_by_org_team:
        log.info("  no admin-mapped identities found; nothing to backfill")
        log.info("=== Migration 079: Complete ===")
        return

    team_rows = (
        client.query(
            f"SELECT {', '.join(_TEAMS_COLUMNS)} FROM teams FINAL WHERE is_active = 1"
        ).result_rows
        or []
    )

    now = datetime.now(timezone.utc)
    matrix: list[list[object]] = []
    for raw_row in team_rows:
        row = dict(zip(_TEAMS_COLUMNS, raw_row, strict=True))
        new_facets = facets_by_org_team.get((str(row["org_id"]), str(row["id"])))
        if not new_facets:
            continue
        existing_manual = sorted(set(row["manual_members"] or []))
        existing_members = sorted(set(row["members"] or []))
        merged_manual = sorted(set(existing_manual) | new_facets)
        merged_members = sorted(set(existing_members) | new_facets)
        if merged_manual == existing_manual and merged_members == existing_members:
            continue  # already backfilled -- idempotent rerun
        row["manual_members"] = merged_manual
        row["members"] = merged_members
        row["updated_at"] = now
        matrix.append([row[column] for column in _TEAMS_COLUMNS])

    if matrix:
        client.insert("teams", matrix, column_names=list(_TEAMS_COLUMNS))
    log.info("  backfilled manual_members for %d team(s)", len(matrix))
    log.info("=== Migration 079: Complete ===")
