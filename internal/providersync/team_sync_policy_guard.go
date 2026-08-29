package providersync

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// teamAutoApplySyncPolicy mirrors Python's AUTO_APPLY_POLICY
// (clickhouse_team_drift_projector.py) -- sync_policy=0, the default,
// auto-applies discovery straight to `teams` with no behavior change.
const teamAutoApplySyncPolicy = 0

const teamSyncPoliciesQuery = "SELECT team_id, sync_policy FROM team_sync_policies FINAL WHERE org_id = {org_id:String} AND team_id IN {team_ids:Array(String)}"

// resolveTeamSyncPolicies batch-reads team_sync_policies (CHAOS-2622) for
// every team id a native collector is about to write to `teams`. A team
// with no row defaults to policy 0 (auto-apply) -- the table's own schema
// default and every existing org's unconfigured state, so a missing map
// entry must never be treated as a block.
func resolveTeamSyncPolicies(
	ctx context.Context, conn driver.Conn, orgID string, teamIDs []string,
) (map[string]int, error) {
	if conn == nil || strings.TrimSpace(orgID) == "" {
		return nil, ErrInvalidConfiguration
	}
	if len(teamIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, teamSyncPoliciesQuery,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_ids", teamIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	policies := make(map[string]int, len(teamIDs))
	for rows.Next() {
		var teamID string
		var policy uint8
		if err := rows.Scan(&teamID, &policy); err != nil {
			return nil, err
		}
		policies[teamID] = int(policy)
	}
	return policies, rows.Err()
}

// applyTeamSyncPolicyGuard is the CHAOS-4444-class fail-safe guard team-lead
// ruled for CHAOS-4431's codex review findings #3 (drift projector bypass)
// and #6 (membership conflict review bypass), 2026-08-28: until the full
// CHAOS-2622 drift-aware projector is ported to this native collector path,
// a team whose sync_policy is NOT the auto-apply default (0) must be left
// completely untouched by this write -- no managed-field overwrite, no
// silent clobber of a flagged-for-review (1) or manual (2) team.
//
// Scoped deliberately narrow: it filters ONLY the `teams` table rows. Team-
// attribution.md:791-797 treats the membership conflict gate (versus a
// manual membership or manual_attribution_fallbacks) as independent of a
// team's sync_policy -- it can fire for a policy-0 team too -- so this guard
// does not, and is not meant to, cover finding #6's membership-conflict
// gap. Returns the filtered team rows plus the native_team_key of every team
// this call skipped, for telemetry and for excluding those keys from the
// readback verifier's claim (a skipped team was never written, so claiming
// it would fail the readback for a team this call deliberately left alone).
func applyTeamSyncPolicyGuard(
	ctx context.Context, conn driver.Conn, orgID string, teams []linearReferenceTeamRow,
) ([]linearReferenceTeamRow, []string, error) {
	if len(teams) == 0 {
		return teams, nil, nil
	}
	teamIDs := make([]string, 0, len(teams))
	for _, team := range teams {
		teamIDs = append(teamIDs, team.ID)
	}
	policies, err := resolveTeamSyncPolicies(ctx, conn, orgID, teamIDs)
	if err != nil {
		return nil, nil, err
	}
	kept := make([]linearReferenceTeamRow, 0, len(teams))
	skipped := make([]string, 0)
	for _, team := range teams {
		if policies[team.ID] != teamAutoApplySyncPolicy {
			skipped = append(skipped, team.ID)
			continue
		}
		kept = append(kept, team)
	}
	return kept, skipped, nil
}
