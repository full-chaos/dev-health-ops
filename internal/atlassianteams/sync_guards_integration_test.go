//go:build integration

package atlassianteams

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestWriteRespectsTeamSyncPolicyGuard is the codex review r1 fix proof
// (finding #2): a team an admin pinned sync_policy=2 (manual) via
// team_sync_policies (CHAOS-2622) must be left completely untouched by
// Write, the same guard every other native team-catalog collector already
// applies before its own `teams` write -- before this fix, Write inserted
// Atlassian Teams rows directly, bypassing it.
func TestWriteRespectsTeamSyncPolicyGuard(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	const org = "org-1"
	now := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)

	// Team A already exists with an admin-set name; sync_policy=2 means this
	// write must not overwrite it, even though the snapshot reports "Platform".
	exec(t, conn, `INSERT INTO teams (id, team_uuid, name, description, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, parent_team_id) VALUES ('`+idA+`', generateUUIDv4(), 'Admin-curated name', NULL, [], [], [], [], 1, '2026-09-01 00:00:00', 'org-1', 'jira', '`+teamA+`', NULL)`)
	exec(t, conn, `INSERT INTO team_sync_policies (org_id, team_id, sync_policy, managed_fields, updated_by, updated_at) VALUES ('org-1', '`+idA+`', 2, [], NULL, '2026-09-01 00:00:00')`)

	g := newGateway(t, standard)
	p := params(everything)
	p.Now = now
	rows, err := Collect(ctx, g.client(), p)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Write(ctx, conn, org, rows, everything)
	if err != nil {
		t.Fatal(err)
	}
	// standard() reports teams A (guarded), B (archived), C -- only B and C write.
	if result.TeamsWritten != 2 {
		t.Fatalf("TeamsWritten = %d, want 2 (team A held back by sync_policy=2)", result.TeamsWritten)
	}
	for _, key := range result.TeamKeys {
		if key == teamA {
			t.Fatalf("TeamKeys reported the policy-guarded team: %v", result.TeamKeys)
		}
	}
	got := lines(t, conn, `SELECT name FROM teams FINAL WHERE org_id = 'org-1' AND id = '`+idA+`'`)
	if len(got) != 1 || got[0] != "Admin-curated name" {
		t.Fatalf("team A name = %v, want the untouched admin-curated name -- sync_policy=2 must leave it alone", got)
	}
}

// TestWriteRespectsMembershipManualConflictGuard is the codex review r1 fix
// proof (finding #3): a native membership contradicting a manually-pinned
// member's team must be staged for review, not written active -- before
// this fix, Write inserted Atlassian Teams membership rows directly,
// bypassing the same manual-membership-conflict guard every other native
// collector's own memberships write already applies.
func TestWriteRespectsMembershipManualConflictGuard(t *testing.T) {
	conn := openClickHouse(t)
	ctx := context.Background()
	const org = "org-1"
	now := time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC)

	// alice-1 is manually pinned to a team OTHER than the one the Atlassian
	// snapshot reports her under (team A) -- a conflict the guard must catch.
	exec(t, conn, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES ('org-1', 'jira', 'other-team', 'jira:alice-1', NULL, NULL, [], 'manual', 1, 200, 1, '2026-09-01 00:00:00', NULL, '2026-09-01 00:00:00')`)

	g := newGateway(t, standard)
	p := params(everything)
	p.Now = now
	rows, err := Collect(ctx, g.client(), p)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Write(ctx, conn, org, rows, everything)
	if err != nil {
		t.Fatal(err)
	}
	// standard() reports alice-1 + bob-2 on team A and carol-3 on team C;
	// alice-1's conflicting native row is held back, leaving bob-2 + carol-3.
	if result.MembershipsWritten != 2 {
		t.Fatalf("MembershipsWritten = %d, want 2 (alice-1's conflicting native row held back)", result.MembershipsWritten)
	}
	got := lines(t, conn, `SELECT concat(team_id, '|', member_id) FROM team_memberships FINAL WHERE org_id = 'org-1' AND provider = 'jira' AND source = 'native' ORDER BY team_id, member_id`)
	for _, row := range got {
		if strings.Contains(row, "jira:alice-1") {
			t.Fatalf("a native membership was written for the manually-conflicted member: %v", got)
		}
	}
	// The manual pin itself is untouched.
	stillManual := lines(t, conn, `SELECT team_id FROM team_memberships FINAL WHERE org_id = 'org-1' AND member_id = 'jira:alice-1' AND source = 'manual'`)
	if len(stillManual) != 1 || stillManual[0] != "other-team" {
		t.Fatalf("the manual pin was disturbed: %v", stillManual)
	}
}
