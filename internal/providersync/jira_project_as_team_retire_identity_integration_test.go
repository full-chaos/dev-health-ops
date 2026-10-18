//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-9999: the retire writes a project-as-team row again inactive; it is a
// writer of teams, so it keeps the stored provider identity by the Carry rule
// (the newest NON-EMPTY id and name over every version, the newest version's
// gone_at). The newest version here is blank and an older one holds the
// identity: a copy of the newest version would write the blank again, and a
// merge would then lose the identity.
func TestRetireJiraProjectAsTeamRowsKeepsTheStoredProviderIdentity(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := uuid.NewString()
	older := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	gone := older.Add(30 * time.Minute)
	insert := `INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key, provider_team_id, provider_name, gone_at) ` +
		`VALUES ('OPS', ?, 'OPS', [], ['OPS'], [], 1, ?, ?, 'jira', 'OPS', ?, ?, ?)`
	if err := conn.Exec(ctx, insert, uuid.New(), older, orgID, "jira-native-4711", "OPS", nil); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, insert, uuid.New(), newer, orgID, "", "", gone); err != nil {
		t.Fatal(err)
	}

	outcome, err := RetireJiraProjectAsTeamRows(ctx, conn, orgID, newer.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.TeamsRetired != 1 || outcome.TeamsWithoutProviderIdentity != 0 {
		t.Fatalf("outcome = %+v, want 1 team retired, 0 without a stored provider identity", outcome)
	}
	if active := countRows(t, ctx, conn, `SELECT count() FROM teams FINAL WHERE org_id = ? AND id = 'OPS' AND is_active = 1`, orgID); active != 0 {
		t.Fatalf("the retired team is still active")
	}

	assertTeamProviderIdentity(ctx, t, conn, orgID, "OPS", "jira-native-4711", "OPS", &gone)
}
