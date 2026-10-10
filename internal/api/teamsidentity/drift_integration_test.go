//go:build integration

package teamsidentity

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func discoveredJiraTeam(key, name string) discoveredTeam {
	associations := pyjson.NewObject()
	associations.Set("project_keys", []string{key})
	associations.Set("provider_org", "https://acme.atlassian.net")
	return discoveredTeam{
		ProviderType: "jira", ProviderTeamID: key, Name: name,
		Associations: associations,
	}
}

// TestProjectTeamAutoApplyImportsThenMerges is CHAOS-6311's own
// reproduction of import_teams' default (AUTO_APPLY, no
// team_sync_policies row) path against a real ClickHouse: a first import
// of a never-before-seen provider team is "imported"; the SAME
// provider_team_id imported again with a changed name is "merged", the
// team_uuid stays stable, and the catalog row's provider/native_team_key
// stay EMPTY/NULL (clickhouse_team_admin.py:413-425's own catalog_row
// construction -- NOT the discovered team's real provider/native_team_key,
// which only the observation row carries) so a later real provider sync
// can never silently reclaim an admin-imported team.
func TestProjectTeamAutoApplyImportsThenMerges(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	const orgID = "org-1"

	first, err := store.projectTeam(ctx, orgID, discoveredJiraTeam("ENG", "Engineering"), "skip")
	if err != nil {
		t.Fatalf("projectTeam (first): %v", err)
	}
	if first.Action != "imported" || first.TeamID != "jira:ENG" {
		t.Fatalf("first = %+v, want action=imported team_id=jira:ENG", first)
	}

	stored, err := store.GetTeam(ctx, orgID, "jira:ENG")
	if err != nil {
		t.Fatalf("GetTeam: %v", err)
	}
	if stored == nil {
		t.Fatal("team was not written to the catalog")
	}
	if stored.Name != "Engineering" {
		t.Errorf("Name = %q, want Engineering", stored.Name)
	}
	if len(stored.ManualMembers) != 0 {
		t.Errorf("a freshly-imported team must start with no members: %+v", stored)
	}

	firstUUID := stored.TeamUUID

	// The catalog row's provider/native_team_key are DELIBERATELY empty/
	// null (clickhouse_team_admin.py:413-425), never the discovered team's
	// real provider/native_team_key -- only the observation row (asserted
	// separately) carries those, so an admin-imported team can never be
	// silently reclaimed by a later real provider sync matching on them.
	rows, err := store.Conn.Query(ctx, `SELECT provider, native_team_key FROM teams FINAL WHERE org_id = {org_id:String} AND id = 'jira:ENG'`, clickhouse.Named("org_id", orgID))
	if err != nil {
		t.Fatalf("query teams row: %v", err)
	}
	if !rows.Next() {
		t.Fatal("no teams row found")
	}
	var provider string
	var nativeTeamKey *string
	if err := rows.Scan(&provider, &nativeTeamKey); err != nil {
		t.Fatalf("scan: %v", err)
	}
	rows.Close()
	// A new imported team takes its provider_type as its origin, no longer
	// the legacy Python provider "".
	if provider != "jira" {
		t.Errorf("catalog row provider = %q, want jira", provider)
	}
	if nativeTeamKey != nil {
		t.Errorf("catalog row native_team_key = %v, want nil", *nativeTeamKey)
	}

	// A second import of the SAME provider_team_id, with a changed name
	// and on_conflict="merge" this time, must merge onto the same catalog
	// row (stable team_uuid), not create a duplicate.
	second, err := store.projectTeam(ctx, orgID, discoveredJiraTeam("ENG", "Engineering (renamed)"), "merge")
	if err != nil {
		t.Fatalf("projectTeam (second): %v", err)
	}
	if second.Action != "merged" {
		t.Fatalf("second.Action = %q, want merged", second.Action)
	}

	restored, err := store.GetTeam(ctx, orgID, "jira:ENG")
	if err != nil {
		t.Fatalf("GetTeam (after merge): %v", err)
	}
	if restored == nil {
		t.Fatal("team missing after merge")
	}
	if restored.Name != "Engineering (renamed)" {
		t.Errorf("Name after merge = %q, want the updated name", restored.Name)
	}
	if restored.TeamUUID != firstUUID {
		t.Errorf("team_uuid changed across merge: %s -> %s, want it stable", firstUUID, restored.TeamUUID)
	}
}

// TestProjectTeamSkipOnConflictOnlyRecordsAnObservation proves
// on_conflict="skip" against an EXISTING team writes an observation
// (record_observation) but never touches the catalog row -- the whole
// point of "skip" (clickhouse_team_admin.py:427-438).
func TestProjectTeamSkipOnConflictOnlyRecordsAnObservation(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	const orgID = "org-1"

	if _, err := store.projectTeam(ctx, orgID, discoveredJiraTeam("ENG", "Engineering"), "skip"); err != nil {
		t.Fatalf("projectTeam (first): %v", err)
	}
	before, err := store.GetTeam(ctx, orgID, "jira:ENG")
	if err != nil || before == nil {
		t.Fatalf("GetTeam (before skip): %v, %+v", err, before)
	}

	result, err := store.projectTeam(ctx, orgID, discoveredJiraTeam("ENG", "Engineering (should be ignored)"), "skip")
	if err != nil {
		t.Fatalf("projectTeam (skip): %v", err)
	}
	if result.Action != "skipped" {
		t.Fatalf("Action = %q, want skipped", result.Action)
	}

	after, err := store.GetTeam(ctx, orgID, "jira:ENG")
	if err != nil || after == nil {
		t.Fatalf("GetTeam (after skip): %v, %+v", err, after)
	}
	if after.Name != before.Name {
		t.Errorf("skip must not change the catalog row: Name = %q, want unchanged %q", after.Name, before.Name)
	}
	if after.UpdatedAt != before.UpdatedAt {
		t.Errorf("skip must not re-write the catalog row at all: UpdatedAt changed")
	}
}

// The team roster is no longer a stored field (CHAOS-9087). A change recorded
// for the retired field `members` before is approved as a status only: the
// approval writes nothing, with or without a provider observation to read.
func TestApplyingAChangeOfTheRetiredRosterFieldWritesNothing(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	const orgID = "org-1"
	if _, err := store.projectTeam(ctx, orgID, discoveredJiraTeam("ENG", "Engineering"), "skip"); err != nil {
		t.Fatalf("projectTeam: %v", err)
	}
	before, err := store.GetTeam(ctx, orgID, "jira:ENG")
	if err != nil || before == nil {
		t.Fatalf("GetTeam: %v, %+v", err, before)
	}
	field := "members"
	if err := store.applyChange(ctx, orgID, reviewChangeRow{
		ChangeID: "c-members", EntityType: "team", TeamID: "jira:ENG", TeamName: "Engineering", Provider: "jira",
		ChangeType: "field_changed", Field: &field, OldValueJSON: `[]`, NewValueJSON: `["x"]`,
	}); err != nil {
		t.Fatalf("applyChange of the retired roster field: %v", err)
	}
	after, err := store.GetTeam(ctx, orgID, "jira:ENG")
	if err != nil || after == nil {
		t.Fatalf("GetTeam: %v, %+v", err, after)
	}
	if !after.UpdatedAt.Equal(before.UpdatedAt) || len(after.ManualMembers) != 0 {
		t.Fatalf("the approval of a retired-field change wrote the team: before %+v, after %+v", before, after)
	}
}

// A team flagged for review whose stored policy still names the retired
// field stages no roster change, and a roster change staged earlier is
// superseded.
func TestFlagForReviewSupersedesAPendingChangeOfTheRetiredRosterField(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	const orgID = "org-1"
	if _, err := store.projectTeam(ctx, orgID, discoveredJiraTeam("ENG", "Engineering"), "skip"); err != nil {
		t.Fatalf("projectTeam: %v", err)
	}
	if err := store.Conn.Exec(ctx, `INSERT INTO team_sync_policies (org_id, team_id, sync_policy, managed_fields, updated_by, updated_at) VALUES (?, 'jira:ENG', 1, ['members', 'name'], NULL, now64(6))`, orgID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	field := "members"
	nativeKey := "ENG"
	if err := store.insertChanges(ctx, []teamDriftChangeRow{{
		OrgID: orgID, ChangeID: "c-members-old", EntityType: entityTypeTeam, EntityID: "jira:ENG", Provider: "jira",
		NativeTeamKey: &nativeKey, ChangeType: changeTypeField, Field: &field, OldValueJSON: `[]`, NewValueJSON: `["x"]`,
		Status: statusPending, FirstSeenAt: now, LastSeenAt: now, UpdatedAt: now,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.projectTeam(ctx, orgID, discoveredJiraTeam("ENG", "Engineering renamed"), "merge"); err != nil {
		t.Fatalf("projectTeam (flagged): %v", err)
	}
	var status string
	if err := store.Conn.QueryRow(ctx, `SELECT status FROM team_drift_changes FINAL WHERE org_id = ? AND change_id = 'c-members-old'`, orgID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != statusSuperseded {
		t.Fatalf("status of the roster change = %q, want %q", status, statusSuperseded)
	}
	var rosterChanges, nameChanges uint64
	if err := store.Conn.QueryRow(ctx, `SELECT countIf(field = 'members' AND status = 'pending'), countIf(field = 'name' AND status = 'pending') FROM team_drift_changes FINAL WHERE org_id = ? AND entity_id = 'jira:ENG'`, orgID).Scan(&rosterChanges, &nameChanges); err != nil {
		t.Fatal(err)
	}
	if rosterChanges != 0 || nameChanges != 1 {
		t.Fatalf("pending changes: roster %d, name %d, want 0 and 1 (the rename is staged, the roster is not a field)", rosterChanges, nameChanges)
	}
}
