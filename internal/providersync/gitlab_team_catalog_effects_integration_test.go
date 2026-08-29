//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func gitlabTeamCatalogIntegrationRows(orgID string, now time.Time) GitLabTeamCatalogRows {
	description := "Root group"
	rootProjectKey := "org/root-svc"
	teamAProjectKey := "org/team-a/svc"
	return GitLabTeamCatalogRows{
		Teams: []gitlabTeamCatalogTeamRow{
			normalizeGitLabTeamRow(orgID, gitlabTeamCatalogGroupPayload{FullPath: "org", Name: "Org", Description: &description}, []string{rootProjectKey}, now),
			normalizeGitLabTeamRow(orgID, gitlabTeamCatalogGroupPayload{FullPath: "org/team-a", Name: "Team A"}, []string{teamAProjectKey}, now),
		},
		Ownership: []gitlabTeamCatalogOwnershipRow{
			normalizeGitLabOwnershipRow(orgID, "gl:org", rootProjectKey, gitlabTeamCatalogBaseSpecificity, now),
			normalizeGitLabOwnershipRow(orgID, "gl:org/team-a", teamAProjectKey, gitlabTeamCatalogBaseSpecificity+gitlabTeamCatalogChildSpecificityStep, now),
		},
		Memberships: mustGitLabMembershipRows(orgID, now),
		Projects: []gitlabTeamCatalogProjectRow{
			mustGitLabProjectRow(orgID, "100", "org/root-svc", now),
			mustGitLabProjectRow(orgID, "101", "org/team-a/svc", now),
		},
	}
}

func mustGitLabMembershipRows(orgID string, now time.Time) []gitlabTeamCatalogMembershipRow {
	row, _, ok := normalizeGitLabMembershipRow(orgID, "gl:org", gitlabTeamCatalogMemberPayload{Username: "root-owner"}, now)
	if !ok {
		panic("normalizeGitLabMembershipRow failed")
	}
	return []gitlabTeamCatalogMembershipRow{row}
}

func mustGitLabProjectRow(orgID, nativeID, path string, now time.Time) gitlabTeamCatalogProjectRow {
	row, ok := normalizeGitLabProjectCatalogRow(orgID, gitlabTeamCatalogProjectPayload{
		ID: json.Number(nativeID), PathWithNamespace: path, Name: path, WebURL: "https://gitlab.example.com/" + path,
	}, now)
	if !ok {
		panic("normalizeGitLabProjectCatalogRow failed")
	}
	return row
}

// TestGitLabTeamCatalogEffectsAgainstMigratedSchema proves the four writes
// this route produces (teams, team_project_ownership, team_memberships,
// projects) round-trip correctly against the REAL migrated ClickHouse
// schema, are tenant-fenced, and are readback-exact -- the same discipline
// TestLinearReferenceCatalogEffectsAgainstMigratedSchema applies to the
// Linear port. This is schema/write-path proof, not a live-GitLab
// differential oracle (that requires real credentials against org
// 70d529e0 and is covered separately).
func TestGitLabTeamCatalogEffectsAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}

	// Synthetic, claim-free-style fencing claim (CHAOS-4431): the effects
	// sink's validateRequest no longer calls claim.Validate() or checks
	// claim.Dataset, only claim.Provider/claim.OrgID -- mirrors
	// LinearReferenceCatalogClickHouseEffects's identical adaptation.
	claim := Claim{Unit: Unit{OrgID: "gitlab-org-a", Provider: "gitlab"}}
	otherClaim := Claim{Unit: Unit{OrgID: "gitlab-org-b", Provider: "gitlab"}}
	now := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)

	rows := gitlabTeamCatalogIntegrationRows(claim.OrgID, now)
	for i := range rows.Teams {
		rows.Teams[i].MembersAuthoritative = true
		rows.Teams[i].Members = []string{}
	}
	rows.Teams[0].Members = []string{"gitlab:root-owner"}

	otherRows := gitlabTeamCatalogIntegrationRows(otherClaim.OrgID, now)
	for i := range otherRows.Teams {
		otherRows.Teams[i].MembersAuthoritative = true
	}

	effects, err := BuildGitLabTeamCatalogEffects(rows, true, true, true)
	if err != nil {
		t.Fatal(err)
	}
	otherEffects, err := BuildGitLabTeamCatalogEffects(otherRows, true, true, true)
	if err != nil {
		t.Fatal(err)
	}

	for _, effect := range effects.Batches() {
		inspection, inspectErr := sink.InspectEffect(ctx, claim, effect)
		if inspectErr != nil || inspection != EffectAbsent {
			t.Fatalf("before write destination=%s inspection=%s error=%v", effect.Destination, inspection, inspectErr)
		}
	}
	for _, effect := range otherEffects.Batches() {
		if err := sink.WriteEffect(ctx, otherClaim, effect); err != nil {
			t.Fatalf("foreign write destination=%s: %v", effect.Destination, err)
		}
	}
	for _, effect := range effects.Batches() {
		inspection, inspectErr := sink.InspectEffect(ctx, claim, effect)
		if inspectErr != nil || inspection != EffectAbsent {
			t.Fatalf("foreign tenant leaked into destination=%s inspection=%s error=%v", effect.Destination, inspection, inspectErr)
		}
	}
	for _, effect := range effects.Batches() {
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write destination=%s: %v", effect.Destination, err)
		}
		inspection, inspectErr := sink.InspectEffect(ctx, claim, effect)
		if inspectErr != nil || inspection != EffectExact {
			t.Fatalf("readback destination=%s inspection=%s error=%v", effect.Destination, inspection, inspectErr)
		}
	}

	// CHAOS-4321: manual_members set by an admin (simulated directly against
	// the table, since only the admin API writes it) must survive a second
	// provider-access sync that only re-observes teams (no fresh roster
	// signal for the SAME rows) -- the write must carry it forward, not
	// silently reset it to [] on the ReplacingMergeTree's next version.
	if err := conn.Exec(ctx, `ALTER TABLE teams UPDATE manual_members = ['manual:owner'] WHERE org_id = ? AND id = ? SETTINGS mutations_sync = 1`, claim.OrgID, "gl:org"); err != nil {
		t.Fatalf("seed manual_members: %v", err)
	}
	rewrite := rows.Teams[0]
	rewrite.UpdatedAt = now.Add(time.Second)
	rewriteEffect, err := effectBatchFromValues(gitlabTeamCatalogTeamsDestination, EffectReadbackRequired, []gitlabTeamCatalogTeamRow{rewrite})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, rewriteEffect); err != nil {
		t.Fatalf("carry-forward write: %v", err)
	}
	var manualMembers []string
	result, err := conn.Query(ctx, `SELECT manual_members FROM teams FINAL WHERE org_id = ? AND id = ?`, claim.OrgID, "gl:org")
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if !result.Next() {
		t.Fatal("expected a row")
	}
	if err := result.Scan(&manualMembers); err != nil {
		t.Fatal(err)
	}
	if len(manualMembers) != 1 || manualMembers[0] != "manual:owner" {
		t.Fatalf("manual_members not carried forward: %v", manualMembers)
	}

	// Teams-only run (MembersAuthoritative=false) must preserve the CURRENT
	// roster rather than overwrite it with an empty one.
	preserve := rows.Teams[0]
	preserve.UpdatedAt = now.Add(2 * time.Second)
	preserve.MembersAuthoritative = false
	preserve.Members = nil
	preserveEffect, err := effectBatchFromValues(gitlabTeamCatalogTeamsDestination, EffectReadbackRequired, []gitlabTeamCatalogTeamRow{preserve})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, preserveEffect); err != nil {
		t.Fatalf("roster-preserving write: %v", err)
	}
	var members []string
	result2, err := conn.Query(ctx, `SELECT members FROM teams FINAL WHERE org_id = ? AND id = ?`, claim.OrgID, "gl:org")
	if err != nil {
		t.Fatal(err)
	}
	defer result2.Close()
	if !result2.Next() {
		t.Fatal("expected a row")
	}
	if err := result2.Scan(&members); err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != "gitlab:root-owner" {
		t.Fatalf("roster not preserved on members-off write: %v", members)
	}
}

// TestGitLabTeamCatalogCollectorFailsClosedOnPaginationTruncation proves the
// TeamCatalogCollector adapter (the interface production callers actually
// invoke) refuses to write a truncated collection -- codex review finding:
// TeamCatalogResult (the shared interface) carries no completeness field
// for a caller to check, so this must fail closed inside the collector
// itself rather than let a partial catalog look like a successful one.
// Uses a REAL ClickHouse connection (not nil) so the failure proven here is
// unambiguously the truncation guard, not the adapter's unrelated
// Sink.Conn==nil precondition.
func TestGitLabTeamCatalogCollectorFailsClosedOnPaginationTruncation(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })

	group := func(id int, fullPath, name string) map[string]any {
		return map[string]any{"id": id, "full_path": fullPath, "name": name, "description": nil}
	}
	fullSubgroupPage := make([]map[string]any, 100)
	for i := range fullSubgroupPage {
		fullSubgroupPage[i] = group(1000+i, "org/sub", "Sub")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.EscapedPath() {
		case "/api/v4/groups/org":
			_ = json.NewEncoder(w).Encode(group(1, "org", "Org"))
		case "/api/v4/groups/org/subgroups":
			_ = json.NewEncoder(w).Encode(fullSubgroupPage)
		case "/api/v4/groups/org/projects", "/api/v4/groups/org%2Fsub/projects":
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	client, err := providerfoundation.NewHTTPClient(
		"gitlab", server.URL, http.DefaultClient,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		lease,
	)
	if err != nil {
		t.Fatal(err)
	}

	orgID := "org-truncation-guard"
	collector := GitLabTeamCatalogCollector{
		Handler: GitLabTeamCatalogRouteHandler{},
		Sink:    GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease},
	}
	ref := TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1"}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	_, err = collector.CollectTeamCatalog(ctx, ref, credential, client, TeamCatalogSelections{Teams: true}, time.Now())
	if err == nil {
		t.Fatal("expected an error for a truncated collection, got nil")
	}
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM teams FINAL WHERE org_id = ? AND provider = 'gitlab'`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("truncated collection must write NOTHING, got %d teams rows", count)
	}
}

// TestGitLabTeamCatalogCollectorPreservesRosterAfterPerGroupMemberFetchFailure
// is the CHAOS-4461 regression proof (ruling extended from GitHub to GitLab,
// team-lead 2026-08-28) at the full collector-adapter level, against a REAL
// ClickHouse write/readback: with members globally selected under non-strict,
// ONE group's /members fetch failing must not wipe that group's roster to []
// -- its existing, previously-persisted roster must survive, while a second,
// healthy group in the same run gets its freshly observed roster. Uses
// newGitLabTeamCatalogFakeServerWithFailingRootMembers (gitlab_team_catalog_
// test.go, same package) -- org's /members returns 500, org/team-a's
// succeeds.
func TestGitLabTeamCatalogCollectorPreservesRosterAfterPerGroupMemberFetchFailure(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	orgID := "org-partial-member-fetch-failure"
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}
	claim := Claim{Unit: Unit{OrgID: orgID, Provider: gitlabTeamCatalogProvider}}
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	// Seed org's existing roster, as if a prior successful run had already
	// written it.
	seedTeam := normalizeGitLabTeamRow(orgID, gitlabTeamCatalogGroupPayload{FullPath: "org", Name: "Org"}, nil, now)
	seedTeam.Members = []string{"gitlab:existing-owner"}
	seedTeam.MembersAuthoritative = true
	if err := sink.writeTeams(ctx, claim, []gitlabTeamCatalogTeamRow{seedTeam}); err != nil {
		t.Fatal(err)
	}

	fake := newGitLabTeamCatalogFakeServerWithFailingRootMembers(t)
	client := gitlabTeamCatalogTestClient(t, fake.URL)
	collector := GitLabTeamCatalogCollector{Handler: GitLabTeamCatalogRouteHandler{}, Sink: sink}
	ref := TeamCatalogReference{OrgID: orgID, SyncRunID: "run-1", Strict: false}
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}

	result, err := collector.CollectTeamCatalog(ctx, ref, credential, client, TeamCatalogSelections{Teams: true, Members: true}, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("collect should soft-fail, not error, under non-strict: %v", err)
	}
	if result.TeamsWritten != 2 {
		t.Fatalf("result=%+v (org's failed member fetch must not exclude it from the teams write)", result)
	}

	roster, err := gitlabExistingTeamRoster(ctx, conn, orgID, []string{"gl:org", "gl:org/team-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(roster["gl:org"]) != 1 || roster["gl:org"][0] != "gitlab:existing-owner" {
		t.Fatalf("org's existing roster was NOT preserved after its member fetch failed: roster=%+v", roster)
	}
	if len(roster["gl:org/team-a"]) != 2 || roster["gl:org/team-a"][0] != "gitlab:alice" {
		t.Fatalf("team-a (healthy fetch) got the wrong roster: roster=%+v", roster)
	}
}
