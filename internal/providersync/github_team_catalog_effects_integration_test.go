//go:build integration

package providersync

import (
	"testing"
	"time"
)

func TestGitHubTeamCatalogEffectsAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}

	orgID := "github-team-catalog-org-a"
	otherOrgID := "github-team-catalog-org-b"
	now := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)

	description := "Platform team"
	team, err := normalizeGitHubTeam(orgID, githubTeamPayload{Slug: "platform", Name: "Platform", Description: &description},
		[]string{"acme/api"}, now)
	if err != nil {
		t.Fatal(err)
	}
	membership, err := normalizeGitHubMembership(orgID, "platform", "octocat", "octocat@example.com", now)
	if err != nil {
		t.Fatal(err)
	}
	team.Members = []string{"github:octocat", "octocat@example.com"}

	otherTeam, err := normalizeGitHubTeam(otherOrgID, githubTeamPayload{Slug: "platform", Name: "Platform"}, nil, now)
	if err != nil {
		t.Fatal(err)
	}

	// Tenant isolation: a foreign org's write must never be visible to this
	// org's roster read.
	if err := sink.WriteTeams(ctx, otherOrgID, []githubTeamRow{otherTeam}); err != nil {
		t.Fatalf("foreign write: %v", err)
	}
	roster, ok := sink.ExistingTeamMembers(ctx, orgID, []string{"gh:platform"})
	if !ok {
		t.Fatal("ExistingTeamMembers reported not-ok before this org ever wrote anything")
	}
	if _, present := roster["gh:platform"]; present {
		t.Fatalf("foreign org's team leaked into this org's roster read: roster=%+v", roster)
	}

	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{team}); err != nil {
		t.Fatalf("write teams: %v", err)
	}
	if err := sink.WriteMemberships(ctx, orgID, []githubMembershipRow{membership}); err != nil {
		t.Fatalf("write memberships: %v", err)
	}

	roster, ok = sink.ExistingTeamMembers(ctx, orgID, []string{"gh:platform"})
	if !ok {
		t.Fatal("ExistingTeamMembers reported not-ok after a real write")
	}
	members, present := roster["gh:platform"]
	if !present || len(members) != 2 || members[0] != "github:octocat" || members[1] != "octocat@example.com" {
		t.Fatalf("roster=%+v", roster)
	}

	// Empty team_ids is a real, confirmed answer (nothing to look up), not a
	// read failure -- must never be confused with the ok=false "could not
	// confirm" case the caller treats as a reason to skip a write.
	empty, ok := sink.ExistingTeamMembers(ctx, orgID, nil)
	if !ok || len(empty) != 0 {
		t.Fatalf("empty team_ids roster=%+v ok=%v", empty, ok)
	}
}

// TestGitHubTeamCatalogWriteTeamsPreservesManualMembers is the CHAOS-4321
// regression proof: teams.manual_members is an admin-override provenance
// column this producer never sets itself (githubTeamRow.ManualMembers's doc
// comment) -- WriteTeams MUST carry the currently-persisted value forward on
// every write, or a bare INSERT with the column omitted sends ClickHouse's
// [] DEFAULT and permanently erases the admin's override once this row's
// updated_at wins under ReplacingMergeTree FINAL.
func TestGitHubTeamCatalogWriteTeamsPreservesManualMembers(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	orgID := "github-team-catalog-org-manual-members"
	now := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)

	// Seed a row with an admin-set manual_members value, exactly as
	// ClickHouseTeamAdminService.add_members would have written it.
	if err := conn.Exec(ctx,
		`INSERT INTO teams (id, team_uuid, name, members, manual_members, is_active, updated_at, org_id, provider) `+
			`VALUES (?, generateUUIDv4(), 'Platform', [], ?, 1, ?, ?, 'github')`,
		"gh:platform", []string{"admin:alice"}, now, orgID,
	); err != nil {
		t.Fatalf("seed admin override: %v", err)
	}

	// A later GitHub sync writes this team again, with no knowledge of
	// manual_members at all (matching _team_rows, which never sets it).
	team, err := normalizeGitHubTeam(orgID, githubTeamPayload{Slug: "platform", Name: "Platform Renamed"}, nil, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteTeams(ctx, orgID, []githubTeamRow{team}); err != nil {
		t.Fatalf("write teams: %v", err)
	}

	result, err := conn.Query(ctx,
		`SELECT name, manual_members FROM teams FINAL WHERE org_id = ? AND id = ?`, orgID, "gh:platform",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if !result.Next() {
		t.Fatal("team row missing after sync write")
	}
	var name string
	var manualMembers []string
	if err := result.Scan(&name, &manualMembers); err != nil {
		t.Fatal(err)
	}
	if name != "Platform Renamed" {
		t.Fatalf("sync write did not take effect: name=%q", name)
	}
	if len(manualMembers) != 1 || manualMembers[0] != "admin:alice" {
		t.Fatalf("manual_members was NOT preserved across a native GitHub sync write: got=%v want=[admin:alice]", manualMembers)
	}
}

func TestGitHubTeamCatalogWriteTeamsRejectsCrossOrgRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	now := time.Date(2026, 8, 10, 12, 34, 56, 789000000, time.UTC)
	team, err := normalizeGitHubTeam("org-a", githubTeamPayload{Slug: "platform", Name: "Platform"}, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteTeams(ctx, "org-b", []githubTeamRow{team}); err != ErrInvalidConfiguration {
		t.Fatalf("cross-org row was not rejected: err=%v", err)
	}
}
