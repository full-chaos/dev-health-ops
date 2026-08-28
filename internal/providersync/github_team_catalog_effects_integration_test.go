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
